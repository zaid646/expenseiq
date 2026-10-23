"""Durability, idempotency, leasing, approval and compensation tests."""

from __future__ import annotations

import time

import fakeredis
import pytest

from app.durable import (
    DurableWorkflow,
    InMemoryStore,
    RedisStore,
    RunLocked,
    RunStatus,
    Step,
    StepStatus,
    WorkflowDefinition,
    build_expense_workflow,
)
from app.durable.demo import InMemoryLedger, InMemoryNotifier


def _definition(calls: list[str], *, fail_on: str | None = None, fail_times: int = 0):
    remaining = {"n": fail_times}

    def make(name: str, value):
        def handler(ctx):
            calls.append(name)
            if name == fail_on and remaining["n"] > 0:
                remaining["n"] -= 1
                raise RuntimeError(f"{name} exploded")
            return value

        return handler

    return WorkflowDefinition(
        name="test-flow",
        steps=[Step("a", make("a", 1)), Step("b", make("b", 2)), Step("c", make("c", 3))],
    )


def _engine(store, definition, **kwargs):
    params = {
        "max_attempts": 3,
        "retry_base_seconds": 0.05,
        "lease_seconds": 30,
        "sleeper": lambda _: None,
    }
    params.update(kwargs)
    return DurableWorkflow(definition, store, **params)


def test_happy_path_checkpoints_every_step():
    calls: list[str] = []
    store = InMemoryStore()
    engine = _engine(store, _definition(calls))
    run = engine.start({"x": 1}, idempotency_key="req-1")

    done = engine.execute(run.run_id)

    assert done.status is RunStatus.COMPLETED
    assert done.outputs == {"a": 1, "b": 2, "c": 3}
    assert calls == ["a", "b", "c"]
    assert all(step.attempts == 1 for step in done.steps)
    kinds = [e["kind"] for e in engine.events(run.run_id)]
    assert kinds[0] == "run_started"
    assert kinds.count("step_completed") == 3
    assert kinds[-1] == "run_completed"


def test_resume_skips_completed_steps_after_failure():
    calls: list[str] = []
    store = InMemoryStore()
    engine = _engine(store, _definition(calls, fail_on="b", fail_times=1), max_attempts=1)
    run = engine.start({"x": 1})

    failed = engine.execute(run.run_id)
    assert failed.status is RunStatus.FAILED
    assert failed.steps[0].status is StepStatus.COMPLETED
    assert calls == ["a", "b"]

    resumed = engine.resume(run.run_id)
    assert resumed.status is RunStatus.COMPLETED
    assert resumed.outputs == {"a": 1, "b": 2, "c": 3}
    assert calls == ["a", "b", "b", "c"]
    assert resumed.steps[0].attempts == 1


def test_retry_uses_exponential_backoff():
    calls: list[str] = []
    delays: list[float] = []
    store = InMemoryStore()
    engine = _engine(
        store,
        _definition(calls, fail_on="b", fail_times=2),
        retry_base_seconds=0.05,
        sleeper=delays.append,
    )
    run = engine.start({})
    done = engine.execute(run.run_id)

    assert done.status is RunStatus.COMPLETED
    assert delays == pytest.approx([0.05, 0.1])
    assert done.steps[1].attempts == 3


def test_idempotency_key_returns_existing_run():
    calls: list[str] = []
    store = InMemoryStore()
    engine = _engine(store, _definition(calls))

    first = engine.start({"x": 1}, idempotency_key="same-request")
    second = engine.start({"x": 1}, idempotency_key="same-request")
    assert first.run_id == second.run_id

    engine.execute(first.run_id)
    assert calls == ["a", "b", "c"]


def test_lease_blocks_second_worker():
    calls: list[str] = []
    store = InMemoryStore()
    engine = _engine(store, _definition(calls))
    run = engine.start({})

    assert store.acquire_lease(run.run_id, "other-worker", 30)
    with pytest.raises(RunLocked):
        engine.execute(run.run_id)
    assert calls == []

    store.release_lease(run.run_id, "other-worker")
    assert engine.execute(run.run_id).status is RunStatus.COMPLETED


def test_timed_out_lease_is_redispatched():
    calls: list[str] = []
    store = InMemoryStore()
    engine = _engine(store, _definition(calls))
    run = engine.start({})

    assert store.acquire_lease(run.run_id, "dead-worker", 0.01)
    time.sleep(0.05)

    done = engine.execute(run.run_id, owner="replacement-worker")

    assert done.status is RunStatus.COMPLETED
    assert calls == ["a", "b", "c"]
    kinds = [event["kind"] for event in engine.events(run.run_id)]
    assert "lease_expired" in kinds
    assert "run_redispatched" in kinds
    expired = next(event for event in engine.events(run.run_id) if event["kind"] == "lease_expired")
    assert expired["previous_owner"] == "dead-worker"


def test_step_timeout_routes_to_compensation():
    compensated: list[str] = []

    def slow(_ctx):
        time.sleep(0.2)
        return "late"

    definition = WorkflowDefinition(
        name="timeout-compensate",
        steps=[
            Step(
                "post",
                lambda _ctx: "posted",
                compensator=lambda _ctx, output: compensated.append(output),
            ),
            Step("slow", slow, timeout_seconds=0.02, on_timeout="compensate"),
        ],
    )
    engine = _engine(InMemoryStore(), definition)
    run = engine.start({})

    failed = engine.execute(run.run_id)

    assert failed.status is RunStatus.FAILED
    assert failed.failure_route == "compensate"
    assert failed.failure["kind"] == "step_timeout"
    assert failed.failure["step"] == "slow"
    kinds = [event["kind"] for event in engine.events(run.run_id)]
    assert "step_timeout" in kinds
    assert "compensation_requested" in kinds

    compensated_run = engine.compensate(run.run_id)
    assert compensated_run.status is RunStatus.COMPENSATED
    assert compensated == ["posted"]


def test_step_timeout_routes_to_approval_then_resumes():
    def slow_once(ctx):
        if ctx.attempt == 1:
            time.sleep(0.2)
        return "ok"

    definition = WorkflowDefinition(
        name="timeout-approval",
        steps=[Step("slow", slow_once, timeout_seconds=0.02, on_timeout="approval")],
    )
    engine = _engine(InMemoryStore(), definition)
    run = engine.start({})

    paused = engine.execute(run.run_id)
    assert paused.status is RunStatus.AWAITING_APPROVAL
    assert paused.failure_route == "approval"
    assert paused.approval["requested"]["kind"] == "step_timeout"
    assert paused.approval["requested"]["step"] == "slow"

    engine.approve(run.run_id, approver="ops", note="retry with more time")
    done = engine.resume(run.run_id)

    assert done.status is RunStatus.COMPLETED
    assert done.outputs == {"slow": "ok"}
    assert done.failure is None
    assert done.failure_route is None


def test_approval_pauses_then_resumes():
    ledger = InMemoryLedger()
    notifier = InMemoryNotifier()
    store = InMemoryStore()
    engine = _engine(
        store, build_expense_workflow(ledger=ledger, notifier=notifier, approval_threshold_usd=100)
    )
    run = engine.start({"expense_id": "exp-1", "amount": 250.0, "merchant": "Acme"})

    paused = engine.execute(run.run_id)
    assert paused.status is RunStatus.AWAITING_APPROVAL
    assert paused.approval is not None
    assert paused.approval["approved"] is False
    assert ledger.entries == {}

    approved = engine.approve(run.run_id, approver="cfo", note="ok")
    assert approved.status is RunStatus.RUNNING

    done = engine.resume(run.run_id)
    assert done.status is RunStatus.COMPLETED
    assert done.approval["approved"] is True
    assert done.approval["by"] == "cfo"
    assert ledger.entries == {"exp-1": "JE-0001"}
    assert notifier.sent == [("exp-1", "Expense exp-1 posted as JE-0001")]


def test_rejected_approval_cannot_resume():
    store = InMemoryStore()
    engine = _engine(store, build_expense_workflow(approval_threshold_usd=10), max_attempts=1)
    run = engine.start({"expense_id": "exp-2", "amount": 50.0, "merchant": "Acme"})
    engine.execute(run.run_id)

    engine.approve(run.run_id, approver="cfo", note="no", approved=False)
    with pytest.raises(ValueError, match="rejected"):
        engine.resume(run.run_id)


def test_compensation_reverses_completed_steps():
    calls: list[str] = []
    reversed_entries: list[tuple[str, str]] = []

    def post(ctx):
        calls.append("post")
        return {"entry_id": "JE-9"}

    def reverse(ctx, output):
        reversed_entries.append((ctx.input["expense_id"], output["entry_id"]))

    definition = WorkflowDefinition(
        name="comp-test",
        steps=[
            Step("validate", lambda ctx: calls.append("validate") or {"ok": True}),
            Step("post", post, compensator=reverse),
            Step("notify", lambda ctx: (_ for _ in ()).throw(RuntimeError("down"))),
        ],
    )
    store = InMemoryStore()
    engine = _engine(store, definition, max_attempts=1)
    run = engine.start({"expense_id": "exp-3"})
    assert engine.execute(run.run_id).status is RunStatus.FAILED

    compensated = engine.compensate(run.run_id)
    assert compensated.status is RunStatus.COMPENSATED
    assert reversed_entries == [("exp-3", "JE-9")]
    assert compensated.steps[1].compensated is True
    assert compensated.steps[2].compensated is False


def test_redis_store_resumes_on_a_new_worker():
    calls: list[str] = []
    fake = fakeredis.FakeRedis(decode_responses=True)
    store_one = RedisStore(fake)
    engine_one = _engine(store_one, _definition(calls, fail_on="c", fail_times=1), max_attempts=1)
    run = engine_one.start({"x": 1}, idempotency_key="redis-req")
    assert engine_one.execute(run.run_id).status is RunStatus.FAILED
    assert calls == ["a", "b", "c"]

    store_two = RedisStore(fake)
    engine_two = _engine(store_two, _definition(calls), max_attempts=2)
    done = engine_two.resume(run.run_id)

    assert done.status is RunStatus.COMPLETED
    assert calls == ["a", "b", "c", "c"]
    assert done.outputs == {"a": 1, "b": 2, "c": 3}
    assert store_two.events(run.run_id)[-1]["kind"] == "run_completed"
    assert engine_two.start({}, idempotency_key="redis-req").run_id == run.run_id


def test_redis_lease_is_visible_to_other_store():
    fake = fakeredis.FakeRedis(decode_responses=True)
    engine = _engine(RedisStore(fake), _definition([]))
    run = engine.start({})
    assert engine.store.acquire_lease(run.run_id, "worker-a", 30)

    other = _engine(RedisStore(fake), _definition([]))
    with pytest.raises(RunLocked):
        other.execute(run.run_id)


def test_unknown_run_raises():
    engine = _engine(InMemoryStore(), _definition([]))
    with pytest.raises(KeyError):
        engine.get("missing")


def test_auto_approved_small_expense_posts_once():
    ledger = InMemoryLedger()
    notifier = InMemoryNotifier()
    store = InMemoryStore()
    engine = _engine(store, build_expense_workflow(ledger=ledger, notifier=notifier))
    run = engine.start(
        {"expense_id": "exp-4", "amount": 42.0, "merchant": "Cafe"}, idempotency_key="exp-4"
    )
    done = engine.execute(run.run_id)
    duplicate = engine.start(
        {"expense_id": "exp-4", "amount": 42.0, "merchant": "Cafe"}, idempotency_key="exp-4"
    )
    engine.execute(duplicate.run_id)

    assert done.status is RunStatus.COMPLETED
    assert done.outputs["policy"]["decision"] == "auto_approved"
    assert ledger.entries == {"exp-4": "JE-0001"}
    assert len(notifier.sent) == 1


def test_compensation_runs_in_reverse_order():
    order: list[str] = []

    def make(name: str) -> Step:
        return Step(
            name,
            lambda ctx, n=name: n,
            compensator=lambda ctx, output: order.append(output),
        )

    definition = WorkflowDefinition(name="order-test", steps=[make("a"), make("b"), make("c")])
    engine = _engine(InMemoryStore(), definition)
    run = engine.start({})
    assert engine.execute(run.run_id).status is RunStatus.COMPLETED

    compensated = engine.compensate(run.run_id)

    assert compensated.status is RunStatus.COMPENSATED
    assert order == ["c", "b", "a"]
