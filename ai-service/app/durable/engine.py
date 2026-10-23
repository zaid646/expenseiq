"""Durable, resumable workflow execution with leases and compensation.

The engine is deliberately storage-agnostic: every state transition is written
through :class:`WorkflowStore`, so a worker can die between any two calls and a
different worker can resume from the last checkpoint. Leases stop two workers
from executing the same run; idempotency keys stop the same request from
creating two runs.

Semantics:

* a step's output is checkpointed before the next step starts;
* completed steps are never re-executed on resume;
* a handler raises :class:`AwaitingApproval` to pause the run (human-in-the-loop);
* retries use exponential backoff and are bounded per step;
* compensation runs completed steps in reverse order.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass, field
from typing import Any

from .models import RunStatus, StepRecord, StepStatus, WorkflowRun
from .store import RunLocked, WorkflowStore


class AwaitingApproval(Exception):
    """Raised by a step handler to pause until a human approves."""

    def __init__(self, request: dict[str, Any]) -> None:
        super().__init__("awaiting approval")
        self.request = request


class StepTimeout(Exception):
    """Raised when a handler exceeds its per-step deadline."""


@dataclass
class StepContext:
    run_id: str
    step: str
    attempt: int
    input: dict[str, Any]
    outputs: dict[str, Any]
    approval: dict[str, Any] | None = None


@dataclass
class Step:
    name: str
    handler: Callable[[StepContext], Any]
    compensator: Callable[[StepContext, Any], None] | None = None
    timeout_seconds: float | None = None
    # "fail" marks the run failed; "approval" pauses for a human; "compensate"
    # marks the run failed and records a compensation route for the caller.
    on_timeout: str = "fail"

    def __post_init__(self) -> None:
        if self.on_timeout not in {"fail", "approval", "compensate"}:
            raise ValueError(f"invalid on_timeout route: {self.on_timeout}")
        if self.timeout_seconds is not None and self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")


@dataclass
class WorkflowDefinition:
    name: str
    steps: list[Step] = field(default_factory=list)


class DurableWorkflow:
    def __init__(
        self,
        definition: WorkflowDefinition,
        store: WorkflowStore,
        *,
        max_attempts: int = 3,
        retry_base_seconds: float = 0.05,
        lease_seconds: int = 30,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.definition = definition
        self.store = store
        self.max_attempts = max_attempts
        self.retry_base_seconds = retry_base_seconds
        self.lease_seconds = lease_seconds
        self.sleeper = sleeper

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def start(
        self,
        payload: dict[str, Any],
        *,
        run_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> WorkflowRun:
        run = WorkflowRun(
            run_id=run_id or uuid.uuid4().hex,
            workflow=self.definition.name,
            input=dict(payload),
            idempotency_key=idempotency_key,
            steps=[StepRecord(name=s.name) for s in self.definition.steps],
        )
        created = self.store.create(run)
        if created.run_id == run.run_id:
            self._event(created.run_id, "run_started", {"workflow": self.definition.name})
        return created

    def get(self, run_id: str) -> WorkflowRun:
        return self.store.load(run_id)

    def events(self, run_id: str) -> list[dict[str, Any]]:
        return self.store.events(run_id)

    def execute(self, run_id: str, *, owner: str | None = None) -> WorkflowRun:
        owner = owner or f"worker-{uuid.uuid4().hex[:8]}"
        now = time.time()
        previous = self.store.lease_holder(run_id)
        if not self.store.acquire_lease(run_id, owner, self.lease_seconds):
            raise RunLocked(f"{run_id} is leased by another worker")
        if previous is not None and previous[0] != owner and previous[1] <= now:
            self._event(
                run_id,
                "lease_expired",
                {"previous_owner": previous[0], "expired_at": previous[1]},
            )
            self._event(
                run_id,
                "run_redispatched",
                {"from_owner": previous[0], "to_owner": owner},
            )
        try:
            run = self.store.load(run_id)
            if run.status in {
                RunStatus.COMPLETED,
                RunStatus.COMPENSATED,
                RunStatus.COMPENSATION_FAILED,
            }:
                return run
            if run.status is RunStatus.AWAITING_APPROVAL:
                return run
            run.status = RunStatus.RUNNING
            run.lock_owner = owner
            run.lock_until = time.time() + self.lease_seconds
            self.store.save(run, owner=owner)

            while run.current_step < len(self.definition.steps):
                index = run.current_step
                record = run.steps[index]
                step = self.definition.steps[index]
                if record.status is StepStatus.COMPLETED:
                    run.current_step += 1
                    self.store.save(run, owner=owner)
                    continue

                result = self._run_step(run, record, step, owner)
                if result is not None:
                    return result
                run = self.store.load(run_id)
                if run.steps[index].status is not StepStatus.COMPLETED:
                    return run
                run.current_step = index + 1
                self.store.save(run, owner=owner)

            run = self.store.load(run_id)
            run.status = RunStatus.COMPLETED
            run.error = None
            run.failure = None
            run.failure_route = None
            self.store.save(run, owner=owner)
            self._event(run.run_id, "run_completed", {})
            return run
        finally:
            self.store.release_lease(run_id, owner)

    def resume(self, run_id: str, *, owner: str | None = None) -> WorkflowRun:
        run = self.store.load(run_id)
        if run.approval is not None and not run.approval.get("approved"):
            raise ValueError(f"{run_id} was rejected and cannot be resumed")
        if run.status is RunStatus.FAILED:
            for step in run.steps:
                if step.status is StepStatus.FAILED:
                    step.attempts = 0
                    step.status = StepStatus.PENDING
                    step.error = None
            run.failure = None
            run.failure_route = None
            self.store.save(run)
        return self.execute(run_id, owner=owner)

    def approve(
        self, run_id: str, *, approver: str, note: str = "", approved: bool = True
    ) -> WorkflowRun:
        run = self.store.load(run_id)
        if run.status is not RunStatus.AWAITING_APPROVAL:
            raise ValueError(f"{run_id} is not awaiting approval")
        run.approval = {
            **(run.approval or {}),
            "approved": approved,
            "by": approver,
            "note": note,
            "at": time.time(),
        }
        run.status = RunStatus.RUNNING if approved else RunStatus.FAILED
        if not approved:
            run.error = f"rejected by {approver}"
        self.store.save(run)
        self._event(
            run_id,
            "approval_decided",
            {"approved": approved, "by": approver, "note": note},
        )
        return run

    def compensate(self, run_id: str, *, owner: str | None = None) -> WorkflowRun:
        owner = owner or f"compensator-{uuid.uuid4().hex[:8]}"
        if not self.store.acquire_lease(run_id, owner, self.lease_seconds):
            raise RunLocked(f"{run_id} is leased by another worker")
        try:
            run = self.store.load(run_id)
            for index in range(len(run.steps) - 1, -1, -1):
                step = self.definition.steps[index]
                record = run.steps[index]
                if record.status is not StepStatus.COMPLETED or record.compensated:
                    continue
                if step.compensator is None:
                    record.compensated = True
                    self.store.save(run, owner=owner)
                    continue
                ctx = StepContext(
                    run_id=run.run_id,
                    step=step.name,
                    attempt=record.attempts,
                    input=run.input,
                    outputs=run.outputs,
                    approval=run.approval,
                )
                try:
                    step.compensator(ctx, record.output)
                except Exception as exc:  # noqa: BLE001 - report, don't hide
                    run.status = RunStatus.COMPENSATION_FAILED
                    run.error = f"compensation failed at {step.name}: {exc}"
                    self.store.save(run, owner=owner)
                    self._event(run.run_id, "compensation_failed", {"step": step.name})
                    return run
                record.compensated = True
                self.store.save(run, owner=owner)
                self._event(run.run_id, "step_compensated", {"step": step.name})
            run.status = RunStatus.COMPENSATED
            self.store.save(run, owner=owner)
            self._event(run.run_id, "run_compensated", {})
            return run
        finally:
            self.store.release_lease(run_id, owner)

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------
    def _run_step(
        self,
        run: WorkflowRun,
        record: StepRecord,
        step: Step,
        owner: str,
    ) -> WorkflowRun | None:
        """Run one step. Returns a run when the workflow must pause/fail."""
        while record.attempts < self.max_attempts:
            record.attempts += 1
            record.status = StepStatus.RUNNING
            record.started_at = time.time()
            record.error = None
            self.store.save(run, owner=owner)
            ctx = StepContext(
                run_id=run.run_id,
                step=step.name,
                attempt=record.attempts,
                input=run.input,
                outputs=run.outputs,
                approval=run.approval,
            )
            try:
                output = self._call_handler(step, ctx)
            except StepTimeout as exc:
                record.status = StepStatus.FAILED
                record.error = str(exc)
                record.finished_at = time.time()
                failure = {
                    "kind": "step_timeout",
                    "step": step.name,
                    "timeout_s": step.timeout_seconds,
                    "reason": str(exc),
                    "route": step.on_timeout,
                }
                run.error = str(exc)
                run.failure = failure
                run.failure_route = step.on_timeout
                self._event(
                    run.run_id,
                    "step_timeout",
                    {
                        "step": step.name,
                        "attempt": record.attempts,
                        "timeout_s": step.timeout_seconds,
                        "route": step.on_timeout,
                    },
                )
                if step.on_timeout == "approval":
                    record.status = StepStatus.PENDING
                    record.started_at = None
                    run.status = RunStatus.AWAITING_APPROVAL
                    run.approval = {"requested": failure, "approved": False}
                    self.store.save(run, owner=owner)
                    self._event(
                        run.run_id,
                        "awaiting_approval",
                        {"step": step.name, "reason": "step_timeout"},
                    )
                    return run
                run.status = RunStatus.FAILED
                self.store.save(run, owner=owner)
                self._event(run.run_id, "run_failed", {"step": step.name})
                if step.on_timeout == "compensate":
                    self._event(
                        run.run_id,
                        "compensation_requested",
                        {"step": step.name, "reason": "step_timeout"},
                    )
                return run
            except AwaitingApproval as exc:
                record.status = StepStatus.PENDING
                record.started_at = None
                run.status = RunStatus.AWAITING_APPROVAL
                run.approval = {"requested": exc.request, "approved": False}
                self.store.save(run, owner=owner)
                self._event(run.run_id, "awaiting_approval", {"step": step.name})
                return run
            except Exception as exc:  # noqa: BLE001 - step failures are data
                record.status = StepStatus.FAILED
                record.error = f"{type(exc).__name__}: {exc}"
                record.finished_at = time.time()
                run.error = record.error
                self.store.save(run, owner=owner)
                self._event(
                    run.run_id,
                    "step_failed",
                    {"step": step.name, "attempt": record.attempts, "error": record.error},
                )
                if record.attempts >= self.max_attempts:
                    run.status = RunStatus.FAILED
                    self.store.save(run, owner=owner)
                    self._event(run.run_id, "run_failed", {"step": step.name})
                    return run
                delay = self.retry_base_seconds * (2 ** (record.attempts - 1))
                self._event(
                    run.run_id,
                    "step_retry",
                    {"step": step.name, "attempt": record.attempts, "delay_s": delay},
                )
                self.sleeper(delay)
                continue

            record.status = StepStatus.COMPLETED
            record.output = output
            record.error = None
            record.finished_at = time.time()
            self.store.save(run, owner=owner)
            self._event(run.run_id, "step_completed", {"step": step.name})
            return None
        return None

    def _call_handler(self, step: Step, ctx: StepContext) -> Any:
        if step.timeout_seconds is None:
            return step.handler(ctx)
        pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"step-{step.name}")
        future = pool.submit(step.handler, ctx)
        try:
            result = future.result(timeout=step.timeout_seconds)
        except FutureTimeout as exc:
            future.cancel()
            pool.shutdown(wait=False, cancel_futures=True)
            raise StepTimeout(
                f"{step.name} exceeded {step.timeout_seconds}s"
            ) from exc
        except BaseException:
            pool.shutdown(wait=True, cancel_futures=True)
            raise
        pool.shutdown(wait=True)
        return result

    def _event(self, run_id: str, kind: str, data: dict[str, Any]) -> None:
        events = self.store.events(run_id)
        self.store.append_event(
            run_id,
            {"seq": len(events) + 1, "kind": kind, "at": time.time(), **data},
        )
