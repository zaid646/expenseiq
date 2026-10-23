# ExpenseIQ AI service (Python · FastAPI)

FastAPI service that performs **receipt extraction**, **GL coding** and **policy checks**
for the ExpenseIQ backend. Inference is delegated to a self-hosted **vLLM** endpoint
(OpenAI-compatible); PII/card numbers are redacted before every call.

```
app/
├── main.py            # FastAPI app + lifespan
├── config.py          # pydantic-settings from ../../.env
├── schemas.py         # pydantic request/response + extraction models
├── llm.py             # async OpenAI-compatible client (vLLM)
├── redaction.py       # Fortress PII / card redaction middleware
├── registry.py        # prompt registry (draft/eval/prod/rollback)
├── self_heal.py       # validate → retry → fallback → human queue
├── tracing.py         # Langfuse wrapper (no-op if unconfigured)
├── metrics.py         # per-request latency / cost accounting
└── routers/
    ├── extract.py     # POST /extract   receipt → structured fields
    ├── code.py        # POST /code      transaction → GL category
    └── policy.py      # POST /policy-check
```

## Run

```bash
uv sync --dev
uv run uvicorn app.main:app --reload --port 8080
```

## Durable workflows

`app/durable/` adds restart-safe execution for multi-step expense actions:
`validate → policy → post → notify`. Every step output is checkpointed, so a
worker that dies mid-run is resumed from the last completed step rather than
replaying side effects. Runs are guarded by Redis leases, requests are
deduplicated by idempotency key, amounts over the threshold pause in
`awaiting_approval`, and failed runs can be compensated in reverse order.

Set `DURABLE_REDIS_URL=redis://localhost:6379/0` to share checkpoints and
leases across workers. Without it the service uses an in-process store (fine
for a single worker and tests; not durable across restarts).

```bash
curl -s localhost:8080/workflows/expenses -H 'content-type: application/json' \
  -d '{"expense_id":"exp-1","amount":42,"merchant":"Cafe","idempotency_key":"req-1"}'

curl -s localhost:8080/workflows/<run_id>/approve -H 'content-type: application/json' \
  -d '{"approver":"controller","note":"receipt verified"}'
curl -s -X POST localhost:8080/workflows/<run_id>/resume
curl -s localhost:8080/workflows/<run_id>/events
```

### Timeouts and lease recovery

A `Step` can set `timeout_seconds` and `on_timeout` (`fail`, `approval`, or
`compensate`). A timed-out step is recorded with a structured `run.failure`
(`kind`, `step`, `timeout_s`, `route`); `approval` pauses the run, while
`compensate` records `compensation_requested` and the run can be compensated in
reverse order. If a worker dies, its Redis lease expires and another worker can
re-dispatch the run; `lease_expired` and `run_redispatched` events preserve the
hand-off.

Tests run against `fakeredis`, so CI needs no Redis server:

```bash
uv run pytest -q
```
