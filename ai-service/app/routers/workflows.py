"""Durable expense workflow endpoints.

Start a run, inspect its checkpoints, decide a pending approval, resume after a
worker failure, or compensate a failed run. The store is Redis when
``DURABLE_REDIS_URL`` is set, otherwise it is process-local (single worker).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..config import get_settings
from ..durable import (
    DurableWorkflow,
    RunLocked,
    RunNotFound,
    build_expense_workflow,
    build_store,
)

router = APIRouter(prefix="/workflows", tags=["workflows"])

_settings = get_settings()
_workflow = DurableWorkflow(
    build_expense_workflow(approval_threshold_usd=_settings.durable_approval_threshold_usd),
    build_store(_settings.durable_redis_url or None),
    max_attempts=_settings.durable_max_attempts,
    retry_base_seconds=_settings.durable_retry_base_seconds,
    lease_seconds=_settings.durable_lease_seconds,
)


class StartExpenseRequest(BaseModel):
    expense_id: str = Field(min_length=1)
    amount: float = Field(gt=0)
    merchant: str = Field(min_length=1)
    currency: str = "USD"
    idempotency_key: str | None = None


class ApprovalDecision(BaseModel):
    approver: str = Field(min_length=1)
    note: str = ""
    approved: bool = True


def _handle(exc: Exception) -> HTTPException:
    if isinstance(exc, RunNotFound):
        return HTTPException(status_code=404, detail="workflow run not found")
    if isinstance(exc, RunLocked):
        return HTTPException(status_code=409, detail="workflow run is leased by another worker")
    return HTTPException(status_code=400, detail=str(exc))


@router.post("/expenses")
def start_expense(req: StartExpenseRequest) -> dict[str, Any]:
    try:
        run = _workflow.start(
            req.model_dump(exclude={"idempotency_key"}),
            idempotency_key=req.idempotency_key,
        )
        return _workflow.execute(run.run_id).to_dict()
    except Exception as exc:  # noqa: BLE001 - mapped to HTTP status
        raise _handle(exc) from exc


@router.get("/{run_id}")
def get_workflow(run_id: str) -> dict[str, Any]:
    try:
        return _workflow.get(run_id).to_dict()
    except Exception as exc:  # noqa: BLE001
        raise _handle(exc) from exc


@router.get("/{run_id}/events")
def get_events(run_id: str) -> list[dict[str, Any]]:
    try:
        return _workflow.events(run_id)
    except Exception as exc:  # noqa: BLE001
        raise _handle(exc) from exc


@router.post("/{run_id}/resume")
def resume_workflow(run_id: str) -> dict[str, Any]:
    try:
        return _workflow.resume(run_id).to_dict()
    except Exception as exc:  # noqa: BLE001
        raise _handle(exc) from exc


@router.post("/{run_id}/approve")
def approve_workflow(run_id: str, req: ApprovalDecision) -> dict[str, Any]:
    try:
        run = _workflow.approve(run_id, approver=req.approver, note=req.note, approved=req.approved)
        return run.to_dict()
    except Exception as exc:  # noqa: BLE001
        raise _handle(exc) from exc


@router.post("/{run_id}/compensate")
def compensate_workflow(run_id: str) -> dict[str, Any]:
    try:
        return _workflow.compensate(run_id).to_dict()
    except Exception as exc:  # noqa: BLE001
        raise _handle(exc) from exc
