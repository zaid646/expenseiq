"""Serialisable run/step records for the durable workflow engine."""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    AWAITING_APPROVAL = "awaiting_approval"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATED = "compensated"
    COMPENSATION_FAILED = "compensation_failed"


class StepStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


TERMINAL = {
    RunStatus.COMPLETED,
    RunStatus.COMPENSATED,
    RunStatus.COMPENSATION_FAILED,
}


@dataclass
class StepRecord:
    name: str
    status: StepStatus = StepStatus.PENDING
    attempts: int = 0
    output: Any = None
    error: str | None = None
    started_at: float | None = None
    finished_at: float | None = None
    compensated: bool = False

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["status"] = str(self.status)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StepRecord:
        data = dict(data)
        data["status"] = StepStatus(data.get("status", StepStatus.PENDING))
        return cls(**data)


@dataclass
class WorkflowRun:
    run_id: str
    workflow: str
    status: RunStatus = RunStatus.PENDING
    input: dict[str, Any] = field(default_factory=dict)
    idempotency_key: str | None = None
    steps: list[StepRecord] = field(default_factory=list)
    current_step: int = 0
    version: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    approval: dict[str, Any] | None = None
    error: str | None = None
    failure: dict[str, Any] | None = None
    failure_route: str | None = None
    lock_owner: str | None = None
    lock_until: float | None = None

    @property
    def outputs(self) -> dict[str, Any]:
        return {s.name: s.output for s in self.steps if s.status is StepStatus.COMPLETED}

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "workflow": self.workflow,
            "status": str(self.status),
            "input": self.input,
            "idempotency_key": self.idempotency_key,
            "steps": [s.to_dict() for s in self.steps],
            "current_step": self.current_step,
            "version": self.version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "approval": self.approval,
            "error": self.error,
            "failure": self.failure,
            "failure_route": self.failure_route,
            "lock_owner": self.lock_owner,
            "lock_until": self.lock_until,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkflowRun:
        data = dict(data)
        data["status"] = RunStatus(data.get("status", RunStatus.PENDING))
        data["steps"] = [StepRecord.from_dict(s) for s in data.get("steps", [])]
        return cls(**data)
