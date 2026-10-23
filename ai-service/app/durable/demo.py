"""A concrete expense workflow used by tests and the HTTP router.

It is intentionally small but exercises the real durability features: a policy
step that pauses for approval above a threshold, an idempotent ledger post, and
compensating entries if a later step fails.
"""

from __future__ import annotations

from typing import Any, Protocol

from .engine import AwaitingApproval, Step, StepContext, WorkflowDefinition


class Ledger(Protocol):
    def post(self, expense_id: str, amount: float) -> str: ...
    def reverse(self, expense_id: str, entry_id: str) -> None: ...


class Notifier(Protocol):
    def send(self, expense_id: str, message: str) -> None: ...


class InMemoryLedger:
    def __init__(self) -> None:
        self.entries: dict[str, str] = {}
        self.reversed: list[str] = []

    def post(self, expense_id: str, amount: float) -> str:
        if expense_id not in self.entries:
            self.entries[expense_id] = f"JE-{len(self.entries) + 1:04d}"
        return self.entries[expense_id]

    def reverse(self, expense_id: str, entry_id: str) -> None:
        self.reversed.append(entry_id)
        self.entries.pop(expense_id, None)


class InMemoryNotifier:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    def send(self, expense_id: str, message: str) -> None:
        self.sent.append((expense_id, message))


def build_expense_workflow(
    *,
    ledger: Ledger | None = None,
    notifier: Notifier | None = None,
    approval_threshold_usd: float = 100.0,
) -> WorkflowDefinition:
    ledger = ledger or InMemoryLedger()
    notifier = notifier or InMemoryNotifier()

    def validate(ctx: StepContext) -> dict[str, Any]:
        amount = float(ctx.input.get("amount", 0))
        merchant = str(ctx.input.get("merchant", "")).strip()
        if amount <= 0 or not merchant:
            raise ValueError("amount must be positive and merchant is required")
        return {
            "amount": amount,
            "merchant": merchant,
            "currency": ctx.input.get("currency", "USD"),
        }

    def policy(ctx: StepContext) -> dict[str, Any]:
        expense = ctx.outputs["validate"]
        if expense["amount"] > approval_threshold_usd:
            if not (ctx.approval and ctx.approval.get("approved")):
                raise AwaitingApproval(
                    {
                        "reason": "amount_over_threshold",
                        "amount": expense["amount"],
                        "threshold": approval_threshold_usd,
                    }
                )
            return {"decision": "approved", "approver": (ctx.approval or {}).get("by")}
        return {"decision": "auto_approved"}

    def post(ctx: StepContext) -> dict[str, Any]:
        expense = ctx.outputs["validate"]
        entry_id = ledger.post(ctx.input["expense_id"], expense["amount"])
        return {"entry_id": entry_id}

    def notify(ctx: StepContext) -> dict[str, Any]:
        expense = ctx.outputs["validate"]
        entry = ctx.outputs["post"]
        notifier.send(
            ctx.input["expense_id"],
            f"Expense {ctx.input['expense_id']} posted as {entry['entry_id']}",
        )
        return {"notified": True, "amount": expense["amount"]}

    def reverse_post(ctx: StepContext, output: Any) -> None:
        ledger.reverse(ctx.input["expense_id"], output["entry_id"])

    return WorkflowDefinition(
        name="expense_approval",
        steps=[
            Step("validate", validate),
            Step("policy", policy),
            Step("post", post, compensator=reverse_post),
            Step("notify", notify),
        ],
    )
