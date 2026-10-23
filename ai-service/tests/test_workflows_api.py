"""HTTP surface for the durable workflow endpoints."""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_expense_workflow_api_completes_small_expense():
    response = client.post(
        "/workflows/expenses",
        json={
            "expense_id": "api-exp-1",
            "amount": 12.5,
            "merchant": "Corner Cafe",
            "idempotency_key": "api-req-1",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
    assert {step["status"] for step in body["steps"]} == {"completed"}

    again = client.post(
        "/workflows/expenses",
        json={
            "expense_id": "api-exp-1",
            "amount": 12.5,
            "merchant": "Corner Cafe",
            "idempotency_key": "api-req-1",
        },
    )
    assert again.status_code == 200
    assert again.json()["run_id"] == body["run_id"]


def test_expense_workflow_api_pauses_for_approval():
    started = client.post(
        "/workflows/expenses",
        json={
            "expense_id": "api-exp-2",
            "amount": 500.0,
            "merchant": "Big Vendor",
            "idempotency_key": "api-req-2",
        },
    )
    assert started.status_code == 200
    body = started.json()
    assert body["status"] == "awaiting_approval"
    run_id = body["run_id"]

    approved = client.post(
        f"/workflows/{run_id}/approve",
        json={"approver": "controller", "note": "receipt verified"},
    )
    assert approved.status_code == 200
    assert approved.json()["approval"]["approved"] is True

    resumed = client.post(f"/workflows/{run_id}/resume")
    assert resumed.status_code == 200
    assert resumed.json()["status"] == "completed"

    events = client.get(f"/workflows/{run_id}/events")
    assert events.status_code == 200
    kinds = [e["kind"] for e in events.json()]
    assert "awaiting_approval" in kinds
    assert kinds[-1] == "run_completed"


def test_workflow_api_unknown_run_is_404():
    assert client.get("/workflows/nope").status_code == 404
