import pytest
from pydantic import ValidationError

from workflow_orchestrator.policy import TRANSITIONS, Order, sign_decision, verify_decision

KEY = "test-signing-key-only-" * 3


@pytest.mark.parametrize(
    "data",
    [
        {"quantity": 0},
        {"quantity": 11},
        {"quantity": "1"},
        {"sku": "unapproved"},
        {"command": "shell"},
        {"not_before": "2026-10-13T12:00:00"},
        {"transient_quote_failures": 4},
    ],
)
def test_request_boundaries(data):
    with pytest.raises(ValidationError):
        Order(**data)


def test_valid_approval():
    request = Order().model_dump()
    assert verify_decision(sign_decision("job", request, KEY), "job", request, KEY)


@pytest.mark.parametrize("change", ["signature", "workflow", "arguments", "expired", "self"])
def test_approval_binding(change):
    request = Order().model_dump()
    payload = sign_decision(
        "job",
        request,
        KEY,
        lifetime=-1 if change == "expired" else 120,
        actor="requester" if change == "self" else "operator",
    )
    if change == "signature":
        payload["signature"] = "forged"
    if change == "arguments":
        request["quantity"] = 2
    with pytest.raises(ValueError):
        verify_decision(payload, "other" if change == "workflow" else "job", request, KEY)


def test_terminal_states_and_illegal_shortcuts():
    assert not TRANSITIONS["SUCCEEDED"]
    assert not TRANSITIONS["FAILED"]
    assert not TRANSITIONS["CANCELLED"]
    assert "SUCCEEDED" not in TRANSITIONS["CREATED"]
    assert "RUNNING" not in TRANSITIONS["WAITING_APPROVAL"]
