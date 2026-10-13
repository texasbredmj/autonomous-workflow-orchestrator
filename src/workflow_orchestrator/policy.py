import hashlib
import hmac
import json
import time
import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, model_validator

TRANSITIONS = {
    "CREATED": {"VALIDATED", "FAILED", "CANCELLED"},
    "VALIDATED": {"PLANNED", "FAILED", "CANCELLED"},
    "PLANNED": {"WAITING_APPROVAL", "FAILED", "CANCELLED"},
    "WAITING_APPROVAL": {"AUTHORIZED", "FAILED", "CANCELLED"},
    "AUTHORIZED": {"RUNNING", "FAILED", "CANCELLED"},
    "RUNNING": {"VERIFYING", "FAILED", "CANCELLED"},
    "VERIFYING": {"SUCCEEDED", "FAILED", "CANCELLED"},
    "SUCCEEDED": set(),
    "FAILED": set(),
    "CANCELLED": set(),
}


class Order(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    requester: str = Field(default="requester", min_length=1, max_length=64)
    sku: str = Field(default="demo-kit", pattern=r"^demo-kit$")
    quantity: int = Field(default=1, ge=1, le=10)
    notify: bool = False
    not_before: str | None = Field(default=None, max_length=64)
    approval_timeout_seconds: int = Field(default=120, ge=1, le=3600)
    pause_after_commit: bool = False
    hold_after_reserve_seconds: int = Field(default=0, ge=0, le=30)
    fail_verification: bool = False
    transient_quote_failures: int = Field(default=0, ge=0, le=3)

    @model_validator(mode="after")
    def scheduled_time(self):
        if self.not_before:
            moment = datetime.fromisoformat(self.not_before)
            if moment.tzinfo is None:
                raise ValueError("not_before must include a timezone")
        return self


def digest(request: dict) -> str:
    return hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()


def sign_decision(
    workflow_id: str, request: dict, key: str, *, approved=True, actor="operator", lifetime=120
) -> dict:
    if len(key) < 32:
        raise ValueError("approval key must contain at least 32 characters")
    body = {
        "workflow_id": workflow_id,
        "request_digest": digest(request),
        "approved": approved,
        "actor": actor,
        "expires_at": time.time() + lifetime,
        "nonce": str(uuid.uuid4()),
    }
    body["signature"] = hmac.new(
        key.encode(), json.dumps(body, sort_keys=True).encode(), hashlib.sha256
    ).hexdigest()
    return body


def verify_decision(payload: dict, workflow_id: str, request: dict, key: str) -> bool:
    if len(key) < 32:
        raise ValueError("approval key unavailable")
    body = dict(payload)
    signature = body.pop("signature", "")
    expected = hmac.new(
        key.encode(), json.dumps(body, sort_keys=True).encode(), hashlib.sha256
    ).hexdigest()
    valid = (
        isinstance(signature, str)
        and hmac.compare_digest(signature, expected)
        and body.get("workflow_id") == workflow_id
        and body.get("request_digest") == digest(request)
        and body.get("actor") == "operator"
        and body["actor"] != request["requester"]
        and isinstance(body.get("approved"), bool)
        and body.get("expires_at", 0) > time.time()
    )
    if not valid:
        raise ValueError("approval is invalid, expired, self-issued, or bound to another request")
    return body["approved"]
