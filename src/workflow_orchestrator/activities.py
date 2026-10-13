import os
import time

from temporalio import activity
from temporalio.exceptions import ApplicationError

from .ledger import Ledger
from .policy import Order, verify_decision


@activity.defn
def validate_order(request: dict) -> dict:
    try:
        return Order.model_validate(request).model_dump()
    except ValueError as exc:
        raise ApplicationError("invalid request", non_retryable=True) from exc


@activity.defn
def inspect_inventory(request: dict) -> dict:
    return Ledger().inventory(request["sku"])


@activity.defn
def quote_order(request: dict) -> dict:
    if activity.info().attempt <= request["transient_quote_failures"]:
        raise ApplicationError("injected transient quote failure")
    inventory = Ledger().inventory(request["sku"])
    return {
        "total_cents": inventory["unit_price"] * request["quantity"],
        "attempt": activity.info().attempt,
    }


@activity.defn
def authorize_decision(data: dict) -> bool:
    try:
        approved = verify_decision(
            data["decision"],
            data["workflow_id"],
            data["request"],
            os.environ["ORCHESTRATOR_APPROVAL_KEY"],
        )
        if approved:
            Ledger().authorize(data["workflow_id"], data["request"])
        return approved
    except (ValueError, TypeError, KeyError) as exc:
        raise ApplicationError("invalid approval", non_retryable=True) from exc


@activity.defn
def reserve_inventory(data: dict) -> dict:
    activity.heartbeat("starting reservation")
    try:
        result = Ledger().reserve(data["workflow_id"], data["request"])
    except ValueError as exc:
        raise ApplicationError(str(exc), non_retryable=True) from exc
    if data["request"]["pause_after_commit"] and activity.info().attempt == 1:
        # Commit is durable; deliberately never acknowledge completion on this attempt.
        while True:
            activity.heartbeat("reservation committed; crash-test checkpoint")
            time.sleep(0.2)
    return {**result, "attempt": activity.info().attempt}


@activity.defn
def notify_outbox(data: dict) -> dict:
    return Ledger().notify(data["workflow_id"])


@activity.defn
def verify_outcome(data: dict) -> dict:
    evidence = Ledger().evidence(data["workflow_id"])
    row, request = evidence["reservation"], data["request"]
    valid = bool(
        row
        and row["state"] == "ACTIVE"
        and row["sku"] == request["sku"]
        and row["quantity"] == request["quantity"]
        and evidence["events"].count("reserved") == 1
        and ("notification_outbox" in evidence["events"]) == request["notify"]
        and not request["fail_verification"]
        and evidence["stock_balance_verified"]
    )
    return {"verified": valid, **evidence}


@activity.defn
def compensate_reservation(data: dict) -> dict:
    return Ledger().compensate(data["workflow_id"], data["request"])


@activity.defn
def persist_report(data: dict) -> dict:
    return Ledger().report(data["workflow_id"], data)


ACTIVITIES = [
    validate_order,
    inspect_inventory,
    quote_order,
    authorize_decision,
    reserve_inventory,
    notify_outbox,
    verify_outcome,
    compensate_reservation,
    persist_report,
]
