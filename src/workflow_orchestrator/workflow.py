import asyncio
from datetime import datetime, timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from .policy import TRANSITIONS


@workflow.defn
class FulfillmentWorkflow:
    def __init__(self):
        self.state = "CREATED"
        self.request = {}
        self.decisions = []
        self.history = []
        self.outputs = {}

    def transition(self, target):
        if target not in TRANSITIONS[self.state]:
            raise ApplicationError(f"illegal transition {self.state}->{target}", non_retryable=True)
        self.state = target
        self.history.append({"state": target, "at": workflow.now().isoformat()})

    async def call(self, name, arguments, *, retries=3, heartbeat=False):
        return await workflow.execute_activity(
            name,
            arguments,
            start_to_close_timeout=timedelta(seconds=15),
            schedule_to_close_timeout=timedelta(seconds=60),
            heartbeat_timeout=timedelta(seconds=3) if heartbeat else None,
            cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=3),
                maximum_attempts=retries,
            ),
        )

    @workflow.signal
    def decision(self, payload: dict):
        if self.state in {"CREATED", "VALIDATED", "PLANNED", "WAITING_APPROVAL"}:
            if len(self.decisions) < 3:
                self.decisions.append(payload)

    @workflow.query
    def status(self) -> dict:
        return {
            "state": self.state,
            "request": self.request,
            "history": self.history,
            "outputs": self.outputs,
        }

    @workflow.run
    async def run(self, request: dict) -> dict:
        started_at = workflow.now()
        workflow_id = workflow.info().workflow_id
        may_have_reserved = False
        try:
            self.request = await self.call("validate_order", request, retries=1)
            self.transition("VALIDATED")
            self.transition("PLANNED")
            if self.request["not_before"]:
                due = datetime.fromisoformat(self.request["not_before"])
                delay = (due - workflow.now()).total_seconds()
                if delay > 86400:
                    raise ApplicationError("schedule exceeds 24-hour budget", non_retryable=True)
                if delay > 0:
                    await workflow.sleep(delay)
            inspected, quote = await asyncio.gather(
                self.call("inspect_inventory", self.request), self.call("quote_order", self.request)
            )
            self.outputs.update(inspect=inspected, quote=quote)
            self.transition("WAITING_APPROVAL")
            approval_deadline = workflow.now() + timedelta(
                seconds=self.request["approval_timeout_seconds"]
            )
            attempts = 0
            while True:
                remaining = (approval_deadline - workflow.now()).total_seconds()
                await workflow.wait_condition(
                    lambda: bool(self.decisions), timeout=max(0, remaining)
                )
                decision = self.decisions.pop(0)
                attempts += 1
                try:
                    permitted = await self.call(
                        "authorize_decision",
                        {"workflow_id": workflow_id, "request": self.request, "decision": decision},
                        retries=1,
                    )
                except Exception:
                    self.outputs["rejected_approvals"] = attempts
                    if attempts >= 3:
                        raise ApplicationError(
                            "approval rejection budget exhausted", non_retryable=True
                        ) from None
                    continue
                if not permitted:
                    raise ApplicationError("operator rejected action", non_retryable=True)
                break
            self.transition("AUTHORIZED")
            self.transition("RUNNING")
            data = {"workflow_id": workflow_id, "request": self.request}
            may_have_reserved = (
                True  # Register compensation before launching a potentially ambiguous action.
            )
            self.outputs["reservation"] = await self.call("reserve_inventory", data, heartbeat=True)
            if self.request["hold_after_reserve_seconds"]:
                await workflow.sleep(self.request["hold_after_reserve_seconds"])
            if self.request["notify"]:
                self.outputs["notification"] = await self.call("notify_outbox", data)
            self.transition("VERIFYING")
            evidence = await self.call("verify_outcome", data)
            self.outputs["verification"] = evidence
            if not evidence["verified"]:
                raise ApplicationError("independent verification failed", non_retryable=True)
            final_history = self.history + [
                {"state": "SUCCEEDED", "at": workflow.now().isoformat()}
            ]
            report = {
                "workflow_id": workflow_id,
                "state": "SUCCEEDED",
                "verified_completion": True,
                "history": final_history,
                "outputs": self.outputs,
            }
            report["metrics"] = {
                "duration_seconds": (workflow.now() - started_at).total_seconds(),
                "max_activity_attempts": 3,
                "model_tokens": 0,
            }
            await self.call("persist_report", report)
            self.state, self.history = "SUCCEEDED", final_history
            return report
        except (Exception, asyncio.CancelledError) as exc:
            if may_have_reserved:

                async def cleanup():
                    return await self.call(
                        "compensate_reservation",
                        {"workflow_id": workflow_id, "request": self.request},
                    )

                try:
                    self.outputs["compensation"] = await asyncio.shield(cleanup())
                except Exception:
                    self.outputs["compensation_required"] = True
            cancelled = isinstance(exc, asyncio.CancelledError)
            self.transition("CANCELLED" if cancelled else "FAILED")
            report = {
                "workflow_id": workflow_id,
                "state": self.state,
                "verified_completion": False,
                "history": self.history,
                "outputs": self.outputs,
                "error": type(exc).__name__,
            }
            report["metrics"] = {
                "duration_seconds": (workflow.now() - started_at).total_seconds(),
                "max_activity_attempts": 3,
                "model_tokens": 0,
            }
            await asyncio.shield(self.call("persist_report", report))
            if cancelled:
                raise
            raise ApplicationError(
                "fulfillment failed; inspect durable report", non_retryable=True
            ) from exc
