"""Real Temporal/PostgreSQL acceptance scenarios with a replaceable worker process."""

import asyncio
import json
import os
import secrets
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from temporalio.client import Client, WorkflowFailureError
from temporalio.common import WorkflowIDReusePolicy

from .ledger import Ledger
from .policy import Order, sign_decision
from .workflow import FulfillmentWorkflow


class Harness:
    def __init__(self, directory: Path):
        self.directory = directory.resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.key = secrets.token_urlsafe(32)
        self.queue = "acceptance-" + str(uuid.uuid4())
        self.url = os.environ.get(
            "APP_DB_URL",
            "postgresql://orchestrator:local-orchestrator-demo@127.0.0.1:15433/orchestrator",
        )
        self.address = os.environ.get("TEMPORAL_ADDRESS", "127.0.0.1:17233")
        self.ledger = Ledger(self.url)
        self.process = None
        self.log = None

    def worker(self):
        self.log = (self.directory / "worker.log").open("ab")
        env = dict(os.environ)
        env.update(
            APP_DB_URL=self.url,
            TEMPORAL_ADDRESS=self.address,
            ORCHESTRATOR_APPROVAL_KEY=self.key,
            TASK_QUEUE=self.queue,
        )
        self.process = subprocess.Popen(
            [sys.executable, "-I", "-m", "workflow_orchestrator.worker"],
            env=env,
            stdout=self.log,
            stderr=self.log,
            stdin=subprocess.DEVNULL,
        )

    def stop(self, *, crash=False):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.kill() if crash else self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
            self.process = None
        if self.log:
            self.log.close()
            self.log = None

    async def __aenter__(self):
        self.ledger.initialize()
        self.client = await Client.connect(self.address)
        self.worker()
        return self

    async def __aexit__(self, *args):
        self.stop()

    async def wait_state(self, handle, state, seconds=30):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("worker exited; inspect worker.log")
            try:
                status = await asyncio.wait_for(handle.query(FulfillmentWorkflow.status), 3)
                if status["state"] == state:
                    return status
            except TimeoutError:
                pass
            await asyncio.sleep(0.1)
        raise TimeoutError(f"workflow did not reach {state}")

    async def start(self, **options):
        request = Order(**options).model_dump()
        identifier = "acceptance-" + str(uuid.uuid4())
        handle = await self.client.start_workflow(
            FulfillmentWorkflow.run,
            request,
            id=identifier,
            task_queue=self.queue,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            execution_timeout=timedelta(hours=26),
        )
        return handle, request

    async def approve(self, handle, request, *, approved=True):
        await handle.signal(
            FulfillmentWorkflow.decision,
            sign_decision(handle.id, request, self.key, approved=approved),
        )

    async def finish(self, handle, *, success=True):
        try:
            result = await asyncio.wait_for(handle.result(), 90)
            assert success, "unexpected workflow success"
        except WorkflowFailureError:
            assert not success, "unexpected workflow failure"
            result = self.ledger.report(handle.id)
        assert result is not None
        history = await handle.fetch_history()
        (self.directory / f"{handle.id}-history.json").write_text(
            history.to_json(), encoding="utf-8"
        )
        return result


async def run_scenarios(directory: Path) -> dict:
    cases = []
    async with Harness(directory) as harness:
        # Crash after the side effect commits but before the activity completion is recorded.
        handle, request = await harness.start(notify=True, pause_after_commit=True)
        await harness.wait_state(handle, "WAITING_APPROVAL")
        assert harness.ledger.evidence(handle.id)["reservation"] is None
        await harness.approve(handle, request)
        deadline = time.monotonic() + 30
        while harness.ledger.evidence(handle.id)["reservation"] is None:
            assert time.monotonic() < deadline
            await asyncio.sleep(0.1)
        initial_pid = harness.process.pid
        harness.stop(crash=True)
        assert harness.ledger.evidence(handle.id)["events"].count("reserved") == 1
        harness.worker()
        result = await harness.finish(handle)
        assert result["verified_completion"] and result["outputs"]["reservation"]["attempt"] >= 2
        assert initial_pid != harness.process.pid
        assert harness.ledger.evidence(handle.id)["events"].count("reserved") == 1
        cases.append(
            {
                "scenario": "worker_crash_after_commit",
                "passed": True,
                "workflow_id": handle.id,
                "worker_replaced": True,
                "reservation_count": 1,
                "activity_attempt": result["outputs"]["reservation"]["attempt"],
            }
        )

        handle, request = await harness.start(transient_quote_failures=2)
        await harness.wait_state(handle, "WAITING_APPROVAL")
        await harness.approve(handle, request)
        result = await harness.finish(handle)
        assert result["outputs"]["quote"]["attempt"] == 3
        assert "notification" not in result["outputs"]
        cases.append(
            {"scenario": "bounded_retry_and_branch_skip", "passed": True, "workflow_id": handle.id}
        )

        handle, request = await harness.start(fail_verification=True, notify=True)
        await harness.wait_state(handle, "WAITING_APPROVAL")
        await harness.approve(handle, request)
        result = await harness.finish(handle, success=False)
        assert result["state"] == "FAILED" and not result["verified_completion"]
        evidence = harness.ledger.evidence(handle.id)
        assert evidence["reservation"]["state"] == "RELEASED"
        assert "notification_outbox" not in evidence["events"]
        cases.append(
            {
                "scenario": "verification_failure_compensates",
                "passed": True,
                "workflow_id": handle.id,
            }
        )

        handle, request = await harness.start(hold_after_reserve_seconds=20)
        await harness.wait_state(handle, "WAITING_APPROVAL")
        await harness.approve(handle, request)
        deadline = time.monotonic() + 30
        while harness.ledger.evidence(handle.id)["reservation"] is None:
            assert time.monotonic() < deadline
            await asyncio.sleep(0.1)
        await handle.cancel()
        result = await harness.finish(handle, success=False)
        assert result["state"] == "CANCELLED"
        assert harness.ledger.evidence(handle.id)["reservation"]["state"] == "RELEASED"
        cases.append(
            {
                "scenario": "cancel_after_reservation_compensates",
                "passed": True,
                "workflow_id": handle.id,
            }
        )

        handle, request = await harness.start()
        await harness.wait_state(handle, "WAITING_APPROVAL")
        bad = sign_decision(handle.id, request, "invalid-key-" * 4)
        await handle.signal(FulfillmentWorkflow.decision, bad)
        await asyncio.sleep(0.5)
        assert harness.ledger.evidence(handle.id)["reservation"] is None
        await harness.approve(handle, request, approved=False)
        result = await harness.finish(handle, success=False)
        assert result["state"] == "FAILED" and result["outputs"]["rejected_approvals"] == 1
        cases.append(
            {
                "scenario": "unauthorized_then_rejected_approval",
                "passed": True,
                "workflow_id": handle.id,
            }
        )

        due = datetime.now(UTC) + timedelta(seconds=3)
        handle, request = await harness.start(not_before=due.isoformat())
        await harness.wait_state(handle, "PLANNED")
        assert harness.ledger.evidence(handle.id)["reservation"] is None
        await harness.wait_state(handle, "WAITING_APPROVAL")
        assert datetime.now(UTC) >= due
        await harness.approve(handle, request)
        assert (await harness.finish(handle))["verified_completion"]
        cases.append(
            {"scenario": "durable_scheduled_start", "passed": True, "workflow_id": handle.id}
        )

        handle, request = await harness.start(transient_quote_failures=3)
        result = await harness.finish(handle, success=False)
        assert (
            result["state"] == "FAILED"
            and harness.ledger.evidence(handle.id)["reservation"] is None
        )
        cases.append(
            {"scenario": "retry_exhaustion_no_effect", "passed": True, "workflow_id": handle.id}
        )

        handle, request = await harness.start(approval_timeout_seconds=1)
        result = await harness.finish(handle, success=False)
        assert (
            result["state"] == "FAILED"
            and harness.ledger.evidence(handle.id)["reservation"] is None
        )
        cases.append(
            {"scenario": "approval_timeout_no_effect", "passed": True, "workflow_id": handle.id}
        )
    summary = {
        "verified_demo": all(case["passed"] for case in cases),
        "scenarios": cases,
        "backend": "Temporal + PostgreSQL",
        "real_worker_process_interruption": True,
    }
    (directory / "demo-report.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main():
    directory = Path(".orchestrator") / str(uuid.uuid4())
    print(json.dumps(asyncio.run(run_scenarios(directory)), indent=2))


if __name__ == "__main__":
    main()
