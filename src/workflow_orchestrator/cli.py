import argparse
import asyncio
import json
import os
import uuid
from datetime import timedelta
from pathlib import Path

from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy

from .ledger import Ledger
from .policy import Order, sign_decision
from .workflow import FulfillmentWorkflow


async def execute(args):
    client = await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "127.0.0.1:17233"))
    if args.command == "start":
        request = Order.model_validate_json(
            Path(args.request).read_text(encoding="utf-8")
        ).model_dump()
        identifier = args.id or "order-" + str(uuid.uuid4())
        await client.start_workflow(
            FulfillmentWorkflow.run,
            request,
            id=identifier,
            task_queue=os.environ.get("TASK_QUEUE", "fulfillment-v1"),
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            execution_timeout=timedelta(hours=26),
        )
        print(json.dumps({"workflow_id": identifier, "submitted": True}))
        return
    handle = client.get_workflow_handle(args.id)
    if args.command in {"approve", "reject"}:
        status = await handle.query(FulfillmentWorkflow.status)
        decision = sign_decision(
            args.id,
            status["request"],
            os.environ["ORCHESTRATOR_APPROVAL_KEY"],
            approved=args.command == "approve",
        )
        await handle.signal(FulfillmentWorkflow.decision, decision)
        print(json.dumps({"decision_submitted": True, "workflow_id": args.id}))
    elif args.command == "cancel":
        await handle.cancel()
        print(json.dumps({"cancellation_requested": True, "workflow_id": args.id}))
    elif args.command == "history":
        history = await handle.fetch_history()
        print(history.to_json())
    else:
        report = Ledger().report(args.id)
        state = (await handle.describe()).status.name
        print(
            json.dumps(
                {
                    "temporal_state": state,
                    "report": report,
                    "live": await handle.query(FulfillmentWorkflow.status) if not report else None,
                },
                indent=2,
            )
        )


def main():
    parser = argparse.ArgumentParser(description="Durable authorized inventory fulfillment")
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start")
    start.add_argument("request")
    start.add_argument("--id")
    for name in ("approve", "reject", "cancel", "status", "history"):
        command = commands.add_parser(name)
        command.add_argument("id")
    asyncio.run(execute(parser.parse_args()))
