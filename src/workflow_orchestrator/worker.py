import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from .activities import ACTIVITIES
from .ledger import Ledger
from .workflow import FulfillmentWorkflow

QUEUE = "fulfillment-v1"


async def main():
    if len(os.environ.get("ORCHESTRATOR_APPROVAL_KEY", "")) < 32:
        raise ValueError("configure an approval signing key of at least 32 characters")
    Ledger().initialize()
    client = await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "127.0.0.1:17233"))
    with ThreadPoolExecutor(max_workers=4) as executor:
        worker = Worker(
            client,
            task_queue=os.environ.get("TASK_QUEUE", QUEUE),
            workflows=[FulfillmentWorkflow],
            activities=ACTIVITIES,
            activity_executor=executor,
            max_concurrent_activities=4,
        )
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
