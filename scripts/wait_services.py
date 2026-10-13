"""Bounded readiness checks for the application database and Temporal default namespace."""

import asyncio
import os
import time

from temporalio.client import Client

from workflow_orchestrator.ledger import Ledger


async def probe():
    with Ledger().connect() as db:
        db.execute("SELECT 1")
    client = await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "127.0.0.1:17233"))
    async for _ in client.list_workflows(page_size=1):
        break


async def main():
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            await asyncio.wait_for(probe(), timeout=6)
            print("PostgreSQL and Temporal namespace ready")
            return
        except Exception:
            await asyncio.sleep(1)
    raise TimeoutError("local services did not become ready within the startup budget")


if __name__ == "__main__":
    asyncio.run(main())
