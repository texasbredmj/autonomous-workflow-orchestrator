import asyncio
import os
import uuid
from pathlib import Path

import pytest

from workflow_orchestrator.demo import run_scenarios
from workflow_orchestrator.ledger import Ledger

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("ORCH_INTEGRATION") != "1",
        reason="set ORCH_INTEGRATION=1 with local services",
    ),
]


def test_real_worker_recovery_and_control_scenarios():
    directory = Path(".orchestrator") / str(uuid.uuid4())
    report = asyncio.run(run_scenarios(directory))
    assert report["verified_demo"] and len(report["scenarios"]) == 8


def test_ledger_idempotency_and_late_reservation_tombstone():
    ledger = Ledger()
    ledger.initialize()
    request = {"sku": "demo-kit", "quantity": 2}
    identifier = "ledger-" + str(uuid.uuid4())
    initial = ledger.inventory("demo-kit")["stock"]
    with pytest.raises(ValueError, match="authorization"):
        ledger.reserve(identifier, request)
    ledger.authorize(identifier, request)
    assert ledger.reserve(identifier, request) == ledger.reserve(identifier, request)
    assert ledger.inventory("demo-kit")["stock"] == initial - 2
    ledger.compensate(identifier, request)
    ledger.compensate(identifier, request)
    assert ledger.inventory("demo-kit")["stock"] == initial
    with pytest.raises(ValueError, match="compensated"):
        ledger.reserve(identifier, request)
    identifier = "late-" + str(uuid.uuid4())
    ledger.authorize(identifier, request)
    ledger.compensate(identifier, request)
    with pytest.raises(ValueError, match="compensated"):
        ledger.reserve(identifier, request)
