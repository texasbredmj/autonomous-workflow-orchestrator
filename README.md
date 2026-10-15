# Autonomous Workflow Orchestrator

## Start Here

[Launch the complete employer demonstration on GitHub](https://github.com/texasbredmj/ai-engineering-portfolio#start-here).

The public portfolio contains this project's runnable employer source, sample fixtures and verified demonstration, plus one Codespaces launch for all eight projects. No separate private-repository access is required. Run `python scripts/project_demo.py 3` from the portfolio Codespace to demonstrate this project.

The instructions below describe this repository's standalone version. The integrated platform and its current verification evidence are maintained in the portfolio.


Project 03 of AI Systems Engineering Lab: durable, bounded task coordination with Temporal,
PostgreSQL, Python, and Docker. Independent of the other portfolio repositories. It coordinates
a real local inventory ledger rather than relying on agent self-reported completion.

## Architecture and business workflow

```text
validate → durable scheduled timer → inspect inventory + quote (parallel)
         → signed human approval → reserve inventory → optional notification outbox
         → independent ledger verification → verified completion report
```

Temporal persists activity results, retries, timers, signals, and execution history in PostgreSQL.
Workers can disappear and be replaced. The application uses a separate PostgreSQL database/role
for atomic reservations, approval grants, idempotency records, compensations, and JSON reports.
The canonical lifecycle is CREATED → VALIDATED → PLANNED → WAITING_APPROVAL → AUTHORIZED →
RUNNING → VERIFYING → SUCCEEDED, with FAILED and CANCELLED exits. Illegal shortcuts are rejected.

Reservation retries use a workflow-specific transactional key. A committed reservation can be
returned again without another stock decrement. Compensation restores stock once, removes an
unsent notification outbox entry, and leaves a tombstone that prevents late reservations.
Verification independently checks reservation contents, one reservation event, notification branch,
and the global stock/active-reservation balance. Execution success alone cannot satisfy completion.

## Install and start local services

Python 3.11+ and Docker Compose are required for the full integration suite. No paid cloud or
real external service is required. Run from this project directory:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.lock .
docker compose up -d postgres temporal
$env:TEMPORAL_ADDRESS = '127.0.0.1:17233'
$env:APP_DB_URL = 'postgresql://orchestrator:local-orchestrator-demo@127.0.0.1:15433/orchestrator'
```

On Linux/macOS use `.venv/bin/python` and `export` for environment variables. Dedicated localhost
ports 17233/15433 avoid the usual Temporal/PostgreSQL ports. The Compose project has its own
network and named volume. Temporal startup takes time on the first image pull.

Compose passwords are **local demonstration fixtures**, not production credentials. The development
Temporal server is loopback-only but lacks TLS/authentication. Protect the machine and configure
production identity and storage separately before any external deployment.

## Run the real interruption demonstration

```powershell
.\.venv\Scripts\python.exe -m workflow_orchestrator.demo
```

The demo creates an ephemeral approval key and unique task queue, starts its own worker process,
and exercises eight acceptance scenarios. It deliberately kills the worker after a reservation
commits but before the activity acknowledges completion. A different worker process then resumes
the same workflow; an activity retry returns the existing reservation, preserving exactly one
reservation event. It also covers transient retry recovery, skipped notification branch, failed
verification and compensation, cancellation after reservation, invalid/rejected approval, scheduled
start, retry exhaustion, and approval timeout. It stops its own workers automatically.

Reports, worker logs, and full Temporal JSON histories are saved under `.orchestrator/UUID/`.
Successful evidence includes `verified_demo: true`, eight passing scenarios, replacement-worker
proof, a retry attempt of at least two, and exactly one reservation. Notifications are unsent local
outbox records; no email, message, or paid action is performed.

## Operate a workflow manually

Generate an approval key locally and start a worker:

```powershell
$env:ORCHESTRATOR_APPROVAL_KEY = .\.venv\Scripts\python.exe -c "import secrets; print(secrets.token_urlsafe(32))"
.\.venv\Scripts\python.exe -m workflow_orchestrator.worker
```

In another terminal configured with the same server/database and signing key:

```powershell
.\.venv\Scripts\orchestrator.exe start examples/order.json --id my-order-001
.\.venv\Scripts\orchestrator.exe status my-order-001
.\.venv\Scripts\orchestrator.exe approve my-order-001
.\.venv\Scripts\orchestrator.exe history my-order-001
```

`reject` submits a signed rejection; `cancel` requests graceful Temporal cancellation. `status`
returns the Temporal execution state and the independently persisted application report.
Wait for WAITING_APPROVAL before approving. A signing key proves possession of the local operator
credential; it does not identify a human through a managed identity provider. Signatures bind the
workflow ID, normalized request, decision, actor, nonce, and expiry. Self-approval is rejected.
The reservation activity requires a matching durable approval grant at the write boundary.

Set `not_before` in the JSON to a timezone-aware ISO timestamp for a one-time durable scheduled
start (maximum 24 hours ahead). Recurring schedule administration is not implemented. Fixed
dependency/branch logic is versioned in the workflow; arbitrary user-defined graphs/code are not
accepted. The fixture fields `pause_after_commit`, `fail_verification`, and transient failures exist
for repeatable local fault tests, not as unrestricted command hooks.

## Tests and CI

```powershell
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\ruff.exe format --check .
.\.venv\Scripts\python.exe -m pytest -q
$env:ORCH_INTEGRATION = '1'
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider --junitxml=.orchestrator/tests.xml
```

The first pytest command runs policy unit tests and explicitly skips service-dependent integration
tests. With services started and environment variables set, the second runs all tests, including
the real process interruption and ledger transaction scenarios. Never treat skipped integration
tests as durability evidence. CI starts real Temporal/PostgreSQL services and runs integration on
Linux, alongside platform policy checks. Dependencies are version-pinned; the lock is not a
hash-verified supply-chain lock.

## Docker worker and shutdown

```text
docker compose build worker
docker compose --profile worker up -d worker
docker compose down
```

Set the approval key before starting the worker profile. The Docker worker is non-root with a
read-only filesystem and dropped capabilities. `down` stops only this Compose project; preserve
its named PostgreSQL volume to retain workflows and ledger records. Do not use `down -v` unless
you intend to destroy this project's durable state. The demo itself does not stop server containers.

## Limits and evidence

Activity attempts are bounded at three, each with a 15-second execution deadline and 60-second
scheduling/retry budget. The client bounds total workflow execution to 26 hours, including
durable scheduled waits. Reservation activities heartbeat; database statements/locks are bounded
at five seconds, and the worker limits activity concurrency to four. Approval lifetime and wait
budgets are explicit. Normal Temporal event replay provides resumption; no unrestricted LLM or
shell access exists.

This local implementation does not claim general distributed exactly-once behavior. The guarantee
is scoped to the transactional PostgreSQL ledger. Real external APIs need their own idempotency
and reconciliation contracts. Forced workflow termination or server execution timeout bypasses
compensation; graceful cancel allows cleanup. If compensation or report persistence cannot complete,
history preserves the
failure and operator intervention is required. Activity/history data can contain business arguments;
protect database, artifacts, and signing keys. Temporal server failure/restart and production HA
need additional reliability testing. No cloud deployment or GitHub publication is implied.

See [architecture](docs/architecture.md), [threat model](docs/threat-model.md), and
[verification evidence](docs/verification.md). The next project in order is the Agent Evaluation
Harness, after this project's verified delivery milestone.
