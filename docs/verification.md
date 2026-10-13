# Project 03 verification record

Executed locally October 13, 2026 for version 0.1.0. This record covers a real Temporal 1.29.1
server backed by PostgreSQL 16, a separate application database/role, and real worker processes.
It does not infer durability from mocked SDK calls or skipped integration tests.

| Check | Observed result |
| --- | --- |
| Full Windows Python 3.14.6 suite with integration enabled | 16 passed in 41.59 seconds |
| Full Linux Python 3.12 container suite with integration enabled | 16 passed in 36.57 seconds |
| Eight workflow acceptance scenarios, each platform | All passed |
| Worker killed after reservation commit, then replaced | Resumed on activity attempt 2; exactly one reservation |
| Verification failure and graceful cancellation | Compensation passed; reservation RELEASED |
| Invalid/rejected approval, approval timeout, retry exhaustion | Failed safely without reservation |
| Durable scheduled start and conditional notification branch | Passed |
| Direct ledger idempotency and late-reservation tombstone | Passed |
| Ruff lint/format and `pip check` | Passed |
| Clean non-editable package installation in non-root Docker worker | Passed |
| Local service readiness probe and installed CLI help | Passed |

There are 14 policy tests and two service integration tests. The larger integration test executes
eight distinct workflow scenarios and saves their reports/history. The ledger test separately checks
authorization at the write boundary, duplicate reservation, idempotent compensation, stock restoration,
and rejection of late reservations after a cancellation tombstone. Default pytest without
`ORCH_INTEGRATION=1` intentionally skips those two integration tests.

Commands used from the project directory:

```powershell
docker compose up -d postgres temporal
$env:APP_DB_URL = 'postgresql://orchestrator:local-orchestrator-demo@127.0.0.1:15433/orchestrator'
.\.venv\Scripts\python.exe scripts/wait_services.py
$env:ORCH_INTEGRATION = '1'
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider --junitxml=.orchestrator/tests-windows.xml
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\ruff.exe format --check .
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\orchestrator.exe --help
docker compose build worker
```

Linux suite command (replace PROJECT_PATH with the absolute project path):

```text
docker compose run --rm --no-deps --entrypoint python -e ORCH_INTEGRATION=1 --volume "PROJECT_PATH:/verification:ro" --workdir /data worker -m pytest /verification/tests -q -p no:cacheprovider --junitxml=/data/tests-linux.xml
```

The worker image installed a normal wheel rather than an editable checkout. Source was mounted
read-only, with reports/test state in its dedicated volume. No unrelated running containers or
databases were changed. Test workers used unique task queues and ephemeral signing keys and
were stopped by the harness. This project's Temporal/PostgreSQL services remain running locally
for exploration; `docker compose down` stops them while retaining their named volume.

The machine-readable [verification summary](evidence/verification-summary.json) records parsed
JUnit counts, all eight scenario outcomes on each platform, replacement-worker evidence, source
hashes, and limitations. Raw local evidence is under `.orchestrator/`, with full JSON Temporal
histories alongside reports. No signing key is written into these artifacts.

Final failures/skips in full acceptance runs: zero. An early test collection attempt ran before
package installation completed; it was rerun after installation. An initial cache-permission
warning was eliminated by disabling pytest cache in the full acceptance command. No failed test
or skipped integration run is represented as passing.

GitHub Actions is configured but has not run remotely for this project. No publication or external
deployment occurred. The scoped exactly-once effect guarantee applies to the PostgreSQL ledger,
not arbitrary external APIs. Temporal administrative authentication/TLS, production secrets,
external reconciliation, recurring schedule management, server/storage failure testing, and workflow
upgrade/replay compatibility require further work before stronger readiness claims.
