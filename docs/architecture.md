# Architecture and decisions

Temporal owns durable scheduling, dependencies, activity retry history, approval signals, and
worker resumption. PostgreSQL hosts Temporal persistence and a separate application database.
The application role cannot administer Temporal's database. `policy.py` validates requests and
approval signatures; `workflow.py` coordinates a fixed versioned dependency graph;
`activities.py` implements fixed typed-by-contract tools; `ledger.py` controls transactions.

The stock inspection and quote activities run in parallel; both must finish before approval.
Reservation depends on authorization; notification depends on reservation and an explicit branch
flag; verification depends on those outcomes. Only verified evidence permits SUCCEEDED.

Every reservation/compensation takes a workflow-specific PostgreSQL advisory transaction lock.
Unique workflow keys and atomic stock changes prevent duplicate reservations. Approval grants
bind the complete normalized request. Compensation is registered before the action, so an
ambiguous activity timeout is eligible for reversal. A compensation tombstone prevents a late
reservation retry from reapplying the effect. Outbox delivery is not implemented, so compensation
may safely remove the unsent notification record.

The workflow contains no database access, wall-clock calls, OS environment access, or secret
reads. Those occur in activities. Temporal's recorded time, timers, and activity results are used
for replay. Stable workflow/activity names are versioned; future changes must account for in-flight
history compatibility. JSON histories and application reports correlate using workflow IDs.

Official references: [Temporal message passing](https://docs.temporal.io/develop/python/workflows/message-passing),
[cancellation](https://docs.temporal.io/develop/python/workflows/cancellation), and
[official PostgreSQL Compose example](https://github.com/temporalio/docker-compose).
The local server version is pinned to the official Compose example's 1.29.1 baseline.
