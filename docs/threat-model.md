# Threat model

Trusted: the local machine, Temporal administrative endpoint, application database role, installed
worker code, and operator signing key. Untrusted: request fields, unsigned approval signals, and
activity responses. The scenario is a local inventory ledger, not a public multi-tenant API.

Controls: strict allowlisted SKU and bounded quantity; no arbitrary tool code/shell/URLs; signed
request-bound approval with expiry and separate actor; durable grant checks before reservation;
transactional idempotency, bounded retries/heartbeats, cancellation tombstones; independent stock
balance checks; separate application database/role; localhost ports and non-root Docker worker.

Residual risks: local endpoint administrators can terminate/reset workflows; forced termination
does not run compensation. Signing-key possession is operator authority, not an authenticated
human identity. Local demonstration credentials are committed intentionally and must be replaced
for production. Secrets stay outside histories, but request data and approval signatures are in
Temporal history. Database administrators can tamper with records; audit is not cryptographically
immutable. An unavailable database can prevent compensation/report persistence. Other ledger writers
must respect the same invariants. The sample does not prove correctness for arbitrary external
side effects or restore server state after total storage loss. No external deployment is authorized.
