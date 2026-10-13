import json
import os

import psycopg
from psycopg.rows import dict_row

from .policy import digest


class Ledger:
    def __init__(self, url=None):
        self.url = url or os.environ["APP_DB_URL"]

    def connect(self):
        return psycopg.connect(
            self.url,
            connect_timeout=5,
            row_factory=dict_row,
            options="-c statement_timeout=5000 -c lock_timeout=5000",
        )

    def initialize(self):
        with self.connect() as db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS inventory (sku TEXT PRIMARY KEY, stock INT NOT NULL
                    CHECK(stock>=0), unit_price INT NOT NULL);
                CREATE TABLE IF NOT EXISTS reservations (workflow_id TEXT PRIMARY KEY,
                    sku TEXT NOT NULL, quantity INT NOT NULL, state TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS events (workflow_id TEXT NOT NULL, kind TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY(workflow_id,kind));
                CREATE TABLE IF NOT EXISTS reports (
                    workflow_id TEXT PRIMARY KEY, report JSONB NOT NULL);
                CREATE TABLE IF NOT EXISTS approvals (
                    workflow_id TEXT PRIMARY KEY, request_digest TEXT NOT NULL);
                ALTER TABLE inventory ADD COLUMN IF NOT EXISTS initial_stock INT DEFAULT 100000;
                INSERT INTO inventory VALUES ('demo-kit',100000,100) ON CONFLICT DO NOTHING;
            """)

    def authorize(self, workflow_id, request):
        with self.connect() as db:
            expected = digest(request)
            db.execute(
                "INSERT INTO approvals VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (workflow_id, expected),
            )
            row = db.execute(
                "SELECT request_digest FROM approvals WHERE workflow_id=%s", (workflow_id,)
            ).fetchone()
            if row["request_digest"] != expected:
                raise ValueError("authorization conflicts with existing request")

    def inventory(self, sku):
        with self.connect() as db:
            row = db.execute("SELECT * FROM inventory WHERE sku=%s", (sku,)).fetchone()
            if not row:
                raise ValueError("unknown inventory item")
            return row

    def reserve(self, workflow_id, request):
        with self.connect() as db:
            db.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (workflow_id,))
            grant = db.execute(
                "SELECT request_digest FROM approvals WHERE workflow_id=%s", (workflow_id,)
            ).fetchone()
            if not grant or grant["request_digest"] != digest(request):
                raise ValueError("matching durable authorization required")
            existing = db.execute(
                "SELECT * FROM reservations WHERE workflow_id=%s", (workflow_id,)
            ).fetchone()
            if existing:
                if existing["sku"] != request["sku"] or existing["quantity"] != request["quantity"]:
                    raise ValueError("reservation idempotency conflict")
                if existing["state"] != "ACTIVE":
                    raise ValueError("reservation has been compensated")
                return existing
            stock = db.execute(
                "SELECT stock FROM inventory WHERE sku=%s FOR UPDATE", (request["sku"],)
            ).fetchone()
            if not stock or stock["stock"] < request["quantity"]:
                raise ValueError("insufficient stock")
            db.execute(
                "UPDATE inventory SET stock=stock-%s WHERE sku=%s",
                (request["quantity"], request["sku"]),
            )
            row = db.execute(
                "INSERT INTO reservations VALUES (%s,%s,%s,'ACTIVE') RETURNING *",
                (workflow_id, request["sku"], request["quantity"]),
            ).fetchone()
            db.execute(
                "INSERT INTO events(workflow_id,kind) VALUES (%s,'reserved')", (workflow_id,)
            )
            return row

    def compensate(self, workflow_id, request):
        with self.connect() as db:
            db.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (workflow_id,))
            row = db.execute(
                "SELECT * FROM reservations WHERE workflow_id=%s", (workflow_id,)
            ).fetchone()
            if row and row["state"] == "ACTIVE":
                db.execute(
                    "UPDATE inventory SET stock=stock+%s WHERE sku=%s",
                    (row["quantity"], row["sku"]),
                )
                db.execute(
                    "UPDATE reservations SET state='RELEASED' WHERE workflow_id=%s", (workflow_id,)
                )
            elif not row:
                # Tombstone prevents a late retry from reserving after cancellation.
                db.execute(
                    "INSERT INTO reservations VALUES (%s,%s,%s,'RELEASED')",
                    (workflow_id, request["sku"], request["quantity"]),
                )
            db.execute(
                "INSERT INTO events(workflow_id,kind) VALUES (%s,'compensated') "
                "ON CONFLICT DO NOTHING",
                (workflow_id,),
            )
            db.execute(
                "DELETE FROM events WHERE workflow_id=%s AND kind='notification_outbox'",
                (workflow_id,),
            )
            return self._evidence(db, workflow_id)

    def notify(self, workflow_id):
        with self.connect() as db:
            row = db.execute(
                "SELECT state FROM reservations WHERE workflow_id=%s", (workflow_id,)
            ).fetchone()
            if not row or row["state"] != "ACTIVE":
                raise ValueError("active reservation required for notification")
            db.execute(
                "INSERT INTO events(workflow_id,kind) VALUES (%s,'notification_outbox') "
                "ON CONFLICT DO NOTHING",
                (workflow_id,),
            )
        return {"outbox_created": True}

    def _evidence(self, db, workflow_id):
        row = db.execute(
            "SELECT * FROM reservations WHERE workflow_id=%s", (workflow_id,)
        ).fetchone()
        events = db.execute(
            "SELECT kind FROM events WHERE workflow_id=%s ORDER BY kind", (workflow_id,)
        ).fetchall()
        balance = db.execute("""
            SELECT i.stock + COALESCE((SELECT SUM(r.quantity) FROM reservations r
                WHERE r.sku=i.sku AND r.state='ACTIVE'),0) = i.initial_stock AS valid
            FROM inventory i WHERE i.sku='demo-kit'
        """).fetchone()
        return {
            "reservation": row,
            "events": [event["kind"] for event in events],
            "stock_balance_verified": bool(balance and balance["valid"]),
        }

    def evidence(self, workflow_id):
        with self.connect() as db:
            return self._evidence(db, workflow_id)

    def report(self, workflow_id, report=None):
        with self.connect() as db:
            if report is not None:
                db.execute(
                    "INSERT INTO reports VALUES (%s,%s) ON CONFLICT(workflow_id) "
                    "DO UPDATE SET report=EXCLUDED.report",
                    (workflow_id, json.dumps(report)),
                )
            row = db.execute(
                "SELECT report FROM reports WHERE workflow_id=%s", (workflow_id,)
            ).fetchone()
            return row["report"] if row else None
