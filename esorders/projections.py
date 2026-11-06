"""Read models built from events: order summaries and daily revenue.

The projection and its processed-event table share one SQLite transaction, so
an event either changes the read model and is marked processed, or neither.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .errors import PermanentError
from .events import Event, UnknownEventType, UnsupportedSchema, upcast
from .log import Log, Record

SCHEMA = """
CREATE TABLE IF NOT EXISTS order_summary (
    order_id       TEXT PRIMARY KEY,
    customer_id    TEXT NOT NULL,
    status         TEXT NOT NULL,
    currency       TEXT NOT NULL,
    item_count     INTEGER NOT NULL,
    total_cents    INTEGER NOT NULL,
    paid_cents     INTEGER NOT NULL,
    refunded_cents INTEGER NOT NULL,
    version        INTEGER NOT NULL,
    updated_at     TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS daily_revenue (
    day            TEXT NOT NULL,
    currency       TEXT NOT NULL,
    gross_cents    INTEGER NOT NULL DEFAULT 0,
    refunded_cents INTEGER NOT NULL DEFAULT 0,
    payments       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, currency)
);
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY
);
"""


class OutOfOrderEvent(Exception):
    """An event arrived before an earlier event of the same order. Treated as transient."""


class Projection:
    def __init__(self, path: str | Path = ":memory:", idempotent: bool = True) -> None:
        self.conn = sqlite3.connect(str(path), isolation_level=None, timeout=30)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)
        self.idempotent = idempotent
        self.applied = 0
        self.duplicates_skipped = 0

    def close(self) -> None:
        self.conn.close()

    def apply(self, event: Event) -> bool:
        """Apply one upcast event. Returns False if it was already processed."""
        cur = self.conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            if self.idempotent:
                cur.execute("INSERT OR IGNORE INTO processed_events VALUES (?)", (event.event_id,))
                if cur.rowcount == 0:
                    cur.execute("ROLLBACK")
                    self.duplicates_skipped += 1
                    return False
            self._apply(cur, event)
        except BaseException:
            cur.execute("ROLLBACK")
            raise
        cur.execute("COMMIT")
        self.applied += 1
        return True

    def _summary_version(self, cur: sqlite3.Cursor, event: Event) -> None:
        row = cur.execute("SELECT version FROM order_summary WHERE order_id = ?", (event.aggregate_id,)).fetchone()
        have = row[0] if row else 0
        if self.idempotent and event.version != have + 1:
            raise OutOfOrderEvent(f"{event.aggregate_id}: got v{event.version}, projection is at v{have}")

    def _apply(self, cur: sqlite3.Cursor, e: Event) -> None:
        d, oid, day = e.data, e.aggregate_id, e.occurred_at[:10]
        self._summary_version(cur, e)
        match e.type:
            case "OrderCreated":
                total = sum(i["qty"] * i["unit_price_cents"] for i in d["items"])
                cur.execute(
                    "INSERT OR REPLACE INTO order_summary VALUES (?, ?, 'created', ?, ?, ?, 0, 0, ?, ?)",
                    (oid, d["customer_id"], d["currency"], len(d["items"]), total, e.version, e.occurred_at),
                )
                return
            case "ItemAdded":
                sets = "item_count = item_count + 1, total_cents = total_cents + ?"
                args: tuple = (d["qty"] * d["unit_price_cents"],)
            case "PaymentCaptured":
                sets, args = "status = 'paid', paid_cents = paid_cents + ?", (d["amount_cents"],)
                self._revenue(cur, oid, day, gross=d["amount_cents"], payments=1)
            case "OrderShipped":
                sets, args = "status = 'shipped'", ()
            case "OrderRefunded":
                sets = (
                    "refunded_cents = refunded_cents + ?, "
                    "status = CASE WHEN refunded_cents + ? >= paid_cents THEN 'refunded' ELSE status END"
                )
                args = (d["amount_cents"], d["amount_cents"])
                self._revenue(cur, oid, day, refunded=d["amount_cents"])
            case "OrderCancelled":
                sets, args = "status = 'cancelled', refunded_cents = refunded_cents + ?", (d["refund_cents"],)
                if d["refund_cents"]:
                    self._revenue(cur, oid, day, refunded=d["refund_cents"])
            case _:
                raise PermanentError(f"no projection rule for {e.type}")
        cur.execute(
            f"UPDATE order_summary SET {sets}, version = ?, updated_at = ? WHERE order_id = ?",
            (*args, e.version, e.occurred_at, oid),
        )

    def _revenue(
        self, cur: sqlite3.Cursor, order_id: str, day: str, gross: int = 0, refunded: int = 0, payments: int = 0
    ) -> None:
        currency = cur.execute("SELECT currency FROM order_summary WHERE order_id = ?", (order_id,)).fetchone()
        currency = currency[0] if currency else "???"
        cur.execute(
            "INSERT INTO daily_revenue VALUES (?, ?, ?, ?, ?) ON CONFLICT (day, currency) DO UPDATE SET "
            "gross_cents = gross_cents + excluded.gross_cents, refunded_cents = refunded_cents + excluded.refunded_cents, "
            "payments = payments + excluded.payments",
            (day, currency, gross, refunded, payments),
        )

    def summaries(self) -> dict[str, dict]:
        cur = self.conn.execute("SELECT * FROM order_summary ORDER BY order_id")
        cols = [c[0] for c in cur.description]
        return {r[0]: dict(zip(cols, r, strict=True)) for r in cur}

    def revenue(self) -> list[dict]:
        cur = self.conn.execute(
            "SELECT day, currency, gross_cents, refunded_cents, gross_cents - refunded_cents, payments "
            "FROM daily_revenue ORDER BY day, currency"
        )
        keys = ("day", "currency", "gross_cents", "refunded_cents", "net_cents", "payments")
        return [dict(zip(keys, r, strict=True)) for r in cur]

    def state(self) -> dict:
        """The full read model, for comparing two projections."""
        return {"orders": self.summaries(), "revenue": self.revenue()}

    def processed_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM processed_events").fetchone()[0]

    def dump(self) -> str:
        return json.dumps(self.state(), sort_keys=True)


def decode(rec: Record) -> Event:
    try:
        return upcast(Event.from_json(rec.value))
    except (UnknownEventType, UnsupportedSchema) as e:
        raise PermanentError(str(e)) from e
    except (ValueError, KeyError, TypeError) as e:
        raise PermanentError(f"cannot decode record at {rec.topic}/{rec.partition}@{rec.offset}: {e}") from e


def projection_handler(projection: Projection):
    def handle(rec: Record) -> None:
        projection.apply(decode(rec))

    return handle


def rebuild_from_log(log: Log, topic: str, projection: Projection) -> dict[str, int]:
    """Replay every partition from its oldest offset into an empty projection."""
    stats = {"records": 0, "applied": 0, "duplicates": 0, "undecodable": 0}
    for part in log.topic(topic).partitions:
        offset = part.start_offset
        while batch := part.read(offset, 500):
            for rec in batch:
                stats["records"] += 1
                try:
                    event = decode(rec)
                except PermanentError:
                    stats["undecodable"] += 1
                    continue
                stats["applied" if projection.apply(event) else "duplicates"] += 1
            offset = batch[-1].offset + 1
    return stats
