"""The write side: an SQLite event store with snapshots and a transactional outbox."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from .events import Event

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    position       INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id       TEXT NOT NULL UNIQUE,
    aggregate_id   TEXT NOT NULL,
    version        INTEGER NOT NULL,
    type           TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    data           TEXT NOT NULL,
    occurred_at    TEXT NOT NULL,
    UNIQUE (aggregate_id, version)
);
CREATE TABLE IF NOT EXISTS snapshots (
    aggregate_id TEXT PRIMARY KEY,
    version      INTEGER NOT NULL,
    schema       INTEGER NOT NULL,
    state        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id     TEXT NOT NULL,
    topic        TEXT NOT NULL,
    key          TEXT NOT NULL,
    payload      TEXT NOT NULL,
    published    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox (published, id);
"""


class ConcurrencyError(Exception):
    """Another writer appended to the aggregate after it was loaded."""

    def __init__(self, aggregate_id: str, expected: int, actual: int) -> None:
        super().__init__(f"{aggregate_id}: expected version {expected}, found {actual}")
        self.aggregate_id, self.expected, self.actual = aggregate_id, expected, actual


@dataclass(frozen=True)
class OutboxRow:
    id: int
    event_id: str
    topic: str
    key: str
    payload: str


def _row_to_event(row: tuple) -> Event:
    return Event(row[0], row[1], row[2], row[3], row[4], json.loads(row[5]), row[6])


_EVENT_COLS = "event_id, aggregate_id, version, type, schema_version, data, occurred_at"


class EventStore:
    def __init__(self, path: str | Path) -> None:
        self.conn = sqlite3.connect(str(path), isolation_level=None, timeout=30, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Cursor]:
        cur = self.conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            yield cur
        except BaseException:
            cur.execute("ROLLBACK")
            raise
        cur.execute("COMMIT")

    def current_version(self, aggregate_id: str) -> int:
        row = self.conn.execute("SELECT COALESCE(MAX(version), 0) FROM events WHERE aggregate_id = ?", (aggregate_id,))
        return row.fetchone()[0]

    def append(self, aggregate_id: str, expected_version: int, events: list[Event], topic: str) -> None:
        """Append events and their outbox rows in one transaction, if the version still matches."""
        with self._tx() as cur:
            actual = cur.execute(
                "SELECT COALESCE(MAX(version), 0) FROM events WHERE aggregate_id = ?", (aggregate_id,)
            ).fetchone()[0]
            if actual != expected_version:
                raise ConcurrencyError(aggregate_id, expected_version, actual)
            for e in events:
                cur.execute(
                    f"INSERT INTO events ({_EVENT_COLS}) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (e.event_id, e.aggregate_id, e.version, e.type, e.schema_version, json.dumps(e.data), e.occurred_at),
                )
                cur.execute(
                    "INSERT INTO outbox (event_id, topic, key, payload) VALUES (?, ?, ?, ?)",
                    (e.event_id, topic, e.aggregate_id, e.to_json()),
                )

    def load(self, aggregate_id: str, after_version: int = 0) -> list[Event]:
        rows = self.conn.execute(
            f"SELECT {_EVENT_COLS} FROM events WHERE aggregate_id = ? AND version > ? ORDER BY version",
            (aggregate_id, after_version),
        )
        return [_row_to_event(r) for r in rows]

    def all_events(self) -> Iterator[Event]:
        for r in self.conn.execute(f"SELECT {_EVENT_COLS} FROM events ORDER BY position"):
            yield _row_to_event(r)

    def aggregate_ids(self) -> list[str]:
        return [r[0] for r in self.conn.execute("SELECT DISTINCT aggregate_id FROM events ORDER BY aggregate_id")]

    def event_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    def save_snapshot(self, aggregate_id: str, version: int, schema: int, state: dict) -> None:
        self.conn.execute(
            "INSERT INTO snapshots VALUES (?, ?, ?, ?) ON CONFLICT (aggregate_id) DO UPDATE SET "
            "version = excluded.version, schema = excluded.schema, state = excluded.state "
            "WHERE excluded.version > snapshots.version",
            (aggregate_id, version, schema, json.dumps(state)),
        )

    def load_snapshot(self, aggregate_id: str) -> tuple[int, int, dict] | None:
        row = self.conn.execute("SELECT version, schema, state FROM snapshots WHERE aggregate_id = ?", (aggregate_id,)).fetchone()
        return (row[0], row[1], json.loads(row[2])) if row else None

    def pending_outbox(self, limit: int) -> list[OutboxRow]:
        rows = self.conn.execute(
            "SELECT id, event_id, topic, key, payload FROM outbox WHERE published = 0 ORDER BY id LIMIT ?", (limit,)
        )
        return [OutboxRow(*r) for r in rows]

    def mark_published(self, ids: list[int]) -> None:
        with self._tx() as cur:
            cur.executemany("UPDATE outbox SET published = 1 WHERE id = ?", [(i,) for i in ids])

    def insert_raw(self, event: Event, topic: str) -> None:
        """Write an event as-is. Used to seed legacy events written by older code."""
        self.append(event.aggregate_id, event.version - 1, [event], topic)
