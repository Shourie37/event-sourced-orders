"""Relay that moves committed outbox rows from the event store onto the log."""

from __future__ import annotations

from .errors import SimulatedCrash
from .log import Producer
from .store import EventStore


class OutboxRelay:
    def __init__(self, store: EventStore, producer: Producer, batch_size: int = 100) -> None:
        self.store = store
        self.producer = producer
        self.batch_size = batch_size
        self.published = 0

    def run_once(self, crash_after: int | None = None) -> int:
        """Publish one batch, then mark it. A crash in between publishes the batch again on restart."""
        rows = self.store.pending_outbox(self.batch_size)
        for i, row in enumerate(rows, start=1):
            self.producer.send(row.topic, row.key, row.payload, headers={"event_id": row.event_id})
            self.published += 1
            if crash_after is not None and i >= crash_after:
                raise SimulatedCrash(f"relay crashed after publishing {i} rows")
        self.store.mark_published([r.id for r in rows])
        return len(rows)

    def drain(self) -> int:
        total = 0
        while n := self.run_once():
            total += n
        return total
