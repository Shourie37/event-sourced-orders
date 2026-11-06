"""Command handlers: load the aggregate, decide, append with a version check."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import UTC, datetime

from .domain import NewEvent, Order
from .events import CURRENT_SCHEMA, Event, upcast
from .store import ConcurrencyError, EventStore

SNAPSHOT_SCHEMA = 1
ORDERS_TOPIC = "orders"


def utc_now() -> datetime:
    return datetime.now(UTC)


class OrderService:
    def __init__(
        self,
        store: EventStore,
        *,
        snapshot_every: int = 0,
        max_retries: int = 3,
        clock: Callable[[], datetime] = utc_now,
        new_id: Callable[[], str] = lambda: str(uuid.uuid4()),
        topic: str = ORDERS_TOPIC,
    ) -> None:
        self.store = store
        self.snapshot_every = snapshot_every
        self.max_retries = max_retries
        self.clock = clock
        self.new_id = new_id
        self.topic = topic
        self.last_load_events_read = 0
        self.conflicts_retried = 0

    def load(self, order_id: str, use_snapshot: bool = True) -> Order:
        order = Order(order_id)
        snap = self.store.load_snapshot(order_id) if use_snapshot else None
        # A snapshot from an older state layout is ignored, not migrated.
        if snap and snap[1] == SNAPSHOT_SCHEMA:
            order = Order.from_snapshot(snap[2])
        events = self.store.load(order_id, after_version=order.version)
        for e in events:
            order.apply(upcast(e))
        self.last_load_events_read = len(events)
        return order

    def _execute(self, order_id: str, decide: Callable[[Order], list[NewEvent]], expected_version: int | None) -> list[Event]:
        for attempt in range(self.max_retries + 1):
            order = self.load(order_id)
            if expected_version is not None and order.version != expected_version:
                raise ConcurrencyError(order_id, expected_version, order.version)
            at = self.clock().isoformat()
            events = [
                Event(self.new_id(), order_id, order.version + i + 1, n.type, CURRENT_SCHEMA[n.type], n.data, at)
                for i, n in enumerate(decide(order))
            ]
            try:
                self.store.append(order_id, order.version, events, self.topic)
            except ConcurrencyError:
                # A caller-supplied version means "only if nothing changed", so never retry it.
                if expected_version is not None or attempt == self.max_retries:
                    raise
                self.conflicts_retried += 1
                continue
            before = order.version
            for e in events:
                order.apply(e)
            n = self.snapshot_every
            if n and order.version // n > before // n:
                self.store.save_snapshot(order_id, order.version, SNAPSHOT_SCHEMA, order.to_snapshot())
            return events
        raise AssertionError("unreachable")

    def place_order(self, order_id: str, customer_id: str, items: list[dict], currency: str = "USD") -> list[Event]:
        return self._execute(order_id, lambda o: o.place(customer_id, items, currency), expected_version=None)

    def add_item(
        self, order_id: str, sku: str, qty: int, unit_price_cents: int, expected_version: int | None = None
    ) -> list[Event]:
        return self._execute(order_id, lambda o: o.add_item(sku, qty, unit_price_cents), expected_version)

    def pay(self, order_id: str, amount_cents: int, method: str = "card", expected_version: int | None = None) -> list[Event]:
        return self._execute(order_id, lambda o: o.pay(amount_cents, method), expected_version)

    def ship(self, order_id: str, carrier: str, tracking_number: str, expected_version: int | None = None) -> list[Event]:
        return self._execute(order_id, lambda o: o.ship(carrier, tracking_number), expected_version)

    def refund(self, order_id: str, amount_cents: int, reason: str, expected_version: int | None = None) -> list[Event]:
        return self._execute(order_id, lambda o: o.refund(amount_cents, reason), expected_version)

    def cancel(self, order_id: str, reason: str, expected_version: int | None = None) -> list[Event]:
        return self._execute(order_id, lambda o: o.cancel(reason), expected_version)
