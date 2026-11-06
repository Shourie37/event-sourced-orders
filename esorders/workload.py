"""Seeded order traffic for the demo and the tests."""

from __future__ import annotations

import random
import uuid
from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from .events import Event
from .service import ORDERS_TOPIC, OrderService
from .store import EventStore

SKUS = ["book", "lamp", "mug", "desk", "chair", "pen", "cable", "plant"]
PRICES = [499, 1250, 1999, 3500, 8900, 12900]
METHODS = ["card", "card", "card", "bank_transfer", "wallet"]


class FakeClock:
    def __init__(self, start: datetime = datetime(2026, 3, 2, 8, 0, tzinfo=UTC)) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs: float) -> None:
        self.now += timedelta(**kwargs)


def seeded_ids(rng: random.Random):
    return lambda: str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _plan(rng: random.Random, oid: str) -> list[tuple[str, tuple]]:
    def item() -> tuple[str, int, int]:
        return rng.choice(SKUS), rng.randint(1, 3), rng.choice(PRICES)

    first = [dict(zip(("sku", "qty", "unit_price_cents"), item(), strict=True)) for _ in range(rng.randint(1, 2))]
    extra = [item() for _ in range(rng.randint(0, 2))]
    total = sum(i["qty"] * i["unit_price_cents"] for i in first) + sum(q * p for _, q, p in extra)
    currency = "EUR" if rng.random() < 0.15 else "USD"
    steps: list[tuple[str, tuple]] = [("place_order", (oid, f"cust-{rng.randint(1, 40):03d}", first, currency))]
    steps += [("add_item", (oid, *x)) for x in extra]
    r = rng.random()
    if r < 0.08:
        return [*steps, ("cancel", (oid, "customer changed mind"))]
    steps.append(("pay", (oid, total, rng.choice(METHODS))))
    if r < 0.13:
        return [*steps, ("cancel", (oid, "out of stock"))]
    if r < 0.22:
        return steps
    steps.append(("ship", (oid, rng.choice(["ups", "dhl", "usps"]), f"TRK{rng.randint(10**7, 10**8 - 1)}")))
    if r > 0.85:
        steps.append(("refund", (oid, total if rng.random() < 0.5 else total // 2, "damaged in transit")))
    return steps


def run_workload(service: OrderService, rng: random.Random, n_orders: int, clock: FakeClock, prefix: str = "ord") -> int:
    """Run n_orders order lifecycles with their steps interleaved. Returns commands run."""
    pending = {f"{prefix}-{i:04d}": deque(_plan(rng, f"{prefix}-{i:04d}")) for i in range(1, n_orders + 1)}
    commands = 0
    while pending:
        oid = rng.choice(list(pending))
        name, args = pending[oid].popleft()
        getattr(service, name)(*args)
        commands += 1
        clock.advance(minutes=rng.randint(1, 8))
        if not pending[oid]:
            del pending[oid]
    return commands


def seed_legacy_orders(store: EventStore, rng: random.Random, at: datetime, count: int = 3) -> list[str]:
    """Write orders in the v1 schema, as an older release of the service did."""
    new_id = seeded_ids(rng)
    ids = []
    for i in range(1, count + 1):
        oid = f"legacy-{i:03d}"
        dollars = f"{rng.randint(5, 90)}.{rng.randint(0, 99):02d}"
        ts = (at + timedelta(minutes=i)).isoformat()
        store.insert_raw(
            Event(new_id(), oid, 1, "OrderCreated", 1, {"customer_id": f"cust-9{i:02d}", "total": dollars}, ts), ORDERS_TOPIC
        )
        cents = int(Decimal(dollars) * 100)
        store.insert_raw(Event(new_id(), oid, 2, "PaymentCaptured", 1, {"amount": cents}, ts), ORDERS_TOPIC)
        ids.append(oid)
    return ids
