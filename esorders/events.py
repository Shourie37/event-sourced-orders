"""Event envelope, schema versions, and upcasters.

Stored events are never rewritten. When an event's shape changes, the old
version stays on disk and an upcaster converts it to the current shape at read time.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from decimal import Decimal

CURRENT_SCHEMA: dict[str, int] = {
    "OrderCreated": 2,
    "ItemAdded": 1,
    "PaymentCaptured": 2,
    "OrderShipped": 1,
    "OrderRefunded": 1,
    "OrderCancelled": 1,
}


class UnknownEventType(ValueError):
    pass


class UnsupportedSchema(ValueError):
    """The event is newer than this reader understands."""


@dataclass(frozen=True)
class Event:
    event_id: str
    aggregate_id: str
    version: int
    type: str
    schema_version: int
    data: dict
    occurred_at: str  # ISO-8601 UTC

    def to_json(self) -> str:
        return json.dumps(self.__dict__, separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> Event:
        d = json.loads(raw)
        return cls(
            event_id=str(d["event_id"]),
            aggregate_id=str(d["aggregate_id"]),
            version=int(d["version"]),
            type=str(d["type"]),
            schema_version=int(d["schema_version"]),
            data=dict(d["data"]),
            occurred_at=str(d["occurred_at"]),
        )


Upcaster = Callable[[dict], dict]
_UPCASTERS: dict[tuple[str, int], Upcaster] = {}


def upcaster(event_type: str, from_version: int) -> Callable[[Upcaster], Upcaster]:
    def register(fn: Upcaster) -> Upcaster:
        _UPCASTERS[(event_type, from_version)] = fn
        return fn

    return register


@upcaster("OrderCreated", 1)
def _order_created_v1(data: dict) -> dict:
    # v1 stored one total as a dollar string and had no line items or currency.
    cents = int(Decimal(data["total"]) * 100)
    return {
        "customer_id": data["customer_id"],
        "currency": "USD",
        "items": [{"sku": "LEGACY-ORDER", "qty": 1, "unit_price_cents": cents}],
    }


@upcaster("PaymentCaptured", 1)
def _payment_captured_v1(data: dict) -> dict:
    # v1 named the field "amount" and only took cards.
    return {"amount_cents": data["amount"], "method": "card"}


def upcast(event: Event) -> Event:
    current = CURRENT_SCHEMA.get(event.type)
    if current is None:
        raise UnknownEventType(event.type)
    if event.schema_version > current:
        raise UnsupportedSchema(f"{event.type} v{event.schema_version} > v{current}")
    data, version = event.data, event.schema_version
    while version < current:
        data = _UPCASTERS[(event.type, version)](data)
        version += 1
    return event if version == event.schema_version else replace(event, data=data, schema_version=version)
