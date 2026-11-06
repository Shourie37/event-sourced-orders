"""The Order aggregate.

State is never stored directly. It is rebuilt by applying events in version
order. Each command checks the current state and returns new events, or raises.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .events import Event

CREATED, PAID, SHIPPED, REFUNDED, CANCELLED = "created", "paid", "shipped", "refunded", "cancelled"
TERMINAL = {REFUNDED, CANCELLED}
PAYMENT_METHODS = {"card", "bank_transfer", "wallet"}


class DomainError(Exception):
    pass


class InvalidCommand(DomainError):
    """The command input is wrong, whatever the state."""


class InvalidTransition(DomainError):
    """The command is not allowed in the current state."""


@dataclass(frozen=True)
class NewEvent:
    type: str
    data: dict


def _check_item(item: dict) -> dict:
    sku, qty, price = item.get("sku"), item.get("qty"), item.get("unit_price_cents")
    if not isinstance(sku, str) or not sku:
        raise InvalidCommand("sku is required")
    if not isinstance(qty, int) or qty <= 0:
        raise InvalidCommand(f"qty must be a positive integer, got {qty!r}")
    if not isinstance(price, int) or price < 0:
        raise InvalidCommand(f"unit_price_cents must be a non-negative integer, got {price!r}")
    return {"sku": sku, "qty": qty, "unit_price_cents": price}


@dataclass
class Order:
    order_id: str
    version: int = 0
    status: str | None = None
    customer_id: str = ""
    currency: str = ""
    items: list[dict] = field(default_factory=list)
    paid_cents: int = 0
    refunded_cents: int = 0

    @property
    def total_cents(self) -> int:
        return sum(i["qty"] * i["unit_price_cents"] for i in self.items)

    def _require(self, *allowed: str) -> None:
        if self.status is None:
            raise InvalidTransition(f"order {self.order_id} does not exist")
        if self.status not in allowed:
            raise InvalidTransition(f"order {self.order_id} is {self.status}; needs {' or '.join(allowed)}")

    # Commands. Each one validates and returns events; none of them change state.

    def place(self, customer_id: str, items: list[dict], currency: str) -> list[NewEvent]:
        if self.status is not None:
            raise InvalidTransition(f"order {self.order_id} already exists")
        if not customer_id:
            raise InvalidCommand("customer_id is required")
        if not items:
            raise InvalidCommand("an order needs at least one item")
        if len(currency) != 3 or not currency.isalpha() or not currency.isupper():
            raise InvalidCommand(f"currency must be an ISO code like USD, got {currency!r}")
        data = {"customer_id": customer_id, "currency": currency, "items": [_check_item(i) for i in items]}
        return [NewEvent("OrderCreated", data)]

    def add_item(self, sku: str, qty: int, unit_price_cents: int) -> list[NewEvent]:
        self._require(CREATED)
        return [NewEvent("ItemAdded", _check_item({"sku": sku, "qty": qty, "unit_price_cents": unit_price_cents}))]

    def pay(self, amount_cents: int, method: str) -> list[NewEvent]:
        self._require(CREATED)
        if self.total_cents <= 0:
            raise InvalidCommand("cannot pay for a zero-value order")
        if amount_cents != self.total_cents:
            raise InvalidCommand(f"payment {amount_cents} does not match order total {self.total_cents}")
        if method not in PAYMENT_METHODS:
            raise InvalidCommand(f"unknown payment method {method!r}")
        return [NewEvent("PaymentCaptured", {"amount_cents": amount_cents, "method": method})]

    def ship(self, carrier: str, tracking_number: str) -> list[NewEvent]:
        self._require(PAID)
        if not carrier or not tracking_number:
            raise InvalidCommand("carrier and tracking_number are required")
        return [NewEvent("OrderShipped", {"carrier": carrier, "tracking_number": tracking_number})]

    def refund(self, amount_cents: int, reason: str) -> list[NewEvent]:
        self._require(PAID, SHIPPED)
        remaining = self.paid_cents - self.refunded_cents
        if not 0 < amount_cents <= remaining:
            raise InvalidCommand(f"refund must be between 1 and {remaining} cents, got {amount_cents}")
        return [NewEvent("OrderRefunded", {"amount_cents": amount_cents, "reason": reason})]

    def cancel(self, reason: str) -> list[NewEvent]:
        # A paid order that has not shipped can still be cancelled; the money goes back.
        self._require(CREATED, PAID)
        refund = self.paid_cents - self.refunded_cents
        return [NewEvent("OrderCancelled", {"reason": reason, "refund_cents": refund})]

    # Event application. This must never fail for an event that a command produced.

    def apply(self, event: Event) -> None:
        if event.version != self.version + 1:
            raise ValueError(f"order {self.order_id}: expected version {self.version + 1}, got {event.version}")
        d = event.data
        match event.type:
            case "OrderCreated":
                self.status, self.customer_id, self.currency = CREATED, d["customer_id"], d["currency"]
                self.items = list(d["items"])
            case "ItemAdded":
                self.items.append(d)
            case "PaymentCaptured":
                self.status, self.paid_cents = PAID, d["amount_cents"]
            case "OrderShipped":
                self.status = SHIPPED
            case "OrderRefunded":
                self.refunded_cents += d["amount_cents"]
                if self.refunded_cents == self.paid_cents:
                    self.status = REFUNDED
            case "OrderCancelled":
                self.status = CANCELLED
                self.refunded_cents += d["refund_cents"]
            case _:
                raise ValueError(f"unknown event type {event.type}")
        self.version = event.version

    @classmethod
    def from_events(cls, order_id: str, events: list[Event]) -> Order:
        order = cls(order_id)
        for e in events:
            order.apply(e)
        return order

    def to_snapshot(self) -> dict:
        return dict(self.__dict__)

    @classmethod
    def from_snapshot(cls, state: dict) -> Order:
        return cls(**state)
