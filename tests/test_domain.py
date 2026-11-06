import pytest
from helpers import ITEMS

from esorders.domain import CANCELLED, PAID, REFUNDED, SHIPPED, InvalidCommand, InvalidTransition, Order
from esorders.service import OrderService
from esorders.store import EventStore


@pytest.fixture
def svc(tmp_path):
    return OrderService(EventStore(tmp_path / "e.db"))


def test_happy_path_rebuilds_from_events(svc):
    svc.place_order("o1", "c1", ITEMS)
    svc.add_item("o1", "pen", 3, 100)
    svc.pay("o1", 7300, "wallet")
    svc.ship("o1", "ups", "TRK1")
    svc.refund("o1", 300, "pen broke")
    order = Order.from_events("o1", svc.store.load("o1"))
    assert (order.status, order.version, order.total_cents, order.paid_cents, order.refunded_cents) == (
        SHIPPED,
        5,
        7300,
        7300,
        300,
    )
    svc.refund("o1", 7000, "rest")
    assert svc.load("o1").status == REFUNDED


@pytest.mark.parametrize(
    "items, currency",
    [
        ([], "USD"),
        ([{"sku": "x", "qty": 0, "unit_price_cents": 1}], "USD"),
        ([{"sku": "", "qty": 1, "unit_price_cents": 1}], "USD"),
        ([{"sku": "x", "qty": 1, "unit_price_cents": -5}], "USD"),
        (ITEMS, "usd"),
        (ITEMS, "DOLLARS"),
    ],
)
def test_place_rejects_bad_input(svc, items, currency):
    with pytest.raises(InvalidCommand):
        svc.place_order("o1", "c1", items, currency)
    assert svc.store.event_count() == 0


def test_cannot_place_twice(svc):
    svc.place_order("o1", "c1", ITEMS)
    with pytest.raises(InvalidTransition):
        svc.place_order("o1", "c1", ITEMS)


def test_payment_must_match_total_and_method(svc):
    svc.place_order("o1", "c1", ITEMS)
    with pytest.raises(InvalidCommand, match="does not match"):
        svc.pay("o1", 6999)
    with pytest.raises(InvalidCommand, match="method"):
        svc.pay("o1", 7000, "iou")
    svc.pay("o1", 7000)
    with pytest.raises(InvalidTransition):
        svc.pay("o1", 7000)


def test_items_only_before_payment(svc):
    svc.place_order("o1", "c1", ITEMS)
    svc.pay("o1", 7000)
    with pytest.raises(InvalidTransition):
        svc.add_item("o1", "pen", 1, 100)


def test_ship_requires_payment(svc):
    svc.place_order("o1", "c1", ITEMS)
    with pytest.raises(InvalidTransition, match="needs paid"):
        svc.ship("o1", "ups", "TRK1")


def test_commands_on_missing_order(svc):
    with pytest.raises(InvalidTransition, match="does not exist"):
        svc.pay("nope", 100)


def test_cancel_paid_order_refunds_everything(svc):
    svc.place_order("o1", "c1", ITEMS)
    svc.pay("o1", 7000)
    [event] = svc.cancel("o1", "out of stock")
    assert event.data["refund_cents"] == 7000
    order = svc.load("o1")
    assert (order.status, order.refunded_cents) == (CANCELLED, 7000)


def test_cannot_cancel_after_shipping(svc):
    svc.place_order("o1", "c1", ITEMS)
    svc.pay("o1", 7000)
    svc.ship("o1", "ups", "TRK1")
    with pytest.raises(InvalidTransition):
        svc.cancel("o1", "too late")


def test_refund_limits(svc):
    svc.place_order("o1", "c1", ITEMS)
    with pytest.raises(InvalidTransition):
        svc.refund("o1", 100, "not paid")
    svc.pay("o1", 7000)
    for bad in (0, -1, 7001):
        with pytest.raises(InvalidCommand):
            svc.refund("o1", bad, "x")
    svc.refund("o1", 4000, "part")
    with pytest.raises(InvalidCommand, match="between 1 and 3000"):
        svc.refund("o1", 3001, "too much")


@pytest.mark.parametrize("final", ["cancel", "refund"])
def test_terminal_states_reject_everything(svc, final):
    svc.place_order("o1", "c1", ITEMS)
    svc.pay("o1", 7000)
    svc.cancel("o1", "x") if final == "cancel" else svc.refund("o1", 7000, "x")
    for call in (
        lambda: svc.pay("o1", 7000),
        lambda: svc.ship("o1", "ups", "T"),
        lambda: svc.refund("o1", 1, "x"),
        lambda: svc.cancel("o1", "x"),
        lambda: svc.add_item("o1", "pen", 1, 1),
    ):
        with pytest.raises(InvalidTransition):
            call()
    assert svc.load("o1").status in (CANCELLED, REFUNDED)


def test_apply_rejects_version_gaps(svc):
    svc.place_order("o1", "c1", ITEMS)
    svc.pay("o1", 7000)
    events = svc.store.load("o1")
    order = Order("o1")
    with pytest.raises(ValueError, match="expected version 1"):
        order.apply(events[1])
    order.apply(events[0])
    order.apply(events[1])
    assert order.status == PAID
