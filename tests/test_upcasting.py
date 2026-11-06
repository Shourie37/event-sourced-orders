import pytest

from esorders.errors import PermanentError
from esorders.events import Event, UnsupportedSchema, upcast
from esorders.log import Record
from esorders.projections import Projection, decode
from esorders.service import OrderService
from esorders.store import EventStore


def v1_created(total="12.34"):
    return Event("e1", "o1", 1, "OrderCreated", 1, {"customer_id": "c1", "total": total}, "2026-01-05T10:00:00+00:00")


def v1_paid(amount=1234):
    return Event("e2", "o1", 2, "PaymentCaptured", 1, {"amount": amount}, "2026-01-05T10:05:00+00:00")


def test_order_created_v1_to_v2():
    e = upcast(v1_created("19.99"))
    assert e.schema_version == 2
    assert e.data == {
        "customer_id": "c1",
        "currency": "USD",
        "items": [{"sku": "LEGACY-ORDER", "qty": 1, "unit_price_cents": 1999}],
    }


def test_decimal_parsing_avoids_float_error():
    # float("0.29") * 100 is 28.999999999999996
    assert upcast(v1_created("0.29")).data["items"][0]["unit_price_cents"] == 29


def test_payment_captured_v1_to_v2():
    assert upcast(v1_paid(500)).data == {"amount_cents": 500, "method": "card"}


def test_current_events_pass_through_unchanged():
    e = Event("e3", "o1", 3, "OrderShipped", 1, {"carrier": "ups", "tracking_number": "T"}, "2026-01-05T11:00:00+00:00")
    assert upcast(e) is e


def test_newer_schema_is_refused():
    with pytest.raises(UnsupportedSchema):
        upcast(Event("e1", "o1", 1, "OrderCreated", 3, {}, "2026-01-05T10:00:00+00:00"))


def test_aggregate_loads_and_continues_from_legacy_events(tmp_path):
    store = EventStore(tmp_path / "e.db")
    store.insert_raw(v1_created(), "orders")
    store.insert_raw(v1_paid(), "orders")
    svc = OrderService(store)
    order = svc.load("o1")
    assert (order.status, order.total_cents, order.paid_cents) == ("paid", 1234, 1234)
    svc.ship("o1", "ups", "TRK1")
    assert store.load("o1")[0].schema_version == 1  # stored events are never rewritten


def test_projection_reads_legacy_records_from_the_log():
    p = Projection()
    for e in (v1_created(), v1_paid()):
        p.apply(decode(Record("orders", 0, 0, 0, "o1", e.to_json(), {})))
    row = p.summaries()["o1"]
    assert (row["status"], row["total_cents"], row["paid_cents"]) == ("paid", 1234, 1234)
    assert p.revenue()[0]["gross_cents"] == 1234


def test_decode_turns_bad_records_into_permanent_errors():
    for value in ("{nope", '{"event_id": "x"}', Event("x", "o1", 1, "Mystery", 1, {}, "t").to_json()):
        with pytest.raises(PermanentError):
            decode(Record("orders", 0, 0, 0, "o1", value, {}))
