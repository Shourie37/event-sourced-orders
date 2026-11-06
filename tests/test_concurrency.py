import threading

import pytest
from helpers import ITEMS

from esorders.service import OrderService
from esorders.store import ConcurrencyError, EventStore


def test_stale_expected_version_is_rejected(tmp_path):
    a = OrderService(EventStore(tmp_path / "e.db"))
    b = OrderService(EventStore(tmp_path / "e.db"))
    a.place_order("o1", "c1", ITEMS)
    seen = b.load("o1").version
    a.pay("o1", 7000, expected_version=seen)
    with pytest.raises(ConcurrencyError) as err:
        b.cancel("o1", "changed mind", expected_version=seen)
    assert (err.value.expected, err.value.actual) == (1, 2)
    assert a.load("o1").status == "paid"


def test_store_checks_version_inside_the_transaction(tmp_path):
    store = EventStore(tmp_path / "e.db")
    svc = OrderService(store)
    [created] = svc.place_order("o1", "c1", ITEMS)
    with pytest.raises(ConcurrencyError):
        store.append("o1", 0, [created], "orders")
    assert store.event_count() == 1
    assert len(store.pending_outbox(10)) == 1


def test_conflict_without_expected_version_reloads_and_retries(tmp_path):
    store = EventStore(tmp_path / "e.db")
    svc = OrderService(store)
    other = OrderService(EventStore(tmp_path / "e.db"))
    svc.place_order("o1", "c1", ITEMS)
    real_append = store.append
    raced = []

    def racing_append(*args, **kwargs):
        if not raced:
            raced.append(True)
            other.add_item("o1", "mug", 1, 1250)  # lands between our load and our append
        return real_append(*args, **kwargs)

    store.append = racing_append
    svc.add_item("o1", "pen", 1, 499)
    order = svc.load("o1")
    assert svc.conflicts_retried == 1
    assert order.version == 3
    assert [i["sku"] for i in order.items] == ["lamp", "mug", "pen"]


def test_conflict_retry_revalidates_against_new_state(tmp_path):
    store = EventStore(tmp_path / "e.db")
    svc = OrderService(store)
    other = OrderService(EventStore(tmp_path / "e.db"))
    svc.place_order("o1", "c1", ITEMS)
    real_append = store.append

    def racing_append(*args, **kwargs):
        if store.current_version("o1") == 1:
            other.cancel("o1", "customer")
        return real_append(*args, **kwargs)

    store.append = racing_append
    from esorders.domain import InvalidTransition

    with pytest.raises(InvalidTransition, match="cancelled"):
        svc.pay("o1", 7000)


def test_many_threads_writing_one_aggregate(tmp_path):
    OrderService(EventStore(tmp_path / "e.db")).place_order("o1", "c1", ITEMS)
    errors = []

    def worker(n: int) -> None:
        svc = OrderService(EventStore(tmp_path / "e.db"), max_retries=200)
        try:
            for i in range(10):
                svc.add_item("o1", f"sku-{n}-{i}", 1, 100)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    events = EventStore(tmp_path / "e.db").load("o1")
    assert [e.version for e in events] == list(range(1, 82))
    assert len({e.data.get("sku") for e in events[1:]}) == 80
