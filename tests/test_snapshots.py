from helpers import ITEMS

from esorders.service import SNAPSHOT_SCHEMA, OrderService
from esorders.store import EventStore


def test_snapshot_load_matches_full_replay(tmp_path):
    svc = OrderService(EventStore(tmp_path / "e.db"), snapshot_every=10)
    svc.place_order("o1", "c1", ITEMS)
    for i in range(35):
        svc.add_item("o1", f"sku-{i}", 1, 100)
    assert svc.store.load_snapshot("o1")[0] == 30
    with_snap = svc.load("o1")
    assert svc.last_load_events_read == 6
    full = svc.load("o1", use_snapshot=False)
    assert svc.last_load_events_read == 36
    assert with_snap == full
    svc.pay("o1", full.total_cents)
    assert svc.load("o1").status == "paid"


def test_snapshot_with_old_schema_is_ignored(tmp_path):
    svc = OrderService(EventStore(tmp_path / "e.db"))
    svc.place_order("o1", "c1", ITEMS)
    svc.store.save_snapshot("o1", 1, SNAPSHOT_SCHEMA + 1, {"layout": "from the future"})
    assert svc.load("o1").status == "created"
    assert svc.last_load_events_read == 1


def test_older_snapshot_never_replaces_newer(tmp_path):
    store = EventStore(tmp_path / "e.db")
    store.save_snapshot("o1", 20, 1, {"v": 20})
    store.save_snapshot("o1", 10, 1, {"v": 10})
    assert store.load_snapshot("o1") == (20, 1, {"v": 20})
