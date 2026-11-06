import pytest
from helpers import World

from esorders.errors import SimulatedCrash
from esorders.events import Event
from esorders.log import Producer
from esorders.outbox import OutboxRelay
from esorders.projections import Projection, rebuild_from_log
from esorders.service import ORDERS_TOPIC
from esorders.store import ConcurrencyError


def test_every_event_gets_exactly_one_outbox_row(tmp_path):
    w = World(tmp_path, n_orders=15)
    rows = w.store.pending_outbox(10_000)
    assert sorted(r.event_id for r in rows) == sorted(e.event_id for e in w.store.all_events())


def test_failed_append_writes_neither_event_nor_outbox_row(tmp_path):
    w = World(tmp_path, n_orders=1, legacy=False)
    before = (w.store.event_count(), len(w.store.pending_outbox(1000)))
    stale = Event("dup", "ord-0001", 1, "OrderShipped", 1, {}, "t")
    with pytest.raises(ConcurrencyError):
        w.store.append("ord-0001", 0, [stale], ORDERS_TOPIC)
    assert (w.store.event_count(), len(w.store.pending_outbox(1000))) == before


def test_relay_crash_republishes_and_consumers_dedupe(tmp_path):
    w = World(tmp_path, n_orders=20)
    with pytest.raises(SimulatedCrash):
        OutboxRelay(w.store, Producer(w.log), batch_size=50).run_once(crash_after=30)
    OutboxRelay(w.store, Producer(w.log)).drain()
    on_log = sum(p.end_offset for p in w.log.topic(ORDERS_TOPIC).partitions)
    assert on_log == w.store.event_count() + 30
    assert w.store.pending_outbox(10) == []
    proj = Projection()
    stats = rebuild_from_log(w.log, ORDERS_TOPIC, proj)
    assert stats["duplicates"] == 30
    assert proj.state() == w.truth().state()


def test_relay_keeps_per_order_order_on_the_log(tmp_path):
    w = World(tmp_path, n_orders=25)
    w.publish()
    last = {}
    for part in w.log.topic(ORDERS_TOPIC).partitions:
        for rec in part.read(0, 10_000):
            e = Event.from_json(rec.value)
            assert e.version == last.get(e.aggregate_id, 0) + 1
            last[e.aggregate_id] = e.version
