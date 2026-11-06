from helpers import World

from esorders.consumer import Consumer
from esorders.groups import GroupCoordinator
from esorders.log import Producer
from esorders.projections import Projection, projection_handler, rebuild_from_log
from esorders.service import ORDERS_TOPIC


def test_replay_from_offset_zero_equals_live_projection(tmp_path):
    w = World(tmp_path, n_orders=60)
    w.publish()
    Producer(w.log).send(ORDERS_TOPIC, "ord-0001", "garbage")
    live = Projection(tmp_path / "live.db")
    g = GroupCoordinator(w.log)
    consumers = [Consumer(w.log, g, "grp", [ORDERS_TOPIC], n, projection_handler(live), dlq_topic="orders.dlq") for n in "ab"]
    for c in consumers:
        c.start()
    while not w.caught_up(g, "grp"):
        for c in consumers:
            c.poll()

    rebuilt = Projection(tmp_path / "rebuilt.db")
    stats = rebuild_from_log(w.log, ORDERS_TOPIC, rebuilt)
    assert stats["undecodable"] == 1
    assert rebuilt.state() == live.state() == w.truth().state()


def test_replay_is_deterministic_and_rerunnable(tmp_path):
    w = World(tmp_path, n_orders=30)
    w.publish()
    first, second = Projection(), Projection()
    rebuild_from_log(w.log, ORDERS_TOPIC, first)
    rebuild_from_log(w.log, ORDERS_TOPIC, second)
    assert first.dump() == second.dump()
    again = rebuild_from_log(w.log, ORDERS_TOPIC, first)  # same projection: everything is a duplicate
    assert again["applied"] == 0 and again["duplicates"] == w.store.event_count()


def test_revenue_uses_event_time_not_processing_time(tmp_path):
    w = World(tmp_path, n_orders=80)
    w.publish()
    proj = Projection()
    rebuild_from_log(w.log, ORDERS_TOPIC, proj)
    days = {r["day"] for r in proj.revenue()}
    assert len(days) >= 2
    paid = sum(e.data.get("amount_cents", e.data.get("amount", 0)) for e in w.store.all_events() if e.type == "PaymentCaptured")
    assert sum(r["gross_cents"] for r in proj.revenue()) == paid
