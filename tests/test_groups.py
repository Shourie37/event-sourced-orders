import itertools

import pytest
from helpers import World

from esorders.consumer import Consumer
from esorders.groups import GroupCoordinator, RebalanceRequired, TopicPartition, range_assign
from esorders.projections import Projection, projection_handler
from esorders.service import ORDERS_TOPIC


@pytest.mark.parametrize("members, partitions", list(itertools.product(range(1, 7), range(1, 9))))
def test_range_assign_gives_each_partition_to_exactly_one_member(members, partitions):
    tps = [TopicPartition("a", p) for p in range(partitions)] + [TopicPartition("b", p) for p in range(3)]
    out = range_assign([f"m{i}" for i in range(members)], tps)
    owned = [tp for tps_ in out.values() for tp in tps_]
    assert sorted(owned) == sorted(tps)
    sizes = [sum(1 for tp in v if tp.topic == "a") for v in out.values()]
    assert max(sizes) - min(sizes) <= 1


class Clock:
    def __init__(self):
        self.ms = 0

    def __call__(self):
        return self.ms


def test_join_leave_and_expiry_bump_generation(tmp_path):
    w = World(tmp_path, n_orders=1)
    clock = Clock()
    g = GroupCoordinator(w.log, session_timeout_ms=1000, clock=clock)
    assert g.join("grp", "a", [ORDERS_TOPIC]) == 1
    assert len(g.assignment("grp", "a")) == 4
    assert g.join("grp", "b", [ORDERS_TOPIC]) == 2
    assert len(g.assignment("grp", "a")) == 2 and len(g.assignment("grp", "b")) == 2
    clock.ms = 900
    g.heartbeat("grp", "b", 2)
    clock.ms = 1500
    assert g.expire_sessions("grp") == ["a"]
    assert g.generation("grp") == 3 and len(g.assignment("grp", "b")) == 4
    g.leave("grp", "b")
    assert g.members("grp") == [] and g.generation("grp") == 4


def test_stale_or_foreign_commits_are_rejected(tmp_path):
    w = World(tmp_path, n_orders=1)
    g = GroupCoordinator(w.log)
    gen_a = g.join("grp", "a", [ORDERS_TOPIC])
    tp0 = TopicPartition(ORDERS_TOPIC, 0)
    g.commit("grp", "a", gen_a, {tp0: 5})
    gen_b = g.join("grp", "b", [ORDERS_TOPIC])
    with pytest.raises(RebalanceRequired):
        g.commit("grp", "a", gen_a, {tp0: 9})  # a zombie from the old generation
    with pytest.raises(RebalanceRequired, match="does not own"):
        g.commit("grp", "b", gen_b, {tp0: 9})  # orders/0 now belongs to a
    assert g.committed("grp", tp0) == 5
    assert GroupCoordinator(w.log).committed("grp", tp0) == 5  # survives a restart


def test_graceful_rebalances_cause_no_redelivery(tmp_path):
    w = World(tmp_path, n_orders=40)
    w.publish()
    proj = Projection()
    g = GroupCoordinator(w.log)

    def member(name):
        c = Consumer(w.log, g, "grp", [ORDERS_TOPIC], name, projection_handler(proj), batch_size=7)
        c.start()
        return c

    a = member("a")
    a.poll()
    b = member("b")
    for c in (a, b):
        c.poll()
    assert set(a.assignment).isdisjoint(b.assignment)
    assert sorted(a.assignment + b.assignment) == [TopicPartition(ORDERS_TOPIC, p) for p in range(4)]
    c3 = member("c")
    a.stop()
    while not w.caught_up(g, "grp"):
        for c in (b, c3):
            c.poll()
    assert proj.duplicates_skipped == 0
    assert proj.state() == w.truth().state()


def test_evicted_member_rejoins_and_new_owner_resumes_from_commit(tmp_path):
    w = World(tmp_path, n_orders=30)
    w.publish()
    proj = Projection()
    clock = Clock()
    g = GroupCoordinator(w.log, session_timeout_ms=1000, clock=clock)
    a = Consumer(w.log, g, "grp", [ORDERS_TOPIC], "a", projection_handler(proj), batch_size=5)
    b = Consumer(w.log, g, "grp", [ORDERS_TOPIC], "b", projection_handler(proj), batch_size=5)
    a.start()
    b.start()
    a.poll()
    b.poll()
    clock.ms = 2000
    b.poll()  # b heartbeats; a has gone quiet
    assert g.expire_sessions("grp") == ["a"]
    b.poll()
    assert len(b.assignment) == 4
    before = a.stats.rebalances
    a.poll()  # a notices it was evicted and rejoins
    assert a.stats.rebalances == before + 1 and len(a.assignment) == 2
    while not w.caught_up(g, "grp"):
        a.poll()
        b.poll()
    assert proj.state() == w.truth().state()
