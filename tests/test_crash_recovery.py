"""Property test: random crashes and membership changes never lose or double-apply an event."""

import random

import pytest
from helpers import World

from esorders.consumer import Consumer
from esorders.errors import SimulatedCrash
from esorders.groups import GroupCoordinator
from esorders.log import Producer
from esorders.outbox import OutboxRelay
from esorders.projections import Projection, projection_handler
from esorders.service import ORDERS_TOPIC

SEEDS = range(60)


def run_chaos(tmp_path, seed: int, projection: Projection) -> dict:
    rng = random.Random(seed)
    w = World(tmp_path, n_orders=rng.randint(8, 30), seed=seed)
    try:
        OutboxRelay(w.store, Producer(w.log), batch_size=rng.randint(5, 40)).run_once(crash_after=rng.randint(1, 10))
    except SimulatedCrash:
        pass
    w.publish()

    clock = [0]
    g = GroupCoordinator(w.log, session_timeout_ms=1000, clock=lambda: clock[0])
    handler = projection_handler(projection)
    members: dict[str, Consumer] = {}
    counter = iter(range(1000))
    events = {"crashes": 0, "joins": 0, "leaves": 0}

    def spawn() -> None:
        name = f"m{next(counter)}"
        c = Consumer(w.log, g, "grp", [ORDERS_TOPIC], name, handler, batch_size=rng.randint(1, 8))
        c.start()
        members[name] = c
        events["joins"] += 1

    spawn()
    spawn()
    for _ in range(20_000):
        if w.caught_up(g, "grp"):
            break
        roll = rng.random()
        name = rng.choice(sorted(members))
        if roll < 0.06 and len(members) < 5:
            spawn()
        elif roll < 0.10 and len(members) > 1:
            members.pop(name).stop()
            events["leaves"] += 1
        elif roll < 0.25:
            try:
                members[name].poll(crash_after=rng.randint(1, 6))
            except SimulatedCrash:
                del members[name]  # no leave(): the coordinator only notices by timeout
                events["crashes"] += 1
        else:
            members[name].poll()
        clock[0] += 100
        g.expire_sessions("grp")
        if not members:
            spawn()
    else:
        pytest.fail("consumers did not catch up")
    return {"world": w, **events}


@pytest.mark.parametrize("seed", SEEDS)
def test_crashes_and_rebalances_never_lose_or_duplicate(tmp_path, seed):
    proj = Projection(tmp_path / "proj.db")
    r = run_chaos(tmp_path, seed, proj)
    w = r["world"]
    assert proj.state() == w.truth().state()
    assert proj.processed_count() == w.store.event_count()
    assert proj.applied == w.store.event_count()


def test_chaos_actually_exercises_crashes():
    import tempfile
    from pathlib import Path

    totals = {"crashes": 0, "dups": 0}
    for seed in range(10):
        with tempfile.TemporaryDirectory() as tmp:
            proj = Projection()
            r = run_chaos(Path(tmp), seed, proj)
            totals["crashes"] += r["crashes"]
            totals["dups"] += proj.duplicates_skipped
    assert totals["crashes"] > 10
    assert totals["dups"] > 100


def test_without_idempotency_the_same_chaos_double_counts(tmp_path):
    naive = Projection(idempotent=False)
    r = run_chaos(tmp_path, 3, naive)
    truth = r["world"].truth()
    gross = lambda p: sum(row["gross_cents"] for row in p.revenue())  # noqa: E731
    assert gross(naive) > gross(truth)
