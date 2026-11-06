"""Command-line demo: python -m esorders demo"""

from __future__ import annotations

import argparse
import random
import sqlite3
import statistics
import tempfile
import time
from collections import Counter
from pathlib import Path

from .consumer import Consumer
from .domain import DomainError
from .errors import SimulatedCrash
from .events import Event, upcast
from .groups import GroupCoordinator
from .log import Log, Producer, RandomPartitioner, Record, key_hash_partitioner
from .ordering import run as run_ordering
from .outbox import OutboxRelay
from .projections import Projection, projection_handler, rebuild_from_log
from .service import ORDERS_TOPIC, OrderService
from .store import ConcurrencyError, EventStore
from .workload import FakeClock, run_workload, seed_legacy_orders, seeded_ids

DLQ_TOPIC = "orders.dlq"
PARTITIONS = 4


class Checks:
    def __init__(self) -> None:
        self.failed = 0

    def __call__(self, label: str, ok: bool) -> None:
        self.failed += not ok
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}")


def money(cents: int) -> str:
    return f"${cents / 100:,.2f}"


def section(title: str) -> None:
    print(f"\n== {title} ==")


class FlakyHandler:
    """Fails the first two tries for one event, like a locked database would."""

    def __init__(self, inner, event_id: str) -> None:
        self.inner, self.event_id, self.failures = inner, event_id, 0

    def __call__(self, rec: Record) -> None:
        if rec.headers.get("event_id") == self.event_id and self.failures < 2:
            self.failures += 1
            raise sqlite3.OperationalError("database is locked")
        self.inner(rec)


def consume_with_crash(
    log: Log, projection: Projection, group: str, crash_after: int, verbose: bool, flaky_event: str | None = None
):
    clock_ms = [0]
    coordinator = GroupCoordinator(log, session_timeout_ms=10_000, clock=lambda: clock_ms[0])
    handler = projection_handler(projection)
    if flaky_event:
        handler = FlakyHandler(handler, flaky_event)

    def make(member: str) -> Consumer:
        c = Consumer(log, coordinator, group, [ORDERS_TOPIC], member, handler, dlq_topic=DLQ_TOPIC, backoff_base_s=0.01)
        c.start()
        return c

    say = print if verbose else (lambda *_: None)
    a, b = make("consumer-a"), make("consumer-b")
    owns = lambda m: [str(tp) for tp in coordinator.assignment(group, m)]  # noqa: E731
    say(f"  consumer-a owns {owns('consumer-a')}, consumer-b owns {owns('consumer-b')}")
    a.poll()
    b.poll()
    try:
        a.poll(crash_after=crash_after)
    except SimulatedCrash as e:
        say(f"  CRASH: {e}")
    a_stats = a.stats
    clock_ms[0] += 11_000
    b.poll()
    evicted = coordinator.expire_sessions(group)
    say(f"  session timeout evicted {evicted}; rebalance to generation {coordinator.generation(group)}")
    b.poll()
    say(f"  consumer-b now owns {owns('consumer-b')}")
    a = make("consumer-a")
    say(f"  consumer-a restarted and rejoined: consumer-a owns {owns('consumer-a')}, consumer-b owns {owns('consumer-b')}")
    while a.lag() or b.lag():
        a.poll()
        b.poll()
    stats = [a_stats, a.stats, b.stats]
    return {
        "delivered": sum(s.delivered for s in stats),
        "retries": sum(s.retries for s in stats),
        "dead_lettered": sum(s.dead_lettered for s in stats),
    }


def demo(workdir: Path, n_orders: int, seed: int) -> int:
    check = Checks()
    rng = random.Random(seed)
    clock = FakeClock()
    log = Log(workdir / "log")
    log.create_topic(ORDERS_TOPIC, PARTITIONS)
    log.create_topic(DLQ_TOPIC, 1, retention_ms=7 * 24 * 3600 * 1000)
    store = EventStore(workdir / "events.db")
    service = OrderService(store, snapshot_every=50, clock=clock, new_id=seeded_ids(rng))
    producer = Producer(log)

    section("1. Write side: commands -> events + outbox (one SQLite transaction)")
    legacy = seed_legacy_orders(store, rng, clock())
    print(f"  seeded {len(legacy)} legacy orders written in schema v1 (dollar strings, no items)")
    commands = run_workload(service, rng, n_orders, clock)
    statuses = Counter(service.load(oid).status for oid in store.aggregate_ids())
    print(f"  ran {commands} commands for {n_orders} orders -> {store.event_count()} events")
    print(f"  final states: {dict(sorted(statuses.items()))}")
    unpaid = _new_order(service)
    cancelled = next((o for o in store.aggregate_ids() if service.load(o).status == "cancelled"), None)
    attempts = [
        ("ship an unpaid order", lambda: service.ship(unpaid, "ups", "TRK1")),
        ("pay the wrong amount", lambda: service.pay(unpaid, 1)),
    ]
    if cancelled:
        attempts.append(("refund a cancelled order", lambda: service.refund(cancelled, 100, "late refund")))
    for label, call in attempts:
        try:
            call()
            check(f"{label} is rejected", False)
        except DomainError as e:
            print(f"  rejected: {label} -> {type(e).__name__}: {e}")

    section("2. Optimistic concurrency")
    oid = _new_order(service)
    version = service.load(oid).version
    service.add_item(oid, "pen", 1, 499, expected_version=version)
    try:
        service.add_item(oid, "mug", 1, 1250, expected_version=version)
        check("second writer with a stale version is rejected", False)
    except ConcurrencyError as e:
        print(f"  clerk 2 saw v{version} but clerk 1 wrote first -> ConcurrencyError: {e}")
        check("second writer with a stale version is rejected", True)

    section("3. Outbox relay with a crash between publish and mark")
    relay = OutboxRelay(store, producer, batch_size=100)
    try:
        relay.run_once(crash_after=40)
    except SimulatedCrash as e:
        print(f"  CRASH: {e}; those 40 rows stay unmarked")
    relay = OutboxRelay(store, producer, batch_size=100)
    relay.drain()
    log_records = sum(p.end_offset for p in log.topic(ORDERS_TOPIC).partitions)
    dupes = log_records - store.event_count()
    print(f"  {store.event_count()} events in the store, {log_records} records on the log ({dupes} duplicates)")
    per_part = [p.end_offset for p in log.topic(ORDERS_TOPIC).partitions]
    print(f"  records per partition (key = order id, crc32 % {PARTITIONS}): {per_part}")
    producer.send(ORDERS_TOPIC, "ord-0007", "{this is not json")
    bad = Event("evt-bad", "ord-0007", 99, "OrderTeleported", 1, {}, clock().isoformat())
    producer.send(ORDERS_TOPIC, "ord-0007", bad.to_json(), {"event_id": bad.event_id})
    print("  injected 2 poison messages: malformed JSON and an unknown event type")

    section("4. Consumer group: crash mid-batch, eviction, rebalance, restart")
    live = Projection(workdir / "projection.db")
    flaky = next(store.all_events()).event_id
    stats = consume_with_crash(log, live, "projections", crash_after=37, verbose=True, flaky_event=flaky)
    print(
        f"  delivered {stats['delivered']}, applied {live.applied}, duplicates skipped {live.duplicates_skipped}, "
        f"retries {stats['retries']}, dead-lettered {stats['dead_lettered']}"
    )
    print(f"  event {flaky[:8]} failed twice with 'database is locked', then passed after backoff of 0.01 s and 0.02 s")
    check("transient failure retried twice, then applied", stats["retries"] == 2)
    for rec in log.read(DLQ_TOPIC, 0, 0, 10):
        print(
            f"  DLQ @{rec.offset}: from {rec.headers['x-original-topic']}/{rec.headers['x-original-partition']}"
            f"@{rec.headers['x-original-offset']} after {rec.headers['x-attempts']} attempt(s): {rec.headers['x-error'][:75]}"
        )

    section("5. Is the read model correct?")
    truth = Projection()
    for e in store.all_events():
        truth.apply(upcast(e))
    check(f"live projection equals a fold over the event store ({len(truth.summaries())} orders)", live.state() == truth.state())
    check(
        f"each event applied once ({live.processed_count()} processed = {store.event_count()} events)",
        live.processed_count() == store.event_count(),
    )
    check("2 poison messages in the dead-letter topic", stats["dead_lettered"] == 2)
    legacy_rows = [live.summaries()[o] for o in legacy]
    print(
        "  legacy v1 orders after upcasting: "
        + ", ".join(f"{r['order_id']} {money(r['total_cents'])} {r['status']}" for r in legacy_rows)
    )
    print("  daily revenue (USD):")
    for row in live.revenue():
        if row["currency"] == "USD":
            print(
                f"    {row['day']}  gross {money(row['gross_cents']):>11}  refunds {money(row['refunded_cents']):>10}"
                f"  net {money(row['net_cents']):>11}  payments {row['payments']}"
            )
    naive = Projection(idempotent=False)
    consume_with_crash(log, naive, "naive-projection", crash_after=37, verbose=False)
    net = lambda p: sum(r["net_cents"] for r in p.revenue())  # noqa: E731
    print(
        f"  same run without the processed-event table: net revenue {money(net(naive))} vs true {money(net(truth))}"
        f" (off by {money(net(naive) - net(truth))})"
    )
    check("naive consumer double counts; idempotent one does not", net(naive) != net(truth) and net(live) == net(truth))

    section("6. Replay: rebuild the projection from offset 0")
    rebuilt = Projection(workdir / "projection-rebuilt.db")
    t0 = time.perf_counter()
    rstats = rebuild_from_log(log, ORDERS_TOPIC, rebuilt)
    ms = (time.perf_counter() - t0) * 1000
    print(
        f"  replayed {rstats['records']} records in {ms:.0f} ms: applied {rstats['applied']}, "
        f"duplicates {rstats['duplicates']}, undecodable {rstats['undecodable']}"
    )
    check("rebuilt projection equals the live projection", rebuilt.state() == live.state())

    section("7. Ordering: key partitioning vs random partitioning")
    events = [upcast(e) for e in store.all_events()]
    keyed = run_ordering(events, key_hash_partitioner, PARTITIONS, seed)
    rand = run_ordering(events, RandomPartitioner(random.Random(seed)), PARTITIONS, seed)
    for name, r in [("key hash", keyed), ("random  ", rand)]:
        print(
            f"  {name}: {r.out_of_order_deliveries:4d} out-of-order deliveries, {r.orders_affected:3d} orders affected, "
            f"{r.wrong_final_status:3d} with the wrong final status (of {r.orders})"
        )
    check("key partitioning keeps every order's events in order", keyed.out_of_order_deliveries == 0)

    section("8. Snapshots")
    snap = snapshot_benchmark(workdir / "snapshots.db")
    print(f"  one order with {snap['events']} events, snapshot at v{snap['snapshot_version']}")
    print(f"  load without snapshot: {snap['full_events']} events read, median {snap['full_ms']:.2f} ms")
    print(
        f"  load with snapshot:    {snap['snap_events']} events read, median {snap['snap_ms']:.2f} ms "
        f"({snap['full_ms'] / snap['snap_ms']:.0f}x faster)"
    )
    check("snapshot load gives the same state", snap["same_state"])

    print(f"\n{'All checks passed.' if not check.failed else f'{check.failed} check(s) FAILED.'}")
    store.close()
    return 1 if check.failed else 0


def _new_order(service: OrderService) -> str:
    oid = f"tmp-{service.new_id()[:8]}"
    service.place_order(oid, "cust-001", [{"sku": "lamp", "qty": 1, "unit_price_cents": 3500}])
    return oid


def snapshot_benchmark(path: Path, items: int = 2000, tail: int = 37) -> dict:
    store = EventStore(path)
    service = OrderService(store, snapshot_every=0, clock=FakeClock())
    oid = "big-order"
    service.place_order(oid, "cust-001", [{"sku": "pen", "qty": 1, "unit_price_cents": 499}])
    at = service.clock().isoformat()
    bulk = [
        Event(service.new_id(), oid, v, "ItemAdded", 1, {"sku": "pen", "qty": 1, "unit_price_cents": 499}, at)
        for v in range(2, items + 2)
    ]
    store.append(oid, 1, bulk, ORDERS_TOPIC)
    order = service.load(oid)
    store.save_snapshot(oid, order.version, 1, order.to_snapshot())
    for _ in range(tail):
        service.add_item(oid, "mug", 1, 1250)

    def median_ms(use_snapshot: bool) -> tuple[float, int]:
        times = []
        for _ in range(15):
            t0 = time.perf_counter()
            service.load(oid, use_snapshot=use_snapshot)
            times.append((time.perf_counter() - t0) * 1000)
        return statistics.median(times), service.last_load_events_read

    full_ms, full_events = median_ms(False)
    snap_ms, snap_events = median_ms(True)
    same = service.load(oid, use_snapshot=False) == service.load(oid, use_snapshot=True)
    result = {
        "events": store.event_count(),
        "snapshot_version": order.version,
        "full_ms": full_ms,
        "full_events": full_events,
        "snap_ms": snap_ms,
        "snap_events": snap_events,
        "same_state": same,
    }
    store.close()
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="esorders", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("demo", help="run the end-to-end demo")
    d.add_argument("--orders", type=int, default=200)
    d.add_argument("--seed", type=int, default=7)
    d.add_argument("--dir", type=Path, help="keep the data here instead of a temp directory")
    args = parser.parse_args(argv)
    if args.dir:
        args.dir.mkdir(parents=True, exist_ok=True)
        return demo(args.dir, args.orders, args.seed)
    with tempfile.TemporaryDirectory() as tmp:
        return demo(Path(tmp), args.orders, args.seed)
