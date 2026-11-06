"""Shows why events are keyed by order ID.

Partitions are read by different consumers at different speeds. Within one
partition the order is fixed; across partitions it is not. If one order's
events land in several partitions, a reader can see "shipped" before "paid".
"""

from __future__ import annotations

import random
import tempfile
from dataclasses import dataclass

from .events import Event
from .log import Log, Partitioner, Producer


@dataclass(frozen=True)
class OrderingResult:
    orders: int
    out_of_order_deliveries: int
    orders_affected: int
    wrong_final_status: int


def run(events: list[Event], partitioner: Partitioner, partitions: int, seed: int) -> OrderingResult:
    rng = random.Random(seed)
    truth: dict[str, tuple[int, str]] = {}
    for e in events:
        truth[e.aggregate_id] = max(truth.get(e.aggregate_id, (0, "")), (e.version, e.type))

    with tempfile.TemporaryDirectory() as tmp:
        log = Log(tmp)
        log.create_topic("orders", partitions)
        producer = Producer(log, partitioner)
        for e in events:
            producer.send("orders", e.aggregate_id, e.to_json())

        # Each step, one partition's consumer reads a small batch. This models consumers that run at different speeds.
        positions = dict.fromkeys(range(partitions), 0)
        last_seen: dict[str, int] = {}
        status: dict[str, str] = {}
        late, affected = 0, set()
        while any(positions[p] < log.partition("orders", p).end_offset for p in positions):
            p = rng.choice([p for p in positions if positions[p] < log.partition("orders", p).end_offset])
            for rec in log.read("orders", p, positions[p], rng.randint(1, 5)):
                e = Event.from_json(rec.value)
                if e.version < last_seen.get(e.aggregate_id, 0):
                    late += 1
                    affected.add(e.aggregate_id)
                last_seen[e.aggregate_id] = max(last_seen.get(e.aggregate_id, 0), e.version)
                status[e.aggregate_id] = e.type  # last write wins, as a naive reader would do
                positions[p] = rec.offset + 1

    wrong = sum(1 for oid, (_, t) in truth.items() if status[oid] != t)
    return OrderingResult(len(truth), late, len(affected), wrong)
