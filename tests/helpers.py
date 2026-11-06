import random
from pathlib import Path

from esorders.events import upcast
from esorders.log import Log, Producer
from esorders.outbox import OutboxRelay
from esorders.projections import Projection
from esorders.service import ORDERS_TOPIC, OrderService
from esorders.store import EventStore
from esorders.workload import FakeClock, run_workload, seed_legacy_orders, seeded_ids

ITEMS = [{"sku": "lamp", "qty": 2, "unit_price_cents": 3500}]


class World:
    def __init__(self, root: Path, n_orders: int = 20, seed: int = 1, partitions: int = 4, legacy: bool = True) -> None:
        self.rng = random.Random(seed)
        self.clock = FakeClock()
        self.log = Log(root / "log")
        self.log.create_topic(ORDERS_TOPIC, partitions)
        self.log.create_topic("orders.dlq", 1)
        self.store = EventStore(root / "events.db")
        self.service = OrderService(self.store, clock=self.clock, new_id=seeded_ids(self.rng))
        if legacy:
            seed_legacy_orders(self.store, self.rng, self.clock())
        run_workload(self.service, self.rng, n_orders, self.clock)

    def publish(self) -> int:
        return OutboxRelay(self.store, Producer(self.log)).drain()

    def truth(self) -> Projection:
        p = Projection()
        for e in self.store.all_events():
            p.apply(upcast(e))
        return p

    def caught_up(self, coordinator, group: str) -> bool:
        from esorders.groups import TopicPartition

        return all(
            coordinator.committed(group, TopicPartition(ORDERS_TOPIC, p.number)) == p.end_offset
            for p in self.log.topic(ORDERS_TOPIC).partitions
        )
