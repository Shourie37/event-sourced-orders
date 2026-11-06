"""A group consumer with at-least-once delivery, retries with backoff, and a dead-letter topic."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from .errors import PermanentError, SimulatedCrash
from .groups import GroupCoordinator, RebalanceRequired, TopicPartition
from .log import Log, OffsetOutOfRange, Producer, Record

Handler = Callable[[Record], None]


@dataclass
class ConsumerStats:
    delivered: int = 0
    handled: int = 0
    retries: int = 0
    dead_lettered: int = 0
    rebalances: int = 0
    offset_resets: int = 0


class Consumer:
    def __init__(
        self,
        log: Log,
        coordinator: GroupCoordinator,
        group: str,
        topics: list[str],
        member_id: str,
        handler: Handler,
        *,
        dlq_topic: str | None = None,
        max_attempts: int = 4,
        backoff_base_s: float = 0.05,
        backoff_max_s: float = 2.0,
        sleep: Callable[[float], None] = time.sleep,
        batch_size: int = 50,
    ) -> None:
        self.log = log
        self.coordinator = coordinator
        self.group = group
        self.topics = topics
        self.member_id = member_id
        self.handler = handler
        self.dlq_topic = dlq_topic
        self.max_attempts = max_attempts
        self.backoff_base_s = backoff_base_s
        self.backoff_max_s = backoff_max_s
        self.sleep = sleep
        self.batch_size = batch_size
        self.producer = Producer(log)
        self.stats = ConsumerStats()
        self.generation = -1
        self.positions: dict[TopicPartition, int] = {}

    def start(self) -> None:
        self.generation = self.coordinator.join(self.group, self.member_id, self.topics)
        self._load_assignment()

    def stop(self) -> None:
        self.coordinator.leave(self.group, self.member_id)

    @property
    def assignment(self) -> list[TopicPartition]:
        return sorted(self.positions)

    def _load_assignment(self) -> None:
        # Uncommitted progress is dropped. The new owner reads from the committed offset.
        self.positions = {}
        for tp in self.coordinator.assignment(self.group, self.member_id):
            committed = self.coordinator.committed(self.group, tp)
            self.positions[tp] = committed if committed is not None else self.log.partition(*tp).start_offset

    def _sync(self) -> None:
        try:
            self.coordinator.heartbeat(self.group, self.member_id, self.generation)
        except RebalanceRequired:
            if self.member_id in self.coordinator.members(self.group):
                self.generation = self.coordinator.generation(self.group)
            else:
                self.generation = self.coordinator.join(self.group, self.member_id, self.topics)
            self.stats.rebalances += 1
            self._load_assignment()

    def _read(self, tp: TopicPartition) -> list[Record]:
        try:
            return self.log.read(tp.topic, tp.partition, self.positions[tp], self.batch_size)
        except OffsetOutOfRange:
            # Retention removed the committed offset. Start again at the oldest record.
            self.stats.offset_resets += 1
            self.positions[tp] = self.log.partition(*tp).start_offset
            return self.log.read(tp.topic, tp.partition, self.positions[tp], self.batch_size)

    def poll(self, crash_after: int | None = None) -> int:
        """Handle one batch per owned partition, then commit. Returns the number of records handled."""
        self._sync()
        done: dict[TopicPartition, int] = {}
        count = 0
        for tp in self.assignment:
            for rec in self._read(tp):
                self.stats.delivered += 1
                self._handle(rec)
                done[tp] = rec.offset + 1
                count += 1
                if crash_after is not None and count >= crash_after:
                    raise SimulatedCrash(f"{self.member_id} crashed after {count} records, before commit")
        if done:
            try:
                self.coordinator.commit(self.group, self.member_id, self.generation, done)
            except RebalanceRequired:
                return count
            self.positions.update(done)
        return count

    def lag(self) -> int:
        return sum(self.log.partition(*tp).end_offset - pos for tp, pos in self.positions.items())

    def backoff(self, attempt: int) -> float:
        return min(self.backoff_max_s, self.backoff_base_s * 2 ** (attempt - 1))

    def _handle(self, rec: Record) -> None:
        attempt = 0
        while True:
            attempt += 1
            try:
                self.handler(rec)
                self.stats.handled += 1
                return
            except SimulatedCrash:
                raise
            except PermanentError as e:
                self._dead_letter(rec, e, attempt)
                return
            except Exception as e:  # noqa: BLE001 - any other failure is treated as transient
                if attempt >= self.max_attempts:
                    self._dead_letter(rec, e, attempt)
                    return
                self.stats.retries += 1
                self.sleep(self.backoff(attempt))

    def _dead_letter(self, rec: Record, error: Exception, attempts: int) -> None:
        if self.dlq_topic is None:
            raise error
        headers = {
            **rec.headers,
            "x-original-topic": rec.topic,
            "x-original-partition": str(rec.partition),
            "x-original-offset": str(rec.offset),
            "x-error": f"{type(error).__name__}: {error}",
            "x-attempts": str(attempts),
        }
        self.producer.send(self.dlq_topic, rec.key, rec.value, headers)
        self.stats.dead_lettered += 1
