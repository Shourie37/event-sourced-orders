"""An append-only, partitioned log on disk. A small, single-process take on Kafka.

Layout on disk:

    <root>/<topic>/meta.json
    <root>/<topic>/<partition>/<base offset>.log   (one JSON record per line)
"""

from __future__ import annotations

import bisect
import json
import os
import random
import time
import zlib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

Partitioner = Callable[[str | None, int], int]


class OffsetOutOfRange(Exception):
    """The offset was removed by retention or is not written yet."""


class UnknownTopic(KeyError):
    pass


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    timestamp_ms: int
    key: str | None
    value: str
    headers: dict[str, str]


class Segment:
    """One file of a partition. Holds a byte-position index in memory."""

    def __init__(self, path: Path, base_offset: int) -> None:
        self.path = path
        self.base_offset = base_offset
        self.positions: list[int] = []
        self.size = 0
        self.max_timestamp_ms = 0
        if not path.exists():
            path.touch()
        self._recover()

    def _recover(self) -> None:
        # A crash can leave half a line at the end. Keep only whole, valid records.
        data = self.path.read_bytes()
        pos = 0
        while pos < len(data):
            end = data.find(b"\n", pos)
            if end == -1:
                break
            try:
                rec = json.loads(data[pos:end])
            except json.JSONDecodeError:
                break
            if rec.get("offset") != self.next_offset:
                break
            self.positions.append(pos)
            self.max_timestamp_ms = max(self.max_timestamp_ms, rec["ts"])
            pos = end + 1
        if pos != len(data):
            with open(self.path, "r+b") as f:
                f.truncate(pos)
        self.size = pos

    @property
    def next_offset(self) -> int:
        return self.base_offset + len(self.positions)

    def append(self, line: bytes, timestamp_ms: int, fsync: bool) -> None:
        with open(self.path, "ab") as f:
            f.write(line)
            f.flush()
            if fsync:
                os.fsync(f.fileno())
        self.positions.append(self.size)
        self.size += len(line)
        self.max_timestamp_ms = max(self.max_timestamp_ms, timestamp_ms)

    def read(self, offset: int, max_records: int) -> list[dict]:
        index = offset - self.base_offset
        if index >= len(self.positions):
            return []
        out = []
        with open(self.path, "rb") as f:
            f.seek(self.positions[index])
            for _ in range(min(max_records, len(self.positions) - index)):
                out.append(json.loads(f.readline()))
        return out


class Partition:
    def __init__(self, topic: str, number: int, directory: Path, segment_bytes: int, fsync: bool) -> None:
        self.topic = topic
        self.number = number
        self.directory = directory
        self.segment_bytes = segment_bytes
        self.fsync = fsync
        directory.mkdir(parents=True, exist_ok=True)
        files = sorted(directory.glob("*.log"), key=lambda p: int(p.stem))
        self.segments = [Segment(p, int(p.stem)) for p in files] or [self._new_segment(0)]

    def _new_segment(self, base_offset: int) -> Segment:
        return Segment(self.directory / f"{base_offset:020d}.log", base_offset)

    @property
    def start_offset(self) -> int:
        return self.segments[0].base_offset

    @property
    def end_offset(self) -> int:
        """The offset the next record gets."""
        return self.segments[-1].next_offset

    def append(self, key: str | None, value: str, headers: dict[str, str], timestamp_ms: int) -> int:
        active = self.segments[-1]
        if active.size >= self.segment_bytes and active.positions:
            active = self._new_segment(active.next_offset)
            self.segments.append(active)
        offset = active.next_offset
        rec = {"offset": offset, "ts": timestamp_ms, "key": key, "value": value, "headers": headers}
        line = (json.dumps(rec, separators=(",", ":")) + "\n").encode()
        active.append(line, timestamp_ms, self.fsync)
        return offset

    def read(self, offset: int, max_records: int) -> list[Record]:
        if offset < self.start_offset:
            raise OffsetOutOfRange(f"{self.topic}/{self.number}: offset {offset} < start {self.start_offset}")
        out: list[Record] = []
        bases = [s.base_offset for s in self.segments]
        i = bisect.bisect_right(bases, offset) - 1
        while i < len(self.segments) and len(out) < max_records:
            for rec in self.segments[i].read(offset, max_records - len(out)):
                out.append(Record(self.topic, self.number, rec["offset"], rec["ts"], rec["key"], rec["value"], rec["headers"]))
                offset = rec["offset"] + 1
            i += 1
        return out

    def enforce_retention(self, now: int, retention_ms: int) -> int:
        """Delete whole closed segments whose newest record is older than the limit."""
        deleted = 0
        while len(self.segments) > 1 and self.segments[0].max_timestamp_ms < now - retention_ms:
            self.segments.pop(0).path.unlink()
            deleted += 1
        return deleted


@dataclass
class Topic:
    name: str
    partitions: list[Partition]
    retention_ms: int | None


class Log:
    def __init__(self, root: str | Path, fsync: bool = False) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.fsync = fsync
        self._topics: dict[str, Topic] = {}
        for meta in self.root.glob("*/meta.json"):
            self._open(meta.parent.name, json.loads(meta.read_text()))

    def _open(self, name: str, meta: dict) -> Topic:
        parts = [
            Partition(name, n, self.root / name / str(n), meta["segment_bytes"], self.fsync) for n in range(meta["partitions"])
        ]
        topic = Topic(name, parts, meta["retention_ms"])
        self._topics[name] = topic
        return topic

    def create_topic(self, name: str, partitions: int, retention_ms: int | None = None, segment_bytes: int = 1 << 20) -> Topic:
        if name in self._topics:
            existing = self._topics[name]
            if len(existing.partitions) != partitions:
                raise ValueError(f"topic {name} exists with {len(existing.partitions)} partitions")
            return existing
        meta = {"partitions": partitions, "retention_ms": retention_ms, "segment_bytes": segment_bytes}
        (self.root / name).mkdir(parents=True, exist_ok=True)
        (self.root / name / "meta.json").write_text(json.dumps(meta))
        return self._open(name, meta)

    def topic(self, name: str) -> Topic:
        try:
            return self._topics[name]
        except KeyError:
            raise UnknownTopic(name) from None

    def partition(self, topic: str, number: int) -> Partition:
        return self.topic(topic).partitions[number]

    def append(
        self,
        topic: str,
        partition: int,
        key: str | None,
        value: str,
        headers: dict[str, str] | None = None,
        timestamp_ms: int | None = None,
    ) -> int:
        ts = now_ms() if timestamp_ms is None else timestamp_ms
        return self.partition(topic, partition).append(key, value, headers or {}, ts)

    def read(self, topic: str, partition: int, offset: int, max_records: int = 100) -> list[Record]:
        return self.partition(topic, partition).read(offset, max_records)

    def enforce_retention(self, now: int | None = None) -> dict[str, int]:
        now = now_ms() if now is None else now
        deleted = {}
        for topic in self._topics.values():
            if topic.retention_ms is not None:
                deleted[topic.name] = sum(p.enforce_retention(now, topic.retention_ms) for p in topic.partitions)
        return deleted


def key_hash_partitioner(key: str | None, partitions: int) -> int:
    # crc32 is stable across processes; Python's hash() is salted per process.
    return zlib.crc32((key or "").encode()) % partitions


class RandomPartitioner:
    """Ignores the key. Used to show what breaks without per-key ordering."""

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng

    def __call__(self, key: str | None, partitions: int) -> int:
        return self.rng.randrange(partitions)


class Producer:
    def __init__(self, log: Log, partitioner: Partitioner = key_hash_partitioner) -> None:
        self.log = log
        self.partitioner = partitioner

    def send(
        self,
        topic: str,
        key: str | None,
        value: str,
        headers: dict[str, str] | None = None,
        timestamp_ms: int | None = None,
    ) -> tuple[int, int]:
        partition = self.partitioner(key, len(self.log.topic(topic).partitions))
        return partition, self.log.append(topic, partition, key, value, headers, timestamp_ms)
