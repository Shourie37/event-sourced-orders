"""Consumer groups: membership, partition assignment, and committed offsets.

Membership lives in memory, as in one broker process. Committed offsets are
written to disk so they survive restarts.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple

from .log import Log, now_ms


class TopicPartition(NamedTuple):
    topic: str
    partition: int

    def __str__(self) -> str:
        return f"{self.topic}/{self.partition}"


class RebalanceRequired(Exception):
    """The member's generation is stale. It must rejoin and reload its assignment."""


@dataclass
class _Group:
    topics: set[str] = field(default_factory=set)
    members: dict[str, int] = field(default_factory=dict)  # member id -> last heartbeat ms
    generation: int = 0
    assignment: dict[str, list[TopicPartition]] = field(default_factory=dict)


def range_assign(members: list[str], partitions: list[TopicPartition]) -> dict[str, list[TopicPartition]]:
    """Kafka's range assignor: per topic, give each member a contiguous block."""
    members = sorted(members)
    out: dict[str, list[TopicPartition]] = {m: [] for m in members}
    if not members:
        return out
    for topic in sorted({tp.topic for tp in partitions}):
        tps = sorted(tp for tp in partitions if tp.topic == topic)
        per, extra = divmod(len(tps), len(members))
        start = 0
        for i, m in enumerate(members):
            size = per + (1 if i < extra else 0)
            out[m].extend(tps[start : start + size])
            start += size
    return out


class GroupCoordinator:
    def __init__(self, log: Log, session_timeout_ms: int = 10_000, clock: Callable[[], int] = now_ms) -> None:
        self.log = log
        self.session_timeout_ms = session_timeout_ms
        self.clock = clock
        self._groups: dict[str, _Group] = {}
        self._dir = log.root / "__consumer_offsets"
        self._dir.mkdir(exist_ok=True)

    def _group(self, name: str) -> _Group:
        return self._groups.setdefault(name, _Group())

    def _rebalance(self, g: _Group) -> None:
        g.generation += 1
        tps = [TopicPartition(t, p) for t in g.topics for p in range(len(self.log.topic(t).partitions))]
        g.assignment = range_assign(list(g.members), tps)

    def join(self, group: str, member_id: str, topics: list[str]) -> int:
        g = self._group(group)
        g.topics.update(topics)
        g.members[member_id] = self.clock()
        self._rebalance(g)
        return g.generation

    def leave(self, group: str, member_id: str) -> None:
        g = self._group(group)
        if g.members.pop(member_id, None) is not None:
            self._rebalance(g)

    def expire_sessions(self, group: str) -> list[str]:
        """Evict members that stopped sending heartbeats, for example after a crash."""
        g = self._group(group)
        cutoff = self.clock() - self.session_timeout_ms
        dead = [m for m, seen in g.members.items() if seen < cutoff]
        for m in dead:
            del g.members[m]
        if dead:
            self._rebalance(g)
        return dead

    def _check(self, g: _Group, member_id: str, generation: int) -> None:
        if member_id not in g.members or generation != g.generation:
            raise RebalanceRequired(f"{member_id} has generation {generation}, group is at {g.generation}")

    def heartbeat(self, group: str, member_id: str, generation: int) -> None:
        g = self._group(group)
        self._check(g, member_id, generation)
        g.members[member_id] = self.clock()

    def generation(self, group: str) -> int:
        return self._group(group).generation

    def members(self, group: str) -> list[str]:
        return sorted(self._group(group).members)

    def assignment(self, group: str, member_id: str) -> list[TopicPartition]:
        return list(self._group(group).assignment.get(member_id, []))

    def _offsets_path(self, group: str) -> Path:
        return self._dir / f"{group}.json"

    def _read_offsets(self, group: str) -> dict[str, int]:
        path = self._offsets_path(group)
        return json.loads(path.read_text()) if path.exists() else {}

    def committed(self, group: str, tp: TopicPartition) -> int | None:
        return self._read_offsets(group).get(str(tp))

    def commit(self, group: str, member_id: str, generation: int, offsets: dict[TopicPartition, int]) -> None:
        """Store the next offset to read for each partition. Only the current owner may commit."""
        g = self._group(group)
        self._check(g, member_id, generation)
        owned = set(g.assignment.get(member_id, []))
        if not set(offsets) <= owned:
            raise RebalanceRequired(f"{member_id} does not own {sorted(map(str, set(offsets) - owned))}")
        stored = self._read_offsets(group)
        stored.update({str(tp): off for tp, off in offsets.items()})
        tmp = self._offsets_path(group).with_suffix(".tmp")
        tmp.write_text(json.dumps(stored, sort_keys=True))
        os.replace(tmp, self._offsets_path(group))
        g.members[member_id] = self.clock()
