import pytest

from esorders.consumer import Consumer
from esorders.groups import GroupCoordinator
from esorders.log import Log, OffsetOutOfRange, Producer, key_hash_partitioner


def test_offsets_are_dense_per_partition_and_survive_reopen(tmp_path):
    log = Log(tmp_path)
    log.create_topic("t", 3)
    for i in range(30):
        log.append("t", i % 3, f"k{i}", f"v{i}")
    reopened = Log(tmp_path)
    for p in range(3):
        recs = reopened.read("t", p, 0, 100)
        assert [r.offset for r in recs] == list(range(10))
        assert [r.value for r in recs] == [f"v{i}" for i in range(p, 30, 3)]
    assert reopened.append("t", 0, "k", "next") == 10


def test_segments_roll_and_reads_cross_segments(tmp_path):
    log = Log(tmp_path)
    log.create_topic("t", 1, segment_bytes=200)
    for i in range(50):
        log.append("t", 0, None, f"value-{i}")
    assert len(log.partition("t", 0).segments) > 5
    assert [r.value for r in log.read("t", 0, 7, 20)] == [f"value-{i}" for i in range(7, 27)]
    assert log.read("t", 0, 50, 10) == []


def test_torn_write_is_truncated_on_open(tmp_path):
    log = Log(tmp_path)
    log.create_topic("t", 1)
    log.append("t", 0, "k", "a")
    log.append("t", 0, "k", "b")
    seg = log.partition("t", 0).segments[-1].path
    with open(seg, "ab") as f:
        f.write(b'{"offset":2,"ts":1,"key":"k","val')  # crash mid-write
    reopened = Log(tmp_path)
    assert [r.value for r in reopened.read("t", 0, 0, 10)] == ["a", "b"]
    assert reopened.append("t", 0, "k", "c") == 2
    assert [r.value for r in Log(tmp_path).read("t", 0, 0, 10)] == ["a", "b", "c"]


def test_retention_deletes_old_closed_segments_only(tmp_path):
    log = Log(tmp_path)
    log.create_topic("t", 1, retention_ms=1000, segment_bytes=150)
    for i in range(20):
        log.append("t", 0, None, f"old-{i}", timestamp_ms=1_000)
    log.append("t", 0, None, "new", timestamp_ms=10_000)
    part = log.partition("t", 0)
    before = len(part.segments)
    deleted = log.enforce_retention(now=10_500)["t"]
    assert deleted == before - 1
    assert len(part.segments) == 1  # the active segment is never deleted
    assert 0 < part.start_offset <= 20 and part.end_offset == 21
    with pytest.raises(OffsetOutOfRange):
        log.read("t", 0, 0)
    assert Log(tmp_path).partition("t", 0).start_offset == part.start_offset


def test_consumer_resets_to_earliest_after_retention(tmp_path):
    log = Log(tmp_path)
    log.create_topic("t", 1, retention_ms=1000, segment_bytes=100)
    seen = []
    coordinator = GroupCoordinator(log)
    c = Consumer(log, coordinator, "g", ["t"], "m1", lambda r: seen.append(r.value))
    c.start()
    log.append("t", 0, None, "first", timestamp_ms=0)
    c.poll()
    for i in range(10):
        log.append("t", 0, None, f"expired-{i}", timestamp_ms=0)
    log.append("t", 0, None, "kept", timestamp_ms=5000)
    log.enforce_retention(now=5000)
    c.poll()
    assert c.stats.offset_resets == 1
    assert seen[0] == "first" and seen[-1] == "kept"
    assert "expired-0" not in seen


def test_same_key_always_goes_to_same_partition(tmp_path):
    log = Log(tmp_path)
    log.create_topic("t", 8)
    producer = Producer(log)
    placements = {}
    for i in range(200):
        key = f"ord-{i % 25}"
        p, _ = producer.send("t", key, "x")
        placements.setdefault(key, set()).add(p)
    assert all(len(ps) == 1 for ps in placements.values())
    assert len({next(iter(ps)) for ps in placements.values()}) > 4  # keys still spread out
    assert key_hash_partitioner("ord-1", 8) == key_hash_partitioner("ord-1", 8)


def test_create_topic_is_idempotent_but_checks_partitions(tmp_path):
    log = Log(tmp_path)
    log.create_topic("t", 2)
    assert len(log.create_topic("t", 2).partitions) == 2
    with pytest.raises(ValueError):
        log.create_topic("t", 3)
