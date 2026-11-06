import pytest

from esorders.consumer import Consumer
from esorders.errors import PermanentError
from esorders.groups import GroupCoordinator, TopicPartition
from esorders.log import Log


@pytest.fixture
def log(tmp_path):
    log = Log(tmp_path)
    log.create_topic("in", 1)
    log.create_topic("dlq", 1)
    return log


def consumer(log, handler, sleeps, **kw):
    c = Consumer(log, GroupCoordinator(log), "g", ["in"], "m", handler, dlq_topic="dlq", sleep=sleeps.append, **kw)
    c.start()
    return c


def test_poison_message_goes_to_dlq_at_once_and_partition_keeps_moving(log):
    for v in ("ok-1", "poison", "ok-2"):
        log.append("in", 0, "k", v)
    handled, sleeps = [], []

    def handler(rec):
        if rec.value == "poison":
            raise PermanentError("cannot parse")
        handled.append(rec.value)

    c = consumer(log, handler, sleeps)
    c.poll()
    assert handled == ["ok-1", "ok-2"] and sleeps == []
    [dead] = log.read("dlq", 0, 0)
    assert dead.value == "poison"
    assert dead.headers["x-original-offset"] == "1"
    assert dead.headers["x-attempts"] == "1"
    assert dead.headers["x-error"] == "PermanentError: cannot parse"
    assert c.coordinator.committed("g", TopicPartition("in", 0)) == 3


def test_transient_error_is_retried_with_exponential_backoff(log):
    log.append("in", 0, "k", "v")
    calls, sleeps = [], []

    def handler(rec):
        calls.append(rec.offset)
        if len(calls) < 3:
            raise TimeoutError("db busy")

    c = consumer(log, handler, sleeps, backoff_base_s=0.05)
    c.poll()
    assert len(calls) == 3
    assert sleeps == [0.05, 0.1]
    assert c.stats.retries == 2 and c.stats.dead_lettered == 0
    assert log.read("dlq", 0, 0) == []


def test_message_that_always_fails_is_dead_lettered_after_max_attempts(log):
    log.append("in", 0, "k", "v")
    sleeps = []

    def handler(rec):
        raise RuntimeError("downstream is down")

    c = consumer(log, handler, sleeps, max_attempts=5, backoff_base_s=0.1, backoff_max_s=0.3)
    c.poll()
    assert sleeps == [0.1, 0.2, 0.3, 0.3]  # capped
    [dead] = log.read("dlq", 0, 0)
    assert dead.headers["x-attempts"] == "5"
    assert "downstream is down" in dead.headers["x-error"]


def test_without_a_dlq_the_error_surfaces_and_nothing_is_committed(log):
    log.append("in", 0, "k", "v")
    g = GroupCoordinator(log)
    c = Consumer(log, g, "g", ["in"], "m", lambda r: (_ for _ in ()).throw(PermanentError("bad")), sleep=lambda s: None)
    c.start()
    with pytest.raises(PermanentError):
        c.poll()
    assert g.committed("g", TopicPartition("in", 0)) is None
