import random

import pytest
from helpers import World

from esorders.events import upcast
from esorders.log import RandomPartitioner, key_hash_partitioner
from esorders.ordering import run


@pytest.fixture(scope="module")
def events(tmp_path_factory):
    w = World(tmp_path_factory.mktemp("w"), n_orders=80)
    return [upcast(e) for e in w.store.all_events()]


@pytest.mark.parametrize("seed", range(10))
def test_key_partitioning_preserves_per_order_order(events, seed):
    r = run(events, key_hash_partitioner, 4, seed)
    assert (r.out_of_order_deliveries, r.wrong_final_status) == (0, 0)


def test_random_partitioning_breaks_ordering(events):
    r = run(events, RandomPartitioner(random.Random(1)), 4, seed=1)
    assert r.out_of_order_deliveries > 0
    assert r.wrong_final_status > 0


def test_one_partition_is_always_ordered(events):
    r = run(events, RandomPartitioner(random.Random(1)), 1, seed=1)
    assert r.out_of_order_deliveries == 0
