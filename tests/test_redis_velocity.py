"""Redis velocity counters: atomicity, window semantics, failure modes."""
import sys
import threading
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytest.importorskip("fakeredis")
from gateway.redis_velocity import NaiveRedisVelocity, RedisVelocity, connect

WORKERS = 50
PER_WORKER = 100
TOTAL = WORKERS * PER_WORKER


@pytest.fixture
def client():
    c = connect(fake=True)
    c.flushall()
    return c


def hammer(counter, key, now_ms, barrier):
    barrier.wait()
    for i in range(PER_WORKER):
        counter.incr_and_count(key, now_ms + i)


def run_hammer(counter, key="card:HOT"):
    barrier = threading.Barrier(WORKERS)
    threads = [threading.Thread(target=hammer,
                                args=(counter, key, 1_000_000, barrier))
               for _ in range(WORKERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def test_lua_counter_is_exact_under_50_workers(client):
    """The whole trim-add-count sequence is one atomic Lua call, so no
    interleaving can lose an increment."""
    v = RedisVelocity(client, window_ms=10_000_000)
    run_hammer(v)
    assert v.count("card:HOT", 1_000_000) == TOTAL


def test_naive_read_modify_write_is_demonstrably_wrong(client):
    """If this ever returns the exact count, the test above proves nothing."""
    naive = NaiveRedisVelocity(client, window_ms=10_000_000)
    results = []

    def worker(barrier):
        barrier.wait()
        for i in range(PER_WORKER):
            results.append(naive.incr_and_count("card:HOT", 1_000_000 + i))

    barrier = threading.Barrier(WORKERS)
    threads = [threading.Thread(target=worker, args=(barrier,))
               for _ in range(WORKERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # The returned counts come from a stale read, so the maximum reported value
    # undershoots the true total that actually landed in the set.
    true_total = client.zcard("naive:card:HOT")
    assert max(results) < true_total, (
        "the racy counter reported the true total ({} vs {}) -- this test is "
        "not exercising a race and cannot be trusted".format(max(results), true_total))


def test_same_millisecond_events_do_not_collapse(client):
    """Two events at the same millisecond must count as two. A sorted-set member
    keyed only on the timestamp silently deduplicates them -- and it does so
    exactly when traffic is densest, which is when the counter matters most."""
    v = RedisVelocity(client, window_ms=60_000)
    for _ in range(5):
        v.incr_and_count("k", 1_000_000)
    assert v.count("k", 1_000_000) == 5


def test_sliding_window_beats_the_boundary_trick(client):
    """A fixed 60s bucket lets 10 at 11:59:59 and 10 at 12:00:01 pass a
    '20 per minute' rule. A sliding window sees 20."""
    v = RedisVelocity(client, window_ms=60_000)
    base = 1_000_000
    for i in range(10):
        v.incr_and_count("k", base + i)
    for i in range(10):
        v.incr_and_count("k", base + 2_000 + i)
    assert v.count("k", base + 2_010) == 20


def test_window_expires(client):
    v = RedisVelocity(client, window_ms=1_000)
    v.incr_and_count("k", 1_000_000)
    assert v.count("k", 1_000_500) == 1
    assert v.count("k", 1_002_000) == 0


def test_ttl_is_set_so_idle_keys_do_not_leak(client):
    """Without PEXPIRE, every card ever seen keeps a sorted set alive forever."""
    v = RedisVelocity(client, window_ms=60_000)
    v.incr_and_count("k", 1_000_000)
    assert client.pttl("vel:k") > 0


def test_unavailable_store_raises_rather_than_returning_zero(client):
    """Returning 0 would read as 'no velocity', which is the most dangerous
    possible answer: it looks like a quiet card."""
    v = RedisVelocity(client, window_ms=60_000)
    v.available = False
    with pytest.raises(RedisVelocity.Unavailable):
        v.incr_and_count("k", 1_000_000)
