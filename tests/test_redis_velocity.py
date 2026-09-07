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
    """A correct counter hands out 1..TOTAL exactly once. The naive
    read-modify-write hands the SAME number to several workers, because they all
    read the count before any of them writes back -- the lost update.

    The read->write interleave is forced with a barrier rather than left to the
    runner's thread scheduling. The earlier version asserted max(results) <
    true_total, which held only when the OS happened to interleave the threads;
    on a fast or serialised runner the race did not occur and this negative
    control silently passed when it should have failed. Forcing every worker to
    read before any writes makes the duplicate deterministic on any runner.
    """
    read_barrier = threading.Barrier(WORKERS, timeout=30)
    naive = NaiveRedisVelocity(client, window_ms=10_000_000,
                               on_race=read_barrier.wait)
    results = []
    lock = threading.Lock()

    def worker(start):
        start.wait()
        local = []
        for i in range(PER_WORKER):
            local.append(naive.incr_and_count("card:HOT", 1_000_000 + i))
        with lock:
            results.extend(local)

    start = threading.Barrier(WORKERS)
    threads = [threading.Thread(target=worker, args=(start,))
               for _ in range(WORKERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # A correct counter would return every value exactly once; repeated values
    # are the fingerprint of the lost update, and cannot appear without a race.
    assert len(results) == TOTAL
    assert len(set(results)) < TOTAL, (
        "every returned count was distinct -- the naive counter did not race, "
        "so this negative control proves nothing")


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


def test_member_generation_is_thread_safe():
    """The bug a real server found and fakeredis did not.

    `incr_and_count` built its sorted-set member from `self._seq += 1`, which is
    a read-modify-write on a plain Python int: LOAD, ADD, STORE, and the
    interpreter may switch between any two of them. Two threads then produced
    the SAME member, ZADD overwrote instead of adding, and the counter was
    silently low -- 7,600 of 10,000 on a real server at 50 threads.

    The Lua was atomic throughout. What was racy was the uniqueness the whole
    scheme depends on, generated by client code OUTSIDE the critical section.
    This asserts the generated members are distinct under contention, which is
    the property the Lua cannot supply for itself.
    """
    import threading

    from gateway.redis_velocity import RedisVelocity, connect

    counter = RedisVelocity(connect(fake=True), window_ms=60_000, namespace="tsm")
    seen, lock = [], threading.Lock()

    def work():
        local = ["{}-{}".format(1, next(counter._seq)) for _ in range(500)]
        with lock:
            seen.extend(local)

    threads = [threading.Thread(target=work) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(seen) == len(set(seen)), (
        "{} duplicate members generated under contention".format(
            len(seen) - len(set(seen))))
