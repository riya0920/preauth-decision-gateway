"""The 50-worker exactness proof.

Both counters get hammered by 50 threads on ONE hot key. The safe one must land
on the exact expected count, every time. The unsafe one must not -- and if it
ever does, this test is not proving anything and should be treated as broken
rather than as good news.
"""
import sys
import threading
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gateway.velocity import SafeCounter, UnsafeCounter

WORKERS = 50
PER_WORKER = 200
TOTAL = WORKERS * PER_WORKER


def hammer(counter, key, now_ms, barrier=None):
    if barrier:
        barrier.wait()
    for i in range(PER_WORKER):
        counter.incr_and_count(key, now_ms + i)


def run_hammer(counter, key="card:HOT"):
    barrier = threading.Barrier(WORKERS)
    threads = [threading.Thread(target=hammer, args=(counter, key, 1_000_000, barrier))
               for _ in range(WORKERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return counter


def test_safe_counter_is_exact_under_50_workers():
    c = run_hammer(SafeCounter(window_ms=10_000_000))
    assert c.count("card:HOT", 1_000_000) == TOTAL, (
        "velocity counter lost increments under concurrency -- a carding attack "
        "running in parallel is exactly the traffic this rule would fail to see")


def test_unsafe_counter_demonstrably_loses_increments():
    """If this ever passes exactly, the exactness test above proves nothing."""
    c = run_hammer(UnsafeCounter(window_ms=10_000_000))
    got = len(c._data["card:HOT"])
    assert got < TOTAL, (
        "the racy counter returned the exact count ({}) -- the concurrency test "
        "is not actually exercising a race and cannot be trusted".format(got))


def test_sliding_window_beats_the_boundary_trick():
    """A fixed 60s bucket lets 10 transactions at 11:59:59 and 10 at 12:00:01
    pass a '20 per minute' rule. A sliding window sees 20."""
    c = SafeCounter(window_ms=60_000)
    base = 1_000_000
    for i in range(10):
        c.incr_and_count("k", base + i)
    for i in range(10):
        c.incr_and_count("k", base + 2_000 + i)
    assert c.count("k", base + 2_010) == 20


def test_window_actually_expires():
    c = SafeCounter(window_ms=1_000)
    c.incr_and_count("k", 1_000_000)
    assert c.count("k", 1_000_500) == 1
    assert c.count("k", 1_002_000) == 0
