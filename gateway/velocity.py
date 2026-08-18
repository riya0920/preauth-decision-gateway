"""Sliding-window velocity counters that are exact under concurrency.

The naive implementation -- read the counter, add one, write it back -- loses
increments whenever two workers interleave, and it loses them *silently*: the
counter is merely low, never wrong-looking. On a fraud gateway that means a
carding attack running 50 requests in parallel is exactly the traffic your
velocity rule fails to see. The failure mode is aligned with the attack.

Two implementations here, and the test hammers both:

  UnsafeCounter  read-modify-write, no lock. Present ON PURPOSE so the exactness
                 test has something to fail against. A concurrency test that has
                 never seen a wrong answer proves nothing.
  SafeCounter    single mutex around the whole read-modify-write. In Redis this
                 is the same idea expressed as a Lua script or a MULTI/EXEC
                 block: the point is that the window trim and the increment are
                 ONE atomic step, not two racing ones.

Sliding window, not fixed bucket: a fixed 60s bucket lets an attacker put 10
transactions at 11:59:59 and 10 at 12:00:01 and never trip a "20 per minute"
rule. Timestamps are kept and trimmed.
"""
from __future__ import annotations

import threading
from collections import defaultdict, deque


class UnsafeCounter:
    """Deliberately racy. Do not copy this into anything."""

    def __init__(self, window_ms: int = 60_000):
        self.window_ms = window_ms
        self._data: dict[str, deque] = defaultdict(deque)

    def incr_and_count(self, key: str, now_ms: int) -> int:
        d = self._data[key]
        # Read phase
        items = list(d)
        cutoff = now_ms - self.window_ms
        items = [t for t in items if t > cutoff]
        items.append(now_ms)
        # ... a context switch here loses the other worker's write entirely
        self._data[key] = deque(items)
        return len(items)


class SafeCounter:
    def __init__(self, window_ms: int = 60_000):
        self.window_ms = window_ms
        self._data: dict[str, deque] = defaultdict(deque)
        self._lock = threading.Lock()
        self.available = True          # flipped by chaos drills

    class Unavailable(Exception):
        pass

    def incr_and_count(self, key: str, now_ms: int) -> int:
        if not self.available:
            raise SafeCounter.Unavailable("velocity store is down")
        with self._lock:
            d = self._data[key]
            cutoff = now_ms - self.window_ms
            while d and d[0] <= cutoff:
                d.popleft()
            d.append(now_ms)
            return len(d)

    def count(self, key: str, now_ms: int) -> int:
        with self._lock:
            d = self._data[key]
            cutoff = now_ms - self.window_ms
            while d and d[0] <= cutoff:
                d.popleft()
            return len(d)
