"""Velocity counters on Redis, with the atomicity done properly.

`velocity.py` proves the semantics with a mutex in one process. That is not what
production looks like: the counter lives in Redis, several gateway instances hit
the same key, and a process-local lock protects nothing at all across them.

The naive Redis version has exactly the same bug as the naive in-process one:

    n = redis.zcard(key)          # read
    redis.zadd(key, {member: ts}) # modify-write
    -> two round trips, and another instance interleaves between them

The fix is not a distributed lock (a round trip to acquire, a round trip to
release, and a correctness argument about what happens when the holder dies).
It is to make the whole read-trim-write-count sequence ONE operation. Redis runs
Lua scripts atomically: nothing else executes on that key while the script runs.

The script below does four things in one atomic step:
  1. trim the sliding window (ZREMRANGEBYSCORE)
  2. add this event (ZADD)
  3. count what remains (ZCARD)
  4. set a TTL so an idle key does not leak memory forever

Sorted sets rather than counters because the window has to SLIDE. A plain INCR
with EXPIRE gives a fixed bucket, and a fixed 60s bucket lets an attacker put 10
transactions at 11:59:59 and 10 at 12:00:01 without ever tripping a "20 per
minute" rule.

fakeredis is used in tests. It executes the same Lua, so the atomicity argument
is exercised rather than asserted -- but it is a single process, so it cannot
prove behaviour across a real cluster. That limitation is stated in the README
rather than glossed.
"""
from __future__ import annotations

# KEYS[1] = velocity key
# ARGV[1] = now (ms), ARGV[2] = window (ms), ARGV[3] = unique member id
SLIDING_WINDOW_LUA = """
local now    = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local member = ARGV[3]

redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
redis.call('ZADD', KEYS[1], now, member)
local count = redis.call('ZCARD', KEYS[1])
redis.call('PEXPIRE', KEYS[1], window)
return count
"""

# Read-only variant: counts without recording. Used by rules that must not
# themselves inflate the counter they are testing.
COUNT_ONLY_LUA = """
local now    = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
return redis.call('ZCARD', KEYS[1])
"""


class RedisVelocity:
    """Sliding-window velocity counter backed by Redis."""

    class Unavailable(Exception):
        pass

    def __init__(self, client, window_ms: int = 60_000, namespace: str = "vel"):
        self.client = client
        self.window_ms = window_ms
        self.namespace = namespace
        self.available = True
        self._incr = client.register_script(SLIDING_WINDOW_LUA)
        self._count = client.register_script(COUNT_ONLY_LUA)
        self._seq = 0

    def _key(self, key: str) -> str:
        return "{}:{}".format(self.namespace, key)

    def incr_and_count(self, key: str, now_ms: int) -> int:
        if not self.available:
            raise RedisVelocity.Unavailable("velocity store is down")
        # The member must be unique per event, or two events at the same
        # millisecond collapse into one sorted-set entry and the counter
        # undercounts exactly when traffic is densest.
        self._seq += 1
        member = "{}-{}".format(now_ms, self._seq)
        try:
            return int(self._incr(keys=[self._key(key)],
                                  args=[now_ms, self.window_ms, member]))
        except Exception as exc:
            raise RedisVelocity.Unavailable(str(exc)) from exc

    def count(self, key: str, now_ms: int) -> int:
        if not self.available:
            raise RedisVelocity.Unavailable("velocity store is down")
        return int(self._count(keys=[self._key(key)],
                               args=[now_ms, self.window_ms]))


class NaiveRedisVelocity:
    """Deliberately racy: read, then modify-write, in two round trips.

    Kept so the exactness test has something to fail against. A concurrency test
    that has never produced a wrong answer is not evidence of anything.
    """

    def __init__(self, client, window_ms: int = 60_000, namespace: str = "naive"):
        self.client = client
        self.window_ms = window_ms
        self.namespace = namespace
        self._seq = 0

    def incr_and_count(self, key: str, now_ms: int) -> int:
        k = "{}:{}".format(self.namespace, key)
        self.client.zremrangebyscore(k, "-inf", now_ms - self.window_ms)
        current = self.client.zcard(k)              # READ
        self._seq += 1
        # ... another instance can interleave here ...
        self.client.zadd(k, {"{}-{}".format(now_ms, self._seq): now_ms})
        return current + 1                          # stale read + 1


def connect(url: str | None = None, fake: bool = False):
    """Real Redis when a URL is given, fakeredis otherwise.

    fakeredis executes the same Lua, so the script's atomicity is genuinely
    exercised. What it cannot exercise is cross-node behaviour, failover, or
    network partitions -- for those the answer is a real cluster, and this repo
    does not have one.
    """
    if fake or url is None:
        import fakeredis
        return fakeredis.FakeStrictRedis(decode_responses=True)
    import redis
    return redis.Redis.from_url(url, decode_responses=True)
