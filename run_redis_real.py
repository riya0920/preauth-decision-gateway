"""The velocity counter against a REAL Redis, not fakeredis.

    python run_redis_real.py --url redis://127.0.0.1:6379/0

fakeredis executes the same Lua, so the atomicity argument was already
exercised rather than asserted. What it could not exercise is everything
between the process and the server: a network round trip, a connection pool, a
server that can be slow, and a server that can go away mid-request. Those are
the properties this measures.

The comparison that matters is not fake-vs-real throughput -- one is a function
call and the other is a socket, so of course the socket loses. It is:

  1. does the Lua still behave atomically when the client is genuinely
     concurrent over a network, rather than concurrent inside one process?
  2. what does the network actually cost against the 20ms velocity budget?
  3. what happens when the server disappears mid-flight -- which is the failure
     the gateway's degradation policy is written for and which fakeredis
     cannot produce?
"""
from __future__ import annotations

import argparse
import statistics
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from gateway.redis_velocity import NaiveRedisVelocity, RedisVelocity, connect

WORKERS = 50
PER_WORKER = 200


def _pct(xs):
    xs = sorted(xs)
    def q(p):
        return xs[min(len(xs) - 1, int(len(xs) * p))]
    return statistics.fmean(xs), q(0.50), q(0.95), q(0.99), xs[-1]


def _hammer(counter, key, workers, per_worker, now_ms):
    lat, lock = [], threading.Lock()

    def work():
        local = []
        for _ in range(per_worker):
            t0 = time.perf_counter()
            counter.incr_and_count(key, now_ms)
            local.append((time.perf_counter() - t0) * 1000)
        with lock:
            lat.extend(local)

    threads = [threading.Thread(target=work) for _ in range(workers)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return lat, time.perf_counter() - t0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="redis://127.0.0.1:6379/0")
    ap.add_argument("--workers", type=int, default=WORKERS)
    ap.add_argument("--per-worker", type=int, default=PER_WORKER)
    args = ap.parse_args()

    expected = args.workers * args.per_worker
    now_ms = 1_800_000_000_000

    print("=" * 80)
    print("VELOCITY COUNTER ON A REAL REDIS")
    print("=" * 80)

    try:
        client = connect(args.url)
        info = client.info()
        client.flushdb()
    except Exception as exc:                                  # noqa: BLE001
        print("no Redis at {}: {}".format(args.url, exc))
        print("start one, or run the fakeredis tests instead:")
        print("   python -m pytest tests/test_redis_velocity.py -q")
        return 1

    print("server : Redis {} ({} mode)".format(
        info.get("redis_version"), info.get("redis_mode", "standalone")))
    print("load   : {} threads x {} increments on ONE hot key = {:,}".format(
        args.workers, args.per_worker, expected))

    # ------------------------------------------------ 1. atomicity, for real
    print("\n" + "=" * 80)
    print("1. ATOMICITY OVER A NETWORK")
    print("-" * 80)
    safe = RedisVelocity(client, window_ms=60_000, namespace="real")
    lat, elapsed = _hammer(safe, "card:hot", args.workers, args.per_worker, now_ms)
    final = safe.count("card:hot", now_ms)
    print("expected count : {:,}".format(expected))
    print("actual count   : {:,}".format(final))
    print("verdict        : {}".format(
        "EXACT" if final == expected else "LOST {:,}".format(expected - final)))
    print("throughput     : {:,.0f} ops/s over {:.2f}s".format(
        expected / elapsed if elapsed else 0, elapsed))

    mean, p50, p95, p99, mx = _pct(lat)
    print("\nlatency (ms)   : mean {:.3f}  p50 {:.3f}  p95 {:.3f}  p99 {:.3f}  max {:.3f}"
          .format(mean, p50, p95, p99, mx))
    print()
    if p99 > 20:
        print("THE VELOCITY STAGE BUDGET IS 20ms AND THIS p99 IS {:.0f}ms.".format(p99))
        print()
        print("That is a real result and it is not Redis being slow: p50 is")
        print("{:.1f}ms. It is {} threads sharing one connection pool and".format(
            p50, args.workers))
        print("queueing for a connection, so most of that tail is time spent")
        print("waiting to be allowed to talk to Redis at all. The in-process")
        print("counter has no pool and therefore no queue, which is exactly the")
        print("cost that swapping it in makes visible.")
        print()
        print("Worth stating rather than quietly fixing: a latency budget")
        print("written against an in-process dependency does not survive that")
        print("dependency becoming a network service. The budget table in the")
        print("README was written against the in-process counter, and this is")
        print("the number that shows what it was not accounting for.")
    else:
        print("p99 {:.3f}ms against a 20ms budget, on loopback to a VM on the".format(p99))
        print("same box. A real deployment adds a datacentre network and a")
        print("shared server; read this as a floor, not an estimate.")

    # -------------------------------------------------- 2. the racy control
    print("\n" + "=" * 80)
    print("2. THE RACY CONTROL, ALSO OVER A NETWORK")
    print("-" * 80)
    print("A concurrency test that has never seen a wrong answer proves nothing,")
    print("so the read-modify-write version stays in the repo as the control.")
    print("Under fakeredis it loses increments; the question is whether a real")
    print("network makes that better or worse.\n")
    naive = NaiveRedisVelocity(client, window_ms=60_000, namespace="naive-real")
    _hammer(naive, "card:hot", args.workers, args.per_worker, now_ms)
    naive_final = naive.count("card:hot", now_ms) if hasattr(naive, "count") else None
    if naive_final is None:
        naive_final = int(client.zcard("naive-real:card:hot"))
    lost = expected - naive_final
    print("expected count : {:,}".format(expected))
    print("actual count   : {:,}".format(naive_final))
    print("LOST           : {:,} ({:.1%})".format(lost, lost / expected))
    print()
    print("The network widens the window between the read and the write, so")
    print("there is more room for a peer to land inside it.")
    print()
    print("A CORRECTION THIS RUN FORCED. The first version of this script")
    print("asserted the racy counter would be `worse, and predictably so`. It")
    print("measured the opposite -- the racy one lost 2% while the SAFE one lost")
    print("24% -- because the safe counter had its own bug: `self._seq += 1` is")
    print("a read-modify-write on a Python int and is not atomic across threads,")
    print("so two events built the same sorted-set member and ZADD overwrote")
    print("rather than added. The Lua was atomic the entire time; the uniqueness")
    print("it depends on was generated by racy code outside it. Fixed with")
    print("itertools.count, and the number above is the post-fix one.")
    print()
    print("The failure mode is the one worth naming -- a lost increment")
    print("makes the counter merely LOW, never wrong-looking, and the traffic")
    print("that triggers it is parallel burst traffic. That is what a carding")
    print("attack looks like. The bug is aligned with the attack.")

    # ---------------------------------------------- 3. the server goes away
    print("\n" + "=" * 80)
    print("3. THE SERVER GOES AWAY MID-REQUEST")
    print("-" * 80)
    print("The failure the degradation policy is written for, and the one")
    print("fakeredis structurally cannot produce -- there is no socket to cut.\n")
    import redis as redis_pkg

    dead = connect("redis://127.0.0.1:6999/0")   # verified closed: 6399 turned out to be OPEN on this box, so the
    # original "dead port" check was quietly testing a live server
    dead_counter = RedisVelocity(dead, window_ms=60_000, namespace="dead")
    t0 = time.perf_counter()
    try:
        dead_counter.incr_and_count("card:hot", now_ms)
        print("unexpected: the call succeeded against a dead port")
    except (redis_pkg.exceptions.ConnectionError, OSError,
            RedisVelocity.Unavailable) as exc:
        dt = (time.perf_counter() - t0) * 1000
        print("raised {} after {:.1f}ms".format(type(exc).__name__, dt))
        print()
        print("It RAISES rather than returning a wrong count, which is the")
        print("correct behaviour and the reason the gateway can distinguish")
        print("'no attack' from 'cannot see'. A counter that returned 0 here")
        print("would report every card as quiet at exactly the moment the")
        print("system went blind.")
        print()
        if dt > 20:
            print("Note the {:.1f}ms: that is longer than the entire 20ms".format(dt))
            print("velocity budget. Failing takes longer than succeeding.")
            print()
            print("THIS PARAGRAPH USED TO SAY THE FIX IS A TIMEOUT SHORTER THAN")
            print("THE BUDGET. That is half right and the wrong half matters")
            print("more often -- see docs/VELOCITY_BUDGET.md, which measured it.")
            print()
            print("A refused connection is not bounded by socket_timeout at all.")
            print("redis-py 8.x defaults every connection to ten retries with")
            print("exponential backoff, and THAT is what takes seconds. Measured")
            print("against a closed port: 3,234ms at a 1.0s timeout and 4,061ms")
            print("at a 15ms one -- tightening the timeout made failure SLOWER,")
            print("because more of the ten attempts fit inside the same wall")
            print("clock while the backoff accumulated. With retries=0 the same")
            print("call fails in 0.4ms, which is the speed of the TCP reset.")
            print()
            print("The timeout does bound the OTHER failure: a server that")
            print("accepts and never replies. 1,003ms at the 1.0s default,")
            print("16ms at 15ms. Two failure modes, two controls, and this")
            print("project conflated them until it measured them apart.")

    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
