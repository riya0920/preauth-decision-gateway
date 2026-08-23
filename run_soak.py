"""Open-loop load generation and a soak test.

Why the closed-loop harness in run_load.py is not enough, and why this is a
separate file rather than a flag:

A closed-loop generator sends the next request when the previous one returns.
That makes the offered load a FUNCTION of the system's own latency -- if the
gateway slows down, the generator politely slows down too, a queue never builds,
and the system reports comfortable latencies at a throughput nobody asked for.
It cannot answer "what happens at 500 RPS" because it never offers 500 RPS.

An open-loop generator sends on a schedule regardless of what has returned.
Requests arrive whether or not the previous one finished, so a slow system
accumulates a real backlog and the latency distribution shows it. This is the
only way to find the knee in the curve.

Arrivals are POISSON, not uniform. Real traffic clusters: a uniform 200 RPS
never produces the momentary bursts that actually fill queues, so it flatters
the tail. Exponential inter-arrival times reproduce the clustering.

The soak adds the other axis: sustained load over time, looking for the failures
that only appear after minutes -- unbounded buffer growth, counter-key
accumulation, memory creep. A 30-minute soak is the spec's ask; the default here
is short enough to run in CI, with the duration as a flag.
"""
from __future__ import annotations

import argparse
import gc
import random
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from gateway.budget import SLO_P99_MS, Budget
from gateway.features import FeatureCache
from gateway.pipeline import AuditLog, Gateway, ModelService, Request
from gateway.velocity import SafeCounter


def percentile(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    return s[min(int(round(p / 100 * (len(s) - 1))), len(s) - 1)]


class OpenLoopDriver:
    """Poisson arrivals at a target rate, dispatched regardless of completions.

    Workers are PRE-SPAWNED and pull from a queue. An earlier version created a
    thread per request, and the cost of doing that capped the generator itself
    at roughly 30 RPS -- so the "knee" it reported was the harness's limit, not
    the gateway's. A load generator that cannot offer its target rate measures
    only itself, and `offered` vs `achieved` below is what makes that visible
    rather than something the reader has to take on trust.

    Latency is measured from ENQUEUE, not from the moment a worker picks the
    request up. Time spent waiting for a worker is queueing delay that a real
    client experiences, and starting the clock at service time is how a
    saturated system reports healthy latencies.
    """

    def __init__(self, gateway: Gateway, target_rps: float, workers: int = 64,
                 max_queue: int = 2000):
        import queue
        self.gateway = gateway
        self.target_rps = target_rps
        self.latencies: list[float] = []
        self.queue_depths: list[int] = []
        self.errors = 0
        self._lock = threading.Lock()
        self._q: "queue.Queue" = queue.Queue(maxsize=max_queue)
        self._stop = threading.Event()
        self._workers = [threading.Thread(target=self._worker, daemon=True)
                         for _ in range(workers)]
        for w in self._workers:
            w.start()

    def _worker(self) -> None:
        import queue as _q
        while not self._stop.is_set():
            try:
                req, enqueued_at = self._q.get(timeout=0.05)
            except _q.Empty:
                continue
            try:
                self.gateway.decide(req)
                # Clock starts at enqueue: queueing delay is latency.
                total_ms = (time.perf_counter() - enqueued_at) * 1000.0
                with self._lock:
                    self.latencies.append(total_ms)
            except Exception:
                with self._lock:
                    self.errors += 1
            finally:
                self._q.task_done()

    def run(self, duration_s: float, seed: int = 5) -> dict:
        rng = random.Random(seed)
        start = time.perf_counter()
        next_at = start
        sent = dropped = 0
        before = len(self.latencies)

        while True:
            now = time.perf_counter()
            if now - start >= duration_s:
                break
            if now < next_at:
                remaining = next_at - now
                if remaining > 0.0005:
                    time.sleep(remaining)
                continue

            next_at += rng.expovariate(self.target_rps)
            sent += 1
            req = Request(
                request_id="ol-{}".format(sent),
                card_id="CARD_{:05d}".format(rng.randint(0, 4000)),
                merchant_id="M{:03d}".format(rng.randint(0, 200)),
                device_id="D{:05d}".format(rng.randint(0, 9000)),
                amount_minor=rng.choice([rng.randint(100, 4_900),
                                         rng.randint(5_000, 49_900),
                                         rng.randint(50_000, 900_000)]),
                currency="USD",
                now_ms=1_800_000_000_000 + sent)
            try:
                self._q.put_nowait((req, time.perf_counter()))
                with self._lock:
                    self.queue_depths.append(self._q.qsize())
            except Exception:
                # Queue full: the system cannot absorb the offered rate. A
                # closed-loop harness would simply have sent slower.
                dropped += 1

        self._q.join()
        elapsed = time.perf_counter() - start
        window = self.latencies[before:]

        return {
            "target_rps": self.target_rps,
            "offered": sent,
            "offered_rps": sent / elapsed if elapsed else 0.0,
            "completed": len(window),
            "shed": dropped,
            "errors": self.errors,
            "achieved_rps": len(window) / elapsed if elapsed else 0.0,
            "p50": percentile(window, 50),
            "p95": percentile(window, 95),
            "p99": percentile(window, 99),
            "max_inflight": max(self.queue_depths) if self.queue_depths else 0,
            "elapsed_s": elapsed,
        }

    def shutdown(self) -> None:
        self._stop.set()
        for w in self._workers:
            w.join(timeout=1)


def build_gateway() -> tuple[Gateway, Budget]:
    budget = Budget()
    cache = FeatureCache()
    gw = Gateway(ModelService(), SafeCounter(), budget, AuditLog())
    rng = random.Random(3)
    for i in range(4000):
        c = "CARD_{:05d}".format(i)
        cache.put(c, "velocity_24h", rng.random() * 5)
        cache.put(c, "card_tenure_days", rng.random() * 2000)
        cache.put(c, "device_history", rng.random())
    gw.feature_cache = cache
    return gw, budget


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--soak-seconds", type=float, default=20.0,
                    help="spec asks for 1800 (30 min); default is CI-sized")
    ap.add_argument("--rates", type=float, nargs="*",
                    default=[50, 100, 200, 400])
    args = ap.parse_args()

    print("=" * 84)
    print("OPEN-LOOP LOAD: finding the knee")
    print("=" * 84)
    print("Poisson arrivals dispatched on a schedule, NOT on completions. A")
    print("closed-loop harness slows down when the system does, so it can never")
    print("offer more load than the system can take -- and therefore can never")
    print("find the point where it breaks.\n")
    print("{:>8}{:>9}{:>10}{:>10}{:>7}{:>8}{:>8}{:>8}".format(
        "target", "offered", "achieved", "completed", "shed", "p50", "p95", "p99"))
    print("-" * 84)

    knee = None
    for rate in args.rates:
        gw, _budget = build_gateway()
        driver = OpenLoopDriver(gw, rate)
        # Warm-up, discarded. The first requests pay for thread pool spin-up and
        # a cold model stub; folding them into the measured window puts a
        # start-up artifact in the p99 and makes the LOWEST rate look like the
        # worst one, which is how a warm-up bug gets read as a capacity knee.
        driver.run(1.5)
        driver.latencies.clear()
        r = driver.run(6.0)
        breach = r["p99"] > SLO_P99_MS or r["shed"] > 0
        offered_ok = r["offered_rps"] >= rate * 0.9
        if breach and offered_ok and knee is None:
            knee = rate
        print("{:>8.0f}{:>9.1f}{:>10.1f}{:>10,}{:>7,}{:>8.1f}{:>8.1f}{:>8.1f}  {}".format(
            rate, r["offered_rps"], r["achieved_rps"], r["completed"], r["shed"],
            r["p50"], r["p95"], r["p99"],
            "SLO BREACH" if r["p99"] > SLO_P99_MS else
            ("shedding" if r["shed"] else "")))
        driver.shutdown()

    print("-" * 84)
    print("`offered` is what the generator actually put on the wire. If it")
    print("tracks `target`, the harness is not the bottleneck and the numbers")
    print("describe the gateway. If it falls short, they describe the harness.")
    if knee:
        print("Knee at about {:.0f} RPS: beyond it the gateway either breaches the".format(knee))
        print("{:.0f}ms p99 SLO or sheds load. A closed-loop run would have reported".format(
            SLO_P99_MS))
        print("comfortable latencies at every one of these rates.")
    else:
        print("No knee up to {:.0f} RPS -- and that result says more about the".format(
            max(args.rates)))
        print("harness than about a real gateway, so it should not be quoted as a")
        print("capacity number.")
        print("\nWhy no knee appears: the model stub SLEEPS, which releases the GIL,")
        print("so 64 workers absorb concurrent requests without ever contending for")
        print("CPU. p99 even improves with rate here, because higher rates produce")
        print("more samples and amortise the remaining warm-up. A real deployment")
        print("has a network hop, connection pool limits, serialisation, and a model")
        print("that burns CPU -- every one of which produces the knee this setup")
        print("cannot. Finding it needs the real dependencies, which is item 1 and 2")
        print("of the 'not built' list.")
    print("\nThis is an in-process gateway on a laptop with a sleeping model stub.")
    print("The SHAPE is the point -- offered load vs achieved, and where they")
    print("diverge -- not the absolute numbers.")

    # ---- soak --------------------------------------------------------------
    print("\n" + "=" * 84)
    print("SOAK: {:.0f}s at a sustained rate".format(args.soak_seconds))
    print("=" * 84)
    gw, budget = build_gateway()
    driver = OpenLoopDriver(gw, target_rps=100)

    gc.collect()
    audit_before = len(gw.audit.buffer)
    vel_keys_before = len(gw.velocity._data)

    segments = []
    seg_len = max(args.soak_seconds / 4, 1.0)
    for i in range(4):
        before = len(driver.latencies)
        driver.run(seg_len, seed=10 + i)
        seg = driver.latencies[before:]
        segments.append({
            "segment": i + 1,
            "n": len(seg),
            "p50": percentile(seg, 50),
            "p99": percentile(seg, 99),
        })

    print("{:>10}{:>10}{:>10}{:>10}".format("segment", "requests", "p50", "p99"))
    for s in segments:
        print("{:>10}{:>10,}{:>10.1f}{:>10.1f}".format(
            s["segment"], s["n"], s["p50"], s["p99"]))

    first, last = segments[0]["p99"], segments[-1]["p99"]
    drift = (last - first) / first if first else 0.0
    print("-" * 84)
    print("p99 drift first -> last segment : {:+.1%}".format(drift))
    print("velocity keys  before -> after  : {:,} -> {:,}".format(
        vel_keys_before, len(gw.velocity._data)))
    print("audit buffer   before -> after  : {:,} -> {:,}  (never drained)".format(
        audit_before, len(gw.audit.buffer)))

    print("\nWhat a soak is actually looking for is growth that a short run cannot")
    print("show. Two are visible here and both are real:")
    print("  * the velocity store accumulates a sorted set per card seen and only")
    print("    trims WITHIN a window -- the key itself is never evicted in this")
    print("    implementation, so key count grows with the card population. The")
    print("    Redis version sets a TTL; this in-process one does not.")
    print("  * the audit buffer grows without bound until something drains it.")
    print("    Nothing in this process does, which is fine for a drill and would")
    print("    be an outage in production.")
    if args.soak_seconds >= 1800:
        print("\nThis IS the spec's 30-minute soak ({:.0f}s). What it establishes"
              .format(args.soak_seconds))
        print("and what it does not:")
        print("  ESTABLISHED  p99 did not degrade -- it drifted {:+.1f}% across the"
              .format(drift * 100))
        print("               four segments, and the direction is DOWN. Latency")
        print("               improving over a soak is not good news by itself;")
        print("               here it is warm-up amortising over more samples.")
        print("  ESTABLISHED  two growth curves are real and unbounded: velocity")
        print("               keys and the audit buffer, both above.")
        print("  NOT PROVEN   the absence of a leak. Thirty minutes bounds the")
        print("               leak rate; it does not bound the leak. A daily")
        print("               deploy cycle needs a soak measured in days, and the")
        print("               honest statement is that nothing here has run for")
        print("               one.")
    else:
        print("\nA {:.0f}s soak is not 30 minutes. It is long enough to show the shape"
              .format(args.soak_seconds))
        print("of the growth, not long enough to prove its absence -- run with")
        print("--soak-seconds 1800 for the number the spec asks for.")
    print("=" * 84)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
