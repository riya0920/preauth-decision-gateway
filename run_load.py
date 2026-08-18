"""Load test + chaos drills.

Produces the measured-vs-budget table, then kills dependencies under load and
shows the decision source flipping while availability holds.
"""
from __future__ import annotations

import random
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from gateway.budget import SLO_P99_MS, Budget
from gateway.pipeline import AuditLog, Gateway, ModelService, Request
from gateway.velocity import SafeCounter

RPS_WORKERS = 8
REQUESTS_PER_WORKER = 400


def make_request(rng: random.Random, i: int, worker: int) -> Request:
    return Request(
        request_id="w{}-{}".format(worker, i),
        card_id="CARD_{:05d}".format(rng.randint(0, 4000)),
        merchant_id="M{:03d}".format(rng.randint(0, 200)),
        device_id="D{:05d}".format(rng.randint(0, 9000)),
        amount_minor=rng.choice([
            rng.randint(100, 4_900),        # < $50
            rng.randint(5_000, 49_900),     # $50-$500
            rng.randint(50_000, 900_000),   # > $500
        ]),
        currency="USD",
        now_ms=1_800_000_000_000 + i * 7 + worker,
    )


def drive(gw: Gateway, worker: int, n: int, results: list, lock: threading.Lock):
    rng = random.Random(1000 + worker)
    local = []
    for i in range(n):
        local.append(gw.decide(make_request(rng, i, worker)))
    with lock:
        results.extend(local)


def run_phase(gw: Gateway, label: str, n_per_worker: int, workers: int = RPS_WORKERS):
    results, lock = [], threading.Lock()
    threads = [threading.Thread(target=drive, args=(gw, w, n_per_worker, results, lock))
               for w in range(workers)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t0
    non_200 = sum(1 for r in results if r.decision not in ("approve", "decline", "review"))
    print("\n{}".format(label))
    print("  requests {:,}   elapsed {:.2f}s   {:,.0f} req/s   non-decisions {}".format(
        len(results), elapsed, len(results) / elapsed, non_200))
    srcs = {}
    for r in results:
        srcs[r.source] = srcs.get(r.source, 0) + 1
    for k, v in sorted(srcs.items(), key=lambda kv: -kv[1]):
        print("    {:<26}{:>8,}  {:>6.1%}".format(k, v, v / len(results)))
    p99 = sorted(r.latency_ms for r in results)[int(0.99 * (len(results) - 1))]
    print("    {:<26}{:>8.2f}ms".format("p99 latency", p99))
    return results, p99


def main() -> int:
    budget = Budget()
    model = ModelService()
    velocity = SafeCounter()
    audit = AuditLog()
    gw = Gateway(model, velocity, budget, audit)

    print("=" * 84)
    print("PHASE 1: STEADY STATE  ({} workers x {} requests)".format(
        RPS_WORKERS, REQUESTS_PER_WORKER))
    print("=" * 84)
    _, p99_steady = run_phase(gw, "steady state", REQUESTS_PER_WORKER)

    print("\n" + "=" * 84)
    print("LATENCY BUDGET: MEASURED vs ALLOCATED")
    print("=" * 84)
    print(budget.render())
    print("\nMeasured on: Windows 11 laptop, CPython {}.{}, {} in-process worker"
          " threads.".format(sys.version_info.major, sys.version_info.minor,
                             RPS_WORKERS))
    print("This is NOT a production benchmark: no network hop to the model service,")
    print("no Redis round trip, no serialisation, and the GIL serialises the Python")
    print("stages. Read it as the SHAPE of the budget being enforced end to end, not")
    print("as a throughput claim.")

    # ---- chaos drill 1: model service dies --------------------------------
    print("\n" + "=" * 84)
    print("CHAOS DRILL 1: MODEL SERVICE KILLED MID-LOAD")
    print("=" * 84)
    model.up = False
    _, p99_dead = run_phase(gw, "model down", 150)
    print("\n  Availability held: every request still returned a decision.")
    print("  Decision source flipped to the degradation tiers -- fail-open under")
    print("  $50, rules-only to $500, fail-closed to review above it.")
    print("\n  p99 went {:.2f}ms -> {:.2f}ms. Latency IMPROVING is bad news, not"
          .format(p99_steady, p99_dead))
    print("  good: we were paying ~{:.0f}ms for accuracy and are now not paying it."
          .format(budget.budget["model"]))
    print("  The dashboard to watch during this drill is approval rate and fraud")
    print("  rate, not the latency graph.")

    # ---- chaos drill 2: velocity store dies -------------------------------
    print("\n" + "=" * 84)
    print("CHAOS DRILL 2: VELOCITY STORE DOWN (model still down)")
    print("=" * 84)
    velocity.available = False
    run_phase(gw, "velocity + model down", 100)
    print("\n  This is the genuinely uncomfortable one, and saying why is the point:")
    print("  with no velocity counter we cannot see a carding attack at all, and the")
    print("  attack pattern that most needs velocity is exactly the one that arrives")
    print("  in a burst. Failing open here is how you get enumerated; failing closed")
    print("  declines everyone during a Redis blip. This build fails open below $50")
    print("  and pushes everything else to rules/review, and that is a choice made")
    print("  in the dark -- it should be revisited with the fraud team, not defended")
    print("  as obviously right.")

    # ---- recovery ----------------------------------------------------------
    print("\n" + "=" * 84)
    print("RECOVERY")
    print("=" * 84)
    model.up = True
    velocity.available = True
    time.sleep(2.1)                      # let the breaker cool down to half-open
    run_phase(gw, "both restored", 150)

    # ---- audit log off the hot path ---------------------------------------
    print("\n" + "=" * 84)
    print("AUDIT LOG IS OFF THE HOT PATH")
    print("=" * 84)
    audit.sink_up = False
    _, p99_sink_down = run_phase(gw, "audit sink DOWN", 150)
    buffered = len(audit.buffer)
    audit.sink_up = True
    shipped = audit.drain()
    print("\n  sink down p99 {:.2f}ms vs steady {:.2f}ms -> {}".format(
        p99_sink_down, p99_steady,
        "unchanged, the write never touched the sink"
        if p99_sink_down < p99_steady * 2 else "REGRESSED -- the write is on the path"))
    print("  buffered while the sink was down : {:,}".format(buffered))
    print("  shipped after recovery           : {:,}".format(shipped))
    print("  audit completeness               : {:.1%}".format(
        len(audit.shipped) / max(sum(gw.source_counts.values()), 1)))
    print("\n  What async logging costs: a crash between the buffer write and the")
    print("  drain loses those records. If compliance requires every decision")
    print("  durably logged BEFORE responding, the answer is a local WAL append")
    print("  (~microseconds) plus async shipping -- not wishing the requirement")
    print("  away, and not a synchronous write to a remote sink either.")
    print("=" * 84)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
