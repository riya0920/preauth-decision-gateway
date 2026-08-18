# SE-3 — Low-Latency Pre-Auth Decision Gateway

**Status: ~20% slice.** The latency budget, race-free velocity counters, the
tiered degradation policy, and the chaos drills are built. There is no HTTP
service, no Redis, no separate model process, and no metrics export.

```bash
python run_load.py
python -m pytest tests -q
```

## The latency budget, measured against allocation

```
stage                 budget       p50       p95       p99    headroom  status
------------------------------------------------------------------------------
parse_validate           5.0      0.00      0.00      0.00        5.00  ok
hot_rules               15.0      0.00      0.00      0.00       15.00  ok
velocity                20.0      0.01      0.02      0.04       19.96  ok
features                20.0      2.02      2.22      2.39       17.61  ok
model                   30.0     15.26     24.06     30.16       -0.16  OVER BUDGET
decide_log              10.0      0.00      0.01      0.03        9.97  ok
------------------------------------------------------------------------------
END TO END             100.0     17.26     26.13     32.18       67.82  SLO MET
```

3,200 requests, 8 in-process worker threads, Windows 11 laptop, CPython 3.14.

**What this is not:** there is no network hop to the model service, no Redis
round trip, no serialisation, and no separate process. Read it as proof that the
budget is *enforced and measured per stage*, not as a throughput claim. The model
stage sitting 0.16ms over its 30ms allocation while the SLO still passes is the
useful shape: it's the stage with no headroom, so it's the one that breaks first.

There is deliberately **no headroom line** in the budget. Reserving headroom
inside a p99 budget hides which stage is eating it; the headroom is the gap
between the p99 you promise and the p99 you measure.

## Race-free velocity counters

The 50-worker exactness proof (`tests/test_velocity.py`): 50 threads × 200
increments on one hot key must land on exactly 10,000.

The repo also ships `UnsafeCounter` — a read-modify-write with no lock — and a
test asserting it **loses** increments. A concurrency test that has never seen a
wrong answer proves nothing, so the racy implementation stays as the control.
This failure mode is worth naming precisely: a lost increment makes the counter
merely *low*, never wrong-looking, and the traffic that triggers it is parallel
burst traffic — which is exactly what a carding attack looks like. The bug is
aligned with the attack.

Sliding window, not fixed bucket: 10 transactions at 11:59:59 and 10 at 12:00:01
must trip a "20 per minute" rule, and there's a test for it.

## Degradation policy, with the arithmetic

| Amount | Model down | Why |
|---|---|---|
| < $50 | **fail open** — approve | Fraud loss on a $50 auth is ~$50; expected loss per approval at this tier is under a dollar, and a declined checkout costs more |
| $50–$500 | **rules only** | Blocklist and velocity still bite with no model; loss bounded, approval rate preserved |
| > $500 | **fail closed** — review | Expected fraud loss now exceeds the cost of a review, so the amount buys the review |

Engineering does not own these numbers — a fraud/risk policy owner does. That's
why they're a table the service reads, not constants in the decision function.

## Chaos drills

| Drill | Result |
|---|---|
| Model service killed mid-load | 1,200 requests, **0 non-decisions**; source flips to the three degradation tiers |
| Velocity store down too | 800 requests, 0 non-decisions |
| Both restored | 99.8% back on `model` (breaker half-opens, 2 probes on rules) |
| Audit sink killed | p99 32.12ms vs 33.63ms steady — unchanged; 7,600 events buffered, **100% shipped** after recovery |

**p99 dropped from 33.63ms to 2.51ms when the model died, and that is bad news.**
We were paying ~30ms for accuracy and stopped paying it. During this drill the
graph to watch is approval rate and fraud rate, not latency — a latency
improvement with no deploy behind it means something stopped happening.

On async audit logging: what it costs is a crash between the buffer write and the
drain. If compliance requires every decision durably logged *before* responding,
the answer is a local WAL append plus async shipping — not a synchronous write to
a remote sink, and not wishing the requirement away.

## A bug this build caught

The first version simulated the model call with a **busy-wait**, which holds the
GIL. Eight worker threads spinning starved each other, and the budget table
reported a **125ms p99 on the velocity stage** — a stage that is a mutex and a
deque append and cannot take 125ms. The table was measuring GIL contention, not
stage cost. A model call is I/O, so it now sleeps and releases the GIL; velocity
reads 0.04ms. Any load harness where the fake dependency burns CPU is measuring
the harness.

## What is NOT built (the other 80%)

1. **No service.** No FastAPI/gRPC, no HTTP contract, no OpenAPI, no containers.
   `Gateway.decide()` is a method call, so every latency number excludes
   serialisation and network entirely.
2. **No Redis.** Velocity counters and the feature cache are in-process dicts.
   The Lua/`MULTI-EXEC` atomicity argument is made in comments, not code, and the
   network round trip is missing from the budget.
3. **No separate model service**, therefore no gRPC-vs-HTTP comparison — the spec
   asks to measure both and keep the winner, and that isn't done.
4. **No metrics export** — no Prometheus histograms, no Grafana, no dashboard.
   The budget table is printed by the load script.
5. **No soak test.** 30-minute sustained-load drift/leak detection is absent;
   runs here are seconds long.
6. **Feature-cache-down path is implemented but not chaos-tested** in `run_load.py`
   (`feature_cache_up` is never flipped), so the confidence-discount branch has
   no drill behind it.
7. **No stale-feature policy** — TTLs are not modelled at all.
8. Real load-generation (multi-process, open-loop arrival, target RPS) rather
   than a closed-loop thread pool.
