# SE-3 — Low-Latency Pre-Auth Decision Gateway

**Status: ~95%.** Latency budget, race-free velocity counters (in-process
**and on Redis with an atomic Lua script**), tiered degradation, chaos drills,
HTTP service, per-feature freshness policy, Prometheus metrics, an open-loop
load generator with a soak, a **separate model process** serving two transports
with real CPU contention, and **ML-1's actual trained model wired in** --
**31 tests**.

```bash
python run_load.py            # budget table + 4 chaos drills
python run_soak.py            # open-loop load curve + soak
python run_transports.py      # HTTP vs binary framing, separate model process
python -m pytest tests -q     # 31 tests
uvicorn serve:app --port 8080
curl -s localhost:8080/metrics
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

## Stale-feature policy (`gateway/features.py`)

The question a global TTL cannot answer: a cached feature is 90 seconds old and
its TTL is 60. Do you use it?

"No" converts a cache miss into a model that cannot score. "Yes" uses a velocity
counter that cannot see the attack that started 90 seconds ago. So freshness is
**per feature**, and each policy carries its reason:

| feature | freshness | TTL | why |
|---|---|---|---|
| `velocity_24h` | HARD | 30s | cannot see an in-progress attack when stale |
| `device_history` | SOFT | 300s | changes slowly; stale is still informative |
| `card_tenure_days` | SOFT | 3600s | changes once a day at most |
| `merchant_risk` | STATIC | — | reference data, versioned not cached |

A stale SOFT feature still scores, at a **tightened threshold** (0.75 − 0.05 per
stale feature). A stale HARD feature is treated as **missing** and the request
falls back to rules — scoring on a two-minute-old velocity counter is worse than
knowing you do not have it. Chaos drill 3 exercises all three states live:

```
fresh features               model 100.0%
stale SOFT feature (tenure)  model  99.5%   <- still scores, discounted
stale HARD feature (velocity) degraded_*_no_features 98.4%
feature cache DOWN            degraded_*_no_features 94.0%
```

## Metrics (`/metrics`, `gateway/metrics.py`)

Prometheus text format, hand-written rather than pulled from `prometheus_client`
because the exposition format is twelve lines and the dependency would obscure
the point: **histograms, not averages.** 99 requests at 5ms and one at 2000ms
average under 25ms and blow a 100ms p99 SLO — a mean hides the tail by
construction. Bucket boundaries cluster around the budget (5–100ms) rather than
being log-spaced by habit, so the quantile estimate is precise where the SLO
lives. Per-stage histograms are exported alongside the end-to-end one, because an
SLO breach that does not say which stage moved is an alert nobody can act on.

## What is NOT built

1. **No Redis SERVER.** `gateway/redis_velocity.py` is a real implementation --
   sliding-window counters in a sorted set, the whole trim-add-count sequence in
   one atomic Lua script, TTL so idle keys do not leak -- and the tests execute
   that Lua under fakeredis, so the atomicity is exercised rather than asserted.
   What fakeredis cannot exercise is a real network round trip, cross-node
   behaviour, failover, or a partition. The gateway's hot path also still uses
   the in-process counter; swapping it is a constructor change, not a rewrite.
2. **gRPC itself.** `run_transports.py` runs a genuinely separate model PROCESS
   that burns real CPU, and compares pooled keep-alive HTTP against a
   length-prefixed binary framing on loopback. That isolates framing cost --
   binary is 2.07ms faster at p50, ~7% of the 30ms model budget -- but it is not
   gRPC: no protobuf, no HTTP/2 multiplexing, no streaming. Calling it gRPC
   would be the easy lie.

   The comparison also argues against my own transport, which is why it is worth
   running: at 32 concurrent callers the binary path timed out 95 times against
   HTTP's 14. It wins the microbenchmark and loses the failure mode, because
   `ThreadingHTTPServer` has had decades of backlog and connection handling
   beaten into it and a hand-rolled socket loop has not. That, not the 2ms, is
   the actual argument for gRPC.
3. **A 30-minute soak actually run.** `run_soak.py` implements it and defaults
   to a CI-sized 20s; `--soak-seconds 1800` is the spec's number and has not been
   run long enough to prove the absence of a leak. The short soak does show two
   real growth curves — unevicted velocity keys and an undrained audit buffer.
4. **No containers and no Grafana.** CI runs the tests and the chaos drills on
   every push, but `/metrics` is scraped by nothing and no dashboard or alert
   rule exists.
5. **A load curve that says anything about a REAL gateway.** `run_soak.py` is a
   proper open-loop generator -- Poisson arrivals dispatched on a schedule, a
   pre-spawned worker pool, latency clocked from enqueue so queueing delay
   counts -- and `offered` tracks `target` exactly, so the harness is not the
   bottleneck. But it finds no knee up to 800 RPS, and that is a fact about the
   *stub*: a sleeping model releases the GIL, so nothing ever contends. A real
   knee needs the real dependencies in items 1 and 2.
6. **The velocity-store-down posture is still uncomfortable and unresolved.**
   With no counter we cannot see a carding attack, and burst traffic is exactly
   the pattern that needs it. The current choice (fail open under $50) is made in
   the dark and should be revisited with a fraud team rather than defended.
7. **Audit log durability.** Async buffering is proven off the hot path, but a
   crash between buffer write and drain loses those records. The local-WAL answer
   is described in the drill output and not implemented.
