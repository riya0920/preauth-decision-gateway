# SE-3 — Low-Latency Pre-Auth Decision Gateway

**Status: ~97%.** Latency budget, race-free velocity counters (in-process
**and on Redis with an atomic Lua script**), tiered degradation, chaos drills,
HTTP service, per-feature freshness policy, Prometheus metrics **including
gauges**, an open-loop load generator, the **spec's full 30-minute soak actually
run**, a **separate model process** serving two transports with real CPU
contention, **ML-1's actual trained model wired in**, a **durable audit WAL in
the service path**, and **alert rules and a dashboard that cannot drift away
from the exporter** -- **57 tests**.

```bash
python run_load.py            # budget table + 4 chaos drills
python run_soak.py            # open-loop load curve + soak
python run_transports.py      # HTTP vs binary framing, separate model process
python run_soak.py --soak-seconds 1800   # the spec's 30-minute soak
python run_wal.py             # audit durability: fsync cost, and the crash
python run_redis_real.py      # the velocity counter on a REAL Redis server
python run_prometheus_drill.py   # load the rules into Prometheus and fire one
python -m pytest tests -q     # 57 tests
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

## The 30-minute soak, actually run

`python run_soak.py --soak-seconds 1800` — 180,053 requests over four segments:

```
   segment  requests       p50       p99
         1    44,670       2.2      26.8
         2    45,170       0.2      16.8
         3    44,998       0.2      13.7
         4    45,215       0.2      16.9
p99 drift first -> last segment : -37.1%
velocity keys  before -> after  : 0 -> 4,001
audit buffer   before -> after  : 0 -> 180,053  (never drained)
```

**Established:** p99 did not degrade — it drifted **down** 37%, which is warm-up
amortising over more samples rather than good news. And two growth curves are
real and unbounded: velocity keys (the in-process store trims *within* a window
and never evicts the key; the Redis version sets a TTL) and the audit buffer.

**Not proven:** the absence of a leak. Thirty minutes bounds the leak *rate*; it
does not bound the leak. A daily deploy cycle needs a soak measured in days, and
nothing here has run for one.

## Audit durability: the WAL

The drill established that async audit logging is nearly free on the hot path —
p99 32.12ms with the sink dead against 33.63ms steady. It also established, *in
words while the code did nothing about it*, what that buffering costs: a crash
between the buffer write and the drain loses those records.

`gateway/wal.py` appends each decision to a **local** log before handing it to
the async buffer. Local rather than remote, because a remote write puts a network
round trip and another service's availability inside the p99. Measured over 4,000
appends:

| fsync | mean | p99 | fsyncs |
|---|---|---|---|
| never | 0.035 ms | 0.118 ms | 0 |
| batch | 0.060 ms | 1.570 ms | 80 |
| always | 1.720 ms | 4.148 ms | 4,000 |

`never` and `batch` are genuinely free against a 100ms budget. **`always` is
not** — 4.1% of the whole budget spent on one fsync per decision, on an idle
laptop SSD with no competing write load. `never` survives a process crash,
`always` survives a machine crash, `batch` bounds the loss to a configurable
window, and that choice belongs to whoever owns the compliance requirement.

The crash, simulated:

| | plain AuditLog | with a WAL |
|---|---|---|
| decisions written | 4,500 | 4,500 |
| in the buffer when it died | 500 | 500 |
| recoverable after restart | 0 | **500** |
| **permanently lost** | **500** | **0** |

Those 500 are not a monitoring gap. Each is a decision the firm made about a
customer's money with no record that it made it, and an adverse-action request
against any of them has no answer.

The WAL is **wired into `serve.py`**, not offered as a library. A durability
mechanism that exists beside the thing making decisions protects nothing —
"available" and "enforced" are different claims. What it does *not* do is replace
shipping: a disk that dies takes it too. It bounds loss to what has not yet
shipped.

## Alert rules and a dashboard that cannot drift

`ops/alerts.yml` (10 rules) and `ops/dashboard.json` (7 panels). **Nothing
scrapes them** — there is no Prometheus and no Grafana here — which is exactly
why they need a test. A rule naming a metric nobody emits never fires, and an
alert that never fires looks identical to a system that is never unhealthy.

`tests/test_alert_rules.py` drives the real service, reads its real `/metrics`,
and asserts every metric named by every rule and every panel is actually
exported. **It found the drift immediately:** the first draft used `preauth_*`
names throughout and the exporter emits `gateway_*`. It also found three metrics
the rules needed and nothing emitted — audit buffer depth, unshipped WAL depth,
and velocity errors — which is why the registry now has **gauges** at all. A
counter cannot express a buffer that drains: it keeps climbing and says nothing
about the current depth, which is the only number an operator can act on.

The rules encode one principle: **page on symptoms the customer feels, ticket on
causes.** The model service being down is a *ticket* — the gateway degrades by
design, and waking someone for a dependency the design already survives is how a
rota stops reading its alerts. What *pages* is the approval rate moving 5 points
against the same time yesterday, whatever the cause turns out to be.

Two rules are worth calling out. `PreauthLatencyImprovedSuspiciously` fires
because a graph got **better**: killing the model took p99 from 33.63ms to
2.51ms, and a latency improvement with no deploy behind it means something
stopped happening. `PreauthNoTraffic` exists because a gateway with no traffic
and a gateway that is down look identical on every other panel — which is also
why the dashboard's **first** panel is request rate rather than latency, and a
test asserts that ordering.

## A real Redis server, and the bug fakeredis could not find

`run_redis_real.py` runs the velocity counter against **Redis 8.0.5**, 50 threads
× 200 increments on one hot key.

**The first run lost 2,400 of 10,000 increments** — and the *racy* control
counter lost only 200. That inverted the claim I had just written, which was that
the racy one would be "worse, and predictably so".

The cause was not the Lua. `incr_and_count` built its sorted-set member from
`self._seq += 1`, and `+=` on a Python int is a read-modify-write: LOAD, ADD,
STORE, with the interpreter free to switch between any two. Two threads produced
the **same** member, `ZADD` overwrote instead of adding, and the count came out
silently low.

**The critical section was atomic the entire time. The uniqueness the whole
scheme depends on was generated by racy client code outside it** — which is a
useful reminder that "the Lua is atomic" is a claim about the Lua and not about
the call site. Fixed with `itertools.count` (a single C-level `next()`), and the
counter is now exact at 10,000/10,000. `test_member_generation_is_thread_safe`
pins it.

### And the budget does not survive the network

```
latency (ms) : mean 26.97   p50 17.68   p95 55.74   p99 142.22
```

**The velocity stage budget is 20ms and the p99 is 142ms.** That is not Redis
being slow — p50 is 17.7ms. It is 50 threads sharing one connection pool and
queueing for a connection, so most of the tail is time spent waiting to be
allowed to talk to Redis at all. The in-process counter has no pool and therefore
no queue.

**A latency budget written against an in-process dependency does not survive that
dependency becoming a network service**, and the budget table earlier in this
README was written against the in-process one.

Third finding: pointed at a dead port, the counter **raises after ~2,000ms**
rather than returning a wrong count. Raising is correct — a counter that returned
0 would report every card as quiet at exactly the moment the system went blind.
But 2,000ms is longer than the entire 20ms budget, so **failing takes longer than
succeeding**, and the gateway needs a client timeout shorter than its own budget
rather than the library default.

## The alert rules, in a real Prometheus

`tests/test_alert_rules.py` asserts every metric the rules name is exported.
That catches drift and it does not answer the next question: **would these rules
ever fire?** A rule can name real metrics and still be unfirable — PromQL that
never evaluates true, a label that is not on the series, a `for` clause longer
than any real incident.

`run_prometheus_drill.py` runs the gateway's **own container**, points a real
Prometheus 3.5.0 at it, and asks Prometheus:

```
promtool check rules            SUCCESS: 10 rules found
container status                running, /health ok, model_up true
gateway_ metric lines exported  123
target preauth-gateway          up
sum(gateway_decisions_total)    200

rules loaded : 10
BROKEN PromQL: 0

   t+   0s   pending
   t+ 288s   firing
```

**All ten rules evaluate cleanly and `PreauthNoTraffic` fired.** Not merely valid
YAML, not merely valid PromQL — it evaluated true against real scraped series and
transitioned all the way to firing.

It is the rule chosen deliberately, because it can be caused **honestly**: stop
sending requests. Forcing a latency breach would mean rigging the gateway, and a
rule proven by a rigged input is proven against the rig. Its `for: 5m` sits on
top of a 5m rate window, so it took 288 seconds of real quiet — the drill waits
rather than pretending.

Running the container also settles a separate open item: **the image runs**, not
just builds. It answered `/health` with `model_up: true` and served 200 authorize
requests.

Three harness bugs this drill had to fix in itself, all worth naming because each
produced a *confident wrong reading*:

- **`health: unknown` is not `health: err`.** A rule reports `unknown` until it
  has been evaluated once. The first version counted that as broken and reported
  "unhealthy: 10" against ten perfectly good rules.
- **Querying before the first scrape lands** returns an empty result set, which
  reads as "the gateway exports nothing" rather than "ask again in a moment".
- **`setsid ... & disown` does not survive `wsl -- bash -lc`.** The whole WSL
  session goes away when the command returns, and Prometheus logged a polite
  "See you next time!" every single time. Kafka had the identical problem.
  systemd owns the process now.

## What is NOT built

1. **Alertmanager.** Rules fire and nothing routes, deduplicates, silences or
   pages. **A firing rule with nowhere to go is a red row on a page nobody has
   open** — which is most of the value of alerting, and it is not here.
2. **Grafana.** `ops/dashboard.json` is asserted against the real exporter by
   tests and has never been rendered by the thing that would render it.
3. **Redis on the gateway's hot path.** The counter is verified against a real
   server; `Gateway` still constructs the in-process one. Swapping it is a
   constructor change, and the Redis findings above say what swapping it costs.
4. **A connection pool sized to the workload**, and a client timeout shorter than
   the 20ms velocity budget. Both are one-liners with a sizing argument behind
   them that this has not made.
5. **gRPC itself.** `run_transports.py` runs a genuinely separate model PROCESS
   and compares pooled keep-alive HTTP against length-prefixed binary framing on
   loopback. Binary is 2.07ms faster at p50 — and at 32 concurrent callers it
   timed out **95 times against HTTP's 14**. It wins the microbenchmark and loses
   the failure mode, and that is the actual argument for gRPC.
6. **A load curve that says anything about a REAL gateway.** `run_soak.py` is a
   proper open-loop generator and `offered` tracks `target` exactly, so the
   harness is not the bottleneck. It finds no knee up to 800 RPS, and that is a
   fact about the *stub*: a sleeping model releases the GIL, so nothing contends.
7. **The velocity-store-down posture is still unresolved.** With no counter the
   gateway cannot see a carding attack, and burst traffic is exactly the pattern
   that needs it. Failing open under $50 is a decision made in the dark.
8. **A multi-day soak.** Thirty minutes bounds the leak rate and does not bound
   the leak; both growth curves it found are still growing.
9. **WAL shipping and truncation.** `recover()` returns what a restart must
   re-ship; nothing ships it and nothing truncates the WAL once records are
   acknowledged.
