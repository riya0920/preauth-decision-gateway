# Low-Latency Pre-Auth Decision Gateway

## What it is

A **pre-auth gateway** is the service that decides, while a card payment is
waiting, whether to approve it, decline it, or send it for review. It has about
**100 ms** (the p99 target: 99 of 100 requests must finish inside it) to parse
the request, run quick rules, count how often the card was used recently, look up
features, score a fraud model, and log the decision.

The hard part is not the happy path. It is what happens when a piece is slow or
down: the model dies, the counter store is unreachable, the audit sink stops
taking writes. The gateway must still answer every request, in a way a risk
owner chose in advance, and must never lose the record of what it decided.

We built that gateway in Python, measured each stage against its own time
budget, and broke its dependencies on purpose to see what it does.

## What we did

1. **Split the 100 ms into per-stage budgets** and measured every stage against
   its own share, so a slow request says which stage moved.
2. **Built race-free velocity counters** (how many times a card was used in the
   last N seconds), in-process and on Redis with an atomic Lua script, plus a
   deliberately racy control that must lose counts.
3. **Wrote a degradation policy by amount**: small payments fail open, middle
   ones fall back to rules, large ones go to review when the model is down.
4. **Added per-feature freshness rules**, so a stale feature is used, discounted,
   or treated as missing depending on what it is.
5. **Ran chaos drills**: killed the model, the counter store, and the audit sink
   under load.
6. **Added a local write-ahead log (WAL)** in the service path, then shipping to
   a sink and safe disk cleanup.
7. **Added Prometheus metrics, 10 alert rules, a 7-panel dashboard and
   Alertmanager routing**, and ran them in real Prometheus 3.5.0 and Grafana 11.3.1.
8. **Ran the 30-minute soak**, an open-loop load curve, a separate model process
   over three transports (HTTP, binary framing, gRPC), and the [Governed Fraud Detection System](https://github.com/riya0920/governed-fraud-detection)'s real trained
   model behind the gateway.

The work was done in several passes. Later passes re-tested earlier claims, and
several found real bugs.

## Results

**Latency budget** (3,200 requests, 8 worker threads, in-process, Windows laptop)

| stage | budget ms | p99 ms |
|---|---|---|
| parse and validate | 5 | 0.00 |
| hot rules | 15 | 0.00 |
| velocity | 20 | 0.04 |
| features | 20 | 2.39 |
| model | 30 | **30.16** (over by 0.16) |
| decide and log | 10 | 0.03 |
| **end to end** | **100** | **32.18** (SLO met) |

This run has no network hop, so it shows the budget is enforced per stage, not
a throughput claim. The model stage has no headroom, so it breaks first.

**Chaos drills**

- Model killed mid-load: 1,200 requests, **0 non-decisions**.
- Model and counter store both down: 800 requests, 0 non-decisions.
- Both restored: 99.8% back on the model.
- Audit sink killed: p99 32.12 ms vs 33.63 ms steady; 7,600 events buffered,
  **100% shipped** after recovery.
- When the model died, p99 **fell** from 33.63 ms to 2.51 ms. That is bad news:
  it means we stopped doing the expensive, accurate work.

**Velocity counters**

- 50 threads x 200 increments on one key: exactly **10,000**, in-process and on
  real Redis 8.0.5.
- Redis across the WSL network: p50 17.7 ms, p99 142 ms, over the 20 ms budget.
- Redis co-located on the same host: p50 **0.33 ms**, p99 **4.95 ms**, p999
  12.04 ms. The whole distribution fits, so the budget stands; the deployment
  is the constraint.

**Audit durability (WAL)**

| fsync mode | mean ms | p99 ms |
|---|---|---|
| never | 0.035 | 0.118 |
| batch | 0.060 | 1.570 |
| always | 1.720 | 4.148 |

- Simulated crash with 500 decisions in the buffer: **500 lost** without the
  WAL, **0 lost** with it.
- Shipping run: 4,800 decisions, sink down for 3 cycles. 4,799 reached the sink,
  1 was routed to a poison list, **none dropped**. Disk grew 53.8 to 161.2 KB
  while the sink was down, then went back to 0.

**Soak and monitoring**

- 30-minute soak: 180,053 requests, p99 drifted **down** 37.1% (warm-up, not a
  win). Two things grow without limit: velocity keys (0 to 4,001) and the
  in-memory audit buffer.
- Real Prometheus: all 10 rules load, **0 broken PromQL**, and
  `PreauthNoTraffic` went to firing after 288 s of real quiet.
- Real Grafana: all 7 panels import; 12 of 13 queries return data. The 13th
  compares to yesterday, and a fresh Prometheus has no yesterday.
- Transports at 32 callers: errors **gRPC 0, binary 80, HTTP 53**.

**Tests:** 107 tests. One transport test is timing-sensitive and failed once in a
full-suite run, then passed on its own.

**Bugs found by testing (and fixed)**

- The fake model used a busy-wait that held Python's GIL, so the velocity stage
  showed a 125 ms p99. It was measuring the harness. The fake now sleeps.
- On real Redis the counter lost 2,400 of 10,000 counts. The Lua was atomic, but
  the unique ID built with `self._seq += 1` in Python was not. Fixed with
  `itertools.count`.
- A Redis failure escaped the handler and returned a **500** instead of falling
  back to rules.
- Redis timeout: 15 ms timed out healthy calls, 250 ms returned 88 of 10,000.
  1.0 s, set from the measured tail, is exact.
- The "dead Redis" test port was actually open, so the test was hitting a live
  server. Moved to a port checked closed.
- Alert rules first used `preauth_*` metric names while the exporter emitted
  `gateway_*`, and three needed metrics did not exist (now gauges).
- Prometheus drill harness bugs: counted `unknown` rule health as broken, queried
  before the first scrape, and lost the process when the WSL session ended.

## Key decisions and why

**Give each stage its own budget, with no hidden headroom line.**
A shared reserve hides which stage is using it. Per-stage numbers tell you
where to look.

**Keep a racy counter in the repo as a control.**
A concurrency test that has never seen a wrong answer proves nothing. A lost
count makes the counter low, and burst traffic (a carding attack) is exactly
what triggers it.

**Degrade by amount, and keep the table outside the code.**
Under $50 a declined checkout costs more than the fraud risk; over $500 the
risk pays for a review. A fraud/risk owner sets these numbers, not engineering.

**Freshness is per feature, not one TTL.**
A stale velocity count cannot see an attack in progress, so it counts as
missing. Slow-changing features still score, at a tighter threshold.

**Write the WAL locally, inside the service.**
A remote write puts another service's availability inside the p99. A WAL that
sits beside the service, unused, protects nothing.

**Delete WAL segments only after the watermark is saved.**
A test runs the wrong order and measures the data it loses, rather than only
checking that the right order works.

**Set timeouts from the measured tail, not from the budget.**
A timeout under the dependency's own median turns every slow-but-fine call into
an outage.

**Page on what customers feel, ticket on causes.**
The model being down is a ticket, since the gateway degrades by design. An
approval-rate shift pages. Alertmanager groups one incident into one message.

**When the counter store is down, decide by exposure.**
Below a value ceiling, approve blind and mark it (`velocity_seen=False`); above,
decline. Always-open is blind during attacks; always-closed turns an outage into
a total outage.

**Metrics are histograms, not averages.**
99 requests at 5 ms and one at 2,000 ms average under 25 ms and still break the
SLO. Buckets cluster around the budget, where precision matters.

## Limits

- The budget table is in-process: no network hop to the model or counter store.
- The load curve shows no knee up to 800 RPS, but that is a fact about the stub
  model, not a real gateway.
- No multi-day soak. 30 minutes bounds the leak rate, not the leak, and both
  growth curves are still growing.
- gRPC uses JSON payloads, not protobuf, because `grpcio-tools` will not build
  on this Python.
- Redis on the hot path is opt-in (`GATEWAY_REDIS_URL`); co-location was
  measured on one machine, not in production.
- Everything ran on one Windows laptop plus WSL.

## How to run

```bash
pip install -r requirements.txt
python run_load.py                        # budget table + chaos drills
python run_soak.py                        # open-loop load curve + short soak
python run_soak.py --soak-seconds 1800    # the full 30-minute soak
python run_transports.py                  # HTTP vs binary vs gRPC, separate model process
python run_wal.py                         # fsync cost and the simulated crash
python run_shipping.py                    # WAL shipping and disk cleanup
python run_redis_real.py                  # velocity counter on a real Redis
python run_velocity_budget.py             # co-located Redis vs the 20 ms budget
python run_prometheus_drill.py            # load the rules into Prometheus, fire one
python run_grafana_drill.py               # render the dashboard in Grafana
python run_pairing.py                     # the Governed Fraud Detection System's real model behind the gateway
python -m pytest tests -q                 # 107 tests
uvicorn serve:app --port 8080             # the service; add GATEWAY_REDIS_URL=... for Redis
curl -s localhost:8080/metrics
```

Full write-ups: [VELOCITY_BUDGET](docs/VELOCITY_BUDGET.md) ·
[SHIPPING](docs/SHIPPING.md) · [GRAFANA](docs/GRAFANA.md)

## Layout

```
serve.py                   HTTP service (FastAPI); WAL wired in
model_server.py            the model as a separate process
gateway/pipeline.py        the stages, degradation tiers, audit log
gateway/budget.py          per-stage budgets and the budget table
gateway/velocity.py        in-process counters (safe and racy control)
gateway/redis_velocity.py  Redis counter with an atomic Lua script
gateway/velocity_policy.py what to do when the counter store is down
gateway/features.py        per-feature freshness rules
gateway/wal.py             local write-ahead log
gateway/shipping.py        WAL shipping and segment cleanup
gateway/metrics.py         Prometheus histograms, counters, gauges
gateway/real_model.py      loads the Governed Fraud Detection System's trained model
ops/                       alert rules, Alertmanager config, dashboard
```
