# SE-3 — the velocity budget, revised from measurement

`gateway/redis_velocity.connect()` ended with an uncomfortable
conclusion: at a p50 of ~18ms and a max near 1.8s, **Redis does not
fit the 20ms velocity budget**, and adopting it means either rewriting
the budget or co-locating Redis so the hop is a loopback rather than a
cross-VM one. That was left as an open question. This answers it.

## 1. Co-location answers it

The same Lua script, same Redis, same window — run from a process on
the same host as the server instead of across the VM boundary.

| | cross-VM | co-located | ratio |
|---|---|---|---|
| p50 | 17.70ms | **0.33ms** | 54x |
| p99 | 142.00ms | **4.95ms** | 29x |
| max | 1800.00ms | **15.30ms** | 118x |

Full co-located distribution (recorded 2026-08-24, n=5,000):

| p50 | p90 | p99 | p999 | max |
|---|---|---|---|---|
| 0.33ms | 0.62ms | 4.95ms | 12.04ms | 15.30ms |

**The whole distribution, including the p999, fits inside the 20ms
allocation.** So the budget does not need rewriting for the healthy
path — the deployment does. That is a different remediation from the
one the earlier comment was heading toward, and a cheaper one.

## 2. A hypothesis this refuted

The expectation was that a velocity window under attack — which is
exactly when the sorted set is largest — would be slower to evaluate,
since `ZREMRANGEBYSCORE` is O(log N + M). It is not:

| members in the window | p50 |
|---|---|
| 410 | 0.28ms |
| 500 | 0.34ms |
| 1,400 | 0.29ms |
| 5,400 | 0.31ms |

A key holding 5,400 members measures the same as one holding 410. At
these sizes the sorted-set operations are dominated by scheduler noise,
and the tail belongs to the round trip rather than to Redis's work — a
bare `PING` measures p99 **6.31ms**, HIGHER than the script's
**4.95ms**. Tuning the script would move nothing.

## 3. What actually bounds a FAILURE, which is not the timeout

`run_redis_real.py` concluded that "the gateway needs a timeout
shorter than its budget rather than relying on the client's default".
**That advice is half right, and the half that is wrong is the half
that matters more often.**

| failure mode | socket_timeout | retry policy | time to fail |
|---|---|---|---|
| connection refused | 1.0s | default (10, exponential) | 3,233.6ms |
| connection refused | 0.015s | default (10, exponential) | 4,061.2ms |
| connection refused | 0.005s | default (10, exponential) | 3,587.4ms |
| connection refused | 1.0s | retries=0 | 0.4ms |
| connection refused | 0.015s | retries=0 | 0.4ms |
| raw TCP, no redis | 1.0s | n/a | 0.5ms |
| accepts, never replies | 1.0s | retries=0 | 1,003.5ms |
| accepts, never replies | 0.015s | retries=0 | 16.0ms |

Read the first three rows together: **tightening the timeout made
failure slower.** 15ms took 4,061ms where 1.0s took 3,234ms.

The mechanism is that `redis-py` 8.x defaults every connection to
`Retry(ExponentialWithJitterBackoff(), retries=10)` for
`ConnectionError`, and that policy — not the socket timeout — governs
how long a dead server takes to fail. A shorter timeout lets more of
the ten attempts complete inside the same wall clock while the backoff
accumulates. `retry_on_timeout=False` was already set in this
codebase in the belief that it disabled retrying. It does not; it is a
separate flag.

And the timeout is irrelevant to a refused connection anyway. The RST
arrives in half a millisecond — the raw TCP row proves it — so with
retries off the client fails at the speed of TCP, **0.4ms**.

### Two failure modes, two controls

| | bounded by | measured |
|---|---|---|
| **connection refused** — Redis is down | the retry policy. The socket timeout does nothing. | 3,234ms → 0.4ms with `retries=0` |
| **accepts but stalls** — GC pause, saturated box, network black hole | `socket_timeout`, and only it. | 1,003ms at 1.0s → 16ms at 15ms |

Conflating them is what the earlier advice did. Both settings are
needed and they defend against different things.

## 4. The revised budget

| stage | allocation |
|---|---|
| `parse_validate` | 5ms |
| `hot_rules` | 15ms |
| `velocity` | 20ms  ← measured p999 12.0ms co-located |
| `features` | 20ms |
| `model` | 30ms |
| `decide_log` | 10ms |
| **total** | **100ms** against a 100ms p99 SLO |

**The 20ms velocity allocation stands, and now has evidence behind it**
where before it was a declared number. What changes is the deployment
constraint attached to it:

- **Redis must be co-located.** Not a preference. Across the VM
  boundary the p50 alone consumed 88% of the allocation and the max
  exceeded the entire end-to-end SLO by 18x.
- **`retries=0` at the client.** Ten retries with exponential backoff
  behind a 20ms budget is a 3-second stall wearing a 20ms label.
- **`socket_timeout` stays above the healthy p99, not below the
  budget.** Setting it from the budget is what timed out healthy calls
  in an earlier attempt: a timeout below the dependency's own median
  turns every slow-but-fine request into an outage.

## 5. And a decision the raise did not make

`RedisVelocity` raises rather than returning zero, because a zero
"would report every card as quiet at exactly the moment the system
went blind". Right, and only half the problem — raising moves the
decision up a level without making it. Nothing above it answered
*Redis is down, this authorisation is in flight, do we approve it?*

`gateway/velocity_policy.py` answers it, and the answer is neither of
the two obvious ones:

- **Fail open always** leaves the system blind precisely when someone
  is hammering it — an overloaded velocity store is what a carding
  attack produces, so an attacker who can knock over Redis has turned
  the fraud control off.
- **Fail closed always** turns a dependency outage into a total
  outage. Nobody runs it, because declining every customer to stop the
  fraud you cannot see costs more than the fraud.

So the dial is **exposure**: below a value ceiling, approve without
velocity and mark the decision; above it, decline. The ceiling belongs
to whoever owns the fraud budget, which is why it is a constructor
argument rather than a constant in a branch.

Three things that matter more than the ceiling:

- **Every blind approval carries `velocity_seen=False`.** Without it
  the post-incident question — *which approvals went out blind?* — has
  no answer, and fraud arriving three days later cannot be attributed
  to the outage that caused it.
- **The failure path is timed too.** A stage that only measures its
  successes reports a healthy p99 through an outage, because the slow
  calls are the ones that raised and never reached the histogram.
- **A slow-but-successful call keeps its count.** It blew the budget
  and it is still correct; discarding it would throw away the one
  piece of fraud signal actually obtained. Counted separately so the
  breach is visible without being fatal.

## What is still not measured

- **No failover.** One Redis. A primary going away mid-request and a
  replica being promoted is the failure this budget most needs to
  survive, and a single node cannot produce it.
- **The co-located numbers are loopback on one box.** A real
  co-located deployment is a sidecar or a same-rack host, which adds a
  real NIC and a real switch. That is larger than loopback and much
  smaller than the cross-VM figures; the honest bracket is between the
  two columns in section 1, and this cannot narrow it further without
  two machines.
- **The ceiling is not calibrated.** It should be the amount at which
  expected fraud loss from being blind exceeds revenue lost by
  declining. That needs a fraud rate and a margin, which are the
  business's numbers and not the gateway's.