"""Revising the velocity budget from measurement, and what bounds a failure.

    python run_velocity_budget.py            # writes the doc from recorded runs
    python run_velocity_budget.py --measure  # re-measures against a live Redis

`gateway/redis_velocity.connect()` ended with an uncomfortable conclusion: at a
p50 of ~18ms and a max near 1.8s over the WSL boundary, Redis does not fit the
20ms velocity budget, and adopting it means either rewriting the budget or
CO-LOCATING Redis so the hop is a loopback. That was the open question and this
answers it with numbers.

The measurements below were taken by running the same Lua script from a process
on the same host as Redis, rather than across the VM boundary. Re-run them with
`--measure` against a reachable Redis; without it the recorded figures are
printed and labelled as recorded.

Writes docs/VELOCITY_BUDGET.md.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from gateway.budget import BUDGET_MS, SLO_P99_MS

# Measured 2026-08-24, Redis 8.0.5, 5,000 operations of the velocity Lua script
# from a process CO-LOCATED with the server. Recorded here rather than
# recomputed on import, so the document is reproducible without a live Redis --
# and labelled as recorded so nobody reads it as a fresh run.
COLOCATED = {"p50": 0.330, "p90": 0.616, "p99": 4.947, "p999": 12.037,
             "max": 15.298, "n": 5000}
PING = {"p50": 0.196, "p99": 6.307, "max": 27.346}
CROSS_VM = {"p50": 17.7, "p99": 142.0, "max": 1800.0}      # from run_redis_real
BY_WINDOW = {410: 0.281, 500: 0.343, 1400: 0.291, 5400: 0.309}
FAILURE = [
    ("connection refused", "1.0s", "default (10, exponential)", 3233.6),
    ("connection refused", "0.015s", "default (10, exponential)", 4061.2),
    ("connection refused", "0.005s", "default (10, exponential)", 3587.4),
    ("connection refused", "1.0s", "retries=0", 0.4),
    ("connection refused", "0.015s", "retries=0", 0.4),
    ("raw TCP, no redis", "1.0s", "n/a", 0.5),
    ("accepts, never replies", "1.0s", "retries=0", 1003.5),
    ("accepts, never replies", "0.015s", "retries=0", 16.0),
]


def measure() -> dict:
    """Re-run the benchmark against a live Redis. Requires one to be reachable
    from THIS process -- which, given the whole point is co-location, means
    running this where Redis is."""
    import time

    from gateway.redis_velocity import RedisVelocity, connect

    url = "redis://127.0.0.1:6379/0"
    client = connect(url)
    client.ping()
    v = RedisVelocity(client, window_ms=60_000, namespace="budget")
    now = int(time.time() * 1000)
    for i in range(500):
        v.incr_and_count("warm", now + i)
    lat = []
    for i in range(5000):
        t0 = time.perf_counter()
        v.incr_and_count("card:{}".format(i % 100), now + i)
        lat.append((time.perf_counter() - t0) * 1000)
    lat.sort()

    def pct(p):
        return lat[min(int(len(lat) * p), len(lat) - 1)]

    return {"p50": pct(.50), "p90": pct(.90), "p99": pct(.99),
            "p999": pct(.999), "max": lat[-1], "n": len(lat)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--measure", action="store_true")
    args = ap.parse_args()

    colocated = COLOCATED
    source = "recorded 2026-08-24"
    if args.measure:
        try:
            colocated = measure()
            source = "measured just now"
        except Exception as exc:                             # noqa: BLE001
            print("could not reach Redis from this process: {}".format(exc))
            print("Falling back to the recorded figures, and saying so rather")
            print("than printing them as if they were fresh.")

    L = []
    add = L.append
    add("# SE-3 — the velocity budget, revised from measurement")
    add("")
    add("`gateway/redis_velocity.connect()` ended with an uncomfortable")
    add("conclusion: at a p50 of ~18ms and a max near 1.8s, **Redis does not")
    add("fit the 20ms velocity budget**, and adopting it means either rewriting")
    add("the budget or co-locating Redis so the hop is a loopback rather than a")
    add("cross-VM one. That was left as an open question. This answers it.")
    add("")

    add("## 1. Co-location answers it")
    add("")
    add("The same Lua script, same Redis, same window — run from a process on")
    add("the same host as the server instead of across the VM boundary.")
    add("")
    add("| | cross-VM | co-located | ratio |")
    add("|---|---|---|---|")
    for k, label in (("p50", "p50"), ("p99", "p99"), ("max", "max")):
        cv, co = CROSS_VM[k], colocated[k]
        add("| {} | {:.2f}ms | **{:.2f}ms** | {:.0f}x |".format(
            label, cv, co, cv / co if co else float("nan")))
    add("")
    add("Full co-located distribution ({}, n={:,}):".format(source, colocated["n"]))
    add("")
    add("| p50 | p90 | p99 | p999 | max |")
    add("|---|---|---|---|---|")
    add("| {:.2f}ms | {:.2f}ms | {:.2f}ms | {:.2f}ms | {:.2f}ms |".format(
        colocated["p50"], colocated["p90"], colocated["p99"],
        colocated["p999"], colocated["max"]))
    add("")
    add("**The whole distribution, including the p999, fits inside the 20ms")
    add("allocation.** So the budget does not need rewriting for the healthy")
    add("path — the deployment does. That is a different remediation from the")
    add("one the earlier comment was heading toward, and a cheaper one.")
    add("")

    add("## 2. A hypothesis this refuted")
    add("")
    add("The expectation was that a velocity window under attack — which is")
    add("exactly when the sorted set is largest — would be slower to evaluate,")
    add("since `ZREMRANGEBYSCORE` is O(log N + M). It is not:")
    add("")
    add("| members in the window | p50 |")
    add("|---|---|")
    for size, p50 in sorted(BY_WINDOW.items()):
        add("| {:,} | {:.2f}ms |".format(size, p50))
    add("")
    add("A key holding 5,400 members measures the same as one holding 410. At")
    add("these sizes the sorted-set operations are dominated by scheduler noise,")
    add("and the tail belongs to the round trip rather than to Redis's work — a")
    add("bare `PING` measures p99 **{:.2f}ms**, HIGHER than the script's".format(
        PING["p99"]))
    add("**{:.2f}ms**. Tuning the script would move nothing.".format(
        colocated["p99"]))
    add("")

    add("## 3. What actually bounds a FAILURE, which is not the timeout")
    add("")
    add("`run_redis_real.py` concluded that \"the gateway needs a timeout")
    add("shorter than its budget rather than relying on the client's default\".")
    add("**That advice is half right, and the half that is wrong is the half")
    add("that matters more often.**")
    add("")
    add("| failure mode | socket_timeout | retry policy | time to fail |")
    add("|---|---|---|---|")
    for mode, to, retry, ms in FAILURE:
        add("| {} | {} | {} | {:,.1f}ms |".format(mode, to, retry, ms))
    add("")
    add("Read the first three rows together: **tightening the timeout made")
    add("failure slower.** 15ms took 4,061ms where 1.0s took 3,234ms.")
    add("")
    add("The mechanism is that `redis-py` 8.x defaults every connection to")
    add("`Retry(ExponentialWithJitterBackoff(), retries=10)` for")
    add("`ConnectionError`, and that policy — not the socket timeout — governs")
    add("how long a dead server takes to fail. A shorter timeout lets more of")
    add("the ten attempts complete inside the same wall clock while the backoff")
    add("accumulates. `retry_on_timeout=False` was already set in this")
    add("codebase in the belief that it disabled retrying. It does not; it is a")
    add("separate flag.")
    add("")
    add("And the timeout is irrelevant to a refused connection anyway. The RST")
    add("arrives in half a millisecond — the raw TCP row proves it — so with")
    add("retries off the client fails at the speed of TCP, **0.4ms**.")
    add("")
    add("### Two failure modes, two controls")
    add("")
    add("| | bounded by | measured |")
    add("|---|---|---|")
    add("| **connection refused** — Redis is down | the retry policy. The socket timeout does nothing. | 3,234ms → 0.4ms with `retries=0` |")
    add("| **accepts but stalls** — GC pause, saturated box, network black hole | `socket_timeout`, and only it. | 1,003ms at 1.0s → 16ms at 15ms |")
    add("")
    add("Conflating them is what the earlier advice did. Both settings are")
    add("needed and they defend against different things.")
    add("")

    add("## 4. The revised budget")
    add("")
    add("| stage | allocation |")
    add("|---|---|")
    for stage, ms in BUDGET_MS.items():
        mark = "  ← measured p999 {:.1f}ms co-located".format(
            colocated["p999"]) if stage == "velocity" else ""
        add("| `{}` | {:.0f}ms{} |".format(stage, ms, mark))
    add("| **total** | **{:.0f}ms** against a {:.0f}ms p99 SLO |".format(
        sum(BUDGET_MS.values()), SLO_P99_MS))
    add("")
    add("**The 20ms velocity allocation stands, and now has evidence behind it**")
    add("where before it was a declared number. What changes is the deployment")
    add("constraint attached to it:")
    add("")
    add("- **Redis must be co-located.** Not a preference. Across the VM")
    add("  boundary the p50 alone consumed 88% of the allocation and the max")
    add("  exceeded the entire end-to-end SLO by 18x.")
    add("- **`retries=0` at the client.** Ten retries with exponential backoff")
    add("  behind a 20ms budget is a 3-second stall wearing a 20ms label.")
    add("- **`socket_timeout` stays above the healthy p99, not below the")
    add("  budget.** Setting it from the budget is what timed out healthy calls")
    add("  in an earlier attempt: a timeout below the dependency's own median")
    add("  turns every slow-but-fine request into an outage.")
    add("")

    add("## 5. And a decision the raise did not make")
    add("")
    add("`RedisVelocity` raises rather than returning zero, because a zero")
    add("\"would report every card as quiet at exactly the moment the system")
    add("went blind\". Right, and only half the problem — raising moves the")
    add("decision up a level without making it. Nothing above it answered")
    add("*Redis is down, this authorisation is in flight, do we approve it?*")
    add("")
    add("`gateway/velocity_policy.py` answers it, and the answer is neither of")
    add("the two obvious ones:")
    add("")
    add("- **Fail open always** leaves the system blind precisely when someone")
    add("  is hammering it — an overloaded velocity store is what a carding")
    add("  attack produces, so an attacker who can knock over Redis has turned")
    add("  the fraud control off.")
    add("- **Fail closed always** turns a dependency outage into a total")
    add("  outage. Nobody runs it, because declining every customer to stop the")
    add("  fraud you cannot see costs more than the fraud.")
    add("")
    add("So the dial is **exposure**: below a value ceiling, approve without")
    add("velocity and mark the decision; above it, decline. The ceiling belongs")
    add("to whoever owns the fraud budget, which is why it is a constructor")
    add("argument rather than a constant in a branch.")
    add("")
    add("Three things that matter more than the ceiling:")
    add("")
    add("- **Every blind approval carries `velocity_seen=False`.** Without it")
    add("  the post-incident question — *which approvals went out blind?* — has")
    add("  no answer, and fraud arriving three days later cannot be attributed")
    add("  to the outage that caused it.")
    add("- **The failure path is timed too.** A stage that only measures its")
    add("  successes reports a healthy p99 through an outage, because the slow")
    add("  calls are the ones that raised and never reached the histogram.")
    add("- **A slow-but-successful call keeps its count.** It blew the budget")
    add("  and it is still correct; discarding it would throw away the one")
    add("  piece of fraud signal actually obtained. Counted separately so the")
    add("  breach is visible without being fatal.")
    add("")

    add("## What is still not measured")
    add("")
    add("- **No failover.** One Redis. A primary going away mid-request and a")
    add("  replica being promoted is the failure this budget most needs to")
    add("  survive, and a single node cannot produce it.")
    add("- **The co-located numbers are loopback on one box.** A real")
    add("  co-located deployment is a sidecar or a same-rack host, which adds a")
    add("  real NIC and a real switch. That is larger than loopback and much")
    add("  smaller than the cross-VM figures; the honest bracket is between the")
    add("  two columns in section 1, and this cannot narrow it further without")
    add("  two machines.")
    add("- **The ceiling is not calibrated.** It should be the amount at which")
    add("  expected fraud loss from being blind exceeds revenue lost by")
    add("  declining. That needs a fraud rate and a margin, which are the")
    add("  business's numbers and not the gateway's.")

    doc = "\n".join(L)
    (ROOT / "docs").mkdir(exist_ok=True)
    (ROOT / "docs" / "VELOCITY_BUDGET.md").write_text(doc, encoding="utf-8")
    print(doc)
    print()
    print("wrote docs/VELOCITY_BUDGET.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
