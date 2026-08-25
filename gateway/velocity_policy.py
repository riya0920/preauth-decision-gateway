"""What the gateway does when it cannot see the velocity counter.

`RedisVelocity` raises rather than returning zero, and its docstring explains
why: "a counter that returned 0 here would report every card as quiet at exactly
the moment the system went blind." That is right and it is only half the
problem. Raising moves the decision up a level; it does not make it.

Nothing above it decided anything. The gateway had no answer to "Redis is down,
this authorisation is in flight, do we approve it?" -- and that question has a
wrong answer in both directions:

  FAIL OPEN, ALWAYS   approve without the velocity check. The system is blind
                      precisely when someone is hammering it, because an
                      overloaded velocity store is what a carding attack
                      produces. An attacker who can knock over Redis has turned
                      the fraud control off, and the cheapest way to knock over
                      a velocity store is to attack it.

  FAIL CLOSED, ALWAYS decline every authorisation while Redis is down. A
                      dependency outage becomes a total outage. Nobody runs
                      this, because declining every customer to stop the fraud
                      you cannot see costs more than the fraud.

SO THE POLICY IS NEITHER, AND THE DIAL IS EXPOSURE. Below a value threshold,
approve without velocity and mark the decision. Above it, decline. The threshold
is the amount at which the expected fraud loss from being blind exceeds the
revenue lost by declining, and it belongs to whoever owns the fraud budget --
which is why it is a constructor argument and not a constant buried in a branch.

THREE THINGS THAT MATTER MORE THAN THE THRESHOLD ITSELF:

  THE DECISION IS MARKED. Every approval made without velocity carries
  `velocity_seen=False`. Without that flag the post-incident question -- "which
  approvals went out blind?" -- has no answer, and the fraud that arrives three
  days later cannot be attributed to the outage that caused it.

  "NO ATTACK" AND "CANNOT SEE" ARE DIFFERENT METRICS. A dashboard that plots
  velocity-triggered declines will show a beautiful flat zero during an outage.
  That is the graph an operator sees at the exact moment they most need to know
  the control is off.

  THE DEADLINE IS THE CALLER'S. redis-py's own retry policy defaults to ten
  attempts with exponential backoff -- measured at 3.2 to 4.1 seconds against a
  closed port, regardless of socket timeout. A stage with a 20ms budget cannot
  delegate its deadline to a library whose backoff schedule it cannot see, so
  the client is configured with zero retries and the budget is enforced here.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

# The measured p999 of the velocity operation against a CO-LOCATED Redis is
# 12.04ms and the max is 15.30ms, so 20ms holds the whole healthy distribution.
# This is set from the measurement, not from the budget: a timeout below the
# dependency's own tail turns every slow-but-fine request into an outage.
VELOCITY_BUDGET_MS = 20.0

# Above this, a decision without velocity is refused rather than marked. Chosen
# by whoever owns the fraud budget; the default is deliberately conservative.
DEFAULT_BLIND_APPROVE_CEILING_MINOR = 10_000        # $100


@dataclass
class VelocityOutcome:
    count: int | None
    seen: bool
    elapsed_ms: float
    over_budget: bool = False
    error: str = ""

    @property
    def blind(self) -> bool:
        return not self.seen


@dataclass
class Decision:
    approved: bool
    reason: str
    velocity_seen: bool
    velocity_count: int | None = None
    elapsed_ms: float = 0.0


class VelocityGate:
    def __init__(self, counter, budget_ms: float = VELOCITY_BUDGET_MS,
                 blind_approve_ceiling_minor: int =
                 DEFAULT_BLIND_APPROVE_CEILING_MINOR,
                 threshold: int = 5):
        self.counter = counter
        self.budget_ms = budget_ms
        self.ceiling = blind_approve_ceiling_minor
        self.threshold = threshold
        self.blind_approvals = 0
        self.blind_declines = 0
        self.over_budget = 0
        self.seen = 0

    # ------------------------------------------------------------- reading
    def read(self, key: str, now_ms: int) -> VelocityOutcome:
        """Read the counter, and report the elapsed time whatever happens.

        The elapsed time is recorded on the FAILURE path too. A stage that only
        measures its successes reports a healthy p99 through an outage, because
        the slow calls are the ones that raised and never reached the histogram.
        """
        t0 = time.perf_counter()
        try:
            count = self.counter.incr_and_count(key, now_ms)
        except Exception as exc:                             # noqa: BLE001
            ms = (time.perf_counter() - t0) * 1000
            return VelocityOutcome(None, False, ms, ms > self.budget_ms,
                                   type(exc).__name__)
        ms = (time.perf_counter() - t0) * 1000
        over = ms > self.budget_ms
        # A call that SUCCEEDED but blew the budget is still a success -- the
        # count is correct and discarding it would throw away the one piece of
        # fraud signal actually obtained. It is counted separately so the budget
        # breach is visible without being fatal.
        return VelocityOutcome(count, True, ms, over)

    # ------------------------------------------------------------ deciding
    def decide(self, key: str, amount_minor: int, now_ms: int) -> Decision:
        out = self.read(key, now_ms)
        if out.over_budget:
            self.over_budget += 1

        if out.seen:
            self.seen += 1
            if out.count > self.threshold:
                return Decision(False, "velocity {} over threshold {}".format(
                    out.count, self.threshold), True, out.count, out.elapsed_ms)
            return Decision(True, "velocity {} within threshold".format(out.count),
                            True, out.count, out.elapsed_ms)

        # Blind. The exposure dial, and nothing else.
        if amount_minor <= self.ceiling:
            self.blind_approvals += 1
            return Decision(
                True,
                "velocity unavailable ({}); approved blind under the {} minor "
                "ceiling".format(out.error, self.ceiling),
                False, None, out.elapsed_ms)

        self.blind_declines += 1
        return Decision(
            False,
            "velocity unavailable ({}); {} minor exceeds the {} minor blind "
            "ceiling".format(out.error, amount_minor, self.ceiling),
            False, None, out.elapsed_ms)

    # ------------------------------------------------------------- metrics
    def stats(self) -> dict:
        total = self.seen + self.blind_approvals + self.blind_declines
        return {
            "decisions": total,
            "velocity_seen": self.seen,
            "blind_approvals": self.blind_approvals,
            "blind_declines": self.blind_declines,
            # The metric that separates "no attack" from "cannot see". A
            # dashboard plotting only velocity-triggered declines shows a flat
            # zero through an outage, which is the graph an operator sees at the
            # moment they most need to know the control is off.
            "blind_rate": (self.blind_approvals + self.blind_declines) / total
                          if total else 0.0,
            "over_budget": self.over_budget,
        }
