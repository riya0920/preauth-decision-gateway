"""What the gateway does when it cannot see the velocity counter."""
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gateway.velocity_policy import Decision, VelocityGate, VelocityOutcome


class FakeCounter:
    def __init__(self, count=1, raises=None, delay_ms=0.0):
        self.count = count
        self.raises = raises
        self.delay_ms = delay_ms
        self.calls = 0

    def incr_and_count(self, key, now_ms):
        self.calls += 1
        if self.delay_ms:
            time.sleep(self.delay_ms / 1000.0)
        if self.raises:
            raise self.raises
        return self.count


NOW = 1_700_000_000_000
SMALL = 5_000        # $50
LARGE = 250_000      # $2,500


# ------------------------------------------------------------ healthy path
def test_a_quiet_card_is_approved():
    g = VelocityGate(FakeCounter(count=2), threshold=5)
    d = g.decide("card:a", SMALL, NOW)
    assert d.approved and d.velocity_seen and d.velocity_count == 2


def test_a_card_over_the_threshold_is_declined():
    g = VelocityGate(FakeCounter(count=9), threshold=5)
    d = g.decide("card:a", SMALL, NOW)
    assert d.approved is False and d.velocity_seen is True


def test_the_threshold_is_exclusive_and_the_boundary_is_pinned():
    """">" and ">=" differ by one whole bucket at the busiest count in the
    distribution, so the convention is tested rather than assumed."""
    assert VelocityGate(FakeCounter(count=5), threshold=5).decide(
        "c", SMALL, NOW).approved is True
    assert VelocityGate(FakeCounter(count=6), threshold=5).decide(
        "c", SMALL, NOW).approved is False


# ----------------------------------------------------------------- blind
def test_a_small_amount_is_approved_blind_and_marked():
    """Fail-closed always would turn a dependency outage into a total outage.
    Nobody runs that, because declining every customer to stop the fraud you
    cannot see costs more than the fraud."""
    g = VelocityGate(FakeCounter(raises=ConnectionError("down")),
                     blind_approve_ceiling_minor=10_000)
    d = g.decide("card:a", SMALL, NOW)
    assert d.approved is True
    assert d.velocity_seen is False, (
        "an approval made without velocity must be distinguishable from one "
        "made with it")
    assert d.velocity_count is None


def test_a_large_amount_is_declined_when_blind():
    """Fail-open always would leave the system blind precisely when someone is
    hammering it -- an overloaded velocity store is what a carding attack
    produces."""
    g = VelocityGate(FakeCounter(raises=ConnectionError("down")),
                     blind_approve_ceiling_minor=10_000)
    d = g.decide("card:a", LARGE, NOW)
    assert d.approved is False and d.velocity_seen is False


def test_the_ceiling_boundary_is_inclusive():
    g = VelocityGate(FakeCounter(raises=ConnectionError("down")),
                     blind_approve_ceiling_minor=10_000)
    assert g.decide("c", 10_000, NOW).approved is True
    assert g.decide("c", 10_001, NOW).approved is False


def test_the_reason_names_the_failure_and_the_ceiling():
    """An operator reading "declined" during an outage needs to know it was the
    outage and not the customer."""
    g = VelocityGate(FakeCounter(raises=ConnectionError("down")),
                     blind_approve_ceiling_minor=10_000)
    d = g.decide("card:a", LARGE, NOW)
    assert "velocity unavailable" in d.reason
    assert "ConnectionError" in d.reason
    assert "10000" in d.reason


def test_a_blind_decision_never_invents_a_count():
    """The failure the raising counter exists to prevent, re-asserted one level
    up: a zero here would report the card as quiet at exactly the moment the
    system went blind."""
    g = VelocityGate(FakeCounter(raises=ConnectionError("down")))
    d = g.decide("card:a", SMALL, NOW)
    assert d.velocity_count is None
    assert d.velocity_count != 0


# ------------------------------------------------------------- the metric
def test_blind_decisions_are_counted_separately_from_quiet_ones():
    """A dashboard plotting velocity-triggered declines shows a flat zero
    through an outage -- the graph an operator sees at the moment they most need
    to know the control is off."""
    good = VelocityGate(FakeCounter(count=1))
    for _ in range(10):
        good.decide("c", SMALL, NOW)
    assert good.stats()["blind_rate"] == 0.0

    bad = VelocityGate(FakeCounter(raises=ConnectionError("down")),
                       blind_approve_ceiling_minor=10_000)
    for _ in range(10):
        bad.decide("c", SMALL, NOW)
    assert bad.stats()["blind_rate"] == 1.0
    assert bad.stats()["blind_approvals"] == 10
    assert bad.stats()["velocity_seen"] == 0


def test_blind_approvals_and_blind_declines_are_distinguished():
    g = VelocityGate(FakeCounter(raises=ConnectionError("down")),
                     blind_approve_ceiling_minor=10_000)
    for _ in range(7):
        g.decide("c", SMALL, NOW)
    for _ in range(3):
        g.decide("c", LARGE, NOW)
    s = g.stats()
    assert s["blind_approvals"] == 7 and s["blind_declines"] == 3


# -------------------------------------------------------------- the budget
def test_the_failure_path_is_timed_too():
    """A stage that only measures its successes reports a healthy p99 through an
    outage, because the slow calls are the ones that raised and never reached
    the histogram."""
    g = VelocityGate(FakeCounter(raises=ConnectionError("down"), delay_ms=30),
                     budget_ms=20.0)
    out = g.read("c", NOW)
    assert out.seen is False
    assert out.elapsed_ms >= 25
    assert out.over_budget is True


def test_a_slow_but_successful_call_keeps_its_count(monkeypatch):
    """A call that blew the budget but returned is still a success -- the count
    is correct, and discarding it throws away the one piece of fraud signal
    actually obtained."""
    g = VelocityGate(FakeCounter(count=3, delay_ms=30), budget_ms=20.0)
    d = g.decide("c", SMALL, NOW)
    assert d.velocity_seen is True and d.velocity_count == 3
    assert g.stats()["over_budget"] == 1


def test_budget_breaches_are_visible_without_being_fatal():
    g = VelocityGate(FakeCounter(count=1, delay_ms=30), budget_ms=20.0)
    for _ in range(3):
        g.decide("c", SMALL, NOW)
    s = g.stats()
    assert s["over_budget"] == 3
    assert s["velocity_seen"] == 3
    assert s["blind_rate"] == 0.0


def test_a_fast_call_does_not_trip_the_budget():
    g = VelocityGate(FakeCounter(count=1), budget_ms=20.0)
    g.decide("c", SMALL, NOW)
    assert g.stats()["over_budget"] == 0


# ----------------------------------------------------- the client settings
def test_the_client_is_configured_with_zero_retries_by_default():
    """redis-py 8.x defaults every connection to ten retries with exponential
    backoff, measured at 3.2-4.1 SECONDS against a closed port regardless of
    socket timeout. A stage with a 20ms budget cannot delegate its deadline to a
    library whose backoff schedule it cannot see.

    `retry_on_timeout=False` does NOT disable that policy -- it was set here in
    the belief that it did.
    """
    import inspect

    from gateway.redis_velocity import connect

    assert inspect.signature(connect).parameters["retries"].default == 0

    src = inspect.getsource(connect)
    assert "retry=Retry(" in src, "the retry policy is left to the library"
    assert "NoBackoff" in src
