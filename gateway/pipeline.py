"""The decision pipeline, with a degradation policy that is a policy, not a
try/except.

Fail-open vs fail-closed is a business decision with arithmetic behind it, and
the arithmetic is written down here so it can be argued with:

  Below $50   FAIL OPEN. Approving a fraudulent $50 auth costs ~$50. Declining a
              legitimate one costs a checkout abandonment plus some fraction of
              the customer relationship -- and at this amount the fraud rate is
              low enough that expected fraud loss per approval is well under a
              dollar. Approve.
  $50-$500    RULES ONLY. The hot rules still catch blocklist and impossible
              values with no model. Expected loss is bounded and the approval
              rate is preserved.
  Above $500  FAIL CLOSED (review, not decline). Expected fraud loss now exceeds
              the friction cost of a review, so the amount buys the review.

Who owns those thresholds: not engineering. A fraud/risk policy owner owns them,
which is why they are a table this service reads rather than constants compiled
into the decision function. Engineering owns making them configurable, audited,
and cheap to change.
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass, field

from .budget import Budget
from .features import FeatureCache, assemble
from .velocity import SafeCounter

# amount ceiling (minor units) -> behaviour when the model is unavailable
DEGRADATION_TIERS = [
    (5_000, "approve"),         # < $50   fail open
    (50_000, "rules_only"),     # < $500  rules decide
    (float("inf"), "review"),   # >= $500 fail closed to manual review
]

BLOCKLIST = {"CARD_BAD_0001", "CARD_BAD_0002"}


@dataclass
class Request:
    request_id: str
    card_id: str
    merchant_id: str
    device_id: str
    amount_minor: int
    currency: str
    now_ms: int


@dataclass
class Decision:
    request_id: str
    decision: str               # approve | decline | review
    source: str                 # model | rules | degraded_<tier> | hot_rule
    score: float | None
    reasons: list[str] = field(default_factory=list)
    latency_ms: float = 0.0
    model_version: str = "preauth-0.1.0"


class CircuitBreaker:
    """Open after `threshold` consecutive failures; half-open after `cooldown`."""

    def __init__(self, threshold: int = 5, cooldown_s: float = 2.0):
        self.threshold = threshold
        self.cooldown_s = cooldown_s
        self.failures = 0
        self.opened_at = None

    @property
    def is_open(self) -> bool:
        if self.opened_at is None:
            return False
        if time.monotonic() - self.opened_at > self.cooldown_s:
            self.opened_at = None            # half-open: allow a probe
            self.failures = 0
            return False
        return True

    def record_failure(self) -> None:
        self.failures += 1
        if self.failures >= self.threshold and self.opened_at is None:
            self.opened_at = time.monotonic()

    def record_success(self) -> None:
        self.failures = 0
        self.opened_at = None


class ModelService:
    """Stands in for a separate scoring service behind gRPC/HTTP."""

    def __init__(self, base_ms: float = 12.0, seed: int = 0):
        self.base_ms = base_ms
        self.up = True
        self.slow = False
        self.rng = random.Random(seed)
        self.calls = 0

    def score(self, req: Request) -> float:
        self.calls += 1
        if not self.up:
            raise ConnectionError("model service unavailable")
        delay = self.base_ms + self.rng.expovariate(1 / 4.0)
        if self.slow:
            delay *= 6
        _simulate_io_ms(delay)
        z = (req.amount_minor / 200_000
             + (0.4 if req.device_id.endswith("9") else 0)
             + self.rng.random() * 0.3)
        return min(0.999, z)


class AuditLog:
    """Async by construction: writes go to a buffer, a background drain ships
    them. The hot path never waits on the sink."""

    def __init__(self):
        self.buffer: list[dict] = []
        self.shipped: list[dict] = []
        self.sink_up = True

    def write(self, record: dict) -> None:
        self.buffer.append(record)          # O(1), no I/O, no lock contention

    def drain(self) -> int:
        if not self.sink_up:
            return 0
        n = len(self.buffer)
        self.shipped.extend(self.buffer)
        self.buffer.clear()
        return n


def _simulate_io_ms(ms: float) -> None:
    """Simulate a dependency call by SLEEPING, not spinning.

    This matters more than it looks. A busy-wait holds the GIL, so eight worker
    threads spinning on a fake "model call" starve each other and every stage's
    measured p99 becomes a reading of GIL contention rather than of that stage.
    An earlier version of this file did exactly that and reported a 125ms p99 on
    the velocity stage -- which is a lock and a deque append, and cannot take
    125ms. A real model call is I/O: the thread blocks and releases the GIL,
    which is what sleep() does.

    Measured sleep overhead on this machine: ~0.4ms at a 1ms target.
    """
    time.sleep(ms / 1000.0)


class Gateway:
    def __init__(self, model: ModelService, velocity: SafeCounter,
                 budget: Budget, audit: AuditLog):
        self.model = model
        self.velocity = velocity
        self.budget = budget
        self.audit = audit
        self.breaker = CircuitBreaker()
        self.feature_cache_up = True
        self.feature_cache: FeatureCache | None = None
        self.source_counts: dict[str, int] = {}

    def decide(self, req: Request) -> Decision:
        t_start = time.perf_counter()
        reasons: list[str] = []

        # -- 1. parse + validate --------------------------------------------
        t = time.perf_counter()
        if req.amount_minor <= 0 or len(req.currency) != 3:
            self.budget.record("parse_validate", _ms_since(t))
            return self._finish(req, "decline", "hot_rule", None,
                                ["invalid_request"], t_start)
        self.budget.record("parse_validate", _ms_since(t))

        # -- 2. hot rules ----------------------------------------------------
        t = time.perf_counter()
        if req.card_id in BLOCKLIST:
            self.budget.record("hot_rules", _ms_since(t))
            return self._finish(req, "decline", "hot_rule", None,
                                ["card_blocklisted"], t_start)
        self.budget.record("hot_rules", _ms_since(t))

        # -- 3. velocity -----------------------------------------------------
        t = time.perf_counter()
        try:
            v_card = self.velocity.incr_and_count("card:" + req.card_id, req.now_ms)
            velocity_available = True
        except SafeCounter.Unavailable:
            v_card, velocity_available = 0, False
            reasons.append("velocity_store_down")
        self.budget.record("velocity", _ms_since(t))

        if velocity_available and v_card > 12:
            return self._finish(req, "decline", "rules", None,
                                reasons + ["velocity_exceeded"], t_start)

        # -- 4. feature assembly ---------------------------------------------
        t = time.perf_counter()
        discount = 0.0
        features_usable = True
        if self.feature_cache is not None:
            _simulate_io_ms(1.5)
            bundle = assemble(self.feature_cache, req.card_id)
            reasons.extend(bundle.reasons())
            discount = bundle.confidence_discount()
            features_usable = bundle.usable_for_model
            features_complete = bundle.complete
        elif self.feature_cache_up:
            _simulate_io_ms(1.5)
            features_complete = True
        else:
            features_complete = False
            reasons.append("features_partial")
        self.budget.record("features", _ms_since(t))

        if not features_usable:
            # A missing HARD feature means the model would score on a lie.
            # Rules-only is the honest fallback, not a degraded model call.
            decision, source = self._degrade(req, velocity_available, v_card)
            return self._finish(req, decision, source + "_no_features", None,
                                reasons, t_start)

        # -- 5. model score, behind a breaker --------------------------------
        t = time.perf_counter()
        score = None
        if not self.breaker.is_open:
            try:
                score = self.model.score(req)
                self.breaker.record_success()
            except Exception:
                self.breaker.record_failure()
                reasons.append("model_unavailable")
        else:
            reasons.append("circuit_open")
        self.budget.record("model", _ms_since(t))

        if score is None:
            decision, source = self._degrade(req, velocity_available, v_card)
            return self._finish(req, decision, source, None, reasons, t_start)

        # -- 6. decision policy ----------------------------------------------
        # Stale SOFT features do not block scoring; they tighten the threshold,
        # because a score built on older inputs deserves less benefit of the doubt.
        threshold = 0.75 - discount
        if discount:
            reasons.append("confidence_discounted:{:.2f}".format(discount))
        elif not features_complete:
            threshold = 0.60
            reasons.append("confidence_discounted")
        decision = "decline" if score >= threshold else "approve"
        if 0.60 <= score < threshold:
            decision = "review"
        return self._finish(req, decision, "model", score, reasons, t_start)

    def _degrade(self, req: Request, velocity_available: bool, v_card: int):
        for ceiling, behaviour in DEGRADATION_TIERS:
            if req.amount_minor < ceiling:
                if behaviour == "approve":
                    return "approve", "degraded_fail_open"
                if behaviour == "rules_only":
                    if velocity_available and v_card > 6:
                        return "decline", "degraded_rules_only"
                    return "approve", "degraded_rules_only"
                return "review", "degraded_fail_closed"
        return "review", "degraded_fail_closed"

    def _finish(self, req: Request, decision: str, source: str,
                score: float | None, reasons: list[str], t_start: float) -> Decision:
        t = time.perf_counter()
        self.audit.write({
            "request_id": req.request_id, "decision": decision, "source": source,
            "score": score, "reasons": list(reasons), "amount_minor": req.amount_minor,
            "model_version": "preauth-0.1.0",
        })
        self.budget.record("decide_log", _ms_since(t))
        total = _ms_since(t_start)
        self.budget.record_total(total)
        self.source_counts[source] = self.source_counts.get(source, 0) + 1
        return Decision(req.request_id, decision, source, score, reasons, total)


def _ms_since(t: float) -> float:
    return (time.perf_counter() - t) * 1000.0
