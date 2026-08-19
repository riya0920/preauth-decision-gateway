"""Feature cache with TTLs and an explicit stale-feature policy.

The question this answers: a cached feature is 90 seconds old and its TTL is 60.
Do you use it?

"No" is the naive answer and it is usually wrong -- it converts a cache miss
into a model that cannot score, which is a worse outcome than a slightly stale
input for most features. "Yes" is also wrong, because some features are only
meaningful when fresh.

So freshness is a per-feature policy, not a global one:

  HARD    unusable when stale. A velocity counter that is two minutes old cannot
          see the attack that started 90 seconds ago -- using it is worse than
          knowing you do not have it.
  SOFT    usable when stale, with a recorded confidence penalty. An account's
          tenure does not change in a minute; refusing to score because that
          value is 90 seconds old is self-harm.
  STATIC  no meaningful TTL. Merchant category does not change intraday.

The decision surface then degrades gracefully: a request with stale SOFT
features scores at a discounted threshold, and a request missing a HARD feature
falls back to rules. Both are recorded on the decision so the audit log shows
WHY a decision was made with less information than usual.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field

HARD, SOFT, STATIC = "hard", "soft", "static"

FEATURE_POLICY = {
    "velocity_24h":     {"ttl_s": 30,   "freshness": HARD,
                         "why": "cannot see an in-progress attack when stale"},
    "device_history":   {"ttl_s": 300,  "freshness": SOFT,
                         "why": "changes slowly; stale is still informative"},
    "card_tenure_days": {"ttl_s": 3600, "freshness": SOFT,
                         "why": "changes once a day at most"},
    "merchant_risk":    {"ttl_s": 0,    "freshness": STATIC,
                         "why": "reference data, versioned not cached"},
}

# How much to discount confidence per stale SOFT feature. Stated as a policy
# constant because it moves the decision threshold, and anything that moves a
# threshold is a business parameter.
SOFT_STALE_PENALTY = 0.05


@dataclass
class CacheEntry:
    value: float
    written_at: float


@dataclass
class FeatureCache:
    """In-process stand-in for Redis. The TTL semantics are what matter here;
    the storage is not the point and is listed as not-built in the README."""
    entries: dict[str, CacheEntry] = field(default_factory=dict)
    available: bool = True
    hits: int = 0
    misses: int = 0
    stale_hits: int = 0

    class Unavailable(Exception):
        pass

    def put(self, key: str, feature: str, value: float, *, age_s: float = 0.0) -> None:
        self.entries[self._k(key, feature)] = CacheEntry(value, time.time() - age_s)

    def get(self, key: str, feature: str) -> tuple[float | None, bool]:
        """Returns (value, is_stale). Raises Unavailable when the store is down."""
        if not self.available:
            raise FeatureCache.Unavailable("feature cache is down")
        entry = self.entries.get(self._k(key, feature))
        if entry is None:
            self.misses += 1
            return None, False
        policy = FEATURE_POLICY.get(feature, {"ttl_s": 60, "freshness": SOFT})
        ttl = policy["ttl_s"]
        stale = ttl > 0 and (time.time() - entry.written_at) > ttl
        self.hits += 1
        if stale:
            self.stale_hits += 1
        return entry.value, stale

    @staticmethod
    def _k(key: str, feature: str) -> str:
        return "{}:{}".format(key, feature)


@dataclass
class FeatureBundle:
    values: dict[str, float] = field(default_factory=dict)
    missing_hard: list[str] = field(default_factory=list)
    stale_soft: list[str] = field(default_factory=list)
    cache_down: bool = False

    @property
    def complete(self) -> bool:
        return not self.missing_hard and not self.stale_soft and not self.cache_down

    @property
    def usable_for_model(self) -> bool:
        """A missing HARD feature means the model would be scoring on a lie."""
        return not self.missing_hard and not self.cache_down

    def confidence_discount(self) -> float:
        return SOFT_STALE_PENALTY * len(self.stale_soft)

    def reasons(self) -> list[str]:
        out = []
        if self.cache_down:
            out.append("feature_cache_down")
        for f in self.missing_hard:
            out.append("missing_hard_feature:" + f)
        for f in self.stale_soft:
            out.append("stale_soft_feature:" + f)
        return out


def assemble(cache: FeatureCache, card_id: str,
             wanted: list[str] | None = None) -> FeatureBundle:
    wanted = wanted or list(FEATURE_POLICY)
    bundle = FeatureBundle()
    try:
        for feature in wanted:
            value, stale = cache.get(card_id, feature)
            policy = FEATURE_POLICY.get(feature, {"freshness": SOFT})
            if value is None:
                if policy["freshness"] == HARD:
                    bundle.missing_hard.append(feature)
                continue
            if stale and policy["freshness"] == HARD:
                # Present but useless. Treated as missing, deliberately.
                bundle.missing_hard.append(feature)
                continue
            if stale and policy["freshness"] == SOFT:
                bundle.stale_soft.append(feature)
            bundle.values[feature] = value
    except FeatureCache.Unavailable:
        bundle.cache_down = True
    return bundle
