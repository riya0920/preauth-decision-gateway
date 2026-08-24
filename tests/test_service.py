"""HTTP service, feature-freshness policy, and metrics tests."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

import serve
from gateway.features import (FEATURE_POLICY, HARD, SOFT, FeatureCache, assemble)
from gateway.metrics import Histogram, Registry


@pytest.fixture(scope="module")
def client():
    with TestClient(serve.app) as c:
        yield c


def _req(**over):
    base = {"request_id": "r1", "card_id": "CARD_00001", "merchant_id": "M001",
            "device_id": "D00001", "amount_minor": 12_000, "currency": "USD",
            "now_ms": 1_800_000_000_000}
    base.update(over)
    return base


# ------------------------------------------------------------------ service
def test_health(client):
    b = client.get("/health").json()
    assert b["status"] == "ok" and b["slo_p99_ms"] == 100.0


def test_authorize_returns_a_decision(client):
    b = client.post("/authorize", json=_req()).json()
    assert b["decision"] in ("approve", "decline", "review")
    assert b["latency_ms"] > 0


def test_float_amount_rejected_at_the_boundary(client):
    """Money is integer minor units, and the schema refuses a float rather than
    truncating it inside the decision logic."""
    assert client.post("/authorize", json=_req(amount_minor=120.55)).status_code == 422


def test_invalid_currency_rejected(client):
    assert client.post("/authorize", json=_req(currency="US")).status_code == 422
    assert client.post("/authorize", json=_req(amount_minor=0)).status_code == 422


def test_blocklisted_card_declines_on_the_hot_rule(client):
    b = client.post("/authorize", json=_req(card_id="CARD_BAD_0001")).json()
    assert b["decision"] == "decline" and b["source"] == "hot_rule"


def test_chaos_endpoint_flips_decision_source(client):
    """Killing the dependency over HTTP proves more than monkeypatching an
    object inside the test process."""
    client.post("/chaos/model/down")
    try:
        b = client.post("/authorize", json=_req(request_id="r-degraded")).json()
        assert b["source"].startswith("degraded")
        assert b["decision"] in ("approve", "decline", "review")
    finally:
        client.post("/chaos/model/up")


def test_metrics_endpoint_emits_prometheus_histograms(client):
    client.post("/authorize", json=_req(request_id="r-metrics"))
    text = client.get("/metrics").text
    assert "# TYPE gateway_decision_latency_ms histogram" in text
    assert 'gateway_decision_latency_ms_bucket{le="+Inf"}' in text
    assert "gateway_decisions_total" in text


# ------------------------------------------------------------- feature cache
def test_hard_feature_when_stale_is_treated_as_missing():
    """A velocity counter two minutes old cannot see an attack that started 90
    seconds ago. Using it is worse than knowing you do not have it."""
    cache = FeatureCache()
    cache.put("CARD1", "velocity_24h", 3.0, age_s=120)
    bundle = assemble(cache, "CARD1", ["velocity_24h"])
    assert "velocity_24h" in bundle.missing_hard
    assert not bundle.usable_for_model


def test_soft_feature_when_stale_is_used_with_a_confidence_discount():
    """Refusing to score because account tenure is 90 seconds old is self-harm."""
    cache = FeatureCache()
    cache.put("CARD1", "card_tenure_days", 900.0, age_s=7200)
    bundle = assemble(cache, "CARD1", ["card_tenure_days"])
    assert "card_tenure_days" in bundle.stale_soft
    assert bundle.usable_for_model
    assert bundle.confidence_discount() > 0
    assert bundle.values["card_tenure_days"] == 900.0


def test_fresh_features_are_complete():
    cache = FeatureCache()
    cache.put("CARD1", "velocity_24h", 2.0)
    cache.put("CARD1", "card_tenure_days", 900.0)
    bundle = assemble(cache, "CARD1", ["velocity_24h", "card_tenure_days"])
    assert bundle.complete and bundle.usable_for_model
    assert bundle.confidence_discount() == 0


def test_cache_down_is_distinguished_from_cache_miss():
    """Different failures need different responses: a miss means score without
    it, a down cache means every request is affected."""
    cache = FeatureCache()
    cache.available = False
    bundle = assemble(cache, "CARD1", ["velocity_24h"])
    assert bundle.cache_down and not bundle.usable_for_model
    assert "feature_cache_down" in bundle.reasons()


def test_every_feature_has_a_declared_freshness_policy():
    """A feature with no policy gets a silent default, which is how a HARD
    feature ends up being used stale."""
    for feature, policy in FEATURE_POLICY.items():
        assert policy["freshness"] in (HARD, SOFT, "static"), feature
        assert policy["why"], "{} has no stated rationale".format(feature)


# ------------------------------------------------------------------ metrics
def test_histogram_quantile_matches_bucket_interpolation():
    h = Histogram("t", "test")
    for _ in range(99):
        h.observe(5.0)
    h.observe(900.0)          # one long tail sample
    assert h.quantile(0.50) <= 5.0
    assert h.quantile(0.99) <= 100.0
    mean = h.sum_value / h.total
    assert mean > 10, "the mean hides the tail -- which is why p99 is exported"


def test_registry_renders_valid_exposition_format():
    reg = Registry()
    reg.histogram("h_ms", "help").observe(3.0)
    reg.counter("c_total", "help").inc({"outcome": "approve"})
    text = reg.render()
    assert "# TYPE h_ms histogram" in text
    assert 'c_total{outcome="approve"} 1' in text
    assert text.endswith("\n")


# ------------------------------------------------- Redis on the hot path
def test_a_dead_velocity_store_degrades_rather_than_500s():
    """Swapping the Redis counter in must not turn a degradation into an error.

    `Gateway` caught only `SafeCounter.Unavailable`, so a `RedisVelocity`
    failure escaped the handler and the request died with a 500 instead of
    falling back to rules. The degradation policy is the whole point of the
    stage -- a gateway that 500s when its counter is down has no policy, it has
    a dependency.
    """
    import gateway.pipeline as pipeline
    from gateway.redis_velocity import RedisVelocity, connect

    assert RedisVelocity.Unavailable in pipeline._VELOCITY_UNAVAILABLE

    dead = RedisVelocity(connect("redis://127.0.0.1:6999/0"), window_ms=60_000)
    with pytest.raises(RedisVelocity.Unavailable):
        dead.incr_and_count("card:x", 1_800_000_000_000)


def test_the_pool_and_timeout_are_set_from_measurement_not_defaults():
    """Two settings that are not library defaults and should be.

    An unbounded-in-name pool serialises 50 threads behind a handful of
    connections, and the default socket timeout lets a dead server hang for
    ~2,000ms -- 100x the stage budget.
    """
    import inspect

    from gateway.redis_velocity import connect

    sig = inspect.signature(connect)
    assert sig.parameters["pool_size"].default >= 32
    timeout = sig.parameters["socket_timeout"].default
    assert 0 < timeout <= 1.0, "timeout must beat the ~2s library default"
