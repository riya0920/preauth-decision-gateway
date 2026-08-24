"""The gateway as an actual HTTP service.

What changes by putting a real server around the pipeline, and why it is worth
doing rather than leaving `Gateway.decide()` as a method call:

  * The decision contract becomes a schema, so a malformed request is rejected
    at the boundary with a 422 rather than reaching the decision logic.
  * Latency now includes serialisation and the ASGI stack, which is where a
    surprising share of a 100ms budget actually goes.
  * /metrics makes the budget observable by something other than a script that
    prints at the end -- which is the difference between a measurement and
    monitoring.

Run:  uvicorn serve:app --port 8080
      curl -s localhost:8080/metrics
"""
from __future__ import annotations

import sys
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Response
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from gateway.budget import SLO_P99_MS, Budget
from gateway.features import FeatureCache
from gateway.metrics import REGISTRY
from gateway.pipeline import AuditLog, Gateway, ModelService, Request
from gateway.wal import AuditWal, DurableAuditLog
from gateway.velocity import SafeCounter

_state: dict = {}


@asynccontextmanager
async def lifespan(_app):
    budget = Budget()
    # The WAL is IN the service path, not beside it. A durability mechanism that
    # exists as a library and is not wired into the thing that makes decisions
    # protects nothing -- and "available" and "enforced" are different claims.
    #
    # fsync=batch by default: bounded loss at a bounded cost. `always` measured
    # 1.72ms mean / 4.15ms p99 per append, which is 4.1% of the 100ms budget
    # spent on one fsync per decision, and that choice belongs to whoever owns
    # the compliance requirement rather than to this line.
    wal = AuditWal(Path(os.environ.get("GATEWAY_WAL", "data/audit.wal.jsonl")),
                   fsync=os.environ.get("GATEWAY_WAL_FSYNC", "batch"))
    audit = DurableAuditLog(wal)
    # GATEWAY_REDIS_URL selects the real counter; absent, the in-process one.
    # Opt-in rather than default, because the Redis path adds a network hop to
    # a 20ms stage and that is a deployment decision, not a library default --
    # and because a service that silently requires Redis to start is a service
    # that will not start.
    redis_url = os.environ.get("GATEWAY_REDIS_URL")
    if redis_url:
        from gateway.redis_velocity import RedisVelocity, connect as redis_connect

        counter = RedisVelocity(redis_connect(redis_url), window_ms=60_000)
    else:
        counter = SafeCounter()
    gw = Gateway(ModelService(), counter, budget, audit)
    gw.feature_cache = FeatureCache()
    _state.update({"gw": gw, "budget": budget, "wal": wal})
    yield
    wal.close()
    _state.clear()


app = FastAPI(title="Pre-auth decision gateway", version="0.2.0", lifespan=lifespan)


class AuthRequest(BaseModel):
    request_id: str
    card_id: str
    merchant_id: str
    device_id: str
    amount_minor: int = Field(gt=0, description="Integer minor units. Never a float.")
    currency: str = Field(min_length=3, max_length=3)
    now_ms: int = Field(gt=0)


class AuthResponse(BaseModel):
    request_id: str
    decision: str
    source: str
    score: float | None
    reasons: list[str]
    latency_ms: float
    model_version: str


@app.get("/health")
def health() -> dict:
    gw: Gateway = _state["gw"]
    return {
        "status": "ok",
        "model_up": gw.model.up,
        "velocity_up": gw.velocity.available,
        "circuit_open": gw.breaker.is_open,
        "slo_p99_ms": SLO_P99_MS,
    }


@app.post("/authorize", response_model=AuthResponse)
def authorize(req: AuthRequest) -> AuthResponse:
    gw: Gateway = _state["gw"]
    decision = gw.decide(Request(**req.model_dump()))

    REGISTRY.histogram(
        "gateway_decision_latency_ms",
        "End-to-end decision latency").observe(decision.latency_ms)
    REGISTRY.counter(
        "gateway_decisions_total",
        "Decisions by outcome and source").inc(
            {"decision": decision.decision, "source": decision.source})

    # Gauges, because these go DOWN as well as up and the current depth is the
    # only number an operator can act on. `ops/alerts.yml` alerts on both, and
    # `test_alert_rules.py` asserts every metric those rules name is actually
    # exported here -- an alert rule referring to a metric nobody emits is
    # silently never going to fire, which is the worst kind of alert to own.
    REGISTRY.gauge(
        "gateway_audit_buffer_size",
        "Audit records buffered and not yet shipped").set(len(gw.audit.buffer))
    wal = getattr(gw.audit, "wal", None)
    if wal is not None:
        REGISTRY.gauge(
            "gateway_audit_wal_unshipped",
            "Audit records on disk not yet acknowledged by the sink").set(
                len(gw.audit.recover()))
    if decision.source.startswith("degraded_"):
        REGISTRY.counter(
            "gateway_velocity_errors_total",
            "Velocity store failures").inc({"source": decision.source})

    return AuthResponse(
        request_id=decision.request_id, decision=decision.decision,
        source=decision.source, score=decision.score, reasons=decision.reasons,
        latency_ms=round(decision.latency_ms, 3),
        model_version=decision.model_version)


@app.get("/metrics")
def metrics() -> Response:
    """Prometheus exposition. Per-stage histograms are exported alongside the
    end-to-end one, because an SLO breach that does not say WHICH stage moved is
    an alert nobody can act on at 3am."""
    budget: Budget = _state["budget"]
    for stage, hist in budget.stages.items():
        h = REGISTRY.histogram("gateway_stage_latency_ms_" + stage,
                               "Latency for stage " + stage)
        # Re-observe only what has not been exported yet.
        exported = h.total
        for sample in hist.samples[exported:]:
            h.observe(sample)
    return Response(content=REGISTRY.render(), media_type="text/plain; version=0.0.4")


@app.post("/chaos/{dependency}/{state}")
def chaos(dependency: str, state: str) -> dict:
    """Chaos control surface. Present so drills can be driven against the running
    service rather than by monkeypatching objects inside a test process --
    killing a dependency in-process proves less than killing it over HTTP."""
    gw: Gateway = _state["gw"]
    up = state == "up"
    if dependency == "model":
        gw.model.up = up
    elif dependency == "velocity":
        gw.velocity.available = up
    elif dependency == "features":
        gw.feature_cache.available = up
        gw.feature_cache_up = up
    else:
        return {"error": "unknown dependency " + dependency}
    return {"dependency": dependency, "up": up}
