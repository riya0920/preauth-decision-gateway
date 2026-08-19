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
from gateway.velocity import SafeCounter

_state: dict = {}


@asynccontextmanager
async def lifespan(_app):
    budget = Budget()
    gw = Gateway(ModelService(), SafeCounter(), budget, AuditLog())
    gw.feature_cache = FeatureCache()
    _state.update({"gw": gw, "budget": budget})
    yield
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
