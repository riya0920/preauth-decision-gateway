"""Separate model process: transport correctness and the contention it creates."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gateway.model_client import (BinaryModelClient, HttpModelClient,
                                  ModelProcess, ModelTransportError,
                                  RemoteModelService)

FEATURES = {"amount_minor": 42_000, "device_id": "D01239", "card_id": "C1"}


@pytest.fixture(scope="module")
def proc():
    p = ModelProcess(http_port=8811, binary_port=8812, cpu_ms=2.0)
    p.start()
    yield p
    p.stop()


def test_both_transports_return_the_same_score(proc):
    """Transport must not change the answer. If it does, one of them is
    mangling the payload and the comparison is meaningless."""
    h = HttpModelClient("http://127.0.0.1:{}".format(proc.http_port))
    b = BinaryModelClient("127.0.0.1", proc.binary_port)
    try:
        assert h.score(FEATURES) == pytest.approx(b.score(FEATURES), abs=0.06)
    finally:
        h.close(); b.close()


def test_scores_are_bounded(proc):
    h = HttpModelClient("http://127.0.0.1:{}".format(proc.http_port))
    try:
        for amt in (1, 50_000, 10_000_000):
            s = h.score({**FEATURES, "amount_minor": amt})
            assert 0.0 <= s <= 1.0
    finally:
        h.close()


def test_binary_framing_survives_a_reconnect(proc):
    """The client drops its socket on any error and reconnects on the next
    call; a stale socket would otherwise poison every subsequent request."""
    b = BinaryModelClient("127.0.0.1", proc.binary_port)
    try:
        assert b.score(FEATURES) > 0
        b._sock.close()          # simulate the peer going away
        b._sock = None
        assert b.score(FEATURES) > 0
    finally:
        b.close()


def test_unreachable_model_raises_transport_error():
    """A dead model must raise, not return a score. Returning 0.0 would read as
    'this transaction is safe', which is the most dangerous default available."""
    b = BinaryModelClient("127.0.0.1", 9)
    with pytest.raises(ModelTransportError):
        b.score(FEATURES)


def test_remote_service_adapter_matches_the_gateway_interface(proc):
    from gateway.pipeline import Request

    svc = RemoteModelService(
        HttpModelClient("http://127.0.0.1:{}".format(proc.http_port)))
    req = Request("r1", "C1", "M1", "D00019", 42_000, "USD", 1_800_000_000_000)
    assert 0.0 <= svc.score(req) <= 1.0
    assert svc.calls == 1

    svc.up = False
    with pytest.raises(ConnectionError):
        svc.score(req)


def test_gateway_runs_against_the_separate_process(proc):
    """End to end: the decision path uses a real network hop and real CPU."""
    from gateway.budget import Budget
    from gateway.pipeline import AuditLog, Gateway, Request
    from gateway.velocity import SafeCounter

    svc = RemoteModelService(
        HttpModelClient("http://127.0.0.1:{}".format(proc.http_port)))
    gw = Gateway(svc, SafeCounter(), Budget(), AuditLog())
    d = gw.decide(Request("r2", "C2", "M1", "D00001", 9_000, "USD",
                          1_800_000_000_000))
    assert d.decision in ("approve", "decline", "review")
    assert d.source == "model"
    assert d.latency_ms > 0
