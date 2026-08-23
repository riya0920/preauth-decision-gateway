"""The alert rules must refer to metrics this service actually exports.

Nothing scrapes `ops/alerts.yml` -- there is no Prometheus server here. That
makes drift free and invisible: a rule naming a metric nobody emits never fires,
and an alert that never fires looks exactly like a system that is never
unhealthy. This is the test that keeps the rules honest without a server.
"""
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

yaml = pytest.importorskip("yaml")
pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

import serve
from gateway.metrics import Registry

RULES = Path(__file__).resolve().parents[1] / "ops" / "alerts.yml"
METRIC = re.compile(r"\bgateway_[a-z_]+\b")

# Suffixes Prometheus derives from a histogram; the exporter emits the base name.
DERIVED = ("_bucket", "_sum", "_count")


@pytest.fixture(scope="module")
def rules():
    return yaml.safe_load(RULES.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def exported():
    """Drive the real service, then read its real /metrics output."""
    with TestClient(serve.app) as client:
        for i in range(5):
            r = client.post("/authorize", json={
                "request_id": "r{}".format(i), "card_id": "c1",
                "merchant_id": "m1", "device_id": "d1",
                "amount_minor": 2500 + i, "currency": "USD",
                "now_ms": 1_800_000_000_000 + i})
            assert r.status_code == 200, r.text
        text = client.get("/metrics").text
    return {line.split("{")[0].split(" ")[0]
            for line in text.splitlines() if line and not line.startswith("#")}


def _base(name):
    for suffix in DERIVED:
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def test_the_rules_file_parses(rules):
    assert rules["groups"] and rules["groups"][0]["rules"]


def test_every_metric_named_by_a_rule_is_exported(rules, exported):
    """The whole point. A rule referring to a metric nobody emits is silently
    never going to fire, which is the worst kind of alert to own."""
    exported_bases = {_base(m) for m in exported}
    missing = set()
    for rule in rules["groups"][0]["rules"]:
        for name in METRIC.findall(rule["expr"]):
            if _base(name) not in exported_bases:
                missing.add(name)
    assert not missing, "alert rules name metrics /metrics does not emit: {}".format(
        sorted(missing))


def test_every_rule_has_a_severity_and_a_for_clause(rules):
    """A rule with no `for` fires on a scrape blip and teaches its audience to
    close it unread, which is worse than not having the rule."""
    for rule in rules["groups"][0]["rules"]:
        assert rule["labels"]["severity"] in ("page", "ticket"), rule["alert"]
        assert rule.get("for"), rule["alert"]


def test_every_rule_says_why_rather_than_only_what(rules):
    for rule in rules["groups"][0]["rules"]:
        desc = rule["annotations"].get("description", "")
        assert len(desc) > 80, "{} has no reasoning attached".format(rule["alert"])


def test_pages_are_outnumbered_by_nothing_and_are_deliberate(rules):
    """Every page is a decision to wake someone. If most rules page, none of
    them mean anything."""
    sev = [r["labels"]["severity"] for r in rules["groups"][0]["rules"]]
    assert sev.count("page") <= len(sev) / 2 + 1


def test_a_dependency_being_down_is_a_ticket_not_a_page(rules):
    """The gateway degrades by design. Waking someone for a dependency the
    design already survives is how a rota stops reading its alerts."""
    by_name = {r["alert"]: r for r in rules["groups"][0]["rules"]}
    assert by_name["PreauthModelServiceDown"]["labels"]["severity"] == "ticket"


def test_the_approval_rate_rule_pages(rules):
    """The one that costs money immediately, and the reason the latency rules
    are not the most important ones here."""
    by_name = {r["alert"]: r for r in rules["groups"][0]["rules"]}
    assert by_name["PreauthApprovalRateShift"]["labels"]["severity"] == "page"


def test_there_is_a_rule_for_latency_getting_better(rules):
    """Killing the model took p99 from 33.63ms to 2.51ms. An improvement with
    no deploy behind it means something stopped happening."""
    names = {r["alert"] for r in rules["groups"][0]["rules"]}
    assert "PreauthLatencyImprovedSuspiciously" in names


def test_there_is_a_rule_for_silence(rules):
    """No traffic and no service look identical on every other panel."""
    names = {r["alert"] for r in rules["groups"][0]["rules"]}
    assert "PreauthNoTraffic" in names


# ------------------------------------------------------------------ gauges
def test_a_gauge_goes_down_as_well_as_up():
    """A counter cannot express a buffer that drains, and current depth is the
    only number an operator can act on."""
    reg = Registry()
    g = reg.gauge("gateway_audit_buffer_size", "depth")
    g.set(180_053)
    assert "gateway_audit_buffer_size 180053" in reg.render()
    g.set(0)
    assert "gateway_audit_buffer_size 0" in reg.render()


def test_gauges_render_with_a_type_line():
    reg = Registry()
    reg.gauge("gateway_audit_wal_unshipped", "unshipped").set(12)
    assert "# TYPE gateway_audit_wal_unshipped gauge" in reg.render()


# --------------------------------------------------------------- dashboard
DASHBOARD = Path(__file__).resolve().parents[1] / "ops" / "dashboard.json"


def test_every_metric_on_the_dashboard_is_exported(exported):
    """Same drift problem, same fix. A panel querying a metric nobody emits
    renders an empty graph, and an empty graph reads as 'nothing is happening'
    rather than as 'this panel is broken'."""
    import json

    board = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    exported_bases = {_base(m) for m in exported}
    missing = set()
    for panel in board["panels"]:
        for target in panel.get("targets", []):
            for name in METRIC.findall(target["expr"]):
                if _base(name) not in exported_bases:
                    missing.add(name)
    assert not missing, "dashboard queries metrics /metrics does not emit: {}".format(
        sorted(missing))


def test_the_first_panel_is_request_rate_not_latency():
    """Panel order is the argument. During the incident this service is most
    likely to have -- the model dying -- latency IMPROVES, so a dashboard that
    opens on a latency graph teaches its readers the wrong question."""
    import json

    board = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    first = min(board["panels"], key=lambda p: (p["gridPos"]["y"], p["gridPos"]["x"]))
    assert "rate" in first["title"].lower()
    assert "latency" not in first["title"].lower()


def test_every_panel_explains_itself():
    import json

    board = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    for panel in board["panels"]:
        assert len(panel.get("description", "")) > 60, panel["title"]
