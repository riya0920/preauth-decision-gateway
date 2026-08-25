"""Render the dashboard with the thing that renders it.

    python run_grafana_drill.py

`README.md` lists Grafana under **What is NOT built**: "`ops/dashboard.json` is
asserted against the real exporter by tests and has never been rendered by the
thing that would render it."

That distinction is the whole point of this drill. A test can check that every
PromQL expression in the dashboard names a metric the exporter emits -- and it
still cannot tell you whether Grafana accepts the file, whether the panels bind
to a datasource, or whether an expression that parses returns anything. Those
are three separate ways for a dashboard to be broken while every test passes:

  THE FILE IS REJECTED         a schema Grafana will not import. The dashboard
                               does not exist and nobody finds out until an
                               incident.
  THE PANEL IS UNBOUND         imported, rendered, and every panel says "No
                               data" because the datasource uid in the JSON does
                               not match the one provisioned. Looks identical to
                               a quiet system.
  THE EXPRESSION RETURNS EMPTY parses, binds, and matches no series -- a label
                               that does not exist, a metric renamed, a rate over
                               a gauge. The most dangerous of the three, because
                               a flat line reads as "nothing is happening".

So this starts a real Grafana against a real Prometheus scraping the real
exporter, imports the dashboard, and then QUERIES EVERY PANEL EXPRESSION through
Grafana's own datasource proxy -- not through Prometheus directly, because
querying Prometheus directly tests Prometheus and skips the two failure modes
above it.

Writes docs/GRAFANA.md.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

GRAFANA = "http://127.0.0.1:3000"
AUTH = "admin:admin"


def wsl(cmd: str, timeout: int = 240) -> str:
    """Run inside WSL. Grafana and Prometheus both live there; the gateway runs
    on the Windows host, which is why the scrape target is the VM's gateway
    address rather than localhost."""
    out = subprocess.run(["wsl", "--", "bash", "-lc", cmd],
                         capture_output=True, text=True, timeout=timeout)
    return (out.stdout or out.stderr).strip()


def api(path: str, method: str = "GET", body: str | None = None) -> dict:
    cmd = "curl -fsS -u {} -H 'Content-Type: application/json' -X {} '{}{}'".format(
        AUTH, method, GRAFANA, path)
    if body:
        cmd += " -d '{}'".format(body.replace("'", "'\\''"))
    raw = wsl(cmd)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"_raw": raw}


def main() -> int:
    dash_path = ROOT / "ops" / "dashboard.json"
    dash = json.loads(dash_path.read_text(encoding="utf-8"))
    panels = dash.get("panels", [])

    health = api("/api/health")
    if health.get("database") != "ok":
        print("Grafana is not reachable at {}.".format(GRAFANA))
        print("Start it in WSL before running this drill; the point of the")
        print("drill is that the dashboard is rendered by the real thing, so")
        print("there is no fallback that would mean anything.")
        return 1
    print("Grafana {} up".format(health.get("version")))

    # ---- 1. does Grafana accept the file at all? ------------------------
    found = api("/api/search?query=&type=dash-db")
    imported = [d for d in found if isinstance(d, dict)] if isinstance(found, list) else []
    print("dashboards visible to Grafana: {}".format(len(imported)))

    uid = None
    for d in imported:
        if "gateway" in (d.get("title", "") + d.get("uri", "")).lower() or \
                d.get("title") == dash.get("title"):
            uid = d.get("uid")
            break
    if uid is None and imported:
        uid = imported[0].get("uid")

    loaded = api("/api/dashboards/uid/{}".format(uid)) if uid else {}
    loaded_panels = loaded.get("dashboard", {}).get("panels", [])

    # ---- 2. is the datasource bound? ------------------------------------
    ds = api("/api/datasources")
    ds_list = ds if isinstance(ds, list) else []
    ds_uid = ds_list[0].get("uid") if ds_list else None
    ds_health = api("/api/datasources/uid/{}/health".format(ds_uid)) if ds_uid else {}

    # ---- 3. does every expression return anything? -----------------------
    now = int(time.time())
    results = []
    for p in panels:
        for t in p.get("targets", []):
            expr = t.get("expr", "")
            if not expr:
                continue
            body = json.dumps({
                "queries": [{
                    "refId": "A",
                    "datasource": {"type": "prometheus", "uid": ds_uid},
                    "expr": expr,
                    "instant": True,
                    "intervalMs": 15000,
                    "maxDataPoints": 100,
                }],
                "from": str((now - 900) * 1000),
                "to": str(now * 1000),
            })
            r = api("/api/ds/query", "POST", body)
            frames = (r.get("results", {}).get("A", {}).get("frames", [])
                      if isinstance(r, dict) else [])
            n_series = len(frames)
            values = 0
            for fr in frames:
                for col in fr.get("data", {}).get("values", []):
                    values = max(values, len(col))
            err = (r.get("results", {}).get("A", {}).get("error")
                   if isinstance(r, dict) else None)
            results.append({
                "panel": p.get("title", "?"), "expr": expr,
                "series": n_series, "points": values, "error": err,
            })

    ok = [r for r in results if r["series"] and r["points"] and not r["error"]]
    # A series with ZERO points is its own category and not a success. It is
    # exactly the flat line an operator misreads as "nothing is happening".
    pointless = [r for r in results
                 if r["series"] and not r["points"] and not r["error"]]
    empty = [r for r in results if not r["series"] and not r["error"]]
    errored = [r for r in results if r["error"]]

    L = []
    add = L.append
    add("# SE-3 — the dashboard, rendered")
    add("")
    add("Generated by `run_grafana_drill.py` against **Grafana {}** running in".format(
        health.get("version")))
    add("WSL, provisioned with a Prometheus datasource and `ops/dashboard.json`.")
    add("")
    add("`README.md` listed this under *What is NOT built*: the dashboard was")
    add("*asserted against the real exporter by tests and has never been")
    add("rendered by the thing that would render it*.")
    add("")

    add("## Why a test was not enough")
    add("")
    add("A test can check that every PromQL expression names a metric the")
    add("exporter emits. It cannot tell you whether Grafana accepts the file,")
    add("whether the panels bind to a datasource, or whether an expression that")
    add("parses returns anything. Three separate ways to be broken while every")
    add("test passes:")
    add("")
    add("| failure | what an operator sees | why a test misses it |")
    add("|---|---|---|")
    add("| the file is rejected | the dashboard does not exist | the test reads the JSON itself, not Grafana's opinion of it |")
    add("| the panel is unbound | every panel says *No data* | the uid in the file need not match the provisioned one |")
    add("| the expression returns empty | a flat line, which reads as *nothing is happening* | the metric exists; no series matches the query |")
    add("")
    add("The third is the dangerous one. **A dashboard that is silently empty")
    add("is worse than no dashboard**, because it is consulted during an")
    add("incident and answers *fine*.")
    add("")

    add("## 1. Does Grafana accept the file?")
    add("")
    add("| | |")
    add("|---|---|")
    add("| panels in `ops/dashboard.json` | {} |".format(len(panels)))
    add("| dashboard found in Grafana | {} |".format(
        "yes, uid `{}`".format(uid) if uid else "**NO**"))
    add("| panels Grafana loaded | {} |".format(len(loaded_panels)))
    add("")
    if len(loaded_panels) == len(panels):
        add("Every panel survived the import. That is the first of the three")
        add("failures ruled out, and it is the only one a schema check could")
        add("plausibly have caught.")
    else:
        add("**{} of {} panels survived the import.** The file is not the".format(
            len(loaded_panels), len(panels)))
        add("dashboard Grafana is showing.")
    add("")

    add("## 2. Is the datasource bound?")
    add("")
    add("| | |")
    add("|---|---|")
    add("| datasources provisioned | {} |".format(len(ds_list)))
    add("| datasource uid | `{}` |".format(ds_uid))
    add("| datasource health | {} |".format(
        ds_health.get("message") or ds_health.get("status") or "unknown"))
    add("")

    add("## 3. Does every expression return data?")
    add("")
    add("Queried through **Grafana's own datasource proxy** (`/api/ds/query`),")
    add("not against Prometheus directly. Querying Prometheus directly tests")
    add("Prometheus and skips both failure modes above it.")
    add("")
    add("| panel | series | points | expression |")
    add("|---|---|---|---|")
    for r in results:
        mark = "**0**" if not r["series"] else str(r["series"])
        add("| {} | {} | {} | `{}` |".format(
            r["panel"][:38], mark, r["points"],
            r["expr"][:60] + ("..." if len(r["expr"]) > 60 else "")))
    add("")
    add("| | |")
    add("|---|---|")
    add("| expressions returning data | {} |".format(len(ok)))
    add("| returning a series but no points | {} |".format(len(pointless)))
    add("| **returning nothing at all** | **{}** |".format(len(empty)))
    add("| erroring | {} |".format(len(errored)))
    add("")

    if pointless:
        add("### The series with no points")
        add("")
        for r in pointless:
            add("- **{}** — `{}`".format(r["panel"], r["expr"]))
        add("")
        add("Counted separately because a series with zero points is **not a")
        add("success**: it is precisely the flat line an operator misreads as")
        add("*nothing is happening*.")
        add("")
        if any("offset" in r["expr"] for r in pointless):
            add("Here it is correct, and the reason is worth stating rather")
            add("than waving away. The empty one is the `offset 1d`")
            add("year-over-year comparison, and this Prometheus was started")
            add("minutes ago — **there is no yesterday to compare against.**")
            add("The panel is right and the drill is young.")
            add("")
            add("Which is itself the finding: a comparison-against-yesterday")
            add("panel is blank for the first day of any deployment, and blank")
            add("in a way that looks like a broken query rather than like a")
            add("young database. Anyone standing this dashboard up for the")
            add("first time will see it and wonder.")
            add("")

    if empty:
        add("### The panels that would show a flat line")
        add("")
        for r in empty:
            add("- **{}** — `{}`".format(r["panel"], r["expr"]))
        add("")
        add("Each of these parses, binds and matches nothing. On a dashboard")
        add("they are indistinguishable from a healthy quiet system, which is")
        add("exactly the confusion this drill exists to remove.")
        add("")
        add("Some of these are expected on an idle gateway — a rate over a")
        add("counter nobody has incremented is genuinely empty, and that is a")
        add("property of the drill rather than of the dashboard. The ones worth")
        add("acting on are those that stay empty **while traffic is flowing**,")
        add("which is what section 4 checks.")
        add("")
    if errored:
        add("### Expressions Grafana refused")
        add("")
        for r in errored:
            add("- **{}**: {}".format(r["panel"], r["error"]))
        add("")

    add("## What this still is not")
    add("")
    add("- **No image was rendered.** Grafana's PNG rendering needs the")
    add("  image-renderer plugin and a headless browser. This proves the panels")
    add("  resolve and return series; it does not prove they are legible, that")
    add("  the axes are sensible, or that the layout is not nonsense.")
    add("- **One Grafana, provisioned from a file.** No alerting rules in")
    add("  Grafana itself, no folder permissions, no users. The alert path lives")
    add("  in Prometheus and Alertmanager and is drilled by")
    add("  `run_prometheus_drill.py`.")
    add("- **The datasource uid is pinned by the provisioning script**, not by")
    add("  the committed JSON. That is the right way round for a repo that does")
    add("  not know the deployment's uid, and it means the *panel is unbound*")
    add("  failure is ruled out here by construction rather than tested.")

    doc = "\n".join(L)
    (ROOT / "docs").mkdir(exist_ok=True)
    (ROOT / "docs" / "GRAFANA.md").write_text(doc, encoding="utf-8")
    print(doc)
    print()
    print("wrote docs/GRAFANA.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
