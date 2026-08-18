"""The latency budget, as a first-class object.

A p99 SLO with no per-stage allocation is a wish. The budget below is the
contract each stage signs, and every stage records its actual distribution so the
measured-vs-budget table can be printed. That table -- not the average latency --
is the artifact.

Budget for a 100ms end-to-end p99:

    parse + validate      5ms
    hot rules            15ms
    velocity checks      20ms
    feature assembly     20ms
    model score          30ms
    decision + log       10ms
    ------------------------
    allocated           100ms

Note there is no headroom line. Reserving headroom inside a p99 budget is
self-deception: the headroom IS the difference between the p99 you promise and
the p99 you measure, and writing it as a line item just hides which stage is
actually eating it.
"""
from __future__ import annotations

import bisect
from dataclasses import dataclass, field

BUDGET_MS = {
    "parse_validate": 5.0,
    "hot_rules": 15.0,
    "velocity": 20.0,
    "features": 20.0,
    "model": 30.0,
    "decide_log": 10.0,
}
SLO_P99_MS = 100.0


@dataclass
class Histogram:
    samples: list[float] = field(default_factory=list)

    def record(self, ms: float) -> None:
        self.samples.append(ms)

    def pct(self, p: float) -> float:
        if not self.samples:
            return 0.0
        s = sorted(self.samples)
        k = min(int(round(p / 100 * (len(s) - 1))), len(s) - 1)
        return s[k]

    def __len__(self) -> int:
        return len(self.samples)


class Budget:
    def __init__(self, budget_ms: dict[str, float] | None = None):
        self.budget = dict(budget_ms or BUDGET_MS)
        self.stages = {k: Histogram() for k in self.budget}
        self.total = Histogram()

    def record(self, stage: str, ms: float) -> None:
        self.stages.setdefault(stage, Histogram()).record(ms)

    def record_total(self, ms: float) -> None:
        self.total.record(ms)

    def table(self) -> list[dict]:
        rows = []
        for stage, budget in self.budget.items():
            h = self.stages[stage]
            p99 = h.pct(99)
            rows.append({
                "stage": stage,
                "budget_ms": budget,
                "p50_ms": h.pct(50),
                "p95_ms": h.pct(95),
                "p99_ms": p99,
                "headroom_ms": budget - p99,
                "over_budget": p99 > budget,
                "n": len(h),
            })
        return rows

    def render(self) -> str:
        lines = ["{:<18}{:>10}{:>10}{:>10}{:>10}{:>12}  {}".format(
            "stage", "budget", "p50", "p95", "p99", "headroom", "status")]
        lines.append("-" * 84)
        for r in self.table():
            lines.append("{:<18}{:>10.1f}{:>10.2f}{:>10.2f}{:>10.2f}{:>12.2f}  {}".format(
                r["stage"], r["budget_ms"], r["p50_ms"], r["p95_ms"], r["p99_ms"],
                r["headroom_ms"], "OVER BUDGET" if r["over_budget"] else "ok"))
        lines.append("-" * 84)
        allocated = sum(self.budget.values())
        p99 = self.total.pct(99)
        lines.append("{:<18}{:>10.1f}{:>10.2f}{:>10.2f}{:>10.2f}{:>12.2f}  {}".format(
            "END TO END", allocated, self.total.pct(50), self.total.pct(95), p99,
            SLO_P99_MS - p99, "SLO MET" if p99 <= SLO_P99_MS else "SLO BREACHED"))
        return "\n".join(lines)
