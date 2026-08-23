"""Prometheus text-format metrics export.

Written by hand rather than pulled from prometheus_client, because the exposition
format is twelve lines of code and the dependency would obscure the one thing
worth showing: **histograms, not averages.**

An average latency is close to useless for an SLO. A p99 target is a statement
about the tail, and a mean hides the tail by construction -- 99 requests at 5ms
and one at 2000ms averages under 25ms and blows the SLO. So every latency here
is exported as a bucketed histogram, which is what lets Prometheus compute a
real quantile across instances (you cannot average p99s from several boxes; you
CAN add their bucket counts).

Bucket boundaries are chosen around the budget in budget.py, not log-spaced by
habit: they cluster where the decisions are (5-100ms) so the quantile estimate
is precise exactly where the SLO lives.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field

# Chosen around the 100ms SLO and the per-stage budget, not log-spaced.
LATENCY_BUCKETS_MS = [1, 2, 5, 10, 20, 30, 40, 50, 75, 100, 150, 250, 500, 1000]


@dataclass
class Histogram:
    name: str
    help_text: str
    buckets: list[float] = field(default_factory=lambda: list(LATENCY_BUCKETS_MS))
    counts: dict[float, int] = field(default_factory=dict)
    total: int = 0
    sum_value: float = 0.0

    def __post_init__(self):
        self.counts = {b: 0 for b in self.buckets}

    def observe(self, value: float) -> None:
        self.total += 1
        self.sum_value += value
        for b in self.buckets:
            if value <= b:
                self.counts[b] += 1

    def quantile(self, q: float) -> float:
        """Bucket-interpolated quantile -- the same approximation Prometheus
        makes, so the number here matches what a dashboard would show rather
        than being a more precise number nobody else can see."""
        if not self.total:
            return 0.0
        target = q * self.total
        prev_b, prev_c = 0.0, 0
        for b in self.buckets:
            c = self.counts[b]
            if c >= target:
                if c == prev_c:
                    return b
                frac = (target - prev_c) / (c - prev_c)
                return prev_b + frac * (b - prev_b)
            prev_b, prev_c = b, c
        return self.buckets[-1]


@dataclass
class Counter:
    name: str
    help_text: str
    values: dict[tuple, int] = field(default_factory=dict)

    def inc(self, labels: dict | None = None, by: int = 1) -> None:
        key = tuple(sorted((labels or {}).items()))
        self.values[key] = self.values.get(key, 0) + by


@dataclass
class Gauge:
    """A value that goes up AND down -- queue depth, buffer size, backlog.

    Counters and histograms cannot express it. A buffer that grows to 180k and
    is then drained has a counter that keeps climbing and says nothing about
    the current depth, which is the only number an operator can act on.
    """
    name: str
    help_text: str
    value: float = 0.0

    def set(self, value: float) -> None:
        self.value = float(value)


class Registry:
    def __init__(self):
        self._lock = threading.Lock()
        self.histograms: dict[str, Histogram] = {}
        self.counters: dict[str, Counter] = {}
        self.gauges: dict[str, Gauge] = {}

    def histogram(self, name: str, help_text: str = "") -> Histogram:
        with self._lock:
            if name not in self.histograms:
                self.histograms[name] = Histogram(name, help_text)
            return self.histograms[name]

    def counter(self, name: str, help_text: str = "") -> Counter:
        with self._lock:
            if name not in self.counters:
                self.counters[name] = Counter(name, help_text)
            return self.counters[name]

    def gauge(self, name: str, help_text: str = "") -> Gauge:
        with self._lock:
            if name not in self.gauges:
                self.gauges[name] = Gauge(name, help_text)
            return self.gauges[name]

    def render(self) -> str:
        """Prometheus text exposition format (version 0.0.4)."""
        lines = []
        for h in self.histograms.values():
            lines.append("# HELP {} {}".format(h.name, h.help_text))
            lines.append("# TYPE {} histogram".format(h.name))
            for b in h.buckets:
                lines.append('{}_bucket{{le="{}"}} {}'.format(h.name, b, h.counts[b]))
            lines.append('{}_bucket{{le="+Inf"}} {}'.format(h.name, h.total))
            lines.append("{}_sum {:.6f}".format(h.name, h.sum_value))
            lines.append("{}_count {}".format(h.name, h.total))
        for c in self.counters.values():
            lines.append("# HELP {} {}".format(c.name, c.help_text))
            lines.append("# TYPE {} counter".format(c.name))
            for key, v in sorted(c.values.items()):
                if key:
                    labels = ",".join('{}="{}"'.format(k, val) for k, val in key)
                    lines.append("{}{{{}}} {}".format(c.name, labels, v))
                else:
                    lines.append("{} {}".format(c.name, v))
        for g in self.gauges.values():
            lines.append("# HELP {} {}".format(g.name, g.help_text))
            lines.append("# TYPE {} gauge".format(g.name))
            lines.append("{} {:g}".format(g.name, g.value))
        return "\n".join(lines) + "\n"


REGISTRY = Registry()
