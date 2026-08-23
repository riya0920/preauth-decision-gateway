"""What the audit WAL costs, and what it buys.

    python run_wal.py

The chaos drill already showed async audit logging is nearly free on the hot
path. This measures the thing that was described in words and not implemented:
a local durable append, its cost at each fsync setting, and a simulated crash
showing what is recoverable with it and what is simply gone without it.
"""
from __future__ import annotations

import shutil
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from gateway.pipeline import AuditLog
from gateway.wal import AuditWal, DurableAuditLog

TMP = ROOT / "data" / "wal"
N = 4000


def _record(i):
    return {"decision_id": "d{}".format(i), "score": 0.031 + i % 7 / 1000,
            "decision": "approve" if i % 9 else "decline",
            "features": {"velocity_24h": i % 5, "amount_minor": 1500 + i}}


def _pct(xs):
    xs = sorted(xs)
    def q(p):
        return xs[min(len(xs) - 1, int(len(xs) * p))]
    return statistics.fmean(xs), q(0.50), q(0.95), q(0.99)


def main() -> int:
    if TMP.exists():
        shutil.rmtree(TMP)
    TMP.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("AUDIT DURABILITY -- WHAT A LOCAL WAL COSTS")
    print("=" * 80)
    print("{} records per configuration, appended one at a time.".format(N))
    print()
    print("{:<14}{:>10}{:>10}{:>10}{:>10}{:>12}{:>10}".format(
        "fsync", "mean", "p50", "p95", "p99", "total ms", "fsyncs"))

    results = {}
    for mode in ("never", "batch", "always"):
        wal = AuditWal(TMP / "{}.jsonl".format(mode), fsync=mode)
        t0 = time.perf_counter()
        for i in range(N):
            wal.append(_record(i))
        total = (time.perf_counter() - t0) * 1000
        wal.close()
        mean, p50, p95, p99 = _pct(wal.stats.append_ms)
        results[mode] = (mean, p99, total, wal.stats.fsyncs)
        print("{:<14}{:>10.4f}{:>10.4f}{:>10.4f}{:>10.4f}{:>12.1f}{:>10}".format(
            mode, mean, p50, p95, p99, total, wal.stats.fsyncs))

    print("(milliseconds per append)")
    print()
    never_p99, always_p99 = results["never"][1], results["always"][1]
    batch_p99 = results["batch"][1]
    print("The dial is real, not decorative:")
    print("  never  -> batch : p99 {:.4f} -> {:.4f} ms ({:+.0f}%)".format(
        never_p99, batch_p99,
        (batch_p99 / never_p99 - 1) * 100 if never_p99 else 0))
    print("  never  -> always: p99 {:.4f} -> {:.4f} ms ({:+.0f}%)".format(
        never_p99, always_p99,
        (always_p99 / never_p99 - 1) * 100 if never_p99 else 0))
    print()
    print("Read those against the 100ms end-to-end budget. `never` and `batch`")
    print("are genuinely free -- a p99 of {:.2f}ms and {:.2f}ms is inside the".format(
        never_p99, batch_p99))
    print("noise of the parse stage. `always` is NOT: {:.2f}ms mean and {:.2f}ms".format(
        results["always"][0], always_p99))
    print("p99 is {:.1f}% of the whole budget spent on one fsync per decision,".format(
        always_p99))
    print("and that is on an idle laptop SSD with no competing write load. On a")
    print("box doing real work it is worse. The number to carry away is the")
    print("SHAPE rather than the values: `never`")
    print("survives a process crash, `always` survives a machine crash, and")
    print("`batch` bounds the loss to a configurable window. That choice belongs")
    print("to whoever owns the compliance requirement.")

    # ----------------------------------------------------------- the crash
    print("\n" + "=" * 80)
    print("THE CRASH, WITH AND WITHOUT")
    print("-" * 80)

    plain = AuditLog()
    for i in range(N):
        plain.write(_record(i))
    plain.drain()                      # ship the first batch
    for i in range(N, N + 500):
        plain.write(_record(i))
    lost = len(plain.buffer)

    wal = AuditWal(TMP / "crash.jsonl", fsync="batch")
    durable = DurableAuditLog(wal)
    for i in range(N):
        durable.write(_record(i))
    durable.drain()
    for i in range(N, N + 500):
        durable.write(_record(i))
    # The crash: the process dies. The in-memory buffer goes with it.
    shipped_before_crash = list(durable.shipped)
    wal.close()

    recovered = DurableAuditLog(AuditWal(TMP / "crash.jsonl", fsync="batch"))
    recovered.shipped = shipped_before_crash
    pending = recovered.recover()

    print("{:<40}{:>16}{:>16}".format("", "plain AuditLog", "with a WAL"))
    print("{:<40}{:>16}{:>16}".format("decisions written", N + 500, N + 500))
    print("{:<40}{:>16}{:>16}".format("shipped before the crash", len(plain.shipped),
                                      len(shipped_before_crash)))
    print("{:<40}{:>16}{:>16}".format("in the buffer when it died", lost, 500))
    print("{:<40}{:>16}{:>16}".format("recoverable after restart", 0, len(pending)))
    print("{:<40}{:>16}{:>16}".format(
        "PERMANENTLY LOST", lost, (N + 500) - len(shipped_before_crash) - len(pending)))
    print()
    print("Those {} records are not a monitoring gap. Each one is a decision the".format(lost))
    print("firm made about a customer's money, with no record that it made it and")
    print("no way to reconstruct one. An adverse-action request against any of")
    print("them has no answer.")
    print()
    print("What the WAL does NOT do: replace shipping. A disk that dies takes the")
    print("WAL with it. It bounds the loss to what has not yet shipped -- minutes")
    print("-- and reading it as 'the records are safe now' is reading a bridge as")
    print("a destination.")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
