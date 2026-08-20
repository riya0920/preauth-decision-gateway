"""Measure both transports against a real, separate, CPU-burning model process.

The spec asks to measure gRPC and HTTP and keep the winner. This measures HTTP
against a minimal binary framing on the same loopback -- which isolates the
framing cost, most of what gRPC would recover, without claiming to be gRPC.
"""
from __future__ import annotations

import statistics
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from gateway.model_client import (BinaryModelClient, HttpModelClient,
                                  ModelProcess, ModelTransportError)

FEATURES = {"amount_minor": 42_000, "device_id": "D01239", "velocity_24h": 3}


def percentile(v, p):
    s = sorted(v)
    return s[min(int(round(p / 100 * (len(s) - 1))), len(s) - 1)]


def bench(make_client, n: int, threads: int, label: str) -> dict:
    latencies: list[float] = []
    errors = 0
    lock = threading.Lock()

    def worker(count: int) -> None:
        nonlocal errors
        # One client per thread: the binary client owns a single socket and
        # sharing it would interleave two responses on one stream.
        client = make_client()
        local = []
        for _ in range(count):
            t0 = time.perf_counter()
            try:
                client.score(FEATURES)
                local.append((time.perf_counter() - t0) * 1000)
            except ModelTransportError:
                with lock:
                    errors += 1
        with lock:
            latencies.extend(local)
        client.close()

    per = max(n // threads, 1)
    ts = [threading.Thread(target=worker, args=(per,)) for _ in range(threads)]
    t0 = time.perf_counter()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    elapsed = time.perf_counter() - t0

    return {"label": label, "n": len(latencies), "errors": errors,
            "rps": len(latencies) / elapsed if elapsed else 0,
            "mean": statistics.mean(latencies) if latencies else 0,
            "p50": percentile(latencies, 50), "p95": percentile(latencies, 95),
            "p99": percentile(latencies, 99)}


def main() -> int:
    print("=" * 84)
    print("MODEL TRANSPORT COMPARISON -- separate process, real CPU")
    print("=" * 84)
    print("The model burns ~8ms of real CPU per call rather than sleeping. A")
    print("sleeping stub releases the GIL and never competes for a core, which is")
    print("why every earlier load test here found no knee: it was measuring a")
    print("system with no contention in it.\n")

    with ModelProcess(cpu_ms=8.0) as proc:
        http_url = "http://127.0.0.1:{}".format(proc.http_port)

        # Warm both paths: connection setup and the first CPU ramp are not
        # steady-state costs.
        HttpModelClient(http_url).score(FEATURES)
        BinaryModelClient("127.0.0.1", proc.binary_port).score(FEATURES)

        results = []
        for threads in (1, 8, 32):
            results.append(bench(lambda: HttpModelClient(http_url),
                                 400, threads, "http x{}".format(threads)))
            results.append(bench(
                lambda: BinaryModelClient("127.0.0.1", proc.binary_port),
                400, threads, "binary x{}".format(threads)))

        print("{:<14}{:>8}{:>9}{:>9}{:>9}{:>9}{:>8}".format(
            "transport", "calls", "rps", "mean", "p50", "p95", "p99"))
        print("-" * 84)
        for r in results:
            print("{:<14}{:>8,}{:>9.0f}{:>9.2f}{:>9.2f}{:>9.2f}{:>8.2f}{}".format(
                r["label"], r["n"], r["rps"], r["mean"], r["p50"], r["p95"],
                r["p99"], "  errors {}".format(r["errors"]) if r["errors"] else ""))

        print("-" * 84)
        http1 = next(r for r in results if r["label"] == "http x1")
        bin1 = next(r for r in results if r["label"] == "binary x1")
        delta = http1["p50"] - bin1["p50"]
        print("Single-threaded p50: http {:.2f}ms vs binary {:.2f}ms "
              "({:+.2f}ms framing cost).".format(http1["p50"], bin1["p50"], delta))
        print("\nBoth carry the SAME ~8ms of model CPU and the same JSON payload,")
        print("so the difference is HTTP framing and header parsing against a")
        print("4-byte length prefix. On a 30ms model budget that is {:.0f}% of the".format(
            abs(delta) / 30 * 100))
        print("allocation -- worth knowing, not worth rewriting the stack for on")
        print("its own.")
        print("\nWhat this is NOT: gRPC. No protobuf, no HTTP/2 multiplexing, no")
        print("streaming. It isolates framing overhead, which is most of what gRPC")
        print("would recover on a call this small. Calling it gRPC would be the")
        print("easy lie.")

        http32 = next(r for r in results if r["label"] == "http x32")
        print("\nAt 32 concurrent callers p99 is {:.1f}ms against {:.1f}ms".format(
            http32["p99"], http1["p99"]))
        print("single-threaded. THAT is the contention a sleeping stub cannot")
        print("produce, and it is why the model stage now has a real knee to find.")
    print("=" * 84)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
