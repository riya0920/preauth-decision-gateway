"""Model scoring over a real transport, in a separate PROCESS.

Until now the "model service" was an object in the gateway's own process that
slept. Sleeping releases the GIL, so it never competed for CPU and the load
curve never found a knee -- the harness was measuring a system with no
contention in it.

A separate process changes three things that matter, all of which were missing:

  REAL SERIALISATION   the request and response are encoded and decoded. On a
                       100ms budget with a 30ms model allocation, JSON encode +
                       decode is not free and belongs in the measurement.
  REAL TRANSPORT       a loopback TCP round trip, with the connection pool,
                       Nagle interaction and kernel scheduling that implies.
  REAL CPU CONTENTION  the model burns actual CPU in another process. Under
                       load the OS has to schedule it against the gateway, which
                       is exactly the pressure that produces a knee.

Two transports are implemented so the spec's "measure both, keep the winner"
can actually be answered:

  HTTP   one request per scoring call over a pooled keep-alive connection.
  UDS-ish  a length-prefixed binary framing over a raw TCP socket, standing in
           for gRPC. It is not gRPC -- no protobuf, no HTTP/2 multiplexing --
           and it is labelled that way. What it isolates is the cost of HTTP
           framing versus a minimal binary frame on the same loopback.

Calling it gRPC would be the easy lie. What this measures is the framing
overhead, which is most of the gap gRPC would close, and the README says so.
"""
from __future__ import annotations

import json
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class ModelTransportError(Exception):
    pass


class HttpModelClient:
    """Pooled keep-alive HTTP client."""

    def __init__(self, base_url: str, timeout_s: float = 0.5):
        import httpx

        self.base_url = base_url
        self.timeout_s = timeout_s
        self._client = httpx.Client(
            base_url=base_url, timeout=timeout_s,
            limits=__import__("httpx").Limits(max_keepalive_connections=64,
                                              max_connections=128))
        self.name = "http"

    def score(self, features: dict) -> float:
        try:
            r = self._client.post("/score", json=features)
            r.raise_for_status()
            return float(r.json()["score"])
        except Exception as exc:
            raise ModelTransportError(str(exc)) from exc

    def close(self) -> None:
        self._client.close()


class BinaryModelClient:
    """Length-prefixed binary framing over raw TCP. A gRPC stand-in, not gRPC.

    One socket per client instance, so callers must not share an instance across
    threads without external synchronisation -- a shared socket interleaves two
    responses on one stream and both callers get garbage. A real gRPC channel
    multiplexes with stream ids; this deliberately does not, and the constraint
    is stated rather than discovered.
    """

    def __init__(self, host: str, port: int, timeout_s: float = 0.5):
        self.addr = (host, port)
        self.timeout_s = timeout_s
        self.name = "binary"
        self._sock: socket.socket | None = None

    def _connect(self) -> socket.socket:
        if self._sock is None:
            s = socket.create_connection(self.addr, timeout=self.timeout_s)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._sock = s
        return self._sock

    def score(self, features: dict) -> float:
        try:
            s = self._connect()
            payload = json.dumps(features).encode()
            s.sendall(struct.pack("!I", len(payload)) + payload)
            header = self._recv_exact(s, 4)
            (n,) = struct.unpack("!I", header)
            body = self._recv_exact(s, n)
            return float(json.loads(body)["score"])
        except Exception as exc:
            self._sock = None            # force reconnect on the next call
            raise ModelTransportError(str(exc)) from exc

    @staticmethod
    def _recv_exact(sock: socket.socket, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ModelTransportError("connection closed mid-frame")
            buf += chunk
        return buf

    def close(self) -> None:
        if self._sock:
            self._sock.close()
            self._sock = None


class ModelProcess:
    """Spawns model_server.py as a child process and waits for readiness."""

    def __init__(self, http_port: int = 8711, binary_port: int = 8712,
                 cpu_ms: float = 8.0):
        self.http_port = http_port
        self.binary_port = binary_port
        self.cpu_ms = cpu_ms
        self.proc: subprocess.Popen | None = None

    def start(self, wait_s: float = 20.0) -> None:
        self.proc = subprocess.Popen(
            [sys.executable, str(ROOT / "model_server.py"),
             "--http-port", str(self.http_port),
             "--binary-port", str(self.binary_port),
             "--cpu-ms", str(self.cpu_ms)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + wait_s
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.binary_port),
                                              timeout=0.5):
                    return
            except OSError:
                time.sleep(0.15)
        self.stop()
        raise ModelTransportError("model process did not become ready")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.proc = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.stop()


class RemoteModelService:
    """Adapter presenting a remote model with the same surface the Gateway used
    for the in-process stub, so swapping transports is a constructor change.

    `up` is kept as an attribute rather than probed, because the chaos drills
    flip it to simulate an outage. A real deployment would infer liveness from
    the circuit breaker's own failure count, which is already there.
    """

    def __init__(self, client, model_version: str = "preauth-model-0.3.0"):
        self.client = client
        self.up = True
        self.slow = False
        self.calls = 0
        self.model_version = model_version

    def score(self, req) -> float:
        self.calls += 1
        if not self.up:
            raise ConnectionError("model service unavailable")
        return self.client.score({
            "amount_minor": req.amount_minor,
            "device_id": req.device_id,
            "card_id": req.card_id,
        })
