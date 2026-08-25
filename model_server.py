"""The model service, as a separate process serving two transports.

It burns real CPU rather than sleeping. That is the whole point: a sleeping stub
releases the GIL and never competes for a core, so a load test against it finds
no knee no matter how hard it is driven. A scoring model is CPU-bound work, and
modelling it as I/O produces a latency curve that flatters the system in exactly
the region the SLO cares about.

Run standalone:  python model_server.py --http-port 8711 --binary-port 8712
"""
from __future__ import annotations

import argparse
import json
import math
import socket
import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CPU_MS = 8.0
MODEL_VERSION = "preauth-model-0.3.0"


def burn_and_score(features: dict, cpu_ms: float) -> float:
    """Deterministic score plus a calibrated amount of real CPU work.

    The loop is a stand-in for the tree traversals a real GBM does. It has to be
    CPU, not sleep: the difference decides whether a load test can find a knee.
    """
    deadline = _now() + cpu_ms / 1000.0
    acc = 0.0
    i = 0
    while _now() < deadline:
        i += 1
        acc += math.sqrt(i % 1000 + 1) * math.sin(i)
    amount = float(features.get("amount_minor", 0))
    device = str(features.get("device_id", ""))
    z = (amount / 200_000.0
         + (0.4 if device.endswith("9") else 0.0)
         + (abs(acc) % 1.0) * 0.05)
    return min(0.999, max(0.0, z))


def _now() -> float:
    import time
    return time.perf_counter()


class Handler(BaseHTTPRequestHandler):
    cpu_ms = CPU_MS
    protocol_version = "HTTP/1.1"        # keep-alive, so the pool is reused

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        try:
            features = json.loads(body)
            score = burn_and_score(features, self.cpu_ms)
            payload = json.dumps({"score": score,
                                  "model_version": MODEL_VERSION}).encode()
            self.send_response(200)
        except Exception as exc:
            payload = json.dumps({"error": str(exc)}).encode()
            self.send_response(500)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass                              # access logs would dominate the trace


def serve_binary(port: int, cpu_ms: float) -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(256)

    def handle(conn: socket.socket) -> None:
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            while True:
                header = _recv_exact(conn, 4)
                if header is None:
                    return
                (n,) = struct.unpack("!I", header)
                body = _recv_exact(conn, n)
                if body is None:
                    return
                score = burn_and_score(json.loads(body), cpu_ms)
                out = json.dumps({"score": score,
                                  "model_version": MODEL_VERSION}).encode()
                conn.sendall(struct.pack("!I", len(out)) + out)
        except Exception:
            pass
        finally:
            conn.close()

    while True:
        conn, _addr = srv.accept()
        threading.Thread(target=handle, args=(conn,), daemon=True).start()


def _recv_exact(sock: socket.socket, n: int):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


# --------------------------------------------------------------------- gRPC
#
# REAL gRPC, WITH JSON PAYLOADS INSTEAD OF PROTOBUF, and the substitution is
# declared rather than glossed. `grpcio-tools` will not build on this Python --
# its protoc wheel fails -- so there is no generated stub. gRPC's generic
# handler API takes raw byte serializers, which is enough to run the actual
# protocol without codegen.
#
# WHAT THAT COSTS: protobuf's compact binary encoding and schema-enforced
# contracts. JSON is larger on the wire, so any BYTES figure here understates
# what real gRPC would do.
#
# WHAT IT DOES NOT COST, and this is the part the comparison is about: HTTP/2
# multiplexing, per-stream flow control, real deadlines with cancellation
# propagation, connection reuse, and status codes. `run_transports.py` found
# that hand-rolled length-prefixed binary framing wins the latency
# microbenchmark and LOSES the failure mode -- 95 timeouts against HTTP's 14 at
# 32 concurrent callers. Those mechanisms are the reason to expect gRPC to
# behave differently under concurrency, and they are all present here.
def serve_grpc(port: int, cpu_ms: float) -> None:
    import grpc
    from concurrent import futures

    def score(request: bytes, context) -> bytes:
        features = json.loads(request.decode())
        s = burn_and_score(features, cpu_ms)
        return json.dumps({"score": s}).encode()

    handler = grpc.method_handlers_generic_handler(
        "preauth.Model",
        {"Score": grpc.unary_unary_rpc_method_handler(
            score,
            request_deserializer=lambda b: b,
            response_serializer=lambda b: b)})

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32),
                         handlers=(handler,))
    server.add_insecure_port("127.0.0.1:{}".format(port))
    server.start()
    server.wait_for_termination()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--http-port", type=int, default=8711)
    ap.add_argument("--binary-port", type=int, default=8712)
    ap.add_argument("--grpc-port", type=int, default=8713)
    ap.add_argument("--cpu-ms", type=float, default=CPU_MS)
    args = ap.parse_args()

    Handler.cpu_ms = args.cpu_ms
    threading.Thread(
        target=serve_binary, args=(args.binary_port, args.cpu_ms),
        daemon=True).start()
    try:
        threading.Thread(
            target=serve_grpc, args=(args.grpc_port, args.cpu_ms),
            daemon=True).start()
    except ImportError:
        # Reported rather than silently skipped: a transport comparison missing
        # a transport must say which one, or the table reads as complete.
        print("grpcio not installed -- gRPC transport not served")
    ThreadingHTTPServer(("127.0.0.1", args.http_port), Handler).serve_forever()


if __name__ == "__main__":
    main()
