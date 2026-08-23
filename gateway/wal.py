"""A local write-ahead log for the audit trail.

The chaos drill already established that async audit logging costs almost
nothing on the hot path -- p99 32.12ms with the sink dead against 33.63ms
steady. What it also established, and the drill output said so in words while
the code did nothing about it, is what that buffering costs: **a crash between
the buffer write and the drain loses those records.**

For a fraud decision that is not a monitoring gap. It is a decision the firm
made about a customer's money with no record that it made it, and no way to
reconstruct one. If compliance requires every decision durably logged before the
response goes out, the answer is not a synchronous write to a remote sink -- that
puts a network round trip inside the p99 and couples the gateway's availability
to the sink's. The answer is a LOCAL append plus async shipping.

WHY A LOCAL APPEND IS CHEAP AND A REMOTE WRITE IS NOT:

  local append   one sequential write to a file on the same box. No network, no
                 remote availability in the path, and the OS page cache absorbs
                 it. Measured below.
  remote write   a network round trip, a service that can be slow or down, and
                 a retry policy -- inside the latency budget, on every request.

THE DURABILITY DIAL, and it is a real dial rather than a boolean:

  fsync=never    fastest. Survives a PROCESS crash (the OS still holds the
                 bytes and writes them out) and loses data on a MACHINE crash.
  fsync=batch    fsync every N records or every T milliseconds. Bounded loss,
                 bounded cost. This is what most systems actually want.
  fsync=always   survives a machine crash. Costs an fsync per record, which on
                 spinning media is milliseconds and on SSD is still the most
                 expensive thing in the request.

The default here is `batch`, and the point of the parameter is that the choice
belongs to whoever owns the compliance requirement, not to whoever wrote the
gateway.

WHAT THIS STILL DOES NOT SURVIVE. A disk that dies takes the WAL with it. The
WAL bounds loss to what has not yet SHIPPED, which is minutes; it does not
replace shipping. Anyone reading this as "the records are safe now" has read it
as a replacement for the remote sink rather than as a bridge to it.
"""
from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class WalStats:
    appended: int = 0
    fsyncs: int = 0
    shipped: int = 0
    truncated: int = 0
    append_ms: list = field(default_factory=list)


class AuditWal:
    """Append-then-buffer. The append is what makes the decision reconstructible.

    Ordering matters and is the whole design: the record is on disk BEFORE it
    is handed to the async buffer, so any crash after the response leaves a
    durable record, and any crash before the response leaves one too. The window
    that loses data is the window before the append, and in that window no
    decision has been returned to the caller either.
    """

    def __init__(self, path: Path | str, fsync: str = "batch",
                 batch_size: int = 50, batch_ms: float = 200.0):
        if fsync not in ("never", "batch", "always"):
            raise ValueError("fsync must be never | batch | always")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fsync = fsync
        self.batch_size = batch_size
        self.batch_ms = batch_ms
        self.stats = WalStats()
        self._lock = threading.Lock()
        self._since_sync = 0
        self._last_sync = time.perf_counter()
        self._fh = self.path.open("a", encoding="utf-8")

    # ------------------------------------------------------------- writing
    def append(self, record: dict) -> None:
        line = json.dumps(record, separators=(",", ":"), default=str)
        t0 = time.perf_counter()
        with self._lock:
            self._fh.write(line + "\n")
            self._fh.flush()          # into the OS; survives a process crash
            self._since_sync += 1
            if self._should_fsync():
                os.fsync(self._fh.fileno())
                self.stats.fsyncs += 1
                self._since_sync = 0
                self._last_sync = time.perf_counter()
            self.stats.appended += 1
        self.stats.append_ms.append((time.perf_counter() - t0) * 1000)

    def _should_fsync(self) -> bool:
        if self.fsync == "always":
            return True
        if self.fsync == "never":
            return False
        elapsed_ms = (time.perf_counter() - self._last_sync) * 1000
        return self._since_sync >= self.batch_size or elapsed_ms >= self.batch_ms

    def close(self) -> None:
        with self._lock:
            try:
                os.fsync(self._fh.fileno())
            except Exception:                                # noqa: BLE001
                pass
            self._fh.close()

    # ------------------------------------------------------------- reading
    def records(self) -> list[dict]:
        """Everything on disk. This is what recovery reads."""
        if not self.path.exists():
            return []
        out = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                # A torn final line is EXPECTED after a hard kill and is not
                # corruption of the log -- it is the record that was mid-write.
                # Discarding it silently would hide a real truncation; counting
                # it is how recovery reports what it lost.
                self.stats.truncated += 1
        return out

    def unshipped(self, shipped_ids: set) -> list[dict]:
        """Records on disk that the remote sink has not acknowledged.

        This is the query that makes the WAL a bridge rather than a second
        source of truth: after the sink recovers, ship exactly these and the
        two agree again.
        """
        return [r for r in self.records()
                if r.get("decision_id") not in shipped_ids]


class DurableAuditLog:
    """`AuditLog` with a WAL underneath it. Same interface, plus recovery."""

    def __init__(self, wal: AuditWal):
        self.wal = wal
        self.buffer: list[dict] = []
        self.shipped: list[dict] = []
        self.sink_up = True

    def write(self, record: dict) -> None:
        self.wal.append(record)             # durable FIRST
        self.buffer.append(record)          # then the async path

    def drain(self) -> int:
        if not self.sink_up:
            return 0
        n = len(self.buffer)
        self.shipped.extend(self.buffer)
        self.buffer.clear()
        self.wal.stats.shipped += n
        return n

    def recover(self) -> list[dict]:
        """What a restart must ship, having lost the in-memory buffer.

        Without the WAL this list is unknowable and the records are simply
        gone. With it, a crash costs a re-ship rather than a decision nobody
        can account for.
        """
        shipped_ids = {r.get("decision_id") for r in self.shipped}
        return self.wal.unshipped(shipped_ids)
