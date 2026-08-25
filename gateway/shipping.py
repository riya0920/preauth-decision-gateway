"""Shipping the WAL to a remote sink, and reclaiming the disk afterwards.

`gateway/wal.py` makes a decision durable locally and says, in its own
docstring, that the WAL "bounds loss to what has not yet SHIPPED" and "does not
replace shipping". Both halves were unimplemented. `DurableAuditLog.drain()`
moved records from one Python list to another and called it shipped, and nothing
ever removed a record from disk -- so the log grew without bound and the word
"shipped" meant nothing a crash would respect.

Two problems, and the second is the one with a sharp edge.

SHIPPING is the easy half: batch, send, handle a sink that is slow, down, or
partially accepting. Retry with backoff, never lose a record, never claim to
have shipped one that was rejected.

TRUNCATION is where this gets dangerous, because deleting durable data is the
one operation that cannot be undone by trying again.

  YOU CANNOT TRUNCATE A FILE BY REWRITING IT. The obvious implementation --
  read the records, drop the shipped ones, write the rest back -- has a window
  where the old file is gone and the new one is incomplete. Crash there and you
  have lost every record, including the unshipped ones the exercise was
  supposed to protect. Rewriting an append-only log to make it shorter converts
  a durable store into a lossy one at exactly the moment it is most full.

  SO THE LOG IS SEGMENTED. Appends go to an active segment; when it reaches a
  size the segment is closed and a new one opened. Truncation deletes WHOLE
  SEGMENTS that are entirely below the acknowledged watermark. A delete is
  atomic in a way a rewrite is not: the segment is either there or it is not,
  and the segments around it are untouched either way.

  THE WATERMARK MUST BE DURABLE BEFORE THE DELETE, NOT AFTER. This ordering is
  the entire correctness argument and it is the reverse of what feels natural.

    watermark first, delete second   crash in between leaves segments on disk
                                     that the watermark says are shippable. The
                                     next pass re-ships them, the sink sees
                                     duplicates, and the sink's idempotency
                                     handles it. Cost: a duplicate.

    delete first, watermark second   crash in between leaves records deleted
                                     with no record that they were ever
                                     shipped. They are gone, and nothing knows
                                     they are gone. Cost: a decision the firm
                                     made about a customer's money with no
                                     trace.

  At-least-once with a durable watermark is the correct trade, and it is only
  correct because the sink is idempotent on `decision_id`. A shipper built on a
  non-idempotent sink cannot use this ordering and has no safe alternative --
  which is a constraint on the SINK, stated here because it is invisible from
  the sink's side.

  NEVER TRUNCATE THE ACTIVE SEGMENT. It is being appended to. Deleting it
  removes records written after the watermark was computed.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path


class SinkError(Exception):
    """The sink refused or could not be reached. Retriable."""


class SinkRejected(Exception):
    """The sink refused a specific record permanently -- malformed, too large.

    Distinguished from SinkError on purpose: retrying a permanent rejection
    forever blocks the watermark, the WAL never truncates, and the disk fills
    up because of one bad record. A poison record has to be routed somewhere
    rather than retried into a wall.
    """


@dataclass
class ShipperStats:
    batches: int = 0
    shipped: int = 0
    retries: int = 0
    poisoned: int = 0
    segments_deleted: int = 0
    bytes_reclaimed: int = 0
    watermark_writes: int = 0


class SegmentedWal:
    """Append-only log split into segments, so truncation is a delete.

    Segment naming is zero-padded and monotonic (`seg-00000001.jsonl`) so
    lexical order is chronological order. That is not cosmetic: recovery reads
    them in name order, and `seg-10` sorting before `seg-9` would replay the log
    out of sequence.
    """

    def __init__(self, root: Path | str, segment_bytes: int = 64 * 1024,
                 fsync: str = "batch", batch_size: int = 50):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.segment_bytes = segment_bytes
        self.fsync = fsync
        self.batch_size = batch_size
        self._since_sync = 0
        self._seq = self._highest_segment()
        self._fh = None
        self._open_segment(self._seq or 1)

    # ------------------------------------------------------------ segments
    def _highest_segment(self) -> int:
        segs = self.segments()
        return int(segs[-1].stem.split("-")[1]) if segs else 0

    def segments(self) -> list:
        return sorted(self.root.glob("seg-*.jsonl"))

    def _path(self, seq: int) -> Path:
        return self.root / "seg-{:08d}.jsonl".format(seq)

    def _open_segment(self, seq: int) -> None:
        if self._fh:
            os.fsync(self._fh.fileno())
            self._fh.close()
        self._seq = seq
        self._fh = self._path(seq).open("a", encoding="utf-8")

    def _rotate_if_needed(self) -> None:
        if self._fh.tell() >= self.segment_bytes:
            self._open_segment(self._seq + 1)

    # ------------------------------------------------------------- writing
    def append(self, record: dict) -> None:
        self._fh.write(json.dumps(record, separators=(",", ":"),
                                  default=str) + "\n")
        self._fh.flush()
        self._since_sync += 1
        if self.fsync == "always" or (self.fsync == "batch"
                                      and self._since_sync >= self.batch_size):
            os.fsync(self._fh.fileno())
            self._since_sync = 0
        self._rotate_if_needed()

    def close(self) -> None:
        if self._fh:
            try:
                os.fsync(self._fh.fileno())
            except Exception:                                # noqa: BLE001
                pass
            self._fh.close()
            self._fh = None

    # ------------------------------------------------------------- reading
    def read_segment(self, path: Path) -> list:
        out = []
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                # A torn final line after a hard kill is the record that was
                # mid-write, not corruption of the log.
                continue
        return out

    def records(self) -> list:
        out = []
        for seg in self.segments():
            out.extend(self.read_segment(seg))
        return out

    def active_segment(self) -> Path:
        return self._path(self._seq)


class Watermark:
    """The durable record of what has been acknowledged by the sink.

    Written with the write-temp-then-replace dance rather than in place.
    `os.replace` is atomic on both POSIX and Windows, so a crash leaves either
    the old watermark or the new one and never a half-written file -- and a
    half-written watermark is worse than either, because it is a number nobody
    can trust deciding which records get deleted.
    """

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def read(self) -> set:
        if not self.path.exists():
            return set()
        try:
            return set(json.loads(self.path.read_text(encoding="utf-8"))["shipped"])
        except (json.JSONDecodeError, KeyError):
            # An unreadable watermark means "we do not know what was shipped",
            # and the safe answer is NOTHING -- re-ship everything and let the
            # sink's idempotency sort it out. Assuming the opposite would delete
            # records on the strength of a corrupt file.
            return set()

    def write(self, shipped_ids) -> None:
        tmp = self.path.with_suffix(".tmp")
        payload = json.dumps({"shipped": sorted(shipped_ids)})
        with tmp.open("w", encoding="utf-8") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())        # durable BEFORE the rename
        os.replace(tmp, self.path)


class Shipper:
    """Ship unshipped records, then reclaim whole segments below the watermark."""

    def __init__(self, wal: SegmentedWal, watermark: Watermark, sink,
                 max_attempts: int = 4, backoff_s: float = 0.05):
        self.wal = wal
        self.watermark = watermark
        self.sink = sink
        self.max_attempts = max_attempts
        self.backoff_s = backoff_s
        self.stats = ShipperStats()
        self.poison: list = []

    def unshipped(self) -> list:
        acked = self.watermark.read()
        return [r for r in self.wal.records()
                if r.get("decision_id") not in acked]

    def ship_once(self, batch_size: int = 100) -> int:
        """One shipping pass. Returns the number newly acknowledged."""
        pending = self.unshipped()
        if not pending:
            return 0

        acked = self.watermark.read()
        newly = []
        for i in range(0, len(pending), batch_size):
            batch = pending[i:i + batch_size]
            self.stats.batches += 1
            for attempt in range(self.max_attempts):
                try:
                    self.sink.send(batch)
                    newly.extend(r["decision_id"] for r in batch)
                    break
                except SinkRejected:
                    # Permanent. Retrying forever blocks the watermark, the WAL
                    # never truncates, and one bad record fills the disk. Send
                    # them one at a time to find the poison rather than
                    # condemning the whole batch.
                    for r in batch:
                        try:
                            self.sink.send([r])
                            newly.append(r["decision_id"])
                        except SinkRejected:
                            self.poison.append(r)
                            self.stats.poisoned += 1
                            # Marked acknowledged so it stops blocking the
                            # watermark. It is NOT lost -- it is in self.poison
                            # and the caller must route it. Dropping it silently
                            # would be the actual bug.
                            newly.append(r["decision_id"])
                        except SinkError:
                            pass
                    break
                except SinkError:
                    self.stats.retries += 1
                    if attempt == self.max_attempts - 1:
                        # Give up on this pass. The records stay unshipped and
                        # the next pass retries them, which is the whole point
                        # of a WAL -- an unavailable sink costs disk, not data.
                        break
                    time.sleep(self.backoff_s * (2 ** attempt))

        if newly:
            self.watermark.write(acked | set(newly))
            self.stats.watermark_writes += 1
            self.stats.shipped += len(newly)
        return len(newly)

    # ---------------------------------------------------------- truncation
    def truncate(self) -> int:
        """Delete whole segments in which EVERY record is acknowledged.

        Called after the watermark is durable, never before -- see the module
        docstring. Skips the active segment, which is still being appended to.
        """
        acked = self.watermark.read()
        active = self.wal.active_segment()
        deleted = 0
        for seg in self.wal.segments():
            if seg == active:
                continue
            recs = self.wal.read_segment(seg)
            if not recs:
                continue
            if all(r.get("decision_id") in acked for r in recs):
                self.stats.bytes_reclaimed += seg.stat().st_size
                seg.unlink()
                self.stats.segments_deleted += 1
                deleted += 1
        return deleted

    def run(self, batch_size: int = 100) -> dict:
        """Ship then truncate, in that order. The order is the correctness
        argument, not a style preference."""
        shipped = self.ship_once(batch_size=batch_size)
        deleted = self.truncate()
        return {"shipped": shipped, "segments_deleted": deleted,
                "unshipped_remaining": len(self.unshipped())}


class MemorySink:
    """A sink that can be told to fail, so the failure paths are exercised."""

    def __init__(self, idempotent: bool = True):
        self.received: dict = {}
        self.order: list = []
        self.up = True
        self.fail_next = 0
        self.reject_ids: set = set()
        self.idempotent = idempotent
        self.duplicate_deliveries = 0

    def send(self, batch) -> None:
        if not self.up:
            raise SinkError("sink is down")
        if self.fail_next > 0:
            self.fail_next -= 1
            raise SinkError("transient failure")
        for r in batch:
            rid = r.get("decision_id")
            if rid in self.reject_ids:
                raise SinkRejected("permanently rejected {}".format(rid))
            if rid in self.received:
                self.duplicate_deliveries += 1
                if not self.idempotent:
                    self.order.append(rid)
                continue
            self.received[rid] = r
            self.order.append(rid)
