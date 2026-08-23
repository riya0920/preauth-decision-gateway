"""The audit WAL: durability, recovery, and the torn write."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gateway.pipeline import AuditLog
from gateway.wal import AuditWal, DurableAuditLog


def _rec(i):
    return {"decision_id": "d{}".format(i), "decision": "approve", "score": 0.03}


def test_records_are_on_disk_before_the_call_returns(tmp_path):
    """The ordering IS the design. If the append happened after the response,
    the window that loses data would be a window in which a decision had
    already been given to a customer."""
    wal = AuditWal(tmp_path / "a.jsonl", fsync="always")
    wal.append(_rec(1))
    assert len(wal.path.read_text(encoding="utf-8").splitlines()) == 1


def test_a_process_crash_loses_nothing_that_was_appended(tmp_path):
    path = tmp_path / "a.jsonl"
    wal = AuditWal(path, fsync="batch")
    for i in range(100):
        wal.append(_rec(i))
    # No close(): the process died.
    assert len(AuditWal(path).records()) == 100


def test_recovery_returns_exactly_what_was_not_shipped(tmp_path):
    wal = AuditWal(tmp_path / "a.jsonl", fsync="batch")
    log = DurableAuditLog(wal)
    for i in range(50):
        log.write(_rec(i))
    log.drain()
    for i in range(50, 70):
        log.write(_rec(i))
    shipped = list(log.shipped)
    wal.close()

    restarted = DurableAuditLog(AuditWal(tmp_path / "a.jsonl"))
    restarted.shipped = shipped
    pending = restarted.recover()
    assert len(pending) == 20
    assert {r["decision_id"] for r in pending} == {
        "d{}".format(i) for i in range(50, 70)}


def test_the_plain_audit_log_loses_the_buffer_and_the_wal_does_not(tmp_path):
    """The comparison the drill described in words and the code did not make."""
    plain = AuditLog()
    for i in range(30):
        plain.write(_rec(i))
    assert len(plain.buffer) == 30
    del plain                                   # the process dies

    wal = AuditWal(tmp_path / "a.jsonl", fsync="batch")
    log = DurableAuditLog(wal)
    for i in range(30):
        log.write(_rec(i))
    wal.close()
    assert len(DurableAuditLog(AuditWal(tmp_path / "a.jsonl")).recover()) == 30


def test_a_torn_final_line_is_counted_not_silently_dropped(tmp_path):
    """A hard kill mid-write leaves a partial line. Discarding it silently
    would hide a real truncation; counting it is how recovery reports loss."""
    path = tmp_path / "a.jsonl"
    wal = AuditWal(path, fsync="always")
    for i in range(5):
        wal.append(_rec(i))
    wal.close()
    with path.open("a", encoding="utf-8") as fh:
        fh.write('{"decision_id": "d5", "dec')     # torn

    reader = AuditWal(path)
    assert len(reader.records()) == 5
    assert reader.stats.truncated == 1


def test_fsync_always_syncs_every_record(tmp_path):
    wal = AuditWal(tmp_path / "a.jsonl", fsync="always")
    for i in range(20):
        wal.append(_rec(i))
    assert wal.stats.fsyncs == 20


def test_fsync_never_syncs_nothing(tmp_path):
    wal = AuditWal(tmp_path / "a.jsonl", fsync="never")
    for i in range(20):
        wal.append(_rec(i))
    assert wal.stats.fsyncs == 0


def test_batch_fsyncs_far_less_often_than_always(tmp_path):
    wal = AuditWal(tmp_path / "a.jsonl", fsync="batch", batch_size=50,
                   batch_ms=10_000)
    for i in range(200):
        wal.append(_rec(i))
    assert 0 < wal.stats.fsyncs <= 5


def test_an_unknown_fsync_mode_is_refused(tmp_path):
    """A typo that silently means 'never' is a durability setting nobody has."""
    with pytest.raises(ValueError, match="fsync"):
        AuditWal(tmp_path / "a.jsonl", fsync="sometimes")


def test_appending_is_thread_safe(tmp_path):
    import threading
    wal = AuditWal(tmp_path / "a.jsonl", fsync="batch")

    def work(base):
        for i in range(200):
            wal.append(_rec(base + i))

    threads = [threading.Thread(target=work, args=(w * 1000,)) for w in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wal.close()

    lines = wal.path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1200
    assert all(json.loads(l) for l in lines), "an interleaved write tore a line"


def test_a_restart_can_append_to_an_existing_wal(tmp_path):
    path = tmp_path / "a.jsonl"
    first = AuditWal(path, fsync="batch")
    first.append(_rec(1))
    first.close()
    second = AuditWal(path, fsync="batch")
    second.append(_rec(2))
    second.close()
    assert len(AuditWal(path).records()) == 2
