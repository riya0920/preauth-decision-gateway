"""Shipping the WAL, and reclaiming disk without losing a decision."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gateway.shipping import (MemorySink, SegmentedWal, Shipper, SinkError,
                              SinkRejected, Watermark)


def _rec(i):
    return {"decision_id": "d{:05d}".format(i), "approved": i % 3 != 0,
            "amount_minor": 1000 + i, "reason": "score"}


def _setup(tmp_path, n=500, segment_bytes=2048, **kw):
    wal = SegmentedWal(tmp_path / "wal", segment_bytes=segment_bytes)
    for i in range(n):
        wal.append(_rec(i))
    sink = MemorySink(**kw)
    shipper = Shipper(wal, Watermark(tmp_path / "watermark.json"), sink)
    return wal, sink, shipper


# ------------------------------------------------------------- segmenting
def test_the_log_rotates_into_multiple_segments(tmp_path):
    wal, _, _ = _setup(tmp_path)
    assert len(wal.segments()) > 3, "one segment means truncation is a rewrite"


def test_segment_names_sort_chronologically(tmp_path):
    """Zero-padded on purpose. `seg-10` sorting before `seg-9` would replay the
    log out of sequence, and recovery reads them in name order."""
    wal = SegmentedWal(tmp_path / "wal", segment_bytes=200)
    for i in range(400):
        wal.append(_rec(i))
    names = [p.name for p in wal.segments()]
    assert names == sorted(names)
    assert len(names) > 10, "need enough segments to cross the 9->10 boundary"
    ids = [r["decision_id"] for r in wal.records()]
    assert ids == sorted(ids), "records came back out of order"


def test_every_record_survives_rotation(tmp_path):
    wal, _, _ = _setup(tmp_path, n=500)
    assert len(wal.records()) == 500


# --------------------------------------------------------------- shipping
def test_a_clean_run_ships_everything_and_reclaims_disk(tmp_path):
    wal, sink, shipper = _setup(tmp_path)
    before = sum(s.stat().st_size for s in wal.segments())
    out = shipper.run()

    assert out["shipped"] == 500
    assert out["unshipped_remaining"] == 0
    assert len(sink.received) == 500
    assert out["segments_deleted"] > 0
    assert shipper.stats.bytes_reclaimed > 0
    assert sum(s.stat().st_size for s in wal.segments()) < before


def test_the_active_segment_is_never_deleted(tmp_path):
    """It is being appended to. Deleting it removes records written after the
    watermark was computed."""
    wal, _, shipper = _setup(tmp_path)
    active = wal.active_segment()
    shipper.run()
    assert active.exists(), "the active segment was truncated"


def test_a_record_written_after_shipping_is_not_lost(tmp_path):
    """The concrete version of the same hazard."""
    wal, sink, shipper = _setup(tmp_path)
    shipper.run()
    wal.append(_rec(9999))
    assert shipper.run()["shipped"] == 1
    assert "d09999" in sink.received


# ---------------------------------------------------------- sink failures
def test_a_dead_sink_costs_disk_not_data(tmp_path):
    """The entire point of a WAL. The sink is unavailable, nothing ships,
    nothing is deleted, and every record is still there when it comes back."""
    wal, sink, shipper = _setup(tmp_path)
    sink.up = False

    out = shipper.run()
    assert out["shipped"] == 0
    assert out["segments_deleted"] == 0
    assert out["unshipped_remaining"] == 500
    assert len(wal.records()) == 500

    sink.up = True
    assert shipper.run()["shipped"] == 500


def test_transient_failures_are_retried(tmp_path):
    wal, sink, shipper = _setup(tmp_path, n=50)
    sink.fail_next = 2
    assert shipper.run()["shipped"] == 50
    assert shipper.stats.retries >= 2


def test_a_sink_that_never_recovers_does_not_delete_anything(tmp_path):
    wal, sink, shipper = _setup(tmp_path, n=200)
    sink.fail_next = 10_000
    shipper.run()
    assert len(wal.records()) == 200
    assert shipper.stats.segments_deleted == 0


# ------------------------------------------------------------ poison record
def test_a_permanently_rejected_record_does_not_block_the_watermark(tmp_path):
    """Retrying a permanent rejection forever blocks the watermark, the WAL
    never truncates, and the disk fills up because of one bad record."""
    wal, sink, shipper = _setup(tmp_path, n=200)
    sink.reject_ids = {"d00100"}

    out = shipper.run()
    assert out["unshipped_remaining"] == 0
    assert out["segments_deleted"] > 0
    assert shipper.stats.poisoned == 1


def test_the_poison_record_is_routed_rather_than_dropped(tmp_path):
    """It is marked acknowledged so it stops blocking, which is only acceptable
    because it is handed to the caller. Dropping it silently would be the actual
    bug."""
    wal, sink, shipper = _setup(tmp_path, n=200)
    sink.reject_ids = {"d00100"}
    shipper.run()

    assert [r["decision_id"] for r in shipper.poison] == ["d00100"]
    assert "d00100" not in sink.received


def test_one_poison_record_does_not_condemn_its_whole_batch(tmp_path):
    """A batch is retried record-by-record to find the poison, rather than
    failing all hundred of them because one was malformed."""
    wal, sink, shipper = _setup(tmp_path, n=200)
    sink.reject_ids = {"d00100"}
    shipper.run()
    assert len(sink.received) == 199


# --------------------------------------------------------------- watermark
def test_the_watermark_survives_a_restart(tmp_path):
    wal, sink, shipper = _setup(tmp_path, n=200)
    shipper.run()

    wal2 = SegmentedWal(tmp_path / "wal", segment_bytes=2048)
    shipper2 = Shipper(wal2, Watermark(tmp_path / "watermark.json"), sink)
    assert shipper2.unshipped() == [], "a restart re-shipped acknowledged records"


def test_the_watermark_is_replaced_atomically_not_written_in_place(tmp_path):
    """A half-written watermark is worse than either version of it: a number
    nobody can trust deciding which records get deleted."""
    wm = Watermark(tmp_path / "wm.json")
    wm.write({"a", "b"})
    assert wm.read() == {"a", "b"}
    wm.write({"a", "b", "c"})
    assert wm.read() == {"a", "b", "c"}
    assert not (tmp_path / "wm.tmp").exists(), "the temp file was left behind"


def test_a_corrupt_watermark_re_ships_rather_than_deletes(tmp_path):
    """"We do not know what was shipped" has a safe answer and an unsafe one.
    Assuming everything was shipped deletes records on the strength of a corrupt
    file."""
    wal, sink, shipper = _setup(tmp_path, n=100)
    shipper.run()
    (tmp_path / "watermark.json").write_text("{not json", encoding="utf-8")

    assert len(shipper.unshipped()) == len(wal.records())
    assert shipper.truncate() == 0, "truncated against an unreadable watermark"


def test_no_watermark_at_all_means_nothing_is_acknowledged(tmp_path):
    wal, _, shipper = _setup(tmp_path, n=50)
    assert len(shipper.unshipped()) == 50
    assert shipper.truncate() == 0


# ------------------------------------------------- the ordering that matters
def test_crash_between_watermark_and_delete_costs_a_duplicate_not_a_record(tmp_path):
    """The ordering argument, executed.

    Watermark first, delete second: a crash in between leaves segments on disk
    that the watermark says are shipped. The next pass re-ships them, the sink
    dedupes, and the cost is a duplicate delivery.

    The reverse order -- delete first, watermark second -- would leave records
    deleted with nothing recording that they were ever shipped. Gone, and
    nothing knows they are gone.
    """
    wal, sink, shipper = _setup(tmp_path, n=200)

    # Ship and persist the watermark, then "crash" before truncating.
    shipped = shipper.ship_once()
    assert shipped == 200
    segments_before = len(wal.segments())

    # Restart: new shipper, same watermark, same segments still on disk.
    wal2 = SegmentedWal(tmp_path / "wal", segment_bytes=2048)
    shipper2 = Shipper(wal2, Watermark(tmp_path / "watermark.json"), sink)

    assert len(wal2.segments()) == segments_before, "segments vanished"
    assert shipper2.unshipped() == [], "the watermark did not survive"
    assert len(sink.received) == 200
    assert shipper2.truncate() > 0, "the deferred truncation never happened"


def test_re_shipping_is_safe_because_the_sink_is_idempotent(tmp_path):
    """At-least-once with a durable watermark is only correct because the sink
    dedupes on decision_id. This is a constraint on the SINK, invisible from the
    sink's side, so it is asserted from here."""
    wal, sink, shipper = _setup(tmp_path, n=100)
    shipper.ship_once()

    # Watermark lost (disk replaced, operator error) but segments intact.
    (tmp_path / "watermark.json").unlink()
    shipper2 = Shipper(wal, Watermark(tmp_path / "watermark.json"), sink)
    shipper2.ship_once()

    assert len(sink.received) == 100, "duplicates reached the sink as new records"
    assert sink.duplicate_deliveries == 100, "no duplicates were attempted at all"


def test_a_non_idempotent_sink_double_counts_and_that_is_why_it_is_a_constraint(tmp_path):
    """Stated as a test rather than a comment: with a non-idempotent sink the
    same ordering produces duplicate entries, so a shipper built on one cannot
    use at-least-once and has no safe alternative."""
    wal, sink, shipper = _setup(tmp_path, n=100, idempotent=False)
    shipper.ship_once()
    (tmp_path / "watermark.json").unlink()
    Shipper(wal, Watermark(tmp_path / "watermark.json"), sink).ship_once()

    assert len(sink.order) == 200, (
        "expected the non-idempotent sink to record every delivery, including "
        "the duplicates -- that is the failure mode being documented")


# ------------------------------------------------------------- truncation
def test_a_partially_shipped_segment_is_not_deleted(tmp_path):
    """Whole segments only. Deleting a segment where one record is unshipped
    loses that record."""
    wal, sink, shipper = _setup(tmp_path, n=300)
    all_ids = [r["decision_id"] for r in wal.records()]
    # Acknowledge everything except one record in the middle.
    Watermark(tmp_path / "watermark.json").write(
        set(all_ids) - {all_ids[len(all_ids) // 2]})

    shipper.truncate()
    remaining = {r["decision_id"] for r in wal.records()}
    assert all_ids[len(all_ids) // 2] in remaining, "an unshipped record was deleted"


def test_truncation_never_loses_an_unshipped_record_under_repeated_passes(tmp_path):
    """The property that matters, checked over many cycles rather than once."""
    wal = SegmentedWal(tmp_path / "wal", segment_bytes=1024)
    sink = MemorySink()
    shipper = Shipper(wal, Watermark(tmp_path / "wm.json"), sink)

    written = 0
    for cycle in range(10):
        for i in range(60):
            wal.append(_rec(written))
            written += 1
        sink.up = cycle % 3 != 1          # the sink is down every third cycle
        shipper.run()
        on_disk = {r["decision_id"] for r in wal.records()}
        acked = Watermark(tmp_path / "wm.json").read()
        assert on_disk | acked == {"d{:05d}".format(i) for i in range(written)}, (
            "a record is neither on disk nor acknowledged -- it is gone")

    sink.up = True
    shipper.run()
    assert len(sink.received) == written


def test_the_WRONG_order_loses_records_and_this_proves_the_claim(tmp_path):
    """The module docstring argues delete-then-watermark loses data. Argued is
    not demonstrated, so this executes the wrong order and measures the loss.

    Without this, the ordering rule is a comment somebody can "simplify" later
    on the grounds that it looks arbitrary.
    """
    wal, sink, shipper = _setup(tmp_path, n=200)
    wm_path = tmp_path / "watermark.json"

    # Send to the sink, then delete segments, then CRASH before the watermark.
    pending = shipper.unshipped()
    sink.send(pending)
    active = wal.active_segment()
    doomed = [s for s in wal.segments() if s != active]
    deleted_ids = {r["decision_id"] for s in doomed
                   for r in wal.read_segment(s)}
    for s in doomed:
        s.unlink()
    # ...crash here. The watermark write never happens.
    assert not wm_path.exists()

    # Restart. What does the system now believe?
    wal2 = SegmentedWal(tmp_path / "wal", segment_bytes=2048)
    shipper2 = Shipper(wal2, Watermark(wm_path), sink)

    on_disk = {r["decision_id"] for r in wal2.records()}
    acknowledged = Watermark(wm_path).read()

    lost = deleted_ids - on_disk - acknowledged
    assert len(lost) > 0, (
        "expected the wrong ordering to lose records; if it no longer does, "
        "the ordering rule in the module docstring needs re-deriving")

    # And the damage is the specific kind that matters: the records are gone
    # from disk AND absent from the watermark, so nothing in the system knows
    # they ever existed. They did reach the sink -- but no local state records
    # that, so a reconciliation against the sink would report them as decisions
    # the gateway never made.
    assert lost.issubset(set(sink.received)), (
        "these reached the sink but the gateway has no trace of having sent "
        "them -- which is exactly the unreconcilable state the ordering "
        "prevents")
    assert shipper2.unshipped() == [r for r in wal2.records()]
