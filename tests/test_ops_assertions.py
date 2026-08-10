"""Reality-vs-intent assertions for the live pipeline.

Two observed failures motivate these:
  - 2026-08-03: a fix shipped declaring "10:20 is LOAD-BEARING"; the Windows
    task stayed at 09:30 for a week and nothing compared the two.
  - 2026-08-07: the evening evidence run died silently. An absent run and a
    healthy quiet night produced identical evidence — none.

The weekend cases get the most attention here, because a heartbeat that cries
wolf every Monday is a heartbeat that gets muted, and a muted alarm is worse
than no alarm.
"""
from __future__ import annotations

from datetime import datetime

import ops_assertions as ops

WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]


# --- scheduled-task drift -----------------------------------------------

def test_task_time_drift_is_caught():
    """THE 2026-08-03 miss."""
    found = ops.check_task_schedule(
        {"Portfolio Optimiser Daily": {"start_time": "09:30", "enabled": True,
                                       "days": WEEKDAYS}},
        {"Portfolio Optimiser Daily": {"start_time": "10:20", "days": WEEKDAYS,
                                       "must_be_enabled": True}})
    assert len(found) == 1
    assert "starts at 09:30, expected 10:20" in found[0]


def test_matching_schedule_is_silent():
    assert ops.check_task_schedule(
        {"T": {"start_time": "10:20", "enabled": True, "days": WEEKDAYS}},
        {"T": {"start_time": "10:20", "days": WEEKDAYS,
               "must_be_enabled": True}}) == []


def test_missing_and_disabled_tasks_are_caught():
    assert "TASK MISSING" in ops.check_task_schedule(
        {}, {"T": {"start_time": "10:20"}})[0]
    assert "TASK DISABLED" in ops.check_task_schedule(
        {"T": {"start_time": "10:20", "enabled": False, "days": None}},
        {"T": {"start_time": "10:20", "must_be_enabled": True}})[0]


def test_day_set_drift_is_caught():
    found = ops.check_task_schedule(
        {"T": {"start_time": "10:20", "enabled": True,
               "days": ["Monday", "Tuesday"]}},
        {"T": {"start_time": "10:20", "days": WEEKDAYS,
               "must_be_enabled": True}})
    assert any("TASK DAYS" in f for f in found)


# --- when was a job actually due? ---------------------------------------

def test_due_time_skips_the_weekend():
    """Monday 11:00 looks back to FRIDAY, not Sunday."""
    mon = datetime(2026, 8, 10, 11, 0)
    due = ops.previous_expected_run(mon, "18:00", WEEKDAYS, grace_minutes=180)
    assert due == datetime(2026, 8, 7, 18, 0)


def test_due_time_respects_grace_before_blaming_a_run():
    """At 10:30, a 10:20 job is 10 minutes old — not late yet, so the due time
    must fall back to the previous occurrence."""
    now = datetime(2026, 8, 10, 10, 30)
    due = ops.previous_expected_run(now, "10:20", WEEKDAYS, grace_minutes=120)
    assert due == datetime(2026, 8, 7, 10, 20)


def test_due_time_uses_today_once_grace_has_passed():
    now = datetime(2026, 8, 10, 14, 0)
    due = ops.previous_expected_run(now, "10:20", WEEKDAYS, grace_minutes=120)
    assert due == datetime(2026, 8, 10, 10, 20)


def test_unparseable_start_time_yields_no_due_time():
    assert ops.previous_expected_run(datetime.now(), "not-a-time", WEEKDAYS) is None
    assert ops.previous_expected_run(datetime.now(), "", WEEKDAYS) is None


# --- heartbeat ------------------------------------------------------------

def _row(job, finished, outcome="ok"):
    return {"job": job, "finished": finished, "outcome": outcome}


def test_friday_run_checked_on_monday_is_healthy():
    """The cry-wolf case. A flat max-age would flag this 65h-old run."""
    found = ops.check_heartbeat(
        [_row("evidence_run", "2026-08-07T18:04:00")],
        {"evidence_run": {"start_time": "18:00", "days": WEEKDAYS,
                          "grace_minutes": 180}},
        now=datetime(2026, 8, 10, 11, 0))
    assert found == []


def test_missed_run_is_caught():
    """THE 2026-08-07 silent death: last success is Thursday, Friday never ran."""
    found = ops.check_heartbeat(
        [_row("evidence_run", "2026-08-06T18:03:00")],
        {"evidence_run": {"start_time": "18:00", "days": WEEKDAYS,
                          "grace_minutes": 180}},
        now=datetime(2026, 8, 10, 11, 0))
    assert len(found) == 1 and "MISSED RUN" in found[0]


def test_a_job_that_ran_and_failed_is_not_healthy():
    found = ops.check_heartbeat(
        [_row("evidence_run", "2026-08-07T18:04:00", outcome="fail")],
        {"evidence_run": {"start_time": "18:00", "days": WEEKDAYS,
                          "grace_minutes": 180}},
        now=datetime(2026, 8, 10, 11, 0))
    assert len(found) == 1 and "RUN FAILED" in found[0]


def test_never_recorded_job_is_reported():
    found = ops.check_heartbeat(
        [], {"daily_auto": {"start_time": "10:20", "days": WEEKDAYS}},
        now=datetime(2026, 8, 10, 14, 0))
    assert len(found) == 1 and "NO RUN RECORDED" in found[0]


def test_latest_entry_wins_regardless_of_file_order():
    found = ops.check_heartbeat(
        [_row("daily_auto", "2026-08-10T10:25:00"),
         _row("daily_auto", "2026-08-06T09:31:00")],
        {"daily_auto": {"start_time": "10:20", "days": WEEKDAYS,
                        "grace_minutes": 120}},
        now=datetime(2026, 8, 10, 14, 0))
    assert found == []


def test_junk_ledger_rows_are_skipped_not_fatal():
    found = ops.check_heartbeat(
        [{"job": "", "finished": "x"}, {"nope": 1},
         _row("daily_auto", "bad-timestamp"),
         _row("daily_auto", "2026-08-10T10:25:00")],
        {"daily_auto": {"start_time": "10:20", "days": WEEKDAYS,
                        "grace_minutes": 120}},
        now=datetime(2026, 8, 10, 14, 0))
    assert found == []


# --- state files and exe freshness ---------------------------------------

def test_missing_and_empty_state_files(tmp_path):
    (tmp_path / "there.json").write_text("{}", encoding="utf-8")
    (tmp_path / "empty.json").write_text("", encoding="utf-8")
    found = ops.check_required_files(
        tmp_path, ["there.json", "empty.json", "gone.json"])
    assert any("MISSING STATE FILE: gone.json" in f for f in found)
    assert any("EMPTY STATE FILE: empty.json" in f for f in found)
    assert not any("there.json" in f for f in found)


def test_exe_stale_when_engine_source_is_newer(tmp_path):
    import os
    import time
    exe = tmp_path / "app.exe"
    exe.write_text("bin", encoding="utf-8")
    src = tmp_path / "engine.py"
    src.write_text("code", encoding="utf-8")
    os.utime(src, (time.time() + 60, time.time() + 60))
    found = ops.check_exe_freshness(exe, [src])
    assert len(found) == 1 and "EXE STALE" in found[0]


def test_exe_fresh_when_sources_are_older(tmp_path):
    import os
    import time
    src = tmp_path / "engine.py"
    src.write_text("code", encoding="utf-8")
    exe = tmp_path / "app.exe"
    exe.write_text("bin", encoding="utf-8")
    os.utime(src, (time.time() - 600, time.time() - 600))
    assert ops.check_exe_freshness(exe, [src]) == []


def test_missing_exe_is_reported(tmp_path):
    assert "EXE MISSING" in ops.check_exe_freshness(tmp_path / "nope.exe", [])[0]


# --- ledger round trip ----------------------------------------------------

def test_ledger_round_trips(tmp_path, monkeypatch):
    monkeypatch.setattr(ops, "_SCRIPT_DIR", tmp_path)
    ops.record("daily_auto", "ok", "verdict=SKIP")
    ops.record("evidence_run", "fail", "HUNG")
    rows = ops.read_ledger(tmp_path / ops.LEDGER_FILENAME)
    assert [r["job"] for r in rows] == ["daily_auto", "evidence_run"]
    assert rows[0]["outcome"] == "ok" and rows[1]["outcome"] == "fail"
    assert rows[0]["detail"] == "verdict=SKIP"


def test_corrupt_ledger_lines_are_skipped(tmp_path):
    p = tmp_path / "ledger.jsonl"
    p.write_text('{"job":"a","finished":"2026-08-10T10:00:00","outcome":"ok"}\n'
                 'not json\n\n'
                 '{"job":"b","finished":"2026-08-10T11:00:00","outcome":"ok"}\n',
                 encoding="utf-8")
    assert [r["job"] for r in ops.read_ledger(p)] == ["a", "b"]


def test_missing_ledger_reads_empty(tmp_path):
    assert ops.read_ledger(tmp_path / "nope.jsonl") == []
