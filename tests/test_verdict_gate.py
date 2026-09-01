"""The [rebal-trigger] verdict must travel WITH the plan it authorises.

The verdict decides whether to trade at all — drift over threshold AND the 6W
cadence satisfied. It used to exist only as a line in run.log that
daily_auto.ps1 grepped, so the automated path was gated and the manual path was
not: `ibkr_paper_exec.py --execute` submitted whatever was in the rec log. On
2026-08-10 that was 8 trades and $71,440 of volume the engine had gated as
within-cadence, and the only thing standing between them and the broker was the
operator remembering the verdict.

These tests pin the two halves: the engine stamps the verdict onto the plan, and
the executor refuses a plan it did not clear.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

import ibkr_paper_exec as ex
from jsonl_logs import append_trade_recommendation_log


# --- executor side: the gate --------------------------------------------

def _entry(verdict=None, skip_reason=None, run_at="2026-08-10T09:32:26"):
    e = {"run_at": run_at, "recommended_trades": [{"ticker": "VLUE.AX"}]}
    if verdict is not None:
        e["verdict"] = verdict
    if skip_reason is not None:
        e["skip_reason"] = skip_reason
    return e


def test_run_verdict_clears():
    ok, lines = ex._verdict_gate(_entry("RUN"), execute=True)
    assert ok
    assert any("cleared" in l.lower() for l in lines)


def test_skip_verdict_refuses_and_says_why():
    """THE case: 2026-08-10, 8 trades gated on cadence."""
    ok, lines = ex._verdict_gate(
        _entry("SKIP", "within-cadence (6d since last fill < 42d)"),
        execute=True)
    assert not ok
    body = "\n".join(lines)
    assert "REFUSING TO EXECUTE" in body
    assert "within-cadence" in body
    assert "--override-verdict" in body


def test_missing_verdict_is_unknown_and_refuses():
    """An entry from an engine predating the stamp is unproven, not approved.
    Failing open here would leave every pre-existing rec-log line executable."""
    ok, lines = ex._verdict_gate(_entry(), execute=True)
    assert not ok
    body = "\n".join(lines)
    assert "UNKNOWN" in body
    assert "predates verdict stamping" in body


@pytest.mark.parametrize("v", ["UNKNOWN", "", None, "HALTED", "garbage"])
def test_only_run_clears_everything_else_refuses(v):
    ok, _ = ex._verdict_gate(_entry(v), execute=True)
    assert not ok


def test_verdict_is_case_insensitive():
    ok, _ = ex._verdict_gate(_entry("run"), execute=True)
    assert ok


def test_preview_is_never_gated_but_is_warned():
    """Reading the plan is how you decide whether to override it."""
    ok, lines = ex._verdict_gate(_entry("SKIP", "within-cadence"), execute=False)
    assert ok
    body = "\n".join(lines)
    assert "PREVIEW is not gated" in body
    assert "would REFUSE" in body


def test_override_proceeds_but_is_never_silent():
    ok, lines = ex._verdict_gate(
        _entry("SKIP", "within-cadence"), execute=True,
        override="manual catch-up after the 08-04 partial fill")
    assert ok
    body = "\n".join(lines)
    assert "OVERRIDE" in body
    assert "manual catch-up after the 08-04 partial fill" in body
    assert "against the engine's decision" in body


def test_empty_override_string_does_not_bypass():
    """argparse default is '' — a bare --execute must not read as an override."""
    ok, _ = ex._verdict_gate(_entry("SKIP", "within-cadence"),
                             execute=True, override="")
    assert not ok


def test_gate_survives_a_junk_entry():
    for bad in ({}, None):
        ok, lines = ex._verdict_gate(bad, execute=True)
        assert not ok and lines


# --- engine side: the stamp ---------------------------------------------

def _write_rec(tmp_path, **kw):
    p = tmp_path / "rec.jsonl"
    trade_df = pd.DataFrame(
        {"Delta Units": [10], "Last Px (AUD)": [37.94], "Curr Units": [2974]},
        index=["VLUE.AX"])
    append_trade_recommendation_log(
        p, selected_mode="ensemble", trade_df=trade_df,
        w_target=pd.Series({"VLUE.AX": 0.605}),
        current_units=pd.Series({"VLUE.AX": 2974}),
        portfolio_value_aud=251022.0, regime_mix=pd.Series(dtype=float),
        expected_brokerage_aud=58.38, expected_cgt_aud=0.0,
        broker_name="IBKR", cgt_mtr=0.30, universe_size=47, **kw)
    return json.loads(p.read_text(encoding="utf-8").strip().splitlines()[-1])


def test_engine_stamps_verdict_and_reason(tmp_path):
    e = _write_rec(tmp_path, verdict="SKIP",
                   skip_reason="within-cadence (6d since last fill < 42d)")
    assert e["verdict"] == "SKIP"
    assert e["skip_reason"].startswith("within-cadence")


def test_unstamped_write_defaults_to_unknown_not_run(tmp_path):
    """The default must be the refusing value. If append_trade_recommendation_log
    ever defaulted to RUN, every caller that forgot the kwarg would silently
    authorise its own plan."""
    e = _write_rec(tmp_path)
    assert e["verdict"] == "UNKNOWN"
    assert e["skip_reason"] == ""


def test_stamped_entry_round_trips_through_the_gate(tmp_path):
    """End to end: what the engine writes is what the executor reads."""
    skip = _write_rec(tmp_path, verdict="SKIP", skip_reason="drift<threshold")
    assert ex._verdict_gate(skip, execute=True)[0] is False
    run = _write_rec(tmp_path, verdict="RUN")
    assert ex._verdict_gate(run, execute=True)[0] is True


# --------------------------------------------------------------------------
# A research sweep must not become the plan the US legs execute (2026-09-01)
# --------------------------------------------------------------------------

def test_evidence_run_suppresses_the_rec_log():
    """The 02:00 US pass loads the LATEST rec-log entry, so the 18:00 evidence
    sweep was silently becoming the plan those legs would chase instead of the
    morning's approved one.

    Never diverged in 39 days — every one was a cadence-gated SKIP. On a RUN
    day it inverts: the morning fills the ASX legs, the 10:30 snapshot moves
    last_position_change_date to today, so the 18:00 run sees 0 days since the
    last fill and writes SKIP. At 02:00 the US legs load that SKIP and refuse —
    ASX traded, US not, and the anchor now claims a fresh rebalance, so it will
    not retry for six weeks.
    """
    src = (Path(__file__).resolve().parent.parent / "Portfolio_Optimiser.py").read_text(encoding="utf-8")
    assert 'PORTOPT_NO_REC_LOG' in src, "the suppression flag must exist"
    i = src.index("_no_rec_log = ")
    block = src[i:i + 2600]
    assert "if _no_rec_log:" in block and "else:" in block, \
        "the append must be gated, not merely warned about"
    # The guard has to sit BEFORE the append, or it logs anyway.
    assert block.index("if _no_rec_log:") < block.index("append_trade_recommendation_log"), \
        "the flag is checked after the write — it would suppress nothing"


def test_the_evidence_wrapper_actually_sets_the_flag():
    """The engine-side guard is inert unless evidence_run.ps1 sets it."""
    ps1 = (Path(__file__).resolve().parent.parent / "evidence_run.ps1").read_text(encoding="utf-8")
    assert 'PORTOPT_NO_REC_LOG' in ps1 and '"1"' in ps1
    assert "SCALE_SENSITIVITY" in ps1, "and must still run the sweep it exists for"


def test_daily_auto_does_not_set_it():
    """The morning run is the one whose plan the pipeline executes — it must
    keep logging, or the US pass has nothing to load at all."""
    ps1 = (Path(__file__).resolve().parent.parent / "daily_auto.ps1").read_text(encoding="utf-8")
    assert "PORTOPT_NO_REC_LOG" not in ps1
