"""Contract/integrity guards for the two integration-only modules.

research_modes (12 CLI backtest drivers) and ppt_export (one 2,278-line deck
builder fused to ~55 engine globals) can't be unit-tested in isolation — they
need the full engine state + network factor data, and are validated by actually
running the CLI modes / a live deck build. What CAN break silently and cheaply
here is the COUPLING CONTRACT: the engine dispatches to these by name and injects
state via _sync_research_modes / _sync_ppt_export before calling. These tests
catch import breakage and function rename/signature drift against that contract.
"""
from __future__ import annotations

import inspect
import sys
from pathlib import Path as _Path

sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import research_modes
import ppt_export

_ENGINE_SRC = (_Path(__file__).resolve().parent.parent / "Portfolio_Optimiser.py").read_text(
    encoding="utf-8", errors="replace")


# === Holdings.Units immutability =============================================
# Source-level guards, because the live pipeline that contains this logic needs
# full engine state + Excel and cannot be unit-tested. The rule they protect has
# now been broken TWICE by different routes, each time silently:
#   2026-06-27  the sheet write passed TARGET units -> engine read its own target
#               back as next run's "current" -> HBRD compounded 2,034 -> 17,122.
#   2026-07-17  the live TLH pass REBOUND `units` to post-swap values before the
#               write, defeating the 2026-06-27 fix through a different door ->
#               a phantom SMH->SOXX harvest was persisted as though executed and
#               drift compounded 0.33 -> 0.66 -> 0.99 across three runs.
# Both are the same failure: the Holdings sheet must record what we HOLD (broker
# truth), never what the engine RECOMMENDS. A live run is not a sufficient guard
# — the first breakage survived a month of them.

def test_holdings_sheet_write_uses_actual_units_not_post_tlh():
    """The sheet write must NOT pass the `units` that the TLH pass rebinds."""
    calls = [ln.strip() for ln in _ENGINE_SRC.splitlines()
             if "_write_holdings_sheet(" in ln and not ln.strip().startswith("#")]
    assert calls, "no _write_holdings_sheet call found — did it get renamed?"
    for call in calls:
        assert "_sheet_units" in call, (
            "Holdings sheet write must pass _sheet_units (the pre-TLH actual "
            f"holdings), not post-swap `units`. Offending call: {call}")


def test_tlh_live_snapshots_actual_units_before_rebinding():
    """The TLH pass must snapshot actual units BEFORE it rebinds `units`."""
    src = _ENGINE_SRC
    snap = src.find("_units_actual_for_sheet = pd.Series(units")
    assert snap != -1, (
        "the live TLH block must snapshot _units_actual_for_sheet before "
        "rebinding `units` to post-swap values")
    rebind = src.find("units = pd.Series(units, dtype=float).copy()", snap)
    assert rebind != -1 and rebind > snap, (
        "the snapshot must come BEFORE the rebind, or it captures post-swap "
        "units and the guard is worthless")


# The 12 CLI drivers the engine dispatches to. Each is called with NO args
# (state is injected via module globals by _sync_research_modes first).
_EXPECTED_DRIVERS = {
    "_run_gfc_stress_test", "_run_scale_analysis", "_run_dev_validation",
    "_run_rebal_skip_sweep", "_run_turnover_penalty_sweep", "_run_walk_forward_cv",
    "_run_attribution", "_run_crash_hedge_test", "_run_crash_hedge_release_sweep",
    "_run_stretch_only_test", "_run_stretch_hedge_sweep", "_run_tilted_ensemble_test",
}


def test_all_expected_drivers_present_and_callable():
    present = {n for n in dir(research_modes)
               if n.startswith("_run_") and callable(getattr(research_modes, n))}
    missing = _EXPECTED_DRIVERS - present
    assert not missing, f"CLI dispatch would break — drivers vanished: {missing}"


def test_drivers_take_no_required_args():
    """The engine invokes each driver with no positional args — a driver that
    grows a required parameter would break dispatch."""
    for name in _EXPECTED_DRIVERS:
        sig = inspect.signature(getattr(research_modes, name))
        required = [p for p in sig.parameters.values()
                    if p.default is inspect._empty
                    and p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY)]
        assert not required, f"{name} grew a required arg: {required}"


def test_research_out_redirects_under_logs(tmp_path, monkeypatch):
    """Research-mode outputs go under APP_DIR/logs/ (gitignored), not repo root."""
    monkeypatch.setattr(research_modes, "APP_DIR", tmp_path)
    out = research_modes._research_out("scale_analysis_summary.json")
    assert out == tmp_path / "logs" / "scale_analysis_summary.json"
    assert out.parent.is_dir()          # dir created

def test_research_out_falls_back_to_cwd_logs(monkeypatch):
    monkeypatch.setattr(research_modes, "APP_DIR", None)
    from pathlib import Path
    out = research_modes._research_out("x.png")
    assert out == Path("logs") / "x.png"


def test_ppt_export_contract():
    assert callable(ppt_export.export_to_ppt)
    params = list(inspect.signature(ppt_export.export_to_ppt).parameters)
    # Engine calls export_to_ppt(results, trades, charts=...) after _sync_ppt_export.
    assert params[:2] == ["results", "trades"]
    assert "charts" in params
