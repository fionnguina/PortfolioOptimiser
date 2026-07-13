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


def test_ppt_export_contract():
    assert callable(ppt_export.export_to_ppt)
    params = list(inspect.signature(ppt_export.export_to_ppt).parameters)
    # Engine calls export_to_ppt(results, trades, charts=...) after _sync_ppt_export.
    assert params[:2] == ["results", "trades"]
    assert "charts" in params
