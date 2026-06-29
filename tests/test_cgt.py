"""Smoke tests for the CGT model.

Validates the FY-netted, 12mo-discount, loss-carry-forward behaviour that
makes the after-tax Sharpe honest. Most "after-tax" engines ignore FY netting
and overstate by 100+ bps.
"""
from __future__ import annotations

import pandas as pd
import pytest

from conftest import extract_funcs  # noqa: F401 (kept for parity with other test files)

# CGT helpers moved to cgt.py (Phase 4 split, 2026-06-29) — import directly.
import sys
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import cgt as _cgt_mod


@pytest.fixture(scope="module")
def cgt():
    return {
        "compute_cgt_tax":        _cgt_mod.compute_cgt_tax,
        "_security_from_row":     _cgt_mod._security_from_row,
        "_trade_delta_col":       _cgt_mod._trade_delta_col,
        "_allocate_sale_to_lots": _cgt_mod._allocate_sale_to_lots,
        "_is_long_term_au":       _cgt_mod._is_long_term_au,
        "TRADE_DELTA_CANDIDATES": _cgt_mod.TRADE_DELTA_CANDIDATES,
    }


def _trade(delta_units: int, ticker: str = "SMH", last_px: float = 100.0) -> pd.DataFrame:
    return pd.DataFrame({
        "Security": [ticker],
        "Curr Units": [abs(delta_units) if delta_units < 0 else 0],
        "Target Units": [0 if delta_units < 0 else abs(delta_units)],
        "Delta Units": [delta_units],
        "Last Px (AUD)": [last_px],
    }).set_index("Security")


def _lots(*rows) -> pd.DataFrame:
    """rows = (security, acq_date_iso, units, cost_per_unit_aud)
    Note: engine treats CostBaseAUD as PER UNIT, not total. See
    _allocate_sale_to_lots line ~4841."""
    return pd.DataFrame(
        [{"Security": s, "AcqDate": pd.Timestamp(d), "Units": u, "CostBaseAUD": c}
         for s, d, u, c in rows]
    )


def test_no_sells_means_no_tax(cgt):
    """A trade plan with only BUYs (positive delta) produces zero CGT."""
    trades = _trade(+100)  # buying
    lots = _lots(("SMH", "2024-01-01", 0, 0.0))
    tax, bkd = cgt["compute_cgt_tax"](
        trades, lots, sale_date=pd.Timestamp("2026-06-18"),
        marginal_rate=0.30, carry_forward_loss=0.0, method="HIFO",
    )
    assert tax == 0.0
    assert bkd["taxable"] == 0.0
    assert bkd["loss_carry_forward"] == 0.0


def test_short_term_gain_taxed_at_full_mtr(cgt):
    """ST gain (<365 days): taxed at full MTR with no discount."""
    trades = _trade(-100, last_px=150.0)  # sell 100 @ $150
    lots = _lots(("SMH", "2026-03-01", 100, 100.0))  # acquired 3 mo ago @ $100
    tax, bkd = cgt["compute_cgt_tax"](
        trades, lots, sale_date=pd.Timestamp("2026-06-18"),
        marginal_rate=0.30, carry_forward_loss=0.0, method="FIFO",
    )
    # Gain = (150 - 100) * 100 = 5,000. Tax = 5,000 * 0.30 = 1,500.
    assert bkd["st_gain"] == pytest.approx(5000.0)
    assert bkd["lt_gain"] == pytest.approx(0.0)
    assert tax == pytest.approx(1500.0)
    assert bkd["loss_carry_forward"] == 0.0


def test_long_term_gain_gets_50pc_discount(cgt):
    """LT gain (>=365 days): taxed on 50% of gain at full MTR."""
    trades = _trade(-100, last_px=150.0)
    lots = _lots(("SMH", "2024-01-01", 100, 100.0))  # acquired >2yr ago
    tax, bkd = cgt["compute_cgt_tax"](
        trades, lots, sale_date=pd.Timestamp("2026-06-18"),
        marginal_rate=0.30, carry_forward_loss=0.0, method="FIFO",
    )
    # Gain = 5,000 LT. Discounted to 2,500. Tax = 2,500 * 0.30 = 750.
    assert bkd["st_gain"] == pytest.approx(0.0)
    assert bkd["lt_gain"] == pytest.approx(5000.0)
    assert bkd["discounted_lt_after_losses"] == pytest.approx(2500.0)
    assert tax == pytest.approx(750.0)


def test_losses_offset_gains_before_discount(cgt):
    """Losses are applied against gains before the 50% LT discount is taken.
    This is the AU rule and the most common modelling error."""
    # Two parcels: one with $5k LT GAIN (sold), one with $3k loss (sold).
    trades = pd.DataFrame({
        "Security": ["SMH", "VLUE.AX"],
        "Curr Units": [100, 50],
        "Target Units": [0, 0],
        "Delta Units": [-100, -50],
        "Last Px (AUD)": [150.0, 80.0],
    }).set_index("Security")
    lots = _lots(
        ("SMH",     "2024-01-01", 100, 100.0),  # LT gain 5,000
        ("VLUE.AX", "2024-01-01",  50, 140.0),  # LT loss 3,000 (sold @80 vs cost 140)
    )
    tax, bkd = cgt["compute_cgt_tax"](
        trades, lots, sale_date=pd.Timestamp("2026-06-18"),
        marginal_rate=0.30, carry_forward_loss=0.0, method="FIFO",
    )
    # LT gain 5,000 - LT loss 3,000 = 2,000 net LT.
    # Discounted to 1,000. Tax = 1,000 * 0.30 = 300.
    assert bkd["lt_gain"] == pytest.approx(2000.0)
    assert tax == pytest.approx(300.0)
    assert bkd["loss_carry_forward"] == 0.0


def test_excess_losses_carry_forward(cgt):
    """If losses exceed gains, the excess flows to loss_carry_forward."""
    trades = pd.DataFrame({
        "Security": ["SMH", "VLUE.AX"],
        "Curr Units": [100, 100],
        "Target Units": [0, 0],
        "Delta Units": [-100, -100],
        "Last Px (AUD)": [150.0, 50.0],
    }).set_index("Security")
    lots = _lots(
        ("SMH",     "2024-01-01", 100, 100.0),  # LT gain 5,000
        ("VLUE.AX", "2024-01-01", 100, 150.0),  # LT loss 10,000 (50 vs 150)
    )
    tax, bkd = cgt["compute_cgt_tax"](
        trades, lots, sale_date=pd.Timestamp("2026-06-18"),
        marginal_rate=0.30, carry_forward_loss=0.0, method="FIFO",
    )
    # Losses (10,000) > gains (5,000). Tax = 0. Carry forward = 5,000.
    assert tax == 0.0
    assert bkd["loss_carry_forward"] == pytest.approx(5000.0)


def test_carry_forward_input_used(cgt):
    """Prior-FY losses passed in via carry_forward_loss reduce taxable."""
    trades = _trade(-100, last_px=150.0)
    lots = _lots(("SMH", "2026-03-01", 100, 100.0))  # ST gain 5,000
    tax, bkd = cgt["compute_cgt_tax"](
        trades, lots, sale_date=pd.Timestamp("2026-06-18"),
        marginal_rate=0.30, carry_forward_loss=2000.0, method="FIFO",
    )
    # ST gain 5,000 - prior losses 2,000 = 3,000 net ST. Tax = 3,000 * 0.30 = 900.
    assert bkd["st_gain"] == pytest.approx(3000.0)
    assert tax == pytest.approx(900.0)


def test_zero_trade_safe(cgt):
    """Empty trade DataFrame returns zero, doesn't crash."""
    trades = pd.DataFrame(columns=["Security", "Curr Units", "Target Units",
                                   "Delta Units", "Last Px (AUD)"]).set_index("Security")
    lots = _lots(("SMH", "2024-01-01", 100, 100.0))
    tax, bkd = cgt["compute_cgt_tax"](
        trades, lots, sale_date=pd.Timestamp("2026-06-18"),
        marginal_rate=0.30, carry_forward_loss=0.0, method="FIFO",
    )
    assert tax == 0.0
    assert bkd["taxable"] == 0.0
