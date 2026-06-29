"""IBKR price-snapshot helpers — pure, testable adapters around the live
feed code in Portfolio_Optimiser.fetch_ibkr_live_prices_native.

Two functions and one constant:

  _ibkr_pick_price        Best available price from an ib_insync Ticker
                          (last → close → bid/ask midpoint), rejecting the
                          -1 sentinel IBKR uses for "no data".
  apply_ibkr_price_override
                          Merge IBKR snapshots into the engine's yfinance
                          last_px series, warning on per-ticker divergence
                          beyond IBKR_DIVERGENCE_WARN_BPS.
  IBKR_DIVERGENCE_WARN_BPS  100 (= 1%). Canonical here; engine imports it.

Used by:
  Portfolio_Optimiser.fetch_ibkr_live_prices_native  (the live snapshot fetch)
  tests/test_ibkr_mapping.py                         (14 regression tests)
"""
from __future__ import annotations

import math
from typing import Optional

import pandas as pd


# Canonical threshold for the engine + tests + any future IBKR tooling.
# Warns when IBKR snapshot diverges from yfinance by more than this on a
# given ticker (helps catch stale yfinance data + dividend-day mismatches).
IBKR_DIVERGENCE_WARN_BPS = 100   # 100 bps = 1%


def _ibkr_pick_price(tk) -> Optional[float]:
    """Best available price from an ib_insync Ticker, or None.

    Requires v > 0: IBKR uses -1.0 as a 'no data' sentinel that's still
    finite, and accepting it would torch downstream cash-flow maths."""
    def _ok(v):
        return (v is not None and isinstance(v, (int, float))
                and math.isfinite(v) and v > 0.0)
    if _ok(tk.last):
        return float(tk.last)
    if _ok(tk.close):
        return float(tk.close)
    if _ok(tk.bid) and _ok(tk.ask):
        return (float(tk.bid) + float(tk.ask)) / 2.0
    return None


def apply_ibkr_price_override(
    last_px_hold: pd.Series,
    ibkr_prices: dict[str, float],
) -> tuple[pd.Series, dict]:
    """Replace yfinance last-prices with IBKR's where available. Returns
    (updated_series, diagnostics)."""
    if not ibkr_prices:
        return last_px_hold, {"n_overridden": 0, "n_warn": 0, "max_bps": 0.0}
    updated = last_px_hold.copy()
    n_over = 0
    n_warn = 0
    max_bps_abs = 0.0
    max_bps_ticker = ""
    for ticker, ibkr_px in ibkr_prices.items():
        if ticker not in updated.index:
            continue
        # Defensive: reject non-positive IBKR prices (sentinel values like -1).
        if not (isinstance(ibkr_px, (int, float)) and math.isfinite(ibkr_px) and ibkr_px > 0):
            print(f"[ibkr-price][WARN] {ticker}: rejecting non-positive IBKR "
                  f"price {ibkr_px} (keeping yfinance)")
            continue
        yf_px = pd.to_numeric(updated.get(ticker), errors="coerce")
        if pd.notna(yf_px) and float(yf_px) > 0:
            diff_bps = (ibkr_px - float(yf_px)) / float(yf_px) * 10_000
            if abs(diff_bps) > max_bps_abs:
                max_bps_abs = abs(diff_bps)
                max_bps_ticker = ticker
            if abs(diff_bps) > IBKR_DIVERGENCE_WARN_BPS:
                print(f"[ibkr-price][WARN] {ticker}: IBKR {ibkr_px:.4f} vs "
                      f"yfinance {float(yf_px):.4f} ({diff_bps:+.1f} bps)")
                n_warn += 1
        updated.loc[ticker] = ibkr_px
        n_over += 1
    return updated, {"n_overridden": n_over, "n_warn": n_warn,
                     "max_bps": max_bps_abs, "max_bps_ticker": max_bps_ticker}
