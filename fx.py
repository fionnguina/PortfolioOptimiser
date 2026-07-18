"""USD/AUD FX conversion (module split #18, 2026-07-10).

get_usd_aud_fx reads the live `fx_usdaud` series (USDAUD=X, built by the engine
at module load); the engine syncs `fx.fx_usdaud = fx_usdaud` right after building
it. Falls back LOUDLY to `default` if the series is missing (a silent 1.50 would
distort every USD valuation). fx_to_aud_for_tickers maps tickers → FX multiplier.
"""
from __future__ import annotations

import pandas as pd

# Synced by the engine after it builds the live FX series (module load ~2650).
fx_usdaud = None


def _last_numeric(series: pd.Series) -> float:
    """Get last numeric value from series."""
    v = series.iloc[-1]
    if isinstance(v, pd.Series):
        v = v.iloc[0]
    return float(v)


def get_usd_aud_fx(default: float = 1.50) -> float:
    """Get latest USD/AUD FX rate from the live `fx_usdaud` series.

    Reads the most recent valid value from the global FX series built at
    startup (line ~1309). Previously fell back silently to `default` if the
    series was missing/empty — that hardcoded 1.50 would distort every USD
    valuation by several percent. Now we LOG LOUDLY when falling back so
    the issue can't pass unnoticed in a live run.

    The `default` parameter is kept for backwards-compat but never used
    silently — see F14 in AUDIT.md.
    """
    try:
        series = globals().get("fx_usdaud")
        if isinstance(series, pd.DataFrame):
            series = series.iloc[:, 0]
        if isinstance(series, pd.Series):
            s = pd.to_numeric(series, errors="coerce").dropna()
            if not s.empty:
                last = _last_numeric(s)
                if last > 0:
                    return last
    except Exception as e:
        print(f"[fx][ERROR] fx_usdaud series unreadable ({type(e).__name__}: {e})")
    # If we got here the live FX series failed. Log loudly + use fallback.
    print(f"[fx][CRITICAL] USD/AUD fetch failed — falling back to hardcoded "
          f"{default}. ALL USD valuations will be distorted. Investigate "
          f"yfinance reachability + the [yf] fx download line.")
    return default


def fx_to_aud_for_tickers(tickers, usd_aud_rate: float) -> pd.Series:
    """Map tickers to FX rates (1.0 for AUD, usd_aud_rate for USD)."""
    out = {}
    for t in map(str, tickers):
        out[t] = 1.0 if t.startswith("^") or t.endswith(".AX") else usd_aud_rate
    return pd.Series(out, name="FX to AUD")


def price_local_to_aud(ticker, price_local, fx_map):
    """Convert a LOCAL-currency price to AUD using `fx_map` (ticker -> AUD rate).

    Returns None when the rate is unknown for a NON-AUD ticker — refusing to
    price it is safer than pricing it wrong, which is exactly the bug that made
    live TLH compare an AUD cost base against a USD price and fabricate a ~30%
    loss (2026-07-17). `.AX` (and `^` index) tickers fall back to 1.0 since
    local IS AUD for them. Pure — used by the live TLH price snapshot and
    unit-tested; keep it in sync with fx_to_aud_for_tickers' AUD rule.
    """
    try:
        rate = pd.Series(fx_map).get(str(ticker))
        rate = float(rate) if rate is not None else None
    except (TypeError, ValueError):
        rate = None
    # None / NaN (rate != rate) / ±inf / non-positive → unusable rate.
    if (rate is None or rate != rate or rate in (float("inf"), float("-inf"))
            or rate <= 0):
        if str(ticker).endswith(".AX") or str(ticker).startswith("^"):
            rate = 1.0
        else:
            return None
    try:
        return float(price_local) * float(rate)
    except (TypeError, ValueError):
        return None
