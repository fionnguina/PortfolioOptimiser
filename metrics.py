"""Pure-math performance metrics — Sharpe, Sortino, MaxDD, IR, CAPM α/β, FF5 α.

Extracted from Portfolio_Optimiser.py for testability + module-split
prep. All functions are pure: take pandas Series/DataFrame inputs,
return float / dict / tuple. No module-level globals, no engine state,
no I/O.

ANNUAL_TRADING_DAYS = 252 is the annualisation constant. Kept local to
this module so callers don't need to import it separately. If the
project ever spans markets with different trading-day counts, this
becomes a per-call parameter, but for AU + US ETF universe 252 is fine.

Used by:
  Portfolio_Optimiser.choose_portfolio_for_tradeplan  (calls _annualized_sharpe)
  Portfolio_Optimiser.compute_oos_metrics             (calls all 5)
  tests/test_metrics_helpers.py                       (regression tests)
"""
from __future__ import annotations

import numpy as np
import pandas as pd


ANNUAL_TRADING_DAYS = 252


def _periods_per_year(r: pd.Series) -> float:
    """Observations per year implied by the series' OWN index.

    The OOS panel is the UNION of the AU and US trading calendars, so a
    "year" there is ~258 rows, not 252. Assuming 252 inflates the implied
    year count (2579 rows read as 10.23y for a 10.0y window) and silently
    understates every annualised figure. Derive it from elapsed time and
    fall back to the constant when the index can't support it (non-datetime
    index, <2 points, or an implausible result from a degenerate span).
    """
    idx = getattr(r, "index", None)
    if not isinstance(idx, pd.DatetimeIndex) or len(r) < 2:
        return float(ANNUAL_TRADING_DAYS)
    span_years = (idx[-1] - idx[0]).days / 365.25
    if span_years <= 0:
        return float(ANNUAL_TRADING_DAYS)
    ppy = len(r) / span_years
    # Sanity band: real daily calendars land ~250-262. Anything outside it
    # means a gappy or sub-daily index — trust the constant instead.
    return float(ppy) if 200.0 <= ppy <= 400.0 else float(ANNUAL_TRADING_DAYS)


def _annualized_sharpe(returns: pd.Series, rf_annual: float,
                       periods_per_year: float | None = None) -> float:
    """Calculate annualized Sharpe ratio.

    `periods_per_year` defaults to ANNUAL_TRADING_DAYS so existing callers
    keep the 252 convention; _series_metrics passes the calendar-derived
    value so the table agrees with its own chart.
    """
    r = pd.to_numeric(returns, errors="coerce").dropna()
    if r.empty:
        return np.nan
    ppy = float(periods_per_year) if periods_per_year else float(ANNUAL_TRADING_DAYS)
    rf_daily = (1.0 + rf_annual) ** (1.0 / ppy) - 1.0
    excess = r - rf_daily
    vol = excess.std(ddof=1)
    if vol <= 0 or not np.isfinite(vol):
        return np.nan
    return excess.mean() / vol * np.sqrt(ppy)


def _series_metrics(ret: pd.Series, rf_annual: float = 0.0) -> dict:
    r = pd.to_numeric(ret, errors="coerce").dropna()
    if r.empty:
        return {"Cumulative Return": np.nan, "Annualised Return": np.nan,
                "Annualised Volatility": np.nan, "Sharpe Ratio": np.nan,
                "Sortino Ratio": np.nan, "Max Drawdown": np.nan}
    cum = (1.0 + r).cumprod()
    total = float(cum.iloc[-1] - 1.0)
    ppy = _periods_per_year(r)
    n_years = len(r) / ppy
    ann_ret = (1.0 + total) ** (1.0 / n_years) - 1.0 if n_years > 0 else np.nan
    ann_vol = float(r.std(ddof=1) * np.sqrt(ppy))
    sharpe = _annualized_sharpe(r, rf_annual, periods_per_year=ppy)

    # Sortino: penalise only downside vol (MAR = 0). Annualised mean / annualised
    # downside semi-deviation. Uses sqrt(mean(min(r,0)^2)) so flat days don't
    # inflate the denominator.
    rf_daily = (1.0 + rf_annual) ** (1.0 / ppy) - 1.0
    excess = r - rf_daily
    downside = np.minimum(excess, 0.0)
    dd_dev = float(np.sqrt(np.mean(downside ** 2)))
    if dd_dev > 0 and np.isfinite(dd_dev):
        sortino = float(excess.mean() * ppy / (dd_dev * np.sqrt(ppy)))
    else:
        sortino = np.nan

    dd = (cum / cum.cummax()) - 1.0
    return {"Cumulative Return": total, "Annualised Return": float(ann_ret),
            "Annualised Volatility": ann_vol, "Sharpe Ratio": float(sharpe),
            "Sortino Ratio": sortino, "Max Drawdown": float(dd.min())}


def _ir_vs_bench(strat: pd.Series, bench: pd.Series) -> float:
    pair = pd.concat([strat.rename("s"), bench.rename("b")], axis=1).dropna()
    if pair.empty:
        return np.nan
    diff = pair["s"] - pair["b"]
    sigma = float(diff.std(ddof=1) * np.sqrt(ANNUAL_TRADING_DAYS))
    return float(diff.mean() * ANNUAL_TRADING_DAYS / sigma) if sigma > 0 else np.nan


def _capm_alpha_beta(strat: pd.Series, bench: pd.Series) -> tuple[float, float]:
    pair = pd.concat([strat.rename("s"), bench.rename("b")], axis=1).dropna()
    if len(pair) < 30:
        return np.nan, np.nan
    X = np.column_stack([np.ones(len(pair)), pair["b"].to_numpy()])
    y = pair["s"].to_numpy()
    try:
        coef, *_ = np.linalg.lstsq(X, y, rcond=None)
        return float(coef[0]) * ANNUAL_TRADING_DAYS, float(coef[1])
    except Exception:
        return np.nan, np.nan


def _ff5_alpha(strat: pd.Series, ff5: pd.DataFrame) -> float:
    if ff5 is None or ff5.empty:
        return np.nan
    cols = ["Mkt-RF", "SMB", "HML", "RMW", "CMA", "MOM", "RF"]
    if not all(c in ff5.columns for c in cols):
        return np.nan
    fac = ff5[cols].copy()
    fac.index = pd.to_datetime(fac.index).tz_localize(None)
    pair = pd.concat([strat.rename("s"), fac], axis=1).dropna()
    if len(pair) < 60:
        return np.nan
    y = (pair["s"] - pair["RF"]).to_numpy()
    X = np.column_stack([np.ones(len(pair)),
                         pair["Mkt-RF"].to_numpy(), pair["SMB"].to_numpy(),
                         pair["HML"].to_numpy(), pair["RMW"].to_numpy(),
                         pair["CMA"].to_numpy(), pair["MOM"].to_numpy()])
    try:
        coef, *_ = np.linalg.lstsq(X, y, rcond=None)
        return float(coef[0]) * ANNUAL_TRADING_DAYS
    except Exception:
        return np.nan
