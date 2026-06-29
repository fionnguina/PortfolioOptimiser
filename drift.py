"""Live vs OOS drift tracker — Tier-1 #3 from AUDIT.md.

Extracted from Portfolio_Optimiser.py for testability + module-split prep.
Three layers of drift detection:

  v1  Recommendation log    written by jsonl_logs.append_trade_recommendation_log
  v2  Fill comparison       compute_fill_drift joins fills_df (from Excel
                             Actual_Fills sheet) against the rec log,
                             computes slippage_bps + fee_delta + adherence.
  v3  Monthly NAV drift     compute_monthly_nav_drift compares live NAV
                             month-over-month vs OOS-expected returns.

Plus `_print_drift_warnings` which prints `[drift][WARN]` lines on any
breach of the DRIFT_* thresholds (canonical here, imported back into the
engine for the config-snapshot log line).

See ARCHITECTURE.md §5 "The drift tracker (Tier-1 #3)" for design.
"""
from __future__ import annotations

import pandas as pd

from jsonl_logs import _load_recommendation_log


# === Drift warning thresholds ================================================
# Tuned conservatively for paper-trading discovery. Tighten once live-trading
# baselines are established (LIVE_TRADING_START_DATE → flip drift v3 active).
DRIFT_MONTHLY_THRESH       = 0.02   # warn if |monthly drift|       > 2%
DRIFT_CUMULATIVE_THRESH    = 0.05   # warn if |cumulative drift|    > 5%
DRIFT_DD_ALERT_THRESH      = -0.10  # warn if live MaxDD            < -10%
DRIFT_SLIPPAGE_BPS_THRESH  = 25.0   # warn if |slippage|            > 25 bps
DRIFT_FEE_MULTIPLIER       = 2.0    # warn if actual fees           > 2x expected


def _match_fill_to_recommendation(fill_row: pd.Series, recs: list[dict]) -> dict | None:
    """Find the most recent recommendation (on or before fill_date) for
    fill_row's ticker. Returns the per-trade rec dict (with px_aud,
    brokerage_aud, etc) or None if no match."""
    if not recs:
        return None
    fill_dt = pd.Timestamp(fill_row["Fill Date"])
    ticker = str(fill_row["Ticker"]).strip()
    best_rec_t = None
    best_trade = None
    for rec in recs:
        try:
            rec_dt = pd.Timestamp(rec["run_at"])
        except Exception:
            continue
        if rec_dt > fill_dt:
            continue
        for t in rec.get("recommended_trades", []):
            if str(t.get("ticker")).strip() != ticker:
                continue
            if best_rec_t is None or rec_dt > best_rec_t:
                best_rec_t = rec_dt
                best_trade = dict(t)
                best_trade["recommendation_at"] = rec_dt.isoformat(timespec="seconds")
    return best_trade


def compute_fill_drift(fills_df: pd.DataFrame, log_path) -> pd.DataFrame:
    """Join Actual_Fills against recommendation log. Returns DataFrame with
    slippage_bps, fee_delta_aud, time_to_fill_days, and adherence flags."""
    if fills_df is None or fills_df.empty:
        return pd.DataFrame()
    recs = _load_recommendation_log(log_path)
    rows: list[dict] = []
    for _, fr in fills_df.iterrows():
        matched = _match_fill_to_recommendation(fr, recs)
        actual_units = float(fr["Units"])
        actual_px = float(fr.get("Px AUD") or 0)
        actual_fees = float(fr.get("Fees AUD") or 0)
        side_actual = "buy" if actual_units > 0 else ("sell" if actual_units < 0 else "flat")
        if matched is None:
            rows.append({
                "Fill Date": fr["Fill Date"], "Ticker": fr["Ticker"],
                "Side Actual": side_actual,
                "Units Actual": actual_units, "Px Actual (AUD)": actual_px,
                "Fees Actual (AUD)": actual_fees,
                "Recommended": False, "Px Recommended (AUD)": None,
                "Units Recommended": None, "Slippage (bps)": None,
                "Fee Expected (AUD)": None, "Fee Delta (AUD)": None,
                "Time-to-Fill (days)": None, "Notes": fr.get("Notes", ""),
            })
            continue
        rec_px = float(matched.get("px_aud") or 0)
        rec_units = int(matched.get("delta_units") or 0)
        rec_broke = float(matched.get("brokerage_aud") or 0)
        rec_at = pd.Timestamp(matched["recommendation_at"])
        # Slippage: + means worse than expected (paid more on buy, got less on sell).
        if rec_px > 0:
            if side_actual == "buy":
                slip_bps = (actual_px - rec_px) / rec_px * 10000.0
            elif side_actual == "sell":
                slip_bps = (rec_px - actual_px) / rec_px * 10000.0
            else:
                slip_bps = 0.0
        else:
            slip_bps = None
        fee_delta = actual_fees - rec_broke
        ttf = (pd.Timestamp(fr["Fill Date"]) - rec_at).total_seconds() / 86400.0
        rows.append({
            "Fill Date": fr["Fill Date"], "Ticker": fr["Ticker"],
            "Side Actual": side_actual,
            "Units Actual": actual_units, "Px Actual (AUD)": actual_px,
            "Fees Actual (AUD)": actual_fees,
            "Recommended": True, "Px Recommended (AUD)": rec_px,
            "Units Recommended": rec_units,
            "Slippage (bps)": round(slip_bps, 2) if slip_bps is not None else None,
            "Fee Expected (AUD)": round(rec_broke, 2),
            "Fee Delta (AUD)": round(fee_delta, 2),
            "Time-to-Fill (days)": round(ttf, 2),
            "Notes": fr.get("Notes", ""),
        })
    return pd.DataFrame(rows)


def compute_live_max_drawdown(nav_series: pd.Series) -> float:
    """Current drawdown from peak. Negative number, 0 if no data."""
    if nav_series is None or nav_series.empty:
        return 0.0
    peak = nav_series.cummax()
    dd = (nav_series / peak - 1.0)
    return float(dd.iloc[-1])  # current drawdown (last observation)


def compute_monthly_nav_drift(
    live_nav: pd.Series,
    oos_returns: pd.Series,
    live_start_date: str | None,
) -> pd.DataFrame:
    """Per-month live vs OOS-expected returns since live_start_date.

    Columns: Month | Live Return | OOS Return | Drift | Cumulative Drift
    """
    if live_start_date is None or live_nav is None or live_nav.empty:
        return pd.DataFrame()
    start = pd.Timestamp(live_start_date)
    nav = live_nav.loc[live_nav.index >= start]
    if nav.empty or len(nav) < 2:
        return pd.DataFrame()
    oos = (oos_returns if oos_returns is not None else pd.Series(dtype=float))
    oos = oos.copy()
    if not oos.empty:
        oos.index = pd.to_datetime(oos.index).tz_localize(None)
    months: list[dict] = []
    # Live NAV resampled to month-end (last observed NAV per month).
    nav_me = nav.resample("ME").last().dropna()
    # Baseline = first NAV in the live period (could be mid-month).
    prev_nav = float(nav.iloc[0])
    prev_dt = nav.index[0]
    for month_end, end_nav in nav_me.items():
        # Skip the first month-end if it's <= baseline date (tautological).
        if month_end <= prev_dt:
            continue
        live_ret = float(end_nav / prev_nav - 1.0) if prev_nav > 0 else 0.0
        # OOS expected return = product of OOS daily returns over the same window.
        if not oos.empty:
            window = oos[(oos.index > prev_dt) & (oos.index <= month_end)]
            oos_ret = float((1.0 + window).prod() - 1.0) if not window.empty else 0.0
        else:
            oos_ret = 0.0
        drift = live_ret - oos_ret
        months.append({
            "Month": month_end.strftime("%Y-%m"),
            "Live Return": round(live_ret, 6),
            "OOS Return": round(oos_ret, 6),
            "Drift": round(drift, 6),
        })
        prev_nav = float(end_nav)
        prev_dt = month_end
    if not months:
        return pd.DataFrame()
    df = pd.DataFrame(months)
    df["Cumulative Drift"] = df["Drift"].cumsum().round(6)
    return df


def _print_drift_warnings(
    fills_drift_df: pd.DataFrame,
    nav_drift_df: pd.DataFrame,
    live_dd: float,
) -> int:
    """Print warnings on threshold breaches. Returns count of warnings issued."""
    n_warn = 0
    # Slippage / fee warnings per fill
    if fills_drift_df is not None and not fills_drift_df.empty:
        slip = pd.to_numeric(fills_drift_df.get("Slippage (bps)"), errors="coerce")
        fee_delta = pd.to_numeric(fills_drift_df.get("Fee Delta (AUD)"), errors="coerce")
        fee_exp = pd.to_numeric(fills_drift_df.get("Fee Expected (AUD)"), errors="coerce")
        # Slippage breach
        slip_breach = fills_drift_df[slip.abs() > DRIFT_SLIPPAGE_BPS_THRESH]
        for _, r in slip_breach.iterrows():
            print(f"[drift][WARN] {r['Ticker']} on {pd.Timestamp(r['Fill Date']).date()}: "
                  f"slippage {r['Slippage (bps)']:+.1f} bps "
                  f"(threshold ±{DRIFT_SLIPPAGE_BPS_THRESH:.0f})")
            n_warn += 1
        # Fee multiplier breach
        fee_breach_mask = (fee_exp > 0) & (fee_delta + fee_exp > DRIFT_FEE_MULTIPLIER * fee_exp)
        fee_breach = fills_drift_df[fee_breach_mask]
        for _, r in fee_breach.iterrows():
            print(f"[drift][WARN] {r['Ticker']} on {pd.Timestamp(r['Fill Date']).date()}: "
                  f"fees {r['Fees Actual (AUD)']:.2f} > {DRIFT_FEE_MULTIPLIER:.0f}x expected "
                  f"({r['Fee Expected (AUD)']:.2f})")
            n_warn += 1
        # Non-adherent fills (recommendation missing)
        non_adherent = fills_drift_df[fills_drift_df["Recommended"] == False]
        if not non_adherent.empty:
            print(f"[drift][WARN] {len(non_adherent)} fill(s) had NO matching "
                  f"recommendation in the log "
                  f"(tickers: {sorted(non_adherent['Ticker'].astype(str).unique().tolist())})")
            n_warn += 1
    # Monthly + cumulative NAV drift
    if nav_drift_df is not None and not nav_drift_df.empty:
        for _, r in nav_drift_df.iterrows():
            if abs(float(r["Drift"])) > DRIFT_MONTHLY_THRESH:
                print(f"[drift][WARN] {r['Month']}: monthly drift "
                      f"{float(r['Drift'])*100:+.2f}% > ±{DRIFT_MONTHLY_THRESH*100:.0f}% "
                      f"(live {float(r['Live Return'])*100:+.2f}% vs OOS "
                      f"{float(r['OOS Return'])*100:+.2f}%)")
                n_warn += 1
        cum = float(nav_drift_df["Cumulative Drift"].iloc[-1])
        if abs(cum) > DRIFT_CUMULATIVE_THRESH:
            print(f"[drift][WARN] cumulative drift since live start: "
                  f"{cum*100:+.2f}% > ±{DRIFT_CUMULATIVE_THRESH*100:.0f}%")
            n_warn += 1
    # Live drawdown alert
    if live_dd < DRIFT_DD_ALERT_THRESH:
        print(f"[drift][WARN] live MaxDD {live_dd*100:+.2f}% < "
              f"{DRIFT_DD_ALERT_THRESH*100:+.0f}% threshold — review regime exposure")
        n_warn += 1
    return n_warn
