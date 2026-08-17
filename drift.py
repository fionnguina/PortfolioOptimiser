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

    def _num(v):
        """float(v) or None — treats NaN and non-numeric as missing so an
        absent Px/Fees column can't masquerade as a real 0 (which would show
        as -100% slippage or a spurious negative fee delta)."""
        try:
            f = float(v)
            return f if f == f else None
        except (TypeError, ValueError):
            return None

    rows: list[dict] = []
    for _, fr in fills_df.iterrows():
        matched = _match_fill_to_recommendation(fr, recs)
        actual_units = float(fr["Units"])
        actual_px = _num(fr.get("Px AUD"))
        actual_fees = _num(fr.get("Fees AUD"))
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
        # Requires a real actual fill price — skip when Px AUD is absent.
        if rec_px > 0 and actual_px is not None:
            if side_actual == "buy":
                slip_bps = (actual_px - rec_px) / rec_px * 10000.0
            elif side_actual == "sell":
                slip_bps = (rec_px - actual_px) / rec_px * 10000.0
            else:
                slip_bps = 0.0
        else:
            slip_bps = None
        # Fee delta only when actual fees were captured (older ledgers have none).
        fee_delta = (actual_fees - rec_broke) if actual_fees is not None else None
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
            "Fee Delta (AUD)": round(fee_delta, 2) if fee_delta is not None else None,
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


def _net_out_fy_tax(oos: pd.Series, oos_taxes: pd.Series | None):
    """Add back the simulated FY tax charge, and report it per day.

    The backtest settles the Australian financial year's CGT at the first
    rebalance after 30 June, subtracting it ADDITIVELY from that segment's
    first realised day (oos_engine: `seg_b.iloc[0] -= cost_frac + tax_frac`),
    so adding the fraction back on that same day is exact rather than an
    approximation.

    Why bother: live NetLiq does NOT book that charge — the live book reserves
    accrued CGT as un-investable cash instead — so comparing the two makes the
    tracker fire every July for a modelling convention rather than a tracking
    error. July 2026 was +5.24pp of "drift", of which ~5.5pp was a 5.53% tax
    settlement on 2026-07-13 and only ~-0.5pp was the real holdings
    difference. Every one of the 10 charges in the backtest lands in July or
    early August, so this recurs annually — and since cumulative drift is a
    cumsum, one false July latches the breach permanently.
    """
    if oos is None or oos.empty or oos_taxes is None or len(oos_taxes) == 0:
        return oos, pd.Series(0.0, index=(oos.index if oos is not None else []))
    taxes = pd.Series(oos_taxes).copy()
    try:
        taxes.index = pd.to_datetime(taxes.index).tz_localize(None)
    except Exception:
        return oos, pd.Series(0.0, index=oos.index)
    ex = oos.copy()
    per_day = pd.Series(0.0, index=oos.index)
    for t, frac in taxes.items():
        try:
            f = float(frac)
        except Exception:
            continue
        if f <= 0:
            continue
        after = ex.index[ex.index > pd.Timestamp(t)]
        if len(after) == 0:
            continue
        d = after[0]
        ex.loc[d] = float(ex.loc[d]) + f
        per_day.loc[d] = per_day.loc[d] + f
    return ex, per_day


def compute_monthly_nav_drift(
    live_nav: pd.Series,
    oos_returns: pd.Series,
    live_start_date: str | None,
    oos_taxes: pd.Series | None = None,
) -> pd.DataFrame:
    """Per-month live vs OOS-expected returns since live_start_date.

    Columns: Month | Live Return | OOS Return | OOS Tax | OOS ex-Tax |
             Drift | Cumulative Drift

    Drift is measured against OOS ex-Tax, so it reflects PERFORMANCE
    divergence. The FY tax settlement is shown in its own column rather than
    masquerading as tracking error — see _net_out_fy_tax.
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
    oos_ex, tax_daily = _net_out_fy_tax(oos, oos_taxes)
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
            mask = (oos.index > prev_dt) & (oos.index <= month_end)
            window = oos[mask]
            oos_ret = float((1.0 + window).prod() - 1.0) if not window.empty else 0.0
            win_ex = oos_ex[mask]
            oos_ret_ex = float((1.0 + win_ex).prod() - 1.0) if not win_ex.empty else 0.0
            tax_in_month = float(tax_daily[mask].sum()) if not window.empty else 0.0
        else:
            oos_ret = oos_ret_ex = tax_in_month = 0.0
        # Compare like with like: live NetLiq does not book the FY tax charge.
        drift = live_ret - oos_ret_ex
        months.append({
            "Month": month_end.strftime("%Y-%m"),
            "Live Return": round(live_ret, 6),
            "OOS Return": round(oos_ret, 6),
            "OOS Tax": round(tax_in_month, 6),
            "OOS ex-Tax": round(oos_ret_ex, 6),
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
                _tax = float(r.get("OOS Tax", 0.0) or 0.0)
                _ex = float(r.get("OOS ex-Tax", r["OOS Return"]))
                _tx = (f", after netting out a {_tax*100:.2f}% FY tax settlement"
                       if _tax > 0 else "")
                print(f"[drift][WARN] {r['Month']}: monthly drift "
                      f"{float(r['Drift'])*100:+.2f}% > ±{DRIFT_MONTHLY_THRESH*100:.0f}% "
                      f"(live {float(r['Live Return'])*100:+.2f}% vs OOS ex-tax "
                      f"{_ex*100:+.2f}%{_tx})")
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
