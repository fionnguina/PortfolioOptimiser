"""Research/diagnostic CLI drivers (module split #18, 2026-07-10).

The 12 --flag research modes: stress-test, scale-analysis, dev-validation, the
rebal-skip / turnover / crash-hedge / stretch / tilted sweeps, walk-forward-cv,
attribution. Leaf drivers (dispatched once each from the engine, then sys.exit).
Diagnostic only — never run in the live pipeline.

Coupling (symtable-verified, gap-free incl. conditional defs): imports run_oos +
factor/cgt/brokerage bits + libs below; the engine injects 6 SHARED helper fns
(compute_oos_metrics/compute_ff5_betas/_normalize_yfinance_close/
_evaluate_sweep_result/_print_sweep_verdict/_apply_data_lockbox) + 9 config/runtime
values via _sync_research_modes() before dispatch. Validated by running the modes.
"""
from __future__ import annotations

import sys
import time
import json

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf

from brokerage import BROKER_CONFIG
from cgt import CGT_CONFIG, _effective_cgt_rate
from factors import (
    get_ff5_mom_daily,
    FACTOR_TILT_LOOKBACK_DAYS,
    FACTOR_TILT_MAX_MAGNITUDE,
    FACTOR_TILT_SHARPE_TO_MAG,
)
from oos_engine import run_oos_ensemble_walk_forward

# Injected by the engine's _sync_research_modes() before the first dispatch.
_apply_data_lockbox = None
_evaluate_sweep_result = None
_normalize_yfinance_close = None
_print_sweep_verdict = None
compute_ff5_betas = None
compute_oos_metrics = None
ANNUAL_TRADING_DAYS = None
APP_DIR = None
CRASH_HEDGE_BASKET = None
CRASH_HEDGE_DD_RELEASE = None
CRASH_HEDGE_DD_TRIGGER = None
CRASH_HEDGE_LOOKBACK_DAYS = None
ENSEMBLE_SLOT_NAMES = ()
REBALANCE_FREQ = None
prices = None


def _run_gfc_stress_test() -> int:
    print("\n" + "=" * 80)
    print("GFC STRESS TEST — ensemble walk-forward through 2007-09 GFC peak")
    print("=" * 80)

    include_stretch_compare = "--stretch-only" in sys.argv
    if include_stretch_compare:
        print("[stress] --stretch-only modifier detected: will run 5-slot AND Stretch-only side-by-side")

    STRESS_TICKERS = ["SPY", "IVV", "QQQ", "IEF", "VWO", "GOLD.AX", "^AORD"]
    START_DATE = "2005-10-01"
    END_DATE = pd.Timestamp.today().normalize().strftime("%Y-%m-%d")

    print(f"[stress] Universe ({len(STRESS_TICKERS)}): {STRESS_TICKERS}")
    print(f"[stress] Window: {START_DATE} → {END_DATE}")
    print(f"[stress] Broker:  {BROKER_CONFIG['name']} | CGT: {CGT_CONFIG.get('marginal_tax_rate', 0.30)*100:.0f}% MTR")

    t0 = time.perf_counter()
    raw = yf.download(STRESS_TICKERS, start=START_DATE, end=END_DATE,
                      interval="1d", auto_adjust=True, threads=False, progress=False)
    px = _normalize_yfinance_close(raw)
    px.index = pd.to_datetime(px.index).tz_localize(None)
    px = px.sort_index().ffill().bfill()
    missing = [t for t in STRESS_TICKERS if t not in px.columns]
    if missing:
        print(f"[stress] WARNING: missing tickers from yfinance: {missing}")
    print(f"[stress] Downloaded {px.shape[0]} days × {px.shape[1]} tickers in {time.perf_counter()-t0:.1f}s")
    print(f"[stress] First all-present row: {px.dropna(how='any').index.min().date() if not px.dropna(how='any').empty else 'never'}")

    fx_raw = yf.download("USDAUD=X", start=START_DATE, end=END_DATE,
                         interval="1d", auto_adjust=True, threads=False, progress=False)
    fx = fx_raw["Close"] if isinstance(fx_raw, pd.DataFrame) else fx_raw
    if isinstance(fx, pd.DataFrame):
        fx = fx.iloc[:, 0]
    fx = pd.to_numeric(fx, errors="coerce").reindex(px.index).ffill().bfill()
    if fx.isna().all():
        print("[stress] FX series empty — falling back to flat 1.50")
        fx = pd.Series(1.50, index=px.index)

    usd_cols = [c for c in px.columns if not str(c).endswith(".AX") and not str(c).startswith("^")]
    px_aud = px.copy()
    if usd_cols:
        px_aud.update(px.loc[:, usd_cols].mul(fx, axis=0))
    px_aud = px_aud.ffill().bfill().dropna(how="all")
    print(f"[stress] AUD-adjusted USD tickers: {usd_cols}")

    print(f"[stress] Running ensemble walk-forward (train=24mo, rebal={REBALANCE_FREQ})...")
    t1 = time.perf_counter()
    out = run_oos_ensemble_walk_forward(
        px_aud,
        train_window_months=24,
        rebalance=REBALANCE_FREQ,
        benchmark_ticker="SPY",
        score_lookback_days=252,
        lambda_temp=3.0,
    )
    strat_rets = out["blended_returns"]
    weights = out["blended_weights"]
    print(f"[stress] Walk-forward done in {time.perf_counter()-t1:.1f}s")

    if strat_rets.empty:
        print("[stress] FAIL — walk-forward returned no returns. Check universe inception dates.")
        return 1

    print(f"[stress] OOS span: {strat_rets.index.min().date()} → {strat_rets.index.max().date()} "
          f"({len(strat_rets)} days, {len(weights)} rebalances)")
    print(f"[stress] Rebal mix: scheduled={out.get('n_scheduled',0)}, "
          f"early-triggered={out.get('n_early_triggered',0)}, "
          f"executed={out.get('n_executed',0)}, skipped={out.get('n_skipped',0)}")

    spy_aud = px_aud["SPY"] if "SPY" in px_aud.columns else None
    aord = px_aud["^AORD"] if "^AORD" in px_aud.columns else None
    spy_ret = spy_aud.pct_change().dropna() if spy_aud is not None else pd.Series(dtype=float)
    aord_ret = aord.pct_change().dropna() if aord is not None else pd.Series(dtype=float)

    full_years = max(1, int(round(len(strat_rets) / ANNUAL_TRADING_DAYS)))
    metrics = compute_oos_metrics(
        strat_returns=strat_rets, spy_returns=spy_ret, aord_returns=aord_ret,
        ff5_factors=None, weights_history=weights,
        horizons_years=(3, 5, 10, full_years),
    )
    print("\n[stress] Full-history metrics (3Y / 5Y / 10Y / since-inception):")
    if not metrics.empty:
        with pd.option_context("display.max_columns", None, "display.width", 240):
            print(metrics.round(4).to_string())

    print("\n[stress] GFC-only window metrics (2007-10-01 → 2009-12-31):")
    gfc_start = pd.Timestamp("2007-10-01")
    gfc_end = pd.Timestamp("2009-12-31")
    strat_gfc = strat_rets[(strat_rets.index >= gfc_start) & (strat_rets.index <= gfc_end)]
    spy_gfc = spy_ret[(spy_ret.index >= gfc_start) & (spy_ret.index <= gfc_end)]
    aord_gfc = aord_ret[(aord_ret.index >= gfc_start) & (aord_ret.index <= gfc_end)]

    if len(strat_gfc) > 0:
        def _summary(r, label):
            if r.empty:
                return f"  {label:18s} (no data)"
            nav = (1 + r).cumprod()
            dd = nav / nav.cummax() - 1
            total_ret = nav.iloc[-1] - 1
            return (f"  {label:18s} TotRet {total_ret*100:+7.2f}%  "
                    f"MaxDD {dd.min()*100:+7.2f}%  "
                    f"VolAnn {r.std()*np.sqrt(ANNUAL_TRADING_DAYS)*100:5.2f}%")
        print(_summary(strat_gfc, "Strategy"))
        print(_summary(spy_gfc, "SPY (AUD)"))
        print(_summary(aord_gfc, "^AORD"))

        nav_strat_gfc = (1 + strat_gfc).cumprod()
        nav_spy_gfc = (1 + spy_gfc).cumprod() if not spy_gfc.empty else None
        dd_strat = (nav_strat_gfc / nav_strat_gfc.cummax() - 1).min()
        if nav_spy_gfc is not None:
            dd_spy = (nav_spy_gfc / nav_spy_gfc.cummax() - 1).min()
            if dd_spy < -0.01:
                print(f"\n  Defense ratio: Strategy MaxDD is {dd_strat/dd_spy*100:.1f}% of SPY MaxDD")
                print(f"  (100% = no defense, 0% = perfect defense)")

    try:
        fig, ax = plt.subplots(figsize=(12, 5))
        nav_strat = (1 + strat_rets).cumprod()
        nav_spy_full = (1 + spy_ret.reindex(strat_rets.index).fillna(0)).cumprod() if not spy_ret.empty else None
        dd_strat_full = nav_strat / nav_strat.cummax() - 1
        ax.plot(dd_strat_full.index, dd_strat_full * 100, label="Strategy (Ensemble)",
                linewidth=1.6, color="#1f4ea1")
        if nav_spy_full is not None:
            dd_spy_full = nav_spy_full / nav_spy_full.cummax() - 1
            ax.plot(dd_spy_full.index, dd_spy_full * 100, label="SPY (AUD)",
                    linewidth=1.1, color="#c44e4e", alpha=0.85)
        ax.axvspan(gfc_start, pd.Timestamp("2009-06-30"),
                   alpha=0.12, color="red", label="GFC")
        ax.set_title(f"GFC Stress Test — Drawdowns ({strat_rets.index.min().date()} → {strat_rets.index.max().date()})")
        ax.set_ylabel("Drawdown (%)")
        ax.axhline(0, color="black", linewidth=0.5)
        ax.legend(loc="lower right")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        out_path = APP_DIR / "gfc_stress_drawdown.png"
        fig.savefig(out_path, dpi=120)
        plt.close(fig)
        print(f"\n[stress] Drawdown chart → {out_path}")
    except Exception as e:
        print(f"[stress] Chart save failed: {e}")

    # Save summary JSON for memory + later comparison
    try:
        nav_full = (1 + strat_rets).cumprod()
        dd_full = nav_full / nav_full.cummax() - 1
        summary = {
            "run_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            "universe": STRESS_TICKERS,
            "broker": BROKER_CONFIG["name"],
            "oos_start": str(strat_rets.index.min().date()),
            "oos_end": str(strat_rets.index.max().date()),
            "full_period_total_return_pct": float((nav_full.iloc[-1] - 1) * 100),
            "full_period_max_drawdown_pct": float(dd_full.min() * 100),
            "gfc_window": {
                "start": str(gfc_start.date()),
                "end": str(gfc_end.date()),
                "strategy_total_return_pct": float(((1 + strat_gfc).cumprod().iloc[-1] - 1) * 100) if len(strat_gfc) else None,
                "strategy_max_dd_pct": float(((1 + strat_gfc).cumprod() / (1 + strat_gfc).cumprod().cummax() - 1).min() * 100) if len(strat_gfc) else None,
                "spy_total_return_pct": float(((1 + spy_gfc).cumprod().iloc[-1] - 1) * 100) if len(spy_gfc) else None,
                "spy_max_dd_pct": float(((1 + spy_gfc).cumprod() / (1 + spy_gfc).cumprod().cummax() - 1).min() * 100) if len(spy_gfc) else None,
            },
            "rebalances": {
                "scheduled": int(out.get("n_scheduled", 0)),
                "early_triggered": int(out.get("n_early_triggered", 0)),
                "executed": int(out.get("n_executed", 0)),
                "skipped": int(out.get("n_skipped", 0)),
            },
        }
        json_path = APP_DIR / "gfc_stress_summary.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        print(f"[stress] Summary JSON → {json_path}")
    except Exception as e:
        print(f"[stress] Summary JSON save failed: {e}")

    # --- Stretch-only comparison run (if --stretch-only modifier present) ---
    if include_stretch_compare:
        print("\n" + "=" * 80)
        print("STRETCH-ONLY COMPARISON — same universe, single-slot allocation")
        print("=" * 80)

        # Find Stretch slot name
        stretch_slot = None
        for name in ENSEMBLE_SLOT_NAMES:
            if "Stretch" in name:
                stretch_slot = name
                break
        if not stretch_slot:
            print("[stress] no Stretch slot found; skipping comparison")
            return 0
        print(f"[stress-stretch] forcing 100% on slot: {stretch_slot}")

        t2 = time.perf_counter()
        out_s = run_oos_ensemble_walk_forward(
            px_aud,
            train_window_months=24,
            rebalance=REBALANCE_FREQ,
            benchmark_ticker="SPY",
            score_lookback_days=252,
            lambda_temp=3.0,
            slot_weights_override={stretch_slot: 1.0},
        )
        strat_rets_s = out_s["blended_returns"]
        weights_s = out_s["blended_weights"]
        print(f"[stress-stretch] walk-forward done in {time.perf_counter()-t2:.1f}s")

        if strat_rets_s.empty:
            print("[stress-stretch] empty returns; aborting comparison.")
            return 0

        # GFC-window stats for Stretch-only
        strat_gfc_s = strat_rets_s[(strat_rets_s.index >= gfc_start)
                                    & (strat_rets_s.index <= gfc_end)]
        nav_strat_gfc_s = (1 + strat_gfc_s).cumprod() if not strat_gfc_s.empty else None
        dd_strat_s = (float((nav_strat_gfc_s / nav_strat_gfc_s.cummax() - 1).min())
                      if nav_strat_gfc_s is not None else float("nan"))
        total_ret_s = (float(nav_strat_gfc_s.iloc[-1] - 1.0)
                       if nav_strat_gfc_s is not None and not nav_strat_gfc_s.empty
                       else float("nan"))

        # === Third run: Stretch-only + crash hedge ON ===
        print(f"\n[stress-stretch-hedge] running Stretch-only WITH crash hedge "
              f"(trigger={CRASH_HEDGE_DD_TRIGGER*100:+.0f}%, "
              f"release={CRASH_HEDGE_DD_RELEASE*100:+.0f}%)")
        t3 = time.perf_counter()
        out_sh = run_oos_ensemble_walk_forward(
            px_aud,
            train_window_months=24,
            rebalance=REBALANCE_FREQ,
            benchmark_ticker="SPY",
            score_lookback_days=252,
            lambda_temp=3.0,
            slot_weights_override={stretch_slot: 1.0},
            crash_hedge=True,
        )
        strat_rets_sh = out_sh["blended_returns"]
        n_hedge_triggers = int(out_sh.get("hedge_n_triggers", 0))
        n_hedge_active = int(out_sh.get("hedge_active_rebals", 0))
        print(f"[stress-stretch-hedge] walk-forward done in {time.perf_counter()-t3:.1f}s")
        print(f"[stress-stretch-hedge] hedge fired {n_hedge_triggers}× across full window, "
              f"active on {n_hedge_active} rebalances")

        strat_gfc_sh = strat_rets_sh[(strat_rets_sh.index >= gfc_start)
                                       & (strat_rets_sh.index <= gfc_end)]
        nav_strat_gfc_sh = (1 + strat_gfc_sh).cumprod() if not strat_gfc_sh.empty else None
        dd_strat_sh = (float((nav_strat_gfc_sh / nav_strat_gfc_sh.cummax() - 1).min())
                       if nav_strat_gfc_sh is not None else float("nan"))
        total_ret_sh = (float(nav_strat_gfc_sh.iloc[-1] - 1.0)
                        if nav_strat_gfc_sh is not None and not nav_strat_gfc_sh.empty
                        else float("nan"))

        # Side-by-side print
        print(f"\n  {'Config':<30}  {'GFC TotRet':>11}  {'GFC MaxDD':>11}  "
              f"{'Defence vs SPY':>15}")
        print(f"  {'-'*30}  {'-'*11}  {'-'*11}  {'-'*15}")
        dd_spy_gfc = float((nav_spy_gfc / nav_spy_gfc.cummax() - 1).min()) \
                      if nav_spy_gfc is not None else float("nan")
        total_ret_blend = float(((1 + strat_gfc).cumprod().iloc[-1] - 1.0)) \
                          if len(strat_gfc) else float("nan")
        dd_strat_blend = float((nav_strat_gfc / nav_strat_gfc.cummax() - 1).min()) \
                          if len(strat_gfc) else float("nan")
        def _def_pct(strat_dd, spy_dd):
            if np.isnan(strat_dd) or np.isnan(spy_dd) or spy_dd >= 0:
                return float("nan")
            return strat_dd / spy_dd * 100
        print(f"  {'5-slot blend (current)':<30}  "
              f"{total_ret_blend*100:>+10.2f}%  "
              f"{dd_strat_blend*100:>+10.2f}%  "
              f"{_def_pct(dd_strat_blend, dd_spy_gfc):>14.1f}%")
        print(f"  {'Stretch-only':<30}  "
              f"{total_ret_s*100:>+10.2f}%  "
              f"{dd_strat_s*100:>+10.2f}%  "
              f"{_def_pct(dd_strat_s, dd_spy_gfc):>14.1f}%")
        print(f"  {'Stretch-only + crash hedge':<30}  "
              f"{total_ret_sh*100:>+10.2f}%  "
              f"{dd_strat_sh*100:>+10.2f}%  "
              f"{_def_pct(dd_strat_sh, dd_spy_gfc):>14.1f}%")
        print(f"  {'SPY (AUD) benchmark':<30}  "
              f"{float(((1 + spy_gfc).cumprod().iloc[-1] - 1) * 100):>+10.2f}%  "
              f"{dd_spy_gfc*100:>+10.2f}%  "
              f"{'100.0%':>15}")
        print(f"\n  (Defence vs SPY: lower = better. 100% = no defence, 0% = perfect.)")

        # Verdict
        print("\n" + "=" * 80)
        print("VERDICT — GFC defence comparison")
        print("=" * 80)
        delta_dd_s = dd_strat_s - dd_strat_blend
        delta_dd_sh = dd_strat_sh - dd_strat_blend
        hedge_saves = dd_strat_sh - dd_strat_s  # negative = hedge protects further
        print(f"  Stretch-only vs 5-slot blend:        ΔMaxDD {delta_dd_s*100:+.1f}%")
        print(f"  Stretch+hedge vs 5-slot blend:       ΔMaxDD {delta_dd_sh*100:+.1f}%")
        print(f"  Hedge's actual contribution:         {hedge_saves*100:+.1f}% "
              f"({'better' if hedge_saves < 0 else 'worse'} than Stretch-only)")
        if dd_strat_sh < dd_strat_blend + 0.02:
            print(f"\n  Reading: Stretch + hedge MATCHES 5-slot defence "
                  f"(MaxDD {dd_strat_sh*100:.1f}% vs blend {dd_strat_blend*100:.1f}%).")
            print(f"  This IS the synthesis we were hunting — modern alpha + tail protection.")
            print(f"  Recommend: ship Stretch-only + crash hedge as production config.")
        elif hedge_saves < -0.03:
            print(f"\n  Reading: hedge meaningfully helps Stretch ({hedge_saves*100:+.1f}% MaxDD) "
                  f"but still trails 5-slot blend by {delta_dd_sh*100:+.1f}%.")
            print(f"  Partial fix — could be acceptable depending on modern uplift weight.")
        else:
            print(f"\n  Reading: hedge doesn't close the gap. Stretch+hedge MaxDD "
                  f"{dd_strat_sh*100:.1f}% vs 5-slot {dd_strat_blend*100:.1f}%.")
            print(f"  Recommend: KEEP 5-slot blend if GFC defence matters more than +4%/yr modern alpha.")

        # Append to JSON
        try:
            summary["stretch_only_comparison"] = {
                "stretch_total_return_pct": float(total_ret_s * 100),
                "stretch_max_dd_pct": float(dd_strat_s * 100),
                "stretch_defence_pct": _def_pct(dd_strat_s, dd_spy_gfc),
                "stretch_hedge_total_return_pct": float(total_ret_sh * 100),
                "stretch_hedge_max_dd_pct": float(dd_strat_sh * 100),
                "stretch_hedge_defence_pct": _def_pct(dd_strat_sh, dd_spy_gfc),
                "stretch_hedge_n_triggers": n_hedge_triggers,
                "blend_defence_pct": _def_pct(dd_strat_blend, dd_spy_gfc),
                "delta_dd_stretch_vs_blend_pct": float(delta_dd_s * 100),
                "delta_dd_stretchhedge_vs_blend_pct": float(delta_dd_sh * 100),
                "hedge_saves_pct": float(hedge_saves * 100),
            }
            json_path = APP_DIR / "gfc_stress_summary.json"
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2)
            print(f"\n[stress] Updated summary JSON → {json_path}")
        except Exception as e:
            print(f"[stress] JSON update failed: {e}")

    print("\n" + "=" * 80)
    print("GFC STRESS TEST COMPLETE")
    print("=" * 80)
    return 0


def _run_scale_analysis() -> int:
    print("\n" + "=" * 88)
    print("SCALE ANALYSIS — net-of-cost OOS performance at six AUM levels")
    print("=" * 88)

    SCALES = [
        ("$10k",  10_000),
        ("$100k", 100_000),
        ("$250k", 250_000),
        ("$500k", 500_000),
        ("$1M",   1_000_000),
        ("$10M",  10_000_000),
    ]

    # Use the live universe (already-downloaded prices) — we want apples-to-
    # apples comparability with the standard OOS report.
    _scale_tickers = [c for c in prices.columns if c != "PortfolioValue"]
    print(f"[scale] universe: {len(_scale_tickers)} tickers")
    print(f"[scale] downloading 12y of long-history data...")

    t0 = time.perf_counter()
    raw = yf.download(_scale_tickers, period="12y", interval="1d",
                      auto_adjust=True, threads=False, progress=False)
    px = _normalize_yfinance_close(raw)
    px.index = pd.to_datetime(px.index).tz_localize(None)
    px = px.sort_index().ffill().bfill()
    fx_raw = yf.download("USDAUD=X", period="12y", interval="1d",
                         auto_adjust=True, threads=False, progress=False)
    fx = fx_raw["Close"] if isinstance(fx_raw, pd.DataFrame) else fx_raw
    if isinstance(fx, pd.DataFrame):
        fx = fx.iloc[:, 0]
    fx = pd.to_numeric(fx, errors="coerce").reindex(px.index).ffill().bfill()
    usd_cols = [c for c in px.columns
                if not str(c).endswith(".AX") and not str(c).startswith("^")]
    px_aud = px.copy()
    if usd_cols:
        px_aud.update(px.loc[:, usd_cols].mul(fx, axis=0))
    px_aud = px_aud.ffill().bfill().dropna(how="all")
    print(f"[scale] data ready ({px_aud.shape[0]} days × {px_aud.shape[1]} tickers) "
          f"in {time.perf_counter()-t0:.1f}s")

    # SPY + ^AORD daily returns for benchmark comparison.
    spy_ret = (px_aud["SPY"].pct_change().dropna()
               if "SPY" in px_aud.columns else pd.Series(dtype=float))
    aord_ret = (px_aud["^AORD"].pct_change().dropna()
                if "^AORD" in px_aud.columns else pd.Series(dtype=float))

    results: list[dict] = []
    for label, nav in SCALES:
        print(f"\n[scale] running OOS at {label} ({nav:,.0f} AUD starting NAV)...")
        t1 = time.perf_counter()
        out = run_oos_ensemble_walk_forward(
            px_aud,
            train_window_months=24,
            rebalance=REBALANCE_FREQ,
            benchmark_ticker="SPY",
            score_lookback_days=252,
            lambda_temp=3.0,
            starting_nav_aud=float(nav),
        )
        strat_rets = out["blended_returns"]
        if strat_rets.empty:
            print(f"[scale] {label}: empty returns, skipping")
            continue

        # Net return metrics
        days = len(strat_rets)
        years = max(days / ANNUAL_TRADING_DAYS, 1e-6)
        nav_curve = (1.0 + strat_rets).cumprod()
        total_ret = float(nav_curve.iloc[-1] - 1.0)
        ann_ret = float((1.0 + total_ret) ** (1.0 / years) - 1.0)
        vol_ann = float(strat_rets.std() * np.sqrt(ANNUAL_TRADING_DAYS))
        sharpe = ann_ret / vol_ann if vol_ann > 0 else 0.0
        downside = strat_rets[strat_rets < 0].std() * np.sqrt(ANNUAL_TRADING_DAYS)
        sortino = ann_ret / float(downside) if float(downside) > 0 else 0.0
        dd = (nav_curve / nav_curve.cummax() - 1.0).min()

        # Cost drags
        cost_ser = out.get("rebalance_costs", pd.Series(dtype=float))
        tax_ser = out.get("rebalance_taxes", pd.Series(dtype=float))
        # rebalance_costs is in fraction-of-NAV. Convert to bps/year.
        brokerage_drag_bps = float(cost_ser.sum() / years * 10_000) if not cost_ser.empty else 0.0
        cgt_drag_bps = float(tax_ser.sum() / years * 10_000) if not tax_ser.empty else 0.0

        # SPY benchmark over the same window
        spy_window = spy_ret.reindex(strat_rets.index).fillna(0.0)
        spy_nav = (1.0 + spy_window).cumprod()
        spy_ann = float(spy_nav.iloc[-1] ** (1.0 / years) - 1.0)
        alpha_vs_spy = ann_ret - spy_ann

        elapsed = time.perf_counter() - t1
        results.append({
            "label": label,
            "starting_nav_aud": int(nav),
            "years": round(years, 2),
            "ann_return": round(ann_ret, 6),
            "ann_volatility": round(vol_ann, 6),
            "sharpe": round(sharpe, 3),
            "sortino": round(sortino, 3),
            "max_drawdown": round(float(dd), 6),
            "brokerage_drag_bps_per_year": round(brokerage_drag_bps, 1),
            "cgt_drag_bps_per_year": round(cgt_drag_bps, 1),
            "total_drag_bps_per_year": round(brokerage_drag_bps + cgt_drag_bps, 1),
            "spy_ann_return": round(spy_ann, 6),
            "alpha_vs_spy": round(alpha_vs_spy, 6),
            "n_rebalances": int(out.get("n_executed", 0)),
            "elapsed_sec": round(elapsed, 1),
        })
        print(f"[scale] {label}: ann_ret {ann_ret*100:+.2f}%  "
              f"Sharpe {sharpe:.2f}  "
              f"MaxDD {float(dd)*100:+.2f}%  "
              f"brok {brokerage_drag_bps:.0f}bps/y  "
              f"CGT {cgt_drag_bps:.0f}bps/y  "
              f"α(SPY) {alpha_vs_spy*100:+.2f}%  "
              f"({elapsed:.1f}s)")

    if not results:
        print("[scale] no results produced.")
        return 1

    # === Comparison table ===
    print("\n" + "=" * 88)
    print("SCALE ANALYSIS RESULTS — net of brokerage + AU CGT (30% MTR)")
    print("=" * 88)
    print(f"  {'AUM':<8} {'Ann Ret':>9} {'Vol':>7} {'Sharpe':>7} "
          f"{'MaxDD':>8} {'Brok':>9} {'CGT':>9} {'Drag':>9} "
          f"{'α(SPY)':>9} {'Rebals':>7}")
    print(f"  {'-'*8} {'-'*9} {'-'*7} {'-'*7} {'-'*8} "
          f"{'-'*9} {'-'*9} {'-'*9} {'-'*9} {'-'*7}")
    for r in results:
        print(f"  {r['label']:<8} "
              f"{r['ann_return']*100:>+8.2f}% "
              f"{r['ann_volatility']*100:>6.2f}% "
              f"{r['sharpe']:>7.2f} "
              f"{r['max_drawdown']*100:>+7.2f}% "
              f"{r['brokerage_drag_bps_per_year']:>7.0f}bps "
              f"{r['cgt_drag_bps_per_year']:>7.0f}bps "
              f"{r['total_drag_bps_per_year']:>7.0f}bps "
              f"{r['alpha_vs_spy']*100:>+8.2f}% "
              f"{r['n_rebalances']:>7}")
    print()
    print(f"  SPY benchmark over same window: {results[0]['spy_ann_return']*100:+.2f}%/yr")
    print()

    # Verdict
    print("=" * 88)
    print("VERDICT")
    print("=" * 88)
    spy_ann = results[0]["spy_ann_return"]
    viable = [r for r in results if r["alpha_vs_spy"] > 0.005]  # >50bps over SPY
    if viable:
        print(f"  Strategy beats SPY (net) at: {', '.join(r['label'] for r in viable)}")
        floor = viable[0]["label"]
        print(f"  Minimum viable AUM (alpha > 50bps vs SPY): {floor}")
    else:
        print(f"  Strategy DOES NOT beat SPY net of costs at any tested scale.")

    # Save JSON
    try:
        summary = {
            "run_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            "spy_ann_return_window": results[0]["spy_ann_return"],
            "scales": results,
        }
        json_path = APP_DIR / "scale_analysis_summary.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        print(f"\n[scale] summary JSON → {json_path}")
    except Exception as e:
        print(f"[scale] summary JSON save failed: {e}")

    # Chart: ann return + drag vs scale
    try:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), sharex=True)
        xs = [r["starting_nav_aud"] for r in results]
        ax1.semilogx(xs, [r["ann_return"]*100 for r in results],
                     marker="o", linewidth=2, label="Strategy (net)")
        ax1.axhline(spy_ann*100, color="red", linestyle="--", alpha=0.6,
                    label=f"SPY ({spy_ann*100:.2f}%/yr)")
        ax1.set_ylabel("Annualised return (%)")
        ax1.set_title("Strategy net return vs starting AUM")
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        ax2.semilogx(xs, [r["brokerage_drag_bps_per_year"] for r in results],
                     marker="s", color="orange", label="Brokerage drag")
        ax2.semilogx(xs, [r["cgt_drag_bps_per_year"] for r in results],
                     marker="^", color="purple", label="CGT drag")
        ax2.semilogx(xs, [r["total_drag_bps_per_year"] for r in results],
                     marker="o", color="black", label="Total drag")
        ax2.set_ylabel("Annual drag (bps)")
        ax2.set_xlabel("Starting AUM (AUD, log scale)")
        ax2.set_title("Cost drag vs starting AUM")
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        fig.tight_layout()
        chart_path = APP_DIR / "scale_analysis_chart.png"
        fig.savefig(chart_path, dpi=120)
        plt.close(fig)
        print(f"[scale] chart → {chart_path}")
    except Exception as e:
        print(f"[scale] chart save failed: {e}")

    print("\n" + "=" * 88)
    print("SCALE ANALYSIS COMPLETE")
    print("=" * 88)
    return 0


def _run_dev_validation() -> int:
    print("\n" + "=" * 88)
    print("DEV / VALIDATION SPLIT — meta-parameter overfit gauge")
    print("=" * 88)

    WINDOWS = [
        # (label, oos_start, oos_end)
        ("DEV",        pd.Timestamp("2015-01-01"), pd.Timestamp("2020-02-19")),
        ("VALIDATION", pd.Timestamp("2020-02-20"), pd.Timestamp.now().normalize()),
    ]
    TRAIN_MONTHS = 24

    _dv_tickers = [c for c in prices.columns if c != "PortfolioValue"]
    print(f"[devval] universe: {len(_dv_tickers)} tickers")
    print(f"[devval] downloading full history (max) for split eval...")

    t0 = time.perf_counter()
    raw = yf.download(_dv_tickers, period="max", interval="1d",
                      auto_adjust=True, threads=False, progress=False)
    px = _normalize_yfinance_close(raw)
    px.index = pd.to_datetime(px.index).tz_localize(None)
    px = px.sort_index().ffill().bfill()
    fx_raw = yf.download("USDAUD=X", period="max", interval="1d",
                         auto_adjust=True, threads=False, progress=False)
    fx = fx_raw["Close"] if isinstance(fx_raw, pd.DataFrame) else fx_raw
    if isinstance(fx, pd.DataFrame):
        fx = fx.iloc[:, 0]
    fx = pd.to_numeric(fx, errors="coerce").reindex(px.index).ffill().bfill()
    usd_cols = [c for c in px.columns
                if not str(c).endswith(".AX") and not str(c).startswith("^")]
    px_aud = px.copy()
    if usd_cols:
        px_aud.update(px.loc[:, usd_cols].mul(fx, axis=0))
    px_aud = px_aud.ffill().bfill().dropna(how="all")
    print(f"[devval] data ready ({px_aud.shape[0]} days × {px_aud.shape[1]} tickers, "
          f"{px_aud.index.min().date()} → {px_aud.index.max().date()}) "
          f"in {time.perf_counter()-t0:.1f}s")

    spy_ret_full = (px_aud["SPY"].pct_change().dropna()
                    if "SPY" in px_aud.columns else pd.Series(dtype=float))

    results: list[dict] = []
    nav_curves: dict[str, pd.Series] = {}
    spy_curves: dict[str, pd.Series] = {}

    for label, oos_start, oos_end in WINDOWS:
        data_start = oos_start - pd.DateOffset(months=TRAIN_MONTHS)
        if px_aud.index.min() > data_start:
            print(f"[devval] {label}: WARNING — history starts "
                  f"{px_aud.index.min().date()}, need {data_start.date()} for "
                  f"{TRAIN_MONTHS}mo preroll. OOS window will start later.")
        slice_px = px_aud.loc[data_start:oos_end]
        if slice_px.empty:
            print(f"[devval] {label}: empty slice, skipping")
            continue
        print(f"\n[devval] {label} window: OOS {oos_start.date()} → {oos_end.date()} "
              f"(data slice {slice_px.index.min().date()} → {slice_px.index.max().date()})")

        t1 = time.perf_counter()
        out = run_oos_ensemble_walk_forward(
            slice_px,
            train_window_months=TRAIN_MONTHS,
            rebalance=REBALANCE_FREQ,
            benchmark_ticker="SPY",
            score_lookback_days=252,
            lambda_temp=3.0,
            starting_nav_aud=1_000_000.0,
        )
        strat_rets = out["blended_returns"]
        if strat_rets.empty:
            print(f"[devval] {label}: empty returns, skipping")
            continue

        days = len(strat_rets)
        years = max(days / ANNUAL_TRADING_DAYS, 1e-6)
        nav_curve = (1.0 + strat_rets).cumprod()
        total_ret = float(nav_curve.iloc[-1] - 1.0)
        ann_ret = float((1.0 + total_ret) ** (1.0 / years) - 1.0)
        vol_ann = float(strat_rets.std() * np.sqrt(ANNUAL_TRADING_DAYS))
        sharpe = ann_ret / vol_ann if vol_ann > 0 else 0.0
        downside = strat_rets[strat_rets < 0].std() * np.sqrt(ANNUAL_TRADING_DAYS)
        sortino = ann_ret / float(downside) if float(downside) > 0 else 0.0
        dd = (nav_curve / nav_curve.cummax() - 1.0).min()

        cost_ser = out.get("rebalance_costs", pd.Series(dtype=float))
        tax_ser = out.get("rebalance_taxes", pd.Series(dtype=float))
        brokerage_drag_bps = float(cost_ser.sum() / years * 10_000) if not cost_ser.empty else 0.0
        cgt_drag_bps = float(tax_ser.sum() / years * 10_000) if not tax_ser.empty else 0.0

        # TLH quantification: count events, total realised loss in AUD, and
        # convert to a tax-saved estimate (loss × effective MTR) → bps/yr drag
        # offset that we can subtract from CGT to gauge the net benefit.
        tlh_events = out.get("tlh_events", []) or []
        tlh_n_events = len(tlh_events)
        tlh_loss_aud = float(sum(e.get("loss_aud", 0.0) for e in tlh_events))
        # Approximate tax saved: assume ~half ST (full MTR), half LT (discounted).
        _eff_st = _effective_cgt_rate(short_term=True)
        _eff_lt = _effective_cgt_rate(short_term=False)
        tlh_tax_saved_est = tlh_loss_aud * (_eff_st + _eff_lt) / 2.0
        tlh_savings_bps = (tlh_tax_saved_est / 1_000_000.0
                           / years * 10_000)

        spy_window = spy_ret_full.reindex(strat_rets.index).fillna(0.0)
        spy_nav = (1.0 + spy_window).cumprod()
        spy_total = float(spy_nav.iloc[-1] - 1.0)
        spy_ann = float((1.0 + spy_total) ** (1.0 / years) - 1.0)
        spy_vol = float(spy_window.std() * np.sqrt(ANNUAL_TRADING_DAYS))
        spy_sharpe = spy_ann / spy_vol if spy_vol > 0 else 0.0
        spy_dd = (spy_nav / spy_nav.cummax() - 1.0).min()
        alpha_vs_spy = ann_ret - spy_ann

        elapsed = time.perf_counter() - t1
        nav_curves[label] = nav_curve
        spy_curves[label] = spy_nav

        results.append({
            "label": label,
            "oos_start": str(oos_start.date()),
            "oos_end": str(oos_end.date()),
            "years": round(years, 2),
            "ann_return": round(ann_ret, 6),
            "ann_volatility": round(vol_ann, 6),
            "sharpe": round(sharpe, 3),
            "sortino": round(sortino, 3),
            "max_drawdown": round(float(dd), 6),
            "brokerage_drag_bps_per_year": round(brokerage_drag_bps, 1),
            "cgt_drag_bps_per_year": round(cgt_drag_bps, 1),
            "tlh_n_events": int(tlh_n_events),
            "tlh_loss_realised_aud": round(tlh_loss_aud, 2),
            "tlh_tax_saved_est_aud": round(tlh_tax_saved_est, 2),
            "tlh_drag_offset_bps_per_year": round(tlh_savings_bps, 1),
            "spy_ann_return": round(spy_ann, 6),
            "spy_sharpe": round(spy_sharpe, 3),
            "spy_max_drawdown": round(float(spy_dd), 6),
            "alpha_vs_spy": round(alpha_vs_spy, 6),
            "n_rebalances": int(out.get("n_executed", 0)),
            "elapsed_sec": round(elapsed, 1),
        })
        print(f"[devval] {label}: ann_ret {ann_ret*100:+.2f}%  "
              f"Sharpe {sharpe:.2f}  Sortino {sortino:.2f}  "
              f"MaxDD {float(dd)*100:+.2f}%  "
              f"α(SPY) {alpha_vs_spy*100:+.2f}%  "
              f"({elapsed:.1f}s)")
        print(f"[devval] {label}: TLH {tlh_n_events} events  "
              f"${tlh_loss_aud:,.0f} loss realised  "
              f"~${tlh_tax_saved_est:,.0f} tax saved  "
              f"{tlh_savings_bps:+.0f} bps/yr drag offset")

    if len(results) < 2:
        print("[devval] need both windows to produce a meaningful split; aborting.")
        return 1

    dev = next(r for r in results if r["label"] == "DEV")
    val = next(r for r in results if r["label"] == "VALIDATION")

    # === Comparison table ===
    print("\n" + "=" * 88)
    print("DEV vs VALIDATION — engine + universe identical, only window changes")
    print("=" * 88)
    print(f"  {'Metric':<22} {'DEV':>14} {'VALIDATION':>14} {'Δ (val-dev)':>14}")
    print(f"  {'-'*22} {'-'*14} {'-'*14} {'-'*14}")

    def _row(name: str, key: str, fmt: str, scale: float = 1.0):
        d, v = dev[key] * scale, val[key] * scale
        delta = v - d
        d_str = format(d, fmt)
        v_str = format(v, fmt)
        delta_str = format(delta, fmt)
        print(f"  {name:<22} {d_str:>14} {v_str:>14} {delta_str:>14}")

    _row("OOS window (yrs)",   "years",                       "+.2f")
    _row("Ann return (%)",     "ann_return",                  "+.2f", 100.0)
    _row("Ann volatility (%)", "ann_volatility",              "+.2f", 100.0)
    _row("Sharpe",             "sharpe",                      "+.2f")
    _row("Sortino",            "sortino",                     "+.2f")
    _row("MaxDD (%)",          "max_drawdown",                "+.2f", 100.0)
    _row("Brokerage (bps/yr)", "brokerage_drag_bps_per_year", "+.0f")
    _row("CGT (bps/yr)",       "cgt_drag_bps_per_year",       "+.0f")
    _row("TLH events",         "tlh_n_events",                "+.0f")
    _row("TLH loss realised",  "tlh_loss_realised_aud",       "+,.0f")
    _row("TLH tax saved est",  "tlh_tax_saved_est_aud",       "+,.0f")
    _row("TLH offset (bps/yr)","tlh_drag_offset_bps_per_year","+.0f")
    _row("SPY ann return (%)", "spy_ann_return",              "+.2f", 100.0)
    _row("SPY Sharpe",         "spy_sharpe",                  "+.2f")
    _row("α vs SPY (%)",       "alpha_vs_spy",                "+.2f", 100.0)
    _row("Rebalances",         "n_rebalances",                "+.0f")

    # === Verdict ===
    print("\n" + "=" * 88)
    print("VERDICT")
    print("=" * 88)
    sharpe_gap = val["sharpe"] - dev["sharpe"]
    alpha_gap = val["alpha_vs_spy"] - dev["alpha_vs_spy"]
    print(f"  Sharpe degradation:   {sharpe_gap:+.2f}  (dev {dev['sharpe']:.2f} → val {val['sharpe']:.2f})")
    print(f"  α(SPY) degradation:   {alpha_gap*100:+.2f}%  "
          f"(dev {dev['alpha_vs_spy']*100:+.2f}% → val {val['alpha_vs_spy']*100:+.2f}%)")
    if sharpe_gap < -0.30:
        print(f"  Reading: LARGE degradation. Strong signal of meta-parameter overfit.")
    elif sharpe_gap < -0.15:
        print(f"  Reading: Moderate degradation. Some overfit risk — investigate which knobs.")
    elif sharpe_gap < 0.05:
        print(f"  Reading: Stable. Engine generalises well across the two windows.")
    else:
        print(f"  Reading: Validation BETTER than dev — likely regime-driven, not overfit.")
    print(f"  Going forward: tune on DEV only; open VALIDATION at most once per change.")

    # Save JSON
    try:
        summary = {
            "run_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            "train_window_months": TRAIN_MONTHS,
            "windows": results,
            "sharpe_gap": round(sharpe_gap, 3),
            "alpha_gap": round(alpha_gap, 6),
        }
        json_path = APP_DIR / "dev_validation_summary.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        print(f"\n[devval] summary JSON → {json_path}")
    except Exception as e:
        print(f"[devval] summary JSON save failed: {e}")

    # Chart: NAV curves on dev and validation, with SPY overlay on each
    try:
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        for ax, label in zip(axes, ["DEV", "VALIDATION"]):
            if label not in nav_curves:
                continue
            n = nav_curves[label]
            s = spy_curves[label]
            ax.plot(n.index, (n / n.iloc[0]) * 100.0,
                    color="C0", linewidth=2, label="Strategy")
            ax.plot(s.index, (s / s.iloc[0]) * 100.0,
                    color="red", linestyle="--", alpha=0.7, label="SPY (AUD)")
            r = next(x for x in results if x["label"] == label)
            ax.set_title(f"{label}: Sharpe {r['sharpe']:.2f}, α {r['alpha_vs_spy']*100:+.2f}%")
            ax.set_ylabel("NAV (rebased = 100)")
            ax.legend()
            ax.grid(True, alpha=0.3)
        fig.suptitle("Dev vs Validation OOS — same engine, disjoint windows", fontsize=12)
        fig.tight_layout()
        chart_path = APP_DIR / "dev_validation_chart.png"
        fig.savefig(chart_path, dpi=120)
        plt.close(fig)
        print(f"[devval] chart → {chart_path}")
    except Exception as e:
        print(f"[devval] chart save failed: {e}")

    print("\n" + "=" * 88)
    print("DEV / VALIDATION SPLIT COMPLETE")
    print("=" * 88)
    return 0


def _run_rebal_skip_sweep() -> int:
    print("\n" + "=" * 88)
    print("SKIP_REBAL_DELTA SWEEP — dev-window tuning")
    print("=" * 88)

    SWEEP_VALUES = [0.03, 0.04, 0.05, 0.06, 0.07]  # 3% baseline → 7%
    TRAIN_MONTHS = 24
    DEV_OOS_START = pd.Timestamp("2015-01-01")
    DEV_OOS_END   = pd.Timestamp("2020-02-19")
    VAL_OOS_START = pd.Timestamp("2020-02-20")
    VAL_OOS_END   = pd.Timestamp.now().normalize()

    _sw_tickers = [c for c in prices.columns if c != "PortfolioValue"]
    print(f"[skip-sweep] universe: {len(_sw_tickers)} tickers")
    print(f"[skip-sweep] downloading full history (max)...")

    t0 = time.perf_counter()
    raw = yf.download(_sw_tickers, period="max", interval="1d",
                      auto_adjust=True, threads=False, progress=False)
    px = _normalize_yfinance_close(raw)
    px.index = pd.to_datetime(px.index).tz_localize(None)
    px = px.sort_index().ffill().bfill()
    fx_raw = yf.download("USDAUD=X", period="max", interval="1d",
                         auto_adjust=True, threads=False, progress=False)
    fx = fx_raw["Close"] if isinstance(fx_raw, pd.DataFrame) else fx_raw
    if isinstance(fx, pd.DataFrame):
        fx = fx.iloc[:, 0]
    fx = pd.to_numeric(fx, errors="coerce").reindex(px.index).ffill().bfill()
    usd_cols = [c for c in px.columns
                if not str(c).endswith(".AX") and not str(c).startswith("^")]
    px_aud = px.copy()
    if usd_cols:
        px_aud.update(px.loc[:, usd_cols].mul(fx, axis=0))
    px_aud = px_aud.ffill().bfill().dropna(how="all")
    print(f"[skip-sweep] data ready ({px_aud.shape[0]} days × {px_aud.shape[1]} tickers) "
          f"in {time.perf_counter()-t0:.1f}s")

    spy_ret_full = (px_aud["SPY"].pct_change().dropna()
                    if "SPY" in px_aud.columns else pd.Series(dtype=float))

    def _eval_window(label: str, oos_start, oos_end, skip_delta: float) -> dict:
        data_start = pd.Timestamp(oos_start) - pd.DateOffset(months=TRAIN_MONTHS)
        slice_px = px_aud.loc[data_start:oos_end]
        t1 = time.perf_counter()
        out = run_oos_ensemble_walk_forward(
            slice_px,
            train_window_months=TRAIN_MONTHS,
            rebalance=REBALANCE_FREQ,
            benchmark_ticker="SPY",
            score_lookback_days=252,
            lambda_temp=3.0,
            starting_nav_aud=1_000_000.0,
            skip_rebal_delta=skip_delta,
        )
        strat_rets = out["blended_returns"]
        if strat_rets.empty:
            return {}
        years = max(len(strat_rets) / ANNUAL_TRADING_DAYS, 1e-6)
        nav_curve = (1.0 + strat_rets).cumprod()
        ann_ret = float(nav_curve.iloc[-1] ** (1.0 / years) - 1.0)
        vol = float(strat_rets.std() * np.sqrt(ANNUAL_TRADING_DAYS))
        sharpe = ann_ret / vol if vol > 0 else 0.0
        downside = strat_rets[strat_rets < 0].std() * np.sqrt(ANNUAL_TRADING_DAYS)
        sortino = ann_ret / float(downside) if float(downside) > 0 else 0.0
        dd = float((nav_curve / nav_curve.cummax() - 1.0).min())

        cost_ser = out.get("rebalance_costs", pd.Series(dtype=float))
        tax_ser = out.get("rebalance_taxes", pd.Series(dtype=float))
        brk_bps = float(cost_ser.sum() / years * 10_000) if not cost_ser.empty else 0.0
        cgt_bps = float(tax_ser.sum() / years * 10_000) if not tax_ser.empty else 0.0

        spy_window = spy_ret_full.reindex(strat_rets.index).fillna(0.0)
        spy_nav = (1.0 + spy_window).cumprod()
        spy_ann = float(spy_nav.iloc[-1] ** (1.0 / years) - 1.0)
        alpha = ann_ret - spy_ann

        n_executed = int(out.get("n_executed", 0))
        n_skipped = int(out.get("n_skipped", 0))
        tlh_n = len(out.get("tlh_events", []) or [])
        elapsed = time.perf_counter() - t1
        return {
            "label": label,
            "skip_delta": skip_delta,
            "years": years,
            "ann_return": ann_ret,
            "sharpe": sharpe,
            "sortino": sortino,
            "max_drawdown": dd,
            "brokerage_bps_per_year": brk_bps,
            "cgt_bps_per_year": cgt_bps,
            "spy_ann_return": spy_ann,
            "alpha_vs_spy": alpha,
            "n_executed": n_executed,
            "n_skipped": n_skipped,
            "tlh_events": tlh_n,
            "elapsed_sec": elapsed,
        }

    # === DEV sweep ===
    print("\n[skip-sweep] running DEV sweep (5 values × walk-forward)...")
    dev_results: list[dict] = []
    for sd in SWEEP_VALUES:
        r = _eval_window("DEV", DEV_OOS_START, DEV_OOS_END, sd)
        if r:
            dev_results.append(r)
            print(f"[skip-sweep] DEV δ={sd*100:.0f}%: "
                  f"ann_ret {r['ann_return']*100:+.2f}%  "
                  f"Sharpe {r['sharpe']:.2f}  "
                  f"MaxDD {r['max_drawdown']*100:+.2f}%  "
                  f"CGT {r['cgt_bps_per_year']:.0f}bps/y  "
                  f"exec/skip {r['n_executed']}/{r['n_skipped']}  "
                  f"({r['elapsed_sec']:.1f}s)")

    if not dev_results:
        print("[skip-sweep] no dev results; aborting.")
        return 1

    # === DEV results table ===
    print("\n" + "=" * 88)
    print("DEV SWEEP RESULTS (only used for picking the winner)")
    print("=" * 88)
    print(f"  {'δ':>5} {'Ann Ret':>9} {'Sharpe':>7} {'Sortino':>8} "
          f"{'MaxDD':>8} {'Brok':>7} {'CGT':>8} {'α(SPY)':>8} "
          f"{'Exec':>5} {'Skip':>5} {'TLH':>4}")
    print(f"  {'-'*5} {'-'*9} {'-'*7} {'-'*8} {'-'*8} "
          f"{'-'*7} {'-'*8} {'-'*8} {'-'*5} {'-'*5} {'-'*4}")
    for r in dev_results:
        print(f"  {r['skip_delta']*100:>4.0f}% "
              f"{r['ann_return']*100:>+8.2f}% "
              f"{r['sharpe']:>7.2f} "
              f"{r['sortino']:>8.2f} "
              f"{r['max_drawdown']*100:>+7.2f}% "
              f"{r['brokerage_bps_per_year']:>5.0f}bps "
              f"{r['cgt_bps_per_year']:>6.0f}bps "
              f"{r['alpha_vs_spy']*100:>+7.2f}% "
              f"{r['n_executed']:>5} {r['n_skipped']:>5} {r['tlh_events']:>4}")

    # === Pick winner: max Sharpe, ann_ret as tiebreaker ===
    winner = max(dev_results, key=lambda r: (round(r['sharpe'], 3), r['ann_return']))
    print(f"\n[skip-sweep] DEV WINNER → δ={winner['skip_delta']*100:.0f}% "
          f"(Sharpe {winner['sharpe']:.2f}, ann_ret {winner['ann_return']*100:+.2f}%)")
    baseline = next((r for r in dev_results if r['skip_delta'] == 0.03), None)
    if baseline and winner['skip_delta'] != 0.03:
        d_sharpe = winner['sharpe'] - baseline['sharpe']
        d_ann = winner['ann_return'] - baseline['ann_return']
        print(f"[skip-sweep] vs DEV baseline (δ=3%): ΔSharpe {d_sharpe:+.3f}, "
              f"Δann_ret {d_ann*100:+.2f}%")

    # === Validation lock-box: ONE shot on winner ===
    print("\n" + "=" * 88)
    print(f"VALIDATION LOCK-BOX — opening once on winning δ={winner['skip_delta']*100:.0f}%")
    print("=" * 88)
    val_winner = _eval_window("VALIDATION", VAL_OOS_START, VAL_OOS_END,
                              winner['skip_delta'])
    if not val_winner:
        print("[skip-sweep] validation evaluation failed; aborting.")
        return 1
    print(f"[skip-sweep] VAL δ={winner['skip_delta']*100:.0f}%: "
          f"ann_ret {val_winner['ann_return']*100:+.2f}%  "
          f"Sharpe {val_winner['sharpe']:.2f}  "
          f"MaxDD {val_winner['max_drawdown']*100:+.2f}%  "
          f"CGT {val_winner['cgt_bps_per_year']:.0f}bps/y  "
          f"α(SPY) {val_winner['alpha_vs_spy']*100:+.2f}%  "
          f"({val_winner['elapsed_sec']:.1f}s)")

    # Also evaluate validation at baseline 3% so we can quote the generalised gain.
    val_baseline = _eval_window("VAL_BASELINE", VAL_OOS_START, VAL_OOS_END, 0.03)
    if val_baseline:
        print(f"[skip-sweep] VAL δ=3% (baseline): "
              f"ann_ret {val_baseline['ann_return']*100:+.2f}%  "
              f"Sharpe {val_baseline['sharpe']:.2f}  "
              f"CGT {val_baseline['cgt_bps_per_year']:.0f}bps/y  "
              f"α(SPY) {val_baseline['alpha_vs_spy']*100:+.2f}%")
        val_d_sharpe = val_winner['sharpe'] - val_baseline['sharpe']
        val_d_ann = val_winner['ann_return'] - val_baseline['ann_return']
        print(f"[skip-sweep] VAL uplift (winner vs δ=3%): "
              f"ΔSharpe {val_d_sharpe:+.3f}, Δann_ret {val_d_ann*100:+.2f}%")

    # === Verdict (honest 4-dim check on validation: winner vs baseline) ===
    print()
    if val_baseline:
        _verdict = _evaluate_sweep_result(
            baseline={
                "sharpe": val_baseline['sharpe'],
                "max_drawdown": val_baseline['max_drawdown'],
                "alpha_vs_spy": val_baseline['alpha_vs_spy'],
                "ann_return": val_baseline['ann_return'],
            },
            treatment={
                "sharpe": val_winner['sharpe'],
                "max_drawdown": val_winner['max_drawdown'],
                "alpha_vs_spy": val_winner['alpha_vs_spy'],
                "ann_return": val_winner['ann_return'],
            },
            label_baseline="δ=3% on VAL",
            label_treatment=f"δ={winner['skip_delta']*100:.0f}% on VAL (winner)",
        )
        _print_sweep_verdict(_verdict)
        if _verdict["verdict"] == "SHIP":
            print(f"\n  Next: set SKIP_REBAL_DELTA = {winner['skip_delta']}")
        else:
            print(f"\n  Next: stay at SKIP_REBAL_DELTA = 0.03 — winning δ does NOT generalise.")
    else:
        print(f"  (validation baseline failed — can't quote generalisation)")

    # Save JSON
    try:
        summary = {
            "run_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            "sweep_values": SWEEP_VALUES,
            "dev_results": dev_results,
            "dev_winner_skip_delta": winner['skip_delta'],
            "val_winner": val_winner,
            "val_baseline": val_baseline,
        }
        json_path = APP_DIR / "rebal_skip_sweep_summary.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"\n[skip-sweep] summary JSON → {json_path}")
    except Exception as e:
        print(f"[skip-sweep] summary JSON save failed: {e}")

    print("\n" + "=" * 88)
    print("REBAL-SKIP SWEEP COMPLETE")
    print("=" * 88)
    return 0


def _run_turnover_penalty_sweep() -> int:
    print("\n" + "=" * 88)
    print("TURNOVER-PENALTY SWEEP — cost-aware solver tuning on DEV")
    print("=" * 88)

    SWEEP_VALUES = [0.0, 1e-4, 5e-4, 1e-3, 5e-3]  # 0 = off (baseline)
    TRAIN_MONTHS = 24
    DEV_OOS_START = pd.Timestamp("2015-01-01")
    DEV_OOS_END   = pd.Timestamp("2020-02-19")
    VAL_OOS_START = pd.Timestamp("2020-02-20")
    VAL_OOS_END   = pd.Timestamp.now().normalize()

    _sw_tickers = [c for c in prices.columns if c != "PortfolioValue"]
    print(f"[turnover-sweep] universe: {len(_sw_tickers)} tickers")
    print(f"[turnover-sweep] downloading full history (max)...")

    t0 = time.perf_counter()
    raw = yf.download(_sw_tickers, period="max", interval="1d",
                      auto_adjust=True, threads=False, progress=False)
    px = _normalize_yfinance_close(raw)
    px.index = pd.to_datetime(px.index).tz_localize(None)
    px = px.sort_index().ffill().bfill()
    fx_raw = yf.download("USDAUD=X", period="max", interval="1d",
                         auto_adjust=True, threads=False, progress=False)
    fx = fx_raw["Close"] if isinstance(fx_raw, pd.DataFrame) else fx_raw
    if isinstance(fx, pd.DataFrame):
        fx = fx.iloc[:, 0]
    fx = pd.to_numeric(fx, errors="coerce").reindex(px.index).ffill().bfill()
    usd_cols = [c for c in px.columns
                if not str(c).endswith(".AX") and not str(c).startswith("^")]
    px_aud = px.copy()
    if usd_cols:
        px_aud.update(px.loc[:, usd_cols].mul(fx, axis=0))
    px_aud = px_aud.ffill().bfill().dropna(how="all")
    print(f"[turnover-sweep] data ready ({px_aud.shape[0]} days × {px_aud.shape[1]} tickers) "
          f"in {time.perf_counter()-t0:.1f}s")

    spy_ret_full = (px_aud["SPY"].pct_change().dropna()
                    if "SPY" in px_aud.columns else pd.Series(dtype=float))

    def _eval_window(label: str, oos_start, oos_end, penalty: float) -> dict:
        data_start = pd.Timestamp(oos_start) - pd.DateOffset(months=TRAIN_MONTHS)
        slice_px = px_aud.loc[data_start:oos_end]
        t1 = time.perf_counter()
        out = run_oos_ensemble_walk_forward(
            slice_px,
            train_window_months=TRAIN_MONTHS,
            rebalance=REBALANCE_FREQ,
            benchmark_ticker="SPY",
            score_lookback_days=252,
            lambda_temp=3.0,
            starting_nav_aud=1_000_000.0,
            turnover_penalty=float(penalty),
        )
        strat_rets = out["blended_returns"]
        if strat_rets.empty:
            return {}
        years = max(len(strat_rets) / ANNUAL_TRADING_DAYS, 1e-6)
        nav_curve = (1.0 + strat_rets).cumprod()
        ann_ret = float(nav_curve.iloc[-1] ** (1.0 / years) - 1.0)
        vol = float(strat_rets.std() * np.sqrt(ANNUAL_TRADING_DAYS))
        sharpe = ann_ret / vol if vol > 0 else 0.0
        downside = strat_rets[strat_rets < 0].std() * np.sqrt(ANNUAL_TRADING_DAYS)
        sortino = ann_ret / float(downside) if float(downside) > 0 else 0.0
        dd = float((nav_curve / nav_curve.cummax() - 1.0).min())

        cost_ser = out.get("rebalance_costs", pd.Series(dtype=float))
        tax_ser = out.get("rebalance_taxes", pd.Series(dtype=float))
        brk_bps = float(cost_ser.sum() / years * 10_000) if not cost_ser.empty else 0.0
        cgt_bps = float(tax_ser.sum() / years * 10_000) if not tax_ser.empty else 0.0

        spy_window = spy_ret_full.reindex(strat_rets.index).fillna(0.0)
        spy_nav = (1.0 + spy_window).cumprod()
        spy_ann = float(spy_nav.iloc[-1] ** (1.0 / years) - 1.0)
        alpha = ann_ret - spy_ann

        n_executed = int(out.get("n_executed", 0))
        n_skipped = int(out.get("n_skipped", 0))
        tlh_n = len(out.get("tlh_events", []) or [])
        elapsed = time.perf_counter() - t1
        return {
            "label": label,
            "turnover_penalty": penalty,
            "years": years,
            "ann_return": ann_ret,
            "sharpe": sharpe,
            "sortino": sortino,
            "max_drawdown": dd,
            "brokerage_bps_per_year": brk_bps,
            "cgt_bps_per_year": cgt_bps,
            "spy_ann_return": spy_ann,
            "alpha_vs_spy": alpha,
            "n_executed": n_executed,
            "n_skipped": n_skipped,
            "tlh_events": tlh_n,
            "elapsed_sec": elapsed,
        }

    # === DEV sweep ===
    print("\n[turnover-sweep] running DEV sweep (5 values × walk-forward)...")
    dev_results: list[dict] = []
    for tp in SWEEP_VALUES:
        r = _eval_window("DEV", DEV_OOS_START, DEV_OOS_END, tp)
        if r:
            dev_results.append(r)
            print(f"[turnover-sweep] DEV γ={tp:g}: "
                  f"ann_ret {r['ann_return']*100:+.2f}%  "
                  f"Sharpe {r['sharpe']:.2f}  "
                  f"MaxDD {r['max_drawdown']*100:+.2f}%  "
                  f"CGT {r['cgt_bps_per_year']:.0f}bps/y  "
                  f"exec/skip {r['n_executed']}/{r['n_skipped']}  "
                  f"({r['elapsed_sec']:.1f}s)")

    if not dev_results:
        print("[turnover-sweep] no dev results; aborting.")
        return 1

    # === DEV results table ===
    print("\n" + "=" * 88)
    print("DEV SWEEP RESULTS (only used for picking the winner)")
    print("=" * 88)
    print(f"  {'γ':>9} {'Ann Ret':>9} {'Sharpe':>7} {'Sortino':>8} "
          f"{'MaxDD':>8} {'Brok':>7} {'CGT':>8} {'α(SPY)':>8} "
          f"{'Exec':>5} {'Skip':>5} {'TLH':>4}")
    print(f"  {'-'*9} {'-'*9} {'-'*7} {'-'*8} {'-'*8} "
          f"{'-'*7} {'-'*8} {'-'*8} {'-'*5} {'-'*5} {'-'*4}")
    for r in dev_results:
        print(f"  {r['turnover_penalty']:>9g} "
              f"{r['ann_return']*100:>+8.2f}% "
              f"{r['sharpe']:>7.2f} "
              f"{r['sortino']:>8.2f} "
              f"{r['max_drawdown']*100:>+7.2f}% "
              f"{r['brokerage_bps_per_year']:>5.0f}bps "
              f"{r['cgt_bps_per_year']:>6.0f}bps "
              f"{r['alpha_vs_spy']*100:>+7.2f}% "
              f"{r['n_executed']:>5} {r['n_skipped']:>5} {r['tlh_events']:>4}")

    # === Pick winner: max Sharpe, ann_ret as tiebreaker ===
    winner = max(dev_results, key=lambda r: (round(r['sharpe'], 3), r['ann_return']))
    baseline = next((r for r in dev_results if r['turnover_penalty'] == 0.0), None)
    print(f"\n[turnover-sweep] DEV WINNER → γ={winner['turnover_penalty']:g} "
          f"(Sharpe {winner['sharpe']:.2f}, ann_ret {winner['ann_return']*100:+.2f}%)")
    if baseline and winner['turnover_penalty'] != 0.0:
        d_sharpe = winner['sharpe'] - baseline['sharpe']
        d_ann = winner['ann_return'] - baseline['ann_return']
        d_cgt = winner['cgt_bps_per_year'] - baseline['cgt_bps_per_year']
        print(f"[turnover-sweep] vs DEV baseline (γ=0): ΔSharpe {d_sharpe:+.3f}, "
              f"Δann_ret {d_ann*100:+.2f}%, ΔCGT {d_cgt:+.0f} bps/yr")

    # === Validation lock-box: ONE shot on winner ===
    print("\n" + "=" * 88)
    print(f"VALIDATION LOCK-BOX — opening once on winning γ={winner['turnover_penalty']:g}")
    print("=" * 88)
    val_winner = _eval_window("VALIDATION", VAL_OOS_START, VAL_OOS_END,
                              winner['turnover_penalty'])
    if not val_winner:
        print("[turnover-sweep] validation evaluation failed; aborting.")
        return 1
    print(f"[turnover-sweep] VAL γ={winner['turnover_penalty']:g}: "
          f"ann_ret {val_winner['ann_return']*100:+.2f}%  "
          f"Sharpe {val_winner['sharpe']:.2f}  "
          f"MaxDD {val_winner['max_drawdown']*100:+.2f}%  "
          f"CGT {val_winner['cgt_bps_per_year']:.0f}bps/y  "
          f"α(SPY) {val_winner['alpha_vs_spy']*100:+.2f}%  "
          f"({val_winner['elapsed_sec']:.1f}s)")

    val_baseline = _eval_window("VAL_BASELINE", VAL_OOS_START, VAL_OOS_END, 0.0)
    if val_baseline:
        print(f"[turnover-sweep] VAL γ=0 (baseline): "
              f"ann_ret {val_baseline['ann_return']*100:+.2f}%  "
              f"Sharpe {val_baseline['sharpe']:.2f}  "
              f"CGT {val_baseline['cgt_bps_per_year']:.0f}bps/y  "
              f"α(SPY) {val_baseline['alpha_vs_spy']*100:+.2f}%")
        val_d_sharpe = val_winner['sharpe'] - val_baseline['sharpe']
        val_d_ann = val_winner['ann_return'] - val_baseline['ann_return']
        val_d_cgt = val_winner['cgt_bps_per_year'] - val_baseline['cgt_bps_per_year']
        print(f"[turnover-sweep] VAL uplift (winner vs γ=0): "
              f"ΔSharpe {val_d_sharpe:+.3f}, Δann_ret {val_d_ann*100:+.2f}%, "
              f"ΔCGT {val_d_cgt:+.0f} bps/yr")

    # === Verdict (honest 4-dim check on validation: winner vs γ=0) ===
    print()
    if val_baseline:
        _verdict = _evaluate_sweep_result(
            baseline={
                "sharpe": val_baseline['sharpe'],
                "max_drawdown": val_baseline['max_drawdown'],
                "alpha_vs_spy": val_baseline['alpha_vs_spy'],
                "ann_return": val_baseline['ann_return'],
            },
            treatment={
                "sharpe": val_winner['sharpe'],
                "max_drawdown": val_winner['max_drawdown'],
                "alpha_vs_spy": val_winner['alpha_vs_spy'],
                "ann_return": val_winner['ann_return'],
            },
            label_baseline="γ=0 on VAL",
            label_treatment=f"γ={winner['turnover_penalty']:g} on VAL (winner)",
        )
        _print_sweep_verdict(_verdict)
        if _verdict["verdict"] == "SHIP":
            print(f"\n  Next: set engine default turnover_penalty = {winner['turnover_penalty']:g}")
        else:
            print(f"\n  Next: keep turnover_penalty = 0 — winning γ does NOT generalise.")
    else:
        print(f"  (validation baseline failed — can't quote generalisation)")

    # Save JSON
    try:
        summary = {
            "run_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            "sweep_values": SWEEP_VALUES,
            "dev_results": dev_results,
            "dev_winner_turnover_penalty": winner['turnover_penalty'],
            "val_winner": val_winner,
            "val_baseline": val_baseline,
        }
        json_path = APP_DIR / "turnover_sweep_summary.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"\n[turnover-sweep] summary JSON → {json_path}")
    except Exception as e:
        print(f"[turnover-sweep] summary JSON save failed: {e}")

    print("\n" + "=" * 88)
    print("TURNOVER-PENALTY SWEEP COMPLETE")
    print("=" * 88)
    return 0


def _run_walk_forward_cv() -> int:
    print("\n" + "=" * 88)
    print("WALK-FORWARD CV — multi-fold OOS evaluation of current engine config")
    print("=" * 88)

    TRAIN_MONTHS = 24
    # Modern universe filter: most .AX ETFs in the engine's universe don't
    # exist before 2013 (e.g. NDQ.AX listed 2015, MTUM.AX 2014, A200.AX 2018).
    # Including 1986-2012 folds in the aggregate distorts the mean Sharpe
    # because the engine ran on a degraded 1-5 ticker subset. Filter to
    # post-2014 so the 24-month training window starts on full universe and
    # the first OOS year is 2016 (covering 10 modern folds for ~10y history).
    MIN_OOS_YEAR = 2016
    _wf_tickers = [c for c in prices.columns if c != "PortfolioValue"]
    print(f"[wf-cv] universe: {len(_wf_tickers)} tickers")
    print(f"[wf-cv] modern-universe filter: OOS folds restricted to year ≥ {MIN_OOS_YEAR}")
    print(f"[wf-cv] downloading full history (max)...")

    t0 = time.perf_counter()
    raw = yf.download(_wf_tickers, period="max", interval="1d",
                      auto_adjust=True, threads=False, progress=False)
    px = _normalize_yfinance_close(raw)
    px.index = pd.to_datetime(px.index).tz_localize(None)
    px = px.sort_index().ffill().bfill()
    fx_raw = yf.download("USDAUD=X", period="max", interval="1d",
                         auto_adjust=True, threads=False, progress=False)
    fx = fx_raw["Close"] if isinstance(fx_raw, pd.DataFrame) else fx_raw
    if isinstance(fx, pd.DataFrame):
        fx = fx.iloc[:, 0]
    fx = pd.to_numeric(fx, errors="coerce").reindex(px.index).ffill().bfill()
    usd_cols = [c for c in px.columns
                if not str(c).endswith(".AX") and not str(c).startswith("^")]
    px_aud = px.copy()
    if usd_cols:
        px_aud.update(px.loc[:, usd_cols].mul(fx, axis=0))
    px_aud = px_aud.ffill().bfill().dropna(how="all")
    print(f"[wf-cv] data ready ({px_aud.shape[0]} days × {px_aud.shape[1]} tickers, "
          f"{px_aud.index.min().date()} → {px_aud.index.max().date()}) "
          f"in {time.perf_counter()-t0:.1f}s")

    # Single engine run over the full window — folds are just slices of the
    # OOS output, not re-runs of the engine. This is cheaper AND preserves
    # the engine's state-dependent lot-book / TLH / cooldown behaviour,
    # which is the whole point of walk-forward.
    print(f"\n[wf-cv] running engine on full history (single walk-forward)...")
    t1 = time.perf_counter()
    out = run_oos_ensemble_walk_forward(
        px_aud,
        train_window_months=TRAIN_MONTHS,
        rebalance=REBALANCE_FREQ,
        benchmark_ticker="SPY",
        score_lookback_days=252,
        lambda_temp=3.0,
        starting_nav_aud=1_000_000.0,
    )
    strat_rets = out["blended_returns"]
    if strat_rets.empty:
        print("[wf-cv] engine returned empty series; aborting.")
        return 1
    print(f"[wf-cv] engine done ({len(strat_rets)} OOS days, "
          f"{strat_rets.index.min().date()} → {strat_rets.index.max().date()}) "
          f"in {time.perf_counter()-t1:.1f}s")

    spy_ret_full = (px_aud["SPY"].pct_change().dropna()
                    if "SPY" in px_aud.columns else pd.Series(dtype=float))

    # Build folds: one per calendar year with ≥150 OOS days to keep
    # annualisation honest. Partial years at start/end are dropped.
    # Pre-MIN_OOS_YEAR folds are skipped (degraded universe — see above).
    MIN_FOLD_DAYS = 150
    years_present = sorted(set(strat_rets.index.year))
    folds: list[dict] = []
    skipped_pre_modern = 0
    for yr in years_present:
        if yr < MIN_OOS_YEAR:
            skipped_pre_modern += 1
            continue
        mask = (strat_rets.index.year == yr)
        chunk = strat_rets.loc[mask]
        if len(chunk) < MIN_FOLD_DAYS:
            continue
        chunk_nav = (1.0 + chunk).cumprod()
        n_days = len(chunk)
        years_in_chunk = n_days / ANNUAL_TRADING_DAYS
        ann_ret = float(chunk_nav.iloc[-1] ** (1.0 / years_in_chunk) - 1.0)
        vol = float(chunk.std() * np.sqrt(ANNUAL_TRADING_DAYS))
        sharpe = ann_ret / vol if vol > 0 else 0.0
        downside = chunk[chunk < 0].std() * np.sqrt(ANNUAL_TRADING_DAYS)
        sortino = ann_ret / float(downside) if float(downside) > 0 else 0.0
        dd = float((chunk_nav / chunk_nav.cummax() - 1.0).min())

        spy_chunk = spy_ret_full.reindex(chunk.index).fillna(0.0)
        spy_nav = (1.0 + spy_chunk).cumprod()
        spy_ann = float(spy_nav.iloc[-1] ** (1.0 / years_in_chunk) - 1.0)
        alpha = ann_ret - spy_ann

        folds.append({
            "year": int(yr),
            "n_days": int(n_days),
            "ann_return": ann_ret,
            "sharpe": sharpe,
            "sortino": sortino,
            "max_drawdown": dd,
            "spy_ann_return": spy_ann,
            "alpha_vs_spy": alpha,
        })

    if len(folds) < 3:
        print(f"[wf-cv] only {len(folds)} folds with ≥{MIN_FOLD_DAYS} days; "
              f"need at least 3 for meaningful aggregation. Aborting.")
        return 1

    if skipped_pre_modern:
        print(f"\n[wf-cv] skipped {skipped_pre_modern} pre-{MIN_OOS_YEAR} folds "
              f"(degraded universe — most .AX ETFs did not exist yet)")

    # === Fold-by-fold table ===
    print("\n" + "=" * 88)
    print(f"PER-FOLD OOS METRICS ({len(folds)} non-overlapping years, "
          f"modern universe only)")
    print("=" * 88)
    print(f"  {'Year':>5} {'Days':>5} {'Ann Ret':>9} {'Sharpe':>7} "
          f"{'Sortino':>8} {'MaxDD':>8} {'SPY Ret':>9} {'α(SPY)':>8}")
    print(f"  {'-'*5} {'-'*5} {'-'*9} {'-'*7} {'-'*8} {'-'*8} {'-'*9} {'-'*8}")
    for r in folds:
        print(f"  {r['year']:>5} {r['n_days']:>5} "
              f"{r['ann_return']*100:>+8.2f}% "
              f"{r['sharpe']:>7.2f} "
              f"{r['sortino']:>8.2f} "
              f"{r['max_drawdown']*100:>+7.2f}% "
              f"{r['spy_ann_return']*100:>+8.2f}% "
              f"{r['alpha_vs_spy']*100:>+7.2f}%")

    # === Aggregate stats ===
    sharpe_arr = np.array([r["sharpe"] for r in folds], dtype=float)
    alpha_arr = np.array([r["alpha_vs_spy"] for r in folds], dtype=float)
    ret_arr = np.array([r["ann_return"] for r in folds], dtype=float)
    dd_arr = np.array([r["max_drawdown"] for r in folds], dtype=float)

    mean_sharpe = float(sharpe_arr.mean())
    std_sharpe = float(sharpe_arr.std(ddof=1)) if len(sharpe_arr) > 1 else 0.0
    se_sharpe = std_sharpe / np.sqrt(len(sharpe_arr))
    t_stat_sharpe = mean_sharpe / se_sharpe if se_sharpe > 0 else 0.0

    mean_alpha = float(alpha_arr.mean())
    std_alpha = float(alpha_arr.std(ddof=1)) if len(alpha_arr) > 1 else 0.0
    se_alpha = std_alpha / np.sqrt(len(alpha_arr))
    t_stat_alpha = mean_alpha / se_alpha if se_alpha > 0 else 0.0

    n_positive_alpha = int((alpha_arr > 0).sum())
    n_beat_spy_sharpe = int((np.array([r["sharpe"] - (r["spy_ann_return"] /
                              (np.std([r["ann_return"]]) + 1e-9))
                              for r in folds]) > 0).sum())

    # === Aggregate report ===
    print("\n" + "=" * 88)
    print("AGGREGATE (mean ± std across folds)")
    print("=" * 88)
    print(f"  Sharpe:        {mean_sharpe:+.2f} ± {std_sharpe:.2f}   "
          f"(SE {se_sharpe:.3f}, t-stat {t_stat_sharpe:+.2f})")
    print(f"  Ann return:    {ret_arr.mean()*100:+.2f}% ± {ret_arr.std(ddof=1)*100:.2f}%")
    print(f"  α vs SPY:      {mean_alpha*100:+.2f}% ± {std_alpha*100:.2f}%   "
          f"(SE {se_alpha*100:.2f}%, t-stat {t_stat_alpha:+.2f})")
    print(f"  MaxDD:         {dd_arr.mean()*100:+.2f}% ± {dd_arr.std(ddof=1)*100:.2f}%   "
          f"(worst single year: {dd_arr.min()*100:+.2f}%)")
    print(f"  Years with α > 0: {n_positive_alpha}/{len(folds)}")

    # === Full-period metrics (NOT fold-mean) ===
    # Fold-mean MaxDD structurally understates multi-year drawdowns: a
    # drawdown spanning a year boundary is split across two folds and
    # each fold only sees its own segment. The 2026-06-19 Stretch+hedge
    # revert happened because fold stats looked fine while the full-period
    # peak-to-trough was materially worse. Production go/no-go decisions
    # must use THESE numbers for MaxDD, not the fold aggregate above.
    strat_modern = strat_rets.loc[strat_rets.index.year >= MIN_OOS_YEAR]
    full_nav = (1.0 + strat_modern).cumprod()
    full_dd = float((full_nav / full_nav.cummax() - 1.0).min())
    _fp_years = max(len(strat_modern) / ANNUAL_TRADING_DAYS, 1e-6)
    full_ann = float(full_nav.iloc[-1] ** (1.0 / _fp_years) - 1.0)
    full_vol = float(strat_modern.std() * np.sqrt(ANNUAL_TRADING_DAYS))
    full_sharpe = full_ann / full_vol if full_vol > 0 else 0.0
    _spy_modern = spy_ret_full.reindex(strat_modern.index).fillna(0.0)
    _spy_fp_nav = (1.0 + _spy_modern).cumprod()
    _spy_fp_ann = float(_spy_fp_nav.iloc[-1] ** (1.0 / _fp_years) - 1.0)
    _spy_fp_dd = float((_spy_fp_nav / _spy_fp_nav.cummax() - 1.0).min())
    print("\n" + "=" * 88)
    print(f"FULL-PERIOD ({strat_modern.index.min().date()} → "
          f"{strat_modern.index.max().date()}, peak-to-trough across folds)")
    print("=" * 88)
    print(f"  Ann return:    {full_ann*100:+.2f}%   (SPY {_spy_fp_ann*100:+.2f}%, "
          f"α {(full_ann-_spy_fp_ann)*100:+.2f}%)")
    print(f"  Sharpe:        {full_sharpe:+.2f}")
    print(f"  MaxDD:         {full_dd*100:+.2f}%   (SPY {_spy_fp_dd*100:+.2f}%)  "
          f"← gate on THIS, not fold-mean")

    # === Verdict ===
    print("\n" + "=" * 88)
    print("VERDICT")
    print("=" * 88)
    if t_stat_alpha > 2.0:
        print(f"  α t-stat {t_stat_alpha:+.2f} > 2 → statistically meaningful positive alpha.")
    elif t_stat_alpha > 1.0:
        print(f"  α t-stat {t_stat_alpha:+.2f} between 1-2 → weak positive signal, not significant.")
    elif t_stat_alpha > -1.0:
        print(f"  α t-stat {t_stat_alpha:+.2f} between -1 and +1 → indistinguishable from zero.")
    else:
        print(f"  α t-stat {t_stat_alpha:+.2f} < -1 → meaningful negative alpha vs SPY.")
    if std_sharpe < 0.30:
        print(f"  Sharpe std {std_sharpe:.2f} < 0.30 → low fold-to-fold variation, engine stable.")
    elif std_sharpe < 0.60:
        print(f"  Sharpe std {std_sharpe:.2f} between 0.30-0.60 → moderate fold variation.")
    else:
        print(f"  Sharpe std {std_sharpe:.2f} > 0.60 → high fold variation, regime-dependent.")
    print(f"  Use this as baseline. For parameter sweeps via walk-forward CV, "
          f"a candidate must improve MEAN sharpe across folds — not just the best single year.")

    # Save JSON
    try:
        summary = {
            "run_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            "n_folds": len(folds),
            "folds": folds,
            "aggregate": {
                "mean_sharpe": round(mean_sharpe, 3),
                "std_sharpe": round(std_sharpe, 3),
                "se_sharpe": round(se_sharpe, 4),
                "t_stat_sharpe": round(t_stat_sharpe, 3),
                "mean_alpha_vs_spy": round(mean_alpha, 6),
                "std_alpha_vs_spy": round(std_alpha, 6),
                "se_alpha_vs_spy": round(se_alpha, 6),
                "t_stat_alpha": round(t_stat_alpha, 3),
                "mean_ann_return": round(float(ret_arr.mean()), 6),
                "mean_max_drawdown": round(float(dd_arr.mean()), 6),
                "worst_max_drawdown": round(float(dd_arr.min()), 6),
                "n_positive_alpha_folds": n_positive_alpha,
            },
            "full_period": {
                "start": str(strat_modern.index.min().date()),
                "end": str(strat_modern.index.max().date()),
                "ann_return": round(full_ann, 6),
                "sharpe": round(full_sharpe, 3),
                "max_drawdown": round(full_dd, 6),
                "spy_ann_return": round(_spy_fp_ann, 6),
                "spy_max_drawdown": round(_spy_fp_dd, 6),
                "alpha_vs_spy": round(full_ann - _spy_fp_ann, 6),
            },
        }
        json_path = APP_DIR / "walk_forward_cv_summary.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"\n[wf-cv] summary JSON → {json_path}")
    except Exception as e:
        print(f"[wf-cv] summary JSON save failed: {e}")

    print("\n" + "=" * 88)
    print("WALK-FORWARD CV COMPLETE")
    print("=" * 88)
    return 0


def _run_attribution() -> int:
    print("\n" + "=" * 88)
    print("PERFORMANCE ATTRIBUTION — where does the engine earn its money?")
    print("=" * 88)

    TRAIN_MONTHS = 24
    MIN_OOS_YEAR = 2016
    _attr_tickers = [c for c in prices.columns if c != "PortfolioValue"]
    print(f"[attr] universe: {len(_attr_tickers)} tickers, OOS filter: year ≥ {MIN_OOS_YEAR}")
    print(f"[attr] downloading full history (max)...")

    t0 = time.perf_counter()
    raw = yf.download(_attr_tickers, period="max", interval="1d",
                      auto_adjust=True, threads=False, progress=False)
    px = _normalize_yfinance_close(raw)
    px.index = pd.to_datetime(px.index).tz_localize(None)
    px = px.sort_index().ffill().bfill()
    fx_raw = yf.download("USDAUD=X", period="max", interval="1d",
                         auto_adjust=True, threads=False, progress=False)
    fx = fx_raw["Close"] if isinstance(fx_raw, pd.DataFrame) else fx_raw
    if isinstance(fx, pd.DataFrame):
        fx = fx.iloc[:, 0]
    fx = pd.to_numeric(fx, errors="coerce").reindex(px.index).ffill().bfill()
    usd_cols = [c for c in px.columns
                if not str(c).endswith(".AX") and not str(c).startswith("^")]
    px_aud = px.copy()
    if usd_cols:
        px_aud.update(px.loc[:, usd_cols].mul(fx, axis=0))
    px_aud = px_aud.ffill().bfill().dropna(how="all")
    print(f"[attr] data ready ({px_aud.shape[0]} days × {px_aud.shape[1]} tickers) "
          f"in {time.perf_counter()-t0:.1f}s")

    print(f"\n[attr] running engine (single walk-forward)...")
    t1 = time.perf_counter()
    out = run_oos_ensemble_walk_forward(
        px_aud,
        train_window_months=TRAIN_MONTHS,
        rebalance=REBALANCE_FREQ,
        benchmark_ticker="SPY",
        score_lookback_days=252,
        lambda_temp=3.0,
        starting_nav_aud=1_000_000.0,
    )
    strat_rets_full = out["blended_returns"]
    if strat_rets_full.empty:
        print("[attr] engine returned empty series; aborting.")
        return 1

    # Slice OOS to modern era
    mask = strat_rets_full.index.year >= MIN_OOS_YEAR
    strat_rets = strat_rets_full.loc[mask]
    if strat_rets.empty:
        print(f"[attr] no OOS days in year ≥ {MIN_OOS_YEAR}; aborting.")
        return 1
    print(f"[attr] engine done ({len(strat_rets)} OOS days "
          f"{strat_rets.index.min().date()} → {strat_rets.index.max().date()}) "
          f"in {time.perf_counter()-t1:.1f}s")

    years = max(len(strat_rets) / ANNUAL_TRADING_DAYS, 1e-6)
    nav_curve = (1.0 + strat_rets).cumprod()
    strat_ann = float(nav_curve.iloc[-1] ** (1.0 / years) - 1.0)
    strat_vol = float(strat_rets.std() * np.sqrt(ANNUAL_TRADING_DAYS))
    strat_sharpe = strat_ann / strat_vol if strat_vol > 0 else 0.0
    spy_ret_full = (px_aud["SPY"].pct_change().dropna()
                    if "SPY" in px_aud.columns else pd.Series(dtype=float))
    spy_window = spy_ret_full.reindex(strat_rets.index).fillna(0.0)
    spy_nav = (1.0 + spy_window).cumprod()
    spy_ann = float(spy_nav.iloc[-1] ** (1.0 / years) - 1.0)
    spy_vol = float(spy_window.std() * np.sqrt(ANNUAL_TRADING_DAYS))
    spy_sharpe = spy_ann / spy_vol if spy_vol > 0 else 0.0
    print(f"\n[attr] BLENDED  ann={strat_ann*100:+.2f}%  "
          f"Sharpe={strat_sharpe:.2f}  "
          f"vs SPY ann={spy_ann*100:+.2f}%  Sharpe={spy_sharpe:.2f}  "
          f"α={strat_ann-spy_ann:+.2%}")

    # === 1) Per-slot attribution ===
    print("\n" + "=" * 88)
    print("1) PER-SLOT ATTRIBUTION")
    print("=" * 88)
    print("  Standalone = each slot's returns if it were held alone (no blending).")
    print("  Contribution = avg softmax weight × slot's actual return inside blend.")
    print()
    slot_rows: list[dict] = []
    cand_rets_df = out.get("per_candidate_returns", pd.DataFrame())
    softmax_df = out.get("softmax_history", pd.DataFrame())
    cand_rets_mod = cand_rets_df.loc[cand_rets_df.index.year >= MIN_OOS_YEAR] \
                     if not cand_rets_df.empty else pd.DataFrame()
    softmax_mod = softmax_df.loc[softmax_df.index.year >= MIN_OOS_YEAR] \
                   if not softmax_df.empty else pd.DataFrame()

    for slot in ENSEMBLE_SLOT_NAMES:
        if slot not in cand_rets_mod.columns:
            continue
        sr = cand_rets_mod[slot].dropna()
        if sr.empty:
            continue
        sr_nav = (1.0 + sr).cumprod()
        s_years = max(len(sr) / ANNUAL_TRADING_DAYS, 1e-6)
        s_ann = float(sr_nav.iloc[-1] ** (1.0 / s_years) - 1.0)
        s_vol = float(sr.std() * np.sqrt(ANNUAL_TRADING_DAYS))
        s_sharpe = s_ann / s_vol if s_vol > 0 else 0.0
        spy_aligned = spy_ret_full.reindex(sr.index).fillna(0.0)
        spy_nav_s = (1.0 + spy_aligned).cumprod()
        spy_ann_s = float(spy_nav_s.iloc[-1] ** (1.0 / s_years) - 1.0)
        s_alpha = s_ann - spy_ann_s
        s_dd = float((sr_nav / sr_nav.cummax() - 1.0).min())

        avg_softmax = float(softmax_mod.get(slot, pd.Series(0.0)).mean()) if not softmax_mod.empty else 0.0
        slot_rows.append({
            "slot": slot,
            "avg_softmax_weight": avg_softmax,
            "standalone_ann_return": s_ann,
            "standalone_sharpe": s_sharpe,
            "standalone_max_drawdown": s_dd,
            "standalone_alpha_vs_spy": s_alpha,
            "contribution_to_blended": avg_softmax * s_ann,
        })

    print(f"  {'Slot':<22} {'AvgWt':>7} {'AnnRet':>9} {'Sharpe':>7} "
          f"{'MaxDD':>8} {'α(SPY)':>8} {'Contrib':>9}")
    print(f"  {'-'*22} {'-'*7} {'-'*9} {'-'*7} {'-'*8} {'-'*8} {'-'*9}")
    for r in slot_rows:
        print(f"  {r['slot']:<22} "
              f"{r['avg_softmax_weight']*100:>6.1f}% "
              f"{r['standalone_ann_return']*100:>+8.2f}% "
              f"{r['standalone_sharpe']:>7.2f} "
              f"{r['standalone_max_drawdown']*100:>+7.2f}% "
              f"{r['standalone_alpha_vs_spy']*100:>+7.2f}% "
              f"{r['contribution_to_blended']*100:>+8.2f}%")
    print()
    print(f"  Blended ann return (sum of contributions, approx): "
          f"{sum(r['contribution_to_blended'] for r in slot_rows)*100:+.2f}%")
    print(f"  Blended ann return (actual realised, with regime switching): "
          f"{strat_ann*100:+.2f}%")
    print(f"  Difference = value added by softmax timing of slot weights.")

    # === 2) Per-asset attribution ===
    print("\n" + "=" * 88)
    print("2) PER-ASSET ATTRIBUTION (cumulative return contribution to blended)")
    print("=" * 88)
    blended_weights_df = out.get("blended_weights", pd.DataFrame())
    asset_contrib: dict[str, float] = {}
    if not blended_weights_df.empty:
        bw = blended_weights_df.copy()
        bw.index = pd.to_datetime(bw.index).tz_localize(None)
        bw = bw.sort_index()
        bw_mod = bw.loc[bw.index.year >= MIN_OOS_YEAR]
        # Daily forward-filled blended weights across the OOS span.
        daily_idx = strat_rets.index
        bw_daily = bw_mod.reindex(daily_idx).ffill().fillna(0.0)
        # Daily asset returns aligned to OOS span.
        asset_rets_daily = px_aud.pct_change().reindex(daily_idx).fillna(0.0)
        # Per-asset cumulative contribution = sum over days of (w_a(d) × r_a(d)).
        # That sum approximately equals the asset's contribution to total blended
        # return (exact under buy-and-hold; near-exact with daily rebalance lag).
        common_tkrs = [c for c in bw_daily.columns if c in asset_rets_daily.columns]
        for tkr in common_tkrs:
            contrib = float((bw_daily[tkr] * asset_rets_daily[tkr]).sum())
            asset_contrib[tkr] = contrib

    sorted_assets = sorted(asset_contrib.items(), key=lambda kv: kv[1], reverse=True)
    n_show = min(10, len(sorted_assets))
    print(f"  TOP {n_show} CONTRIBUTORS (cumulative return added):")
    print(f"  {'Ticker':<10} {'Cum Contrib':>13}")
    print(f"  {'-'*10} {'-'*13}")
    for tkr, c in sorted_assets[:n_show]:
        print(f"  {tkr:<10} {c*100:>+12.2f}%")
    print()
    print(f"  BOTTOM {n_show} CONTRIBUTORS:")
    print(f"  {'Ticker':<10} {'Cum Contrib':>13}")
    print(f"  {'-'*10} {'-'*13}")
    for tkr, c in sorted_assets[-n_show:]:
        print(f"  {tkr:<10} {c*100:>+12.2f}%")
    total_contrib = sum(c for _, c in sorted_assets)
    print(f"\n  Sum of all asset contributions: {total_contrib*100:+.2f}%")
    print(f"  Blended cumulative return (actual): {(nav_curve.iloc[-1] - 1.0)*100:+.2f}%")

    # === 3) Per-regime attribution ===
    print("\n" + "=" * 88)
    print("3) PER-REGIME ATTRIBUTION (SPY 20d/50d SMA cross)")
    print("=" * 88)
    spy_px = px_aud["SPY"] if "SPY" in px_aud.columns else pd.Series(dtype=float)
    regime_rows = []
    if not spy_px.empty:
        sma20 = spy_px.rolling(20).mean()
        sma50 = spy_px.rolling(50).mean()
        regime_flag = (sma20 > sma50).reindex(strat_rets.index).ffill()
        for label, mask in [("BULL (SMA20>SMA50)", regime_flag == True),
                            ("BEAR (SMA20<SMA50)", regime_flag == False)]:
            chunk = strat_rets.loc[mask]
            spy_chunk = spy_window.loc[mask]
            if chunk.empty:
                continue
            chunk_nav = (1.0 + chunk).cumprod()
            n = len(chunk)
            y = max(n / ANNUAL_TRADING_DAYS, 1e-6)
            ann_ret = float(chunk_nav.iloc[-1] ** (1.0 / y) - 1.0)
            vol = float(chunk.std() * np.sqrt(ANNUAL_TRADING_DAYS))
            sh = ann_ret / vol if vol > 0 else 0.0
            spy_nav_r = (1.0 + spy_chunk).cumprod()
            spy_ann_r = float(spy_nav_r.iloc[-1] ** (1.0 / y) - 1.0)
            spy_vol_r = float(spy_chunk.std() * np.sqrt(ANNUAL_TRADING_DAYS))
            spy_sh = spy_ann_r / spy_vol_r if spy_vol_r > 0 else 0.0
            dd = float((chunk_nav / chunk_nav.cummax() - 1.0).min())
            # Average softmax slot weights during this regime
            softmax_in_regime = pd.Series(dtype=float)
            if not softmax_mod.empty:
                sm_idx = softmax_mod.index[softmax_mod.index.isin(chunk.index)]
                if not sm_idx.empty:
                    softmax_in_regime = softmax_mod.loc[sm_idx].mean()
            regime_rows.append({
                "regime": label,
                "n_days": n,
                "frac_of_oos": n / len(strat_rets),
                "ann_return": ann_ret,
                "sharpe": sh,
                "max_drawdown": dd,
                "spy_ann_return": spy_ann_r,
                "spy_sharpe": spy_sh,
                "alpha_vs_spy": ann_ret - spy_ann_r,
                "avg_slot_weights": {k: float(v) for k, v in softmax_in_regime.items()},
            })

    print(f"  {'Regime':<20} {'Days':>5} {'%OOS':>5} {'AnnRet':>9} "
          f"{'Sharpe':>7} {'MaxDD':>8} {'SPY Ret':>9} {'SPY Sh':>7} {'α':>8}")
    print(f"  {'-'*20} {'-'*5} {'-'*5} {'-'*9} {'-'*7} {'-'*8} {'-'*9} {'-'*7} {'-'*8}")
    for r in regime_rows:
        print(f"  {r['regime']:<20} "
              f"{r['n_days']:>5} "
              f"{r['frac_of_oos']*100:>4.0f}% "
              f"{r['ann_return']*100:>+8.2f}% "
              f"{r['sharpe']:>7.2f} "
              f"{r['max_drawdown']*100:>+7.2f}% "
              f"{r['spy_ann_return']*100:>+8.2f}% "
              f"{r['spy_sharpe']:>7.2f} "
              f"{r['alpha_vs_spy']*100:>+7.2f}%")
    print()
    for r in regime_rows:
        if r["avg_slot_weights"]:
            mix = " · ".join(f"{k.split(' ')[0]} {v*100:.0f}%"
                              for k, v in r["avg_slot_weights"].items())
            print(f"  {r['regime']} avg slot mix: {mix}")

    # === Verdict ===
    print("\n" + "=" * 88)
    print("VERDICT")
    print("=" * 88)
    if slot_rows:
        best_slot = max(slot_rows, key=lambda r: r["standalone_alpha_vs_spy"])
        worst_slot = min(slot_rows, key=lambda r: r["standalone_alpha_vs_spy"])
        print(f"  Best α-vs-SPY slot:  {best_slot['slot']}  "
              f"α={best_slot['standalone_alpha_vs_spy']*100:+.2f}%  "
              f"(avg weight {best_slot['avg_softmax_weight']*100:.0f}%)")
        print(f"  Worst α-vs-SPY slot: {worst_slot['slot']}  "
              f"α={worst_slot['standalone_alpha_vs_spy']*100:+.2f}%  "
              f"(avg weight {worst_slot['avg_softmax_weight']*100:.0f}%)")
    if regime_rows:
        bull = next((r for r in regime_rows if "BULL" in r["regime"]), None)
        bear = next((r for r in regime_rows if "BEAR" in r["regime"]), None)
        if bull and bear:
            print(f"  Bull-regime α: {bull['alpha_vs_spy']*100:+.2f}%   "
                  f"Bear-regime α: {bear['alpha_vs_spy']*100:+.2f}%")
            if bear["alpha_vs_spy"] > bull["alpha_vs_spy"] + 0.05:
                print(f"  Reading: engine outperforms SPY in BEAR regimes — "
                      f"consistent with vol-managed-beta thesis.")
            elif bull["alpha_vs_spy"] > bear["alpha_vs_spy"] + 0.05:
                print(f"  Reading: engine outperforms in BULL regimes — "
                      f"unusual for a vol-managed strategy, possibly noise.")
            else:
                print(f"  Reading: regime α difference small — engine is "
                      f"regime-neutral on raw return.")

    # Save JSON
    try:
        summary = {
            "run_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            "oos_window": {
                "start": str(strat_rets.index.min().date()),
                "end": str(strat_rets.index.max().date()),
                "n_days": len(strat_rets),
                "years": round(years, 2),
            },
            "blended": {
                "ann_return": round(strat_ann, 6),
                "sharpe": round(strat_sharpe, 3),
                "spy_ann_return": round(spy_ann, 6),
                "spy_sharpe": round(spy_sharpe, 3),
                "alpha_vs_spy": round(strat_ann - spy_ann, 6),
            },
            "per_slot": slot_rows,
            "per_asset_top": [{"ticker": t, "cum_contribution": round(c, 6)}
                              for t, c in sorted_assets[:n_show]],
            "per_asset_bottom": [{"ticker": t, "cum_contribution": round(c, 6)}
                                 for t, c in sorted_assets[-n_show:]],
            "per_regime": regime_rows,
        }
        json_path = APP_DIR / "attribution_summary.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"\n[attr] summary JSON → {json_path}")
    except Exception as e:
        print(f"[attr] summary JSON save failed: {e}")

    print("\n" + "=" * 88)
    print("ATTRIBUTION COMPLETE")
    print("=" * 88)
    return 0


def _run_crash_hedge_test() -> int:
    print("\n" + "=" * 88)
    print("CRASH-HEDGE A/B TEST — walk-forward CV (hedge ON vs OFF)")
    print("=" * 88)
    print(f"  trigger:  SPY DD ≤ {CRASH_HEDGE_DD_TRIGGER*100:+.0f}% (rolling 1y peak)")
    print(f"  release:  SPY DD ≥ {CRASH_HEDGE_DD_RELEASE*100:+.0f}% (hysteresis)")
    print(f"  basket:   {CRASH_HEDGE_BASKET}")

    TRAIN_MONTHS = 24
    MIN_OOS_YEAR = 2016
    MIN_FOLD_DAYS = 150

    _hh_tickers = [c for c in prices.columns if c != "PortfolioValue"]
    # Ensure hedge basket tickers are in the universe
    for tkr in CRASH_HEDGE_BASKET.keys():
        if tkr not in _hh_tickers:
            print(f"[hedge-test] WARNING: hedge basket ticker {tkr} not in universe — will be auto-added")
            _hh_tickers.append(tkr)
    print(f"[hedge-test] universe: {len(_hh_tickers)} tickers, OOS folds: year ≥ {MIN_OOS_YEAR}")
    print(f"[hedge-test] downloading full history (max)...")

    t0 = time.perf_counter()
    raw = yf.download(_hh_tickers, period="max", interval="1d",
                      auto_adjust=True, threads=False, progress=False)
    px = _normalize_yfinance_close(raw)
    px.index = pd.to_datetime(px.index).tz_localize(None)
    px = px.sort_index().ffill().bfill()
    fx_raw = yf.download("USDAUD=X", period="max", interval="1d",
                         auto_adjust=True, threads=False, progress=False)
    fx = fx_raw["Close"] if isinstance(fx_raw, pd.DataFrame) else fx_raw
    if isinstance(fx, pd.DataFrame):
        fx = fx.iloc[:, 0]
    fx = pd.to_numeric(fx, errors="coerce").reindex(px.index).ffill().bfill()
    usd_cols = [c for c in px.columns
                if not str(c).endswith(".AX") and not str(c).startswith("^")]
    px_aud = px.copy()
    if usd_cols:
        px_aud.update(px.loc[:, usd_cols].mul(fx, axis=0))
    px_aud = px_aud.ffill().bfill().dropna(how="all")
    print(f"[hedge-test] data ready ({px_aud.shape[0]} days × {px_aud.shape[1]} tickers) "
          f"in {time.perf_counter()-t0:.1f}s")

    spy_ret_full = (px_aud["SPY"].pct_change().dropna()
                    if "SPY" in px_aud.columns else pd.Series(dtype=float))

    def _run_and_fold(hedge_on: bool) -> tuple[pd.Series, dict]:
        t1 = time.perf_counter()
        out = run_oos_ensemble_walk_forward(
            px_aud,
            train_window_months=TRAIN_MONTHS,
            rebalance=REBALANCE_FREQ,
            benchmark_ticker="SPY",
            score_lookback_days=252,
            lambda_temp=3.0,
            starting_nav_aud=1_000_000.0,
            crash_hedge=hedge_on,
        )
        elapsed = time.perf_counter() - t1
        info = {
            "elapsed_sec": elapsed,
            "n_executed": int(out.get("n_executed", 0)),
            "n_skipped": int(out.get("n_skipped", 0)),
            "hedge_active_rebals": int(out.get("hedge_active_rebals", 0)),
            "hedge_n_triggers": int(out.get("hedge_n_triggers", 0)),
            "hedge_events": out.get("hedge_events", []) or [],
            "cgt_bps_per_year": 0.0,
        }
        # CGT drag
        tax_ser = out.get("rebalance_taxes", pd.Series(dtype=float))
        strat_rets = out["blended_returns"]
        if not tax_ser.empty and not strat_rets.empty:
            years = max(len(strat_rets) / ANNUAL_TRADING_DAYS, 1e-6)
            info["cgt_bps_per_year"] = float(tax_ser.sum() / years * 10_000)
        return strat_rets, info

    def _fold_metrics(rets: pd.Series, min_year: int) -> list[dict]:
        if rets.empty:
            return []
        folds = []
        for yr in sorted(set(rets.index.year)):
            if yr < min_year:
                continue
            mask = (rets.index.year == yr)
            chunk = rets.loc[mask]
            if len(chunk) < MIN_FOLD_DAYS:
                continue
            chunk_nav = (1.0 + chunk).cumprod()
            n_days = len(chunk)
            years_in_chunk = n_days / ANNUAL_TRADING_DAYS
            ann_ret = float(chunk_nav.iloc[-1] ** (1.0 / years_in_chunk) - 1.0)
            vol = float(chunk.std() * np.sqrt(ANNUAL_TRADING_DAYS))
            sharpe = ann_ret / vol if vol > 0 else 0.0
            dd = float((chunk_nav / chunk_nav.cummax() - 1.0).min())
            spy_chunk = spy_ret_full.reindex(chunk.index).fillna(0.0)
            spy_nav = (1.0 + spy_chunk).cumprod()
            spy_ann = float(spy_nav.iloc[-1] ** (1.0 / years_in_chunk) - 1.0)
            alpha = ann_ret - spy_ann
            folds.append({
                "year": int(yr),
                "ann_return": ann_ret,
                "sharpe": sharpe,
                "max_drawdown": dd,
                "spy_ann_return": spy_ann,
                "alpha_vs_spy": alpha,
            })
        return folds

    # === Baseline: hedge OFF ===
    print(f"\n[hedge-test] running BASELINE (hedge OFF)...")
    base_rets, base_info = _run_and_fold(hedge_on=False)
    print(f"[hedge-test] baseline done in {base_info['elapsed_sec']:.1f}s "
          f"({len(base_rets)} OOS days)")
    base_folds = _fold_metrics(base_rets, MIN_OOS_YEAR)

    # === Treatment: hedge ON ===
    print(f"\n[hedge-test] running TREATMENT (hedge ON)...")
    hedge_rets, hedge_info = _run_and_fold(hedge_on=True)
    print(f"[hedge-test] treatment done in {hedge_info['elapsed_sec']:.1f}s "
          f"({len(hedge_rets)} OOS days)")
    print(f"[hedge-test] hedge triggered {hedge_info['hedge_n_triggers']}× "
          f"(OFF→ON transitions), active on {hedge_info['hedge_active_rebals']} rebalances")
    if hedge_info["hedge_events"]:
        print(f"[hedge-test] state transitions:")
        for e in hedge_info["hedge_events"][:20]:
            print(f"  {pd.Timestamp(e['date']).date()}  {e['transition']:<3}  "
                  f"(SPY DD {e['spy_dd']*100:+.1f}%)")
        if len(hedge_info["hedge_events"]) > 20:
            print(f"  ... and {len(hedge_info['hedge_events']) - 20} more")
    hedge_folds = _fold_metrics(hedge_rets, MIN_OOS_YEAR)

    if len(base_folds) < 3 or len(hedge_folds) < 3:
        print("[hedge-test] insufficient folds; aborting.")
        return 1

    # Align folds by year
    base_by_year = {r["year"]: r for r in base_folds}
    hedge_by_year = {r["year"]: r for r in hedge_folds}
    common_years = sorted(set(base_by_year.keys()) & set(hedge_by_year.keys()))

    # === Per-fold comparison table ===
    print("\n" + "=" * 88)
    print(f"PER-FOLD COMPARISON ({len(common_years)} years)")
    print("=" * 88)
    print(f"  {'Year':>5}    {'Baseline':>20}    {'+ Crash Hedge':>20}    {'Δ':>15}")
    print(f"  {'':>5}    {'Sharpe / α / MaxDD':>20}    {'Sharpe / α / MaxDD':>20}    "
          f"{'ΔSharpe Δα':>15}")
    print(f"  {'-'*5}    {'-'*20}    {'-'*20}    {'-'*15}")
    for yr in common_years:
        b = base_by_year[yr]
        h = hedge_by_year[yr]
        d_sh = h["sharpe"] - b["sharpe"]
        d_al = h["alpha_vs_spy"] - b["alpha_vs_spy"]
        print(f"  {yr:>5}    "
              f"{b['sharpe']:>+5.2f} / {b['alpha_vs_spy']*100:>+5.1f}% / {b['max_drawdown']*100:>+5.1f}%    "
              f"{h['sharpe']:>+5.2f} / {h['alpha_vs_spy']*100:>+5.1f}% / {h['max_drawdown']*100:>+5.1f}%    "
              f"{d_sh:>+5.2f}   {d_al*100:>+5.1f}%")

    # === Aggregate ===
    base_sh = np.array([r["sharpe"] for r in base_folds if r["year"] in common_years])
    hedge_sh = np.array([r["sharpe"] for r in hedge_folds if r["year"] in common_years])
    base_al = np.array([r["alpha_vs_spy"] for r in base_folds if r["year"] in common_years])
    hedge_al = np.array([r["alpha_vs_spy"] for r in hedge_folds if r["year"] in common_years])
    base_dd = np.array([r["max_drawdown"] for r in base_folds if r["year"] in common_years])
    hedge_dd = np.array([r["max_drawdown"] for r in hedge_folds if r["year"] in common_years])
    base_rt = np.array([r["ann_return"] for r in base_folds if r["year"] in common_years])
    hedge_rt = np.array([r["ann_return"] for r in hedge_folds if r["year"] in common_years])

    print("\n" + "=" * 88)
    print("AGGREGATE (mean ± std across folds)")
    print("=" * 88)
    print(f"  Sharpe:        baseline {base_sh.mean():+.2f} ± {base_sh.std(ddof=1):.2f}   "
          f"hedge {hedge_sh.mean():+.2f} ± {hedge_sh.std(ddof=1):.2f}   "
          f"Δ {hedge_sh.mean() - base_sh.mean():+.2f}")
    print(f"  Ann return:    baseline {base_rt.mean()*100:+.2f}%   "
          f"hedge {hedge_rt.mean()*100:+.2f}%   "
          f"Δ {(hedge_rt.mean() - base_rt.mean())*100:+.2f}%")
    print(f"  α vs SPY:      baseline {base_al.mean()*100:+.2f}% ± {base_al.std(ddof=1)*100:.2f}%   "
          f"hedge {hedge_al.mean()*100:+.2f}% ± {hedge_al.std(ddof=1)*100:.2f}%   "
          f"Δ {(hedge_al.mean() - base_al.mean())*100:+.2f}%")
    print(f"  MaxDD:         baseline {base_dd.mean()*100:+.2f}% (worst {base_dd.min()*100:+.2f}%)   "
          f"hedge {hedge_dd.mean()*100:+.2f}% (worst {hedge_dd.min()*100:+.2f}%)   "
          f"Δ {(hedge_dd.mean() - base_dd.mean())*100:+.2f}%")
    print(f"  CGT bps/yr:    baseline {base_info['cgt_bps_per_year']:.0f}   "
          f"hedge {hedge_info['cgt_bps_per_year']:.0f}   "
          f"Δ {hedge_info['cgt_bps_per_year'] - base_info['cgt_bps_per_year']:+.0f}")

    # === Verdict (honest 4-dimension check via central helper) ===
    print()
    _verdict = _evaluate_sweep_result(
        baseline={
            "sharpe": float(base_sh.mean()),
            "max_drawdown": float(base_dd.min()),  # worst-fold MaxDD as proxy
            "alpha_vs_spy": float(base_al.mean()),
            "ann_return": float(base_rt.mean()),
        },
        treatment={
            "sharpe": float(hedge_sh.mean()),
            "max_drawdown": float(hedge_dd.min()),
            "alpha_vs_spy": float(hedge_al.mean()),
            "ann_return": float(hedge_rt.mean()),
        },
        label_baseline="hedge OFF",
        label_treatment="hedge ON",
    )
    _print_sweep_verdict(_verdict)
    print(f"\n  N triggers in OOS window: {hedge_info['hedge_n_triggers']}")
    print(f"  Hedge active on {hedge_info['hedge_active_rebals']} rebalances "
          f"(out of {hedge_info['n_executed'] + hedge_info['n_skipped']})")

    # Save JSON
    try:
        summary = {
            "run_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            "config": {
                "dd_trigger": CRASH_HEDGE_DD_TRIGGER,
                "dd_release": CRASH_HEDGE_DD_RELEASE,
                "lookback_days": CRASH_HEDGE_LOOKBACK_DAYS,
                "basket": CRASH_HEDGE_BASKET,
            },
            "baseline": {
                "info": base_info,
                "folds": base_folds,
                "aggregate": {
                    "mean_sharpe": round(float(base_sh.mean()), 3),
                    "std_sharpe": round(float(base_sh.std(ddof=1)), 3),
                    "mean_alpha": round(float(base_al.mean()), 6),
                    "mean_max_drawdown": round(float(base_dd.mean()), 6),
                },
            },
            "hedge": {
                "info": {k: v for k, v in hedge_info.items() if k != "hedge_events"},
                "events": [{"date": str(pd.Timestamp(e["date"]).date()),
                            "transition": e["transition"],
                            "spy_dd": round(e["spy_dd"], 4)}
                           for e in hedge_info["hedge_events"]],
                "folds": hedge_folds,
                "aggregate": {
                    "mean_sharpe": round(float(hedge_sh.mean()), 3),
                    "std_sharpe": round(float(hedge_sh.std(ddof=1)), 3),
                    "mean_alpha": round(float(hedge_al.mean()), 6),
                    "mean_max_drawdown": round(float(hedge_dd.mean()), 6),
                },
            },
            "uplift": {
                "sharpe": round(float(d_sharpe), 3),
                "alpha": round(float(d_alpha), 6),
                "max_drawdown": round(float(d_dd), 6),
            },
        }
        json_path = APP_DIR / "crash_hedge_test_summary.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"\n[hedge-test] summary JSON → {json_path}")
    except Exception as e:
        print(f"[hedge-test] summary JSON save failed: {e}")

    print("\n" + "=" * 88)
    print("CRASH-HEDGE TEST COMPLETE")
    print("=" * 88)
    return 0


def _run_crash_hedge_release_sweep() -> int:
    print("\n" + "=" * 88)
    print("CRASH-HEDGE RELEASE-THRESHOLD SWEEP")
    print("=" * 88)
    SWEEP_RELEASES = [-0.03, -0.05, -0.08, -0.10, -0.12]
    TRIGGER_FIXED = CRASH_HEDGE_DD_TRIGGER  # -0.15
    TRAIN_MONTHS = 24
    MIN_OOS_YEAR = 2016
    MIN_FOLD_DAYS = 150
    print(f"  trigger fixed at {TRIGGER_FIXED*100:+.0f}% DD; "
          f"sweeping release ∈ {[f'{r*100:+.0f}%' for r in SWEEP_RELEASES]}")
    print(f"  basket: {CRASH_HEDGE_BASKET}")

    _hh_tickers = [c for c in prices.columns if c != "PortfolioValue"]
    for tkr in CRASH_HEDGE_BASKET.keys():
        if tkr not in _hh_tickers:
            _hh_tickers.append(tkr)
    print(f"[hedge-rel] universe: {len(_hh_tickers)} tickers, "
          f"OOS folds: year ≥ {MIN_OOS_YEAR}")
    print(f"[hedge-rel] downloading full history (max)...")

    t0 = time.perf_counter()
    raw = yf.download(_hh_tickers, period="max", interval="1d",
                      auto_adjust=True, threads=False, progress=False)
    px = _normalize_yfinance_close(raw)
    px.index = pd.to_datetime(px.index).tz_localize(None)
    px = px.sort_index().ffill().bfill()
    fx_raw = yf.download("USDAUD=X", period="max", interval="1d",
                         auto_adjust=True, threads=False, progress=False)
    fx = fx_raw["Close"] if isinstance(fx_raw, pd.DataFrame) else fx_raw
    if isinstance(fx, pd.DataFrame):
        fx = fx.iloc[:, 0]
    fx = pd.to_numeric(fx, errors="coerce").reindex(px.index).ffill().bfill()
    usd_cols = [c for c in px.columns
                if not str(c).endswith(".AX") and not str(c).startswith("^")]
    px_aud = px.copy()
    if usd_cols:
        px_aud.update(px.loc[:, usd_cols].mul(fx, axis=0))
    px_aud = px_aud.ffill().bfill().dropna(how="all")
    print(f"[hedge-rel] data ready ({px_aud.shape[0]} days × {px_aud.shape[1]} tickers) "
          f"in {time.perf_counter()-t0:.1f}s")

    spy_ret_full = (px_aud["SPY"].pct_change().dropna()
                    if "SPY" in px_aud.columns else pd.Series(dtype=float))

    def _run_with_release(release: float | None) -> tuple[list[dict], dict]:
        # release=None → hedge OFF (baseline)
        is_baseline = release is None
        t1 = time.perf_counter()
        out = run_oos_ensemble_walk_forward(
            px_aud,
            train_window_months=TRAIN_MONTHS,
            rebalance=REBALANCE_FREQ,
            benchmark_ticker="SPY",
            score_lookback_days=252,
            lambda_temp=3.0,
            starting_nav_aud=1_000_000.0,
            crash_hedge=(not is_baseline),
            crash_hedge_dd_release=release,
        )
        elapsed = time.perf_counter() - t1
        strat_rets = out["blended_returns"]
        info = {
            "elapsed_sec": elapsed,
            "n_triggers": int(out.get("hedge_n_triggers", 0)),
            "n_active": int(out.get("hedge_active_rebals", 0)),
        }
        if strat_rets.empty:
            return [], info
        folds = []
        for yr in sorted(set(strat_rets.index.year)):
            if yr < MIN_OOS_YEAR:
                continue
            mask = (strat_rets.index.year == yr)
            chunk = strat_rets.loc[mask]
            if len(chunk) < MIN_FOLD_DAYS:
                continue
            chunk_nav = (1.0 + chunk).cumprod()
            n_days = len(chunk)
            yic = n_days / ANNUAL_TRADING_DAYS
            ann_ret = float(chunk_nav.iloc[-1] ** (1.0 / yic) - 1.0)
            vol = float(chunk.std() * np.sqrt(ANNUAL_TRADING_DAYS))
            sharpe = ann_ret / vol if vol > 0 else 0.0
            dd = float((chunk_nav / chunk_nav.cummax() - 1.0).min())
            spy_chunk = spy_ret_full.reindex(chunk.index).fillna(0.0)
            spy_nav = (1.0 + spy_chunk).cumprod()
            spy_ann = float(spy_nav.iloc[-1] ** (1.0 / yic) - 1.0)
            folds.append({
                "year": int(yr),
                "ann_return": ann_ret,
                "sharpe": sharpe,
                "max_drawdown": dd,
                "alpha_vs_spy": ann_ret - spy_ann,
            })
        return folds, info

    # === Baseline (no hedge) ===
    print(f"\n[hedge-rel] running BASELINE (hedge OFF)...")
    base_folds, base_info = _run_with_release(None)
    print(f"[hedge-rel] baseline done in {base_info['elapsed_sec']:.1f}s")

    # === Sweep ===
    results: list[dict] = []
    for rel in SWEEP_RELEASES:
        print(f"\n[hedge-rel] running release={rel*100:+.0f}%...")
        folds, info = _run_with_release(rel)
        if not folds:
            continue
        sharpe_arr = np.array([r["sharpe"] for r in folds])
        alpha_arr = np.array([r["alpha_vs_spy"] for r in folds])
        dd_arr = np.array([r["max_drawdown"] for r in folds])
        results.append({
            "release": rel,
            "n_triggers": info["n_triggers"],
            "n_active_rebals": info["n_active"],
            "mean_sharpe": float(sharpe_arr.mean()),
            "std_sharpe": float(sharpe_arr.std(ddof=1)),
            "mean_alpha": float(alpha_arr.mean()),
            "mean_max_drawdown": float(dd_arr.mean()),
            "worst_max_drawdown": float(dd_arr.min()),
            "folds": folds,
            "elapsed_sec": info["elapsed_sec"],
        })
        print(f"[hedge-rel] release={rel*100:+.0f}%: "
              f"mean Sharpe {sharpe_arr.mean():+.2f}±{sharpe_arr.std(ddof=1):.2f}  "
              f"mean α {alpha_arr.mean()*100:+.2f}%  "
              f"mean MaxDD {dd_arr.mean()*100:+.2f}%  "
              f"triggers={info['n_triggers']}  "
              f"({info['elapsed_sec']:.1f}s)")

    if not results:
        print("[hedge-rel] no results; aborting.")
        return 1

    # === Aggregate table ===
    base_sh = np.array([r["sharpe"] for r in base_folds])
    base_al = np.array([r["alpha_vs_spy"] for r in base_folds])
    base_dd = np.array([r["max_drawdown"] for r in base_folds])
    base_mean_sh = float(base_sh.mean())
    base_mean_al = float(base_al.mean())
    base_mean_dd = float(base_dd.mean())

    print("\n" + "=" * 88)
    print(f"AGGREGATE RESULTS — baseline (no hedge) vs hedge at different releases")
    print(f"({len(base_folds)} modern folds, trigger fixed at {TRIGGER_FIXED*100:+.0f}% DD)")
    print("=" * 88)
    print(f"  {'Config':>12}  {'Sharpe':>14}  {'α vs SPY':>14}  {'MaxDD':>9}  "
          f"{'Trigs':>6}  {'Δ Sharpe':>10}")
    print(f"  {'-'*12}  {'-'*14}  {'-'*14}  {'-'*9}  {'-'*6}  {'-'*10}")
    print(f"  {'baseline':>12}  {f'{base_mean_sh:+.2f}±{base_sh.std(ddof=1):.2f}':>14}  "
          f"{f'{base_mean_al*100:+.2f}%':>14}  "
          f"{f'{base_mean_dd*100:+.2f}%':>9}  {'-':>6}  {'-':>10}")
    for r in results:
        d_sh = r["mean_sharpe"] - base_mean_sh
        rel_pct = r["release"] * 100
        sh_mean = r["mean_sharpe"]
        sh_std = r["std_sharpe"]
        al_pct = r["mean_alpha"] * 100
        dd_pct = r["mean_max_drawdown"] * 100
        n_trig = r["n_triggers"]
        sharpe_cell = f"{sh_mean:+.2f}±{sh_std:.2f}"
        alpha_cell = f"{al_pct:+.2f}%"
        dd_cell = f"{dd_pct:+.2f}%"
        rel_cell = f"rel={rel_pct:+.0f}%"
        print(f"  {rel_cell:>12}  {sharpe_cell:>14}  {alpha_cell:>14}  "
              f"{dd_cell:>9}  {n_trig:>6}  {d_sh:>+9.2f}")

    # === Winner pick ===
    winner = max(results, key=lambda r: (round(r["mean_sharpe"], 3), r["mean_alpha"]))
    print(f"\n[hedge-rel] WINNER → release={winner['release']*100:+.0f}%  "
          f"Sharpe {winner['mean_sharpe']:+.2f}  α {winner['mean_alpha']*100:+.2f}%  "
          f"vs baseline Sharpe {base_mean_sh:+.2f}  Δ {winner['mean_sharpe']-base_mean_sh:+.2f}")

    # === Verdict (honest 4-dim check on the winning release) ===
    print()
    # Pull winner's per-fold series into the standard 4-tuple
    _win_folds = winner["folds"]
    _win_sh = np.array([f["sharpe"] for f in _win_folds])
    _win_al = np.array([f["alpha_vs_spy"] for f in _win_folds])
    _win_rt = np.array([f["ann_return"] for f in _win_folds])
    _win_dd = np.array([f["max_drawdown"] for f in _win_folds])
    _base_yr_dd = np.array([f["max_drawdown"] for f in base_folds])
    _base_rt = np.array([f["ann_return"] for f in base_folds])
    _verdict = _evaluate_sweep_result(
        baseline={
            "sharpe": float(base_mean_sh),
            "max_drawdown": float(_base_yr_dd.min()),
            "alpha_vs_spy": float(base_mean_al),
            "ann_return": float(_base_rt.mean()),
        },
        treatment={
            "sharpe": float(winner["mean_sharpe"]),
            "max_drawdown": float(_win_dd.min()),
            "alpha_vs_spy": float(winner["mean_alpha"]),
            "ann_return": float(_win_rt.mean()),
        },
        label_baseline="no hedge",
        label_treatment=f"hedge release={winner['release']*100:+.0f}%",
    )
    _print_sweep_verdict(_verdict)
    if _verdict["verdict"] == "SHIP":
        print(f"\n  Next: update CRASH_HEDGE_DD_RELEASE = {winner['release']} "
              f"and rerun live-pipeline; check metrics_history.")

    # Save JSON
    try:
        summary = {
            "run_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            "trigger": TRIGGER_FIXED,
            "basket": CRASH_HEDGE_BASKET,
            "baseline": {
                "mean_sharpe": round(base_mean_sh, 3),
                "std_sharpe": round(float(base_sh.std(ddof=1)), 3),
                "mean_alpha": round(base_mean_al, 6),
                "mean_max_drawdown": round(base_mean_dd, 6),
                "folds": base_folds,
            },
            "sweep": results,
            "winner_release": winner["release"],
        }
        json_path = APP_DIR / "crash_hedge_release_sweep_summary.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"\n[hedge-rel] summary JSON → {json_path}")
    except Exception as e:
        print(f"[hedge-rel] summary JSON save failed: {e}")

    print("\n" + "=" * 88)
    print("CRASH-HEDGE RELEASE SWEEP COMPLETE")
    print("=" * 88)
    return 0


def _run_stretch_only_test() -> int:
    print("\n" + "=" * 88)
    print("STRETCH-ONLY A/B TEST — walk-forward CV (5-slot blend vs Stretch-only)")
    print("=" * 88)

    TRAIN_MONTHS = 24
    MIN_OOS_YEAR = 2016
    MIN_FOLD_DAYS = 150

    # Find the Stretch slot name (last entry by convention but be safe)
    stretch_slot = None
    for name in ENSEMBLE_SLOT_NAMES:
        if "Stretch" in name:
            stretch_slot = name
            break
    if not stretch_slot:
        print("[stretch] no Stretch slot found in ENSEMBLE_SLOTS; aborting.")
        return 1
    print(f"  Stretch slot: {stretch_slot}")

    _so_tickers = [c for c in prices.columns if c != "PortfolioValue"]
    print(f"[stretch] universe: {len(_so_tickers)} tickers, OOS folds: year ≥ {MIN_OOS_YEAR}")
    print(f"[stretch] downloading full history (max)...")

    t0 = time.perf_counter()
    raw = yf.download(_so_tickers, period="max", interval="1d",
                      auto_adjust=True, threads=False, progress=False)
    px = _normalize_yfinance_close(raw)
    px.index = pd.to_datetime(px.index).tz_localize(None)
    px = px.sort_index().ffill().bfill()
    fx_raw = yf.download("USDAUD=X", period="max", interval="1d",
                         auto_adjust=True, threads=False, progress=False)
    fx = fx_raw["Close"] if isinstance(fx_raw, pd.DataFrame) else fx_raw
    if isinstance(fx, pd.DataFrame):
        fx = fx.iloc[:, 0]
    fx = pd.to_numeric(fx, errors="coerce").reindex(px.index).ffill().bfill()
    usd_cols = [c for c in px.columns
                if not str(c).endswith(".AX") and not str(c).startswith("^")]
    px_aud = px.copy()
    if usd_cols:
        px_aud.update(px.loc[:, usd_cols].mul(fx, axis=0))
    px_aud = px_aud.ffill().bfill().dropna(how="all")
    print(f"[stretch] data ready ({px_aud.shape[0]} days × {px_aud.shape[1]} tickers) "
          f"in {time.perf_counter()-t0:.1f}s")

    spy_ret_full = (px_aud["SPY"].pct_change().dropna()
                    if "SPY" in px_aud.columns else pd.Series(dtype=float))

    def _run(override):
        # override=None → full 5-slot blend; override=dict → forced slot weights
        t1 = time.perf_counter()
        out = run_oos_ensemble_walk_forward(
            px_aud,
            train_window_months=TRAIN_MONTHS,
            rebalance=REBALANCE_FREQ,
            benchmark_ticker="SPY",
            score_lookback_days=252,
            lambda_temp=3.0,
            starting_nav_aud=1_000_000.0,
            slot_weights_override=override,
        )
        elapsed = time.perf_counter() - t1
        strat_rets = out["blended_returns"]
        cost_ser = out.get("rebalance_costs", pd.Series(dtype=float))
        tax_ser = out.get("rebalance_taxes", pd.Series(dtype=float))
        return strat_rets, cost_ser, tax_ser, elapsed

    def _fold_metrics(rets: pd.Series) -> list[dict]:
        if rets.empty:
            return []
        folds = []
        for yr in sorted(set(rets.index.year)):
            if yr < MIN_OOS_YEAR:
                continue
            mask = (rets.index.year == yr)
            chunk = rets.loc[mask]
            if len(chunk) < MIN_FOLD_DAYS:
                continue
            chunk_nav = (1.0 + chunk).cumprod()
            n_days = len(chunk)
            yic = n_days / ANNUAL_TRADING_DAYS
            ann_ret = float(chunk_nav.iloc[-1] ** (1.0 / yic) - 1.0)
            vol = float(chunk.std() * np.sqrt(ANNUAL_TRADING_DAYS))
            sharpe = ann_ret / vol if vol > 0 else 0.0
            dd = float((chunk_nav / chunk_nav.cummax() - 1.0).min())
            spy_chunk = spy_ret_full.reindex(chunk.index).fillna(0.0)
            spy_nav = (1.0 + spy_chunk).cumprod()
            spy_ann = float(spy_nav.iloc[-1] ** (1.0 / yic) - 1.0)
            folds.append({
                "year": int(yr),
                "ann_return": ann_ret,
                "sharpe": sharpe,
                "max_drawdown": dd,
                "alpha_vs_spy": ann_ret - spy_ann,
            })
        return folds

    print(f"\n[stretch] running BASELINE (5-slot ensemble)...")
    base_rets, base_costs, base_taxes, base_t = _run(None)
    print(f"[stretch] baseline done in {base_t:.1f}s ({len(base_rets)} OOS days)")

    print(f"\n[stretch] running TREATMENT (100% Stretch only)...")
    stretch_override = {stretch_slot: 1.0}
    stretch_rets, stretch_costs, stretch_taxes, stretch_t = _run(stretch_override)
    print(f"[stretch] treatment done in {stretch_t:.1f}s ({len(stretch_rets)} OOS days)")

    base_folds = _fold_metrics(base_rets)
    stretch_folds = _fold_metrics(stretch_rets)
    if len(base_folds) < 3 or len(stretch_folds) < 3:
        print("[stretch] insufficient folds; aborting.")
        return 1

    base_by = {r["year"]: r for r in base_folds}
    str_by = {r["year"]: r for r in stretch_folds}
    common_years = sorted(set(base_by.keys()) & set(str_by.keys()))

    # === Per-fold ===
    print("\n" + "=" * 88)
    print(f"PER-FOLD COMPARISON ({len(common_years)} years)")
    print("=" * 88)
    print(f"  {'Year':>5}   {'5-slot':>22}     {'Stretch-only':>22}     {'Δ':>15}")
    print(f"  {'':>5}   {'Sharpe / α / MaxDD':>22}     {'Sharpe / α / MaxDD':>22}     "
          f"{'ΔSharpe Δα':>15}")
    print(f"  {'-'*5}   {'-'*22}     {'-'*22}     {'-'*15}")
    for yr in common_years:
        b = base_by[yr]
        s = str_by[yr]
        d_sh = s["sharpe"] - b["sharpe"]
        d_al = s["alpha_vs_spy"] - b["alpha_vs_spy"]
        print(f"  {yr:>5}   "
              f"{b['sharpe']:>+5.2f} / {b['alpha_vs_spy']*100:>+6.1f}% / {b['max_drawdown']*100:>+5.1f}%     "
              f"{s['sharpe']:>+5.2f} / {s['alpha_vs_spy']*100:>+6.1f}% / {s['max_drawdown']*100:>+5.1f}%     "
              f"{d_sh:>+5.2f}   {d_al*100:>+6.1f}%")

    # === Aggregate ===
    base_sh = np.array([base_by[y]["sharpe"] for y in common_years])
    str_sh = np.array([str_by[y]["sharpe"] for y in common_years])
    base_al = np.array([base_by[y]["alpha_vs_spy"] for y in common_years])
    str_al = np.array([str_by[y]["alpha_vs_spy"] for y in common_years])
    base_dd = np.array([base_by[y]["max_drawdown"] for y in common_years])
    str_dd = np.array([str_by[y]["max_drawdown"] for y in common_years])
    base_rt = np.array([base_by[y]["ann_return"] for y in common_years])
    str_rt = np.array([str_by[y]["ann_return"] for y in common_years])

    years = len(stretch_rets) / ANNUAL_TRADING_DAYS
    base_cgt = float(base_taxes.sum() / years * 10_000) if not base_taxes.empty else 0.0
    str_cgt = float(stretch_taxes.sum() / years * 10_000) if not stretch_taxes.empty else 0.0
    base_brk = float(base_costs.sum() / years * 10_000) if not base_costs.empty else 0.0
    str_brk = float(stretch_costs.sum() / years * 10_000) if not stretch_costs.empty else 0.0

    print("\n" + "=" * 88)
    print("AGGREGATE (mean ± std across folds)")
    print("=" * 88)
    print(f"  Sharpe:        5-slot {base_sh.mean():+.2f} ± {base_sh.std(ddof=1):.2f}   "
          f"Stretch {str_sh.mean():+.2f} ± {str_sh.std(ddof=1):.2f}   "
          f"Δ {str_sh.mean() - base_sh.mean():+.2f}")
    print(f"  Ann return:    5-slot {base_rt.mean()*100:+.2f}%   "
          f"Stretch {str_rt.mean()*100:+.2f}%   "
          f"Δ {(str_rt.mean() - base_rt.mean())*100:+.2f}%")
    print(f"  α vs SPY:      5-slot {base_al.mean()*100:+.2f}% ± {base_al.std(ddof=1)*100:.2f}%   "
          f"Stretch {str_al.mean()*100:+.2f}% ± {str_al.std(ddof=1)*100:.2f}%   "
          f"Δ {(str_al.mean() - base_al.mean())*100:+.2f}%")
    print(f"  MaxDD:         5-slot {base_dd.mean()*100:+.2f}% (worst {base_dd.min()*100:+.2f}%)   "
          f"Stretch {str_dd.mean()*100:+.2f}% (worst {str_dd.min()*100:+.2f}%)   "
          f"Δ {(str_dd.mean() - base_dd.mean())*100:+.2f}%")
    print(f"  Brokerage:     5-slot {base_brk:.0f} bps/yr   Stretch {str_brk:.0f} bps/yr")
    print(f"  CGT:           5-slot {base_cgt:.0f} bps/yr   Stretch {str_cgt:.0f} bps/yr   "
          f"Δ {str_cgt - base_cgt:+.0f}")

    # === Verdict (honest 4-dimension check via central helper) ===
    print()
    _verdict = _evaluate_sweep_result(
        baseline={
            "sharpe": float(base_sh.mean()),
            "max_drawdown": float(base_dd.min()),
            "alpha_vs_spy": float(base_al.mean()),
            "ann_return": float(base_rt.mean()),
        },
        treatment={
            "sharpe": float(str_sh.mean()),
            "max_drawdown": float(str_dd.min()),
            "alpha_vs_spy": float(str_al.mean()),
            "ann_return": float(str_rt.mean()),
        },
        label_baseline="5-slot blend",
        label_treatment="Stretch-only",
    )
    _print_sweep_verdict(_verdict)

    # Save JSON
    try:
        summary = {
            "run_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            "5_slot": {
                "folds": base_folds,
                "mean_sharpe": round(float(base_sh.mean()), 3),
                "mean_alpha": round(float(base_al.mean()), 6),
                "mean_max_drawdown": round(float(base_dd.mean()), 6),
                "cgt_bps_per_year": round(base_cgt, 1),
            },
            "stretch_only": {
                "folds": stretch_folds,
                "mean_sharpe": round(float(str_sh.mean()), 3),
                "mean_alpha": round(float(str_al.mean()), 6),
                "mean_max_drawdown": round(float(str_dd.mean()), 6),
                "cgt_bps_per_year": round(str_cgt, 1),
            },
            "delta": {
                "sharpe": round(float(d_sharpe), 3),
                "alpha": round(float(d_alpha), 6),
                "max_drawdown": round(float(d_dd), 6),
            },
        }
        json_path = APP_DIR / "stretch_only_test_summary.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"\n[stretch] summary JSON → {json_path}")
    except Exception as e:
        print(f"[stretch] summary JSON save failed: {e}")

    print("\n" + "=" * 88)
    print("STRETCH-ONLY TEST COMPLETE")
    print("=" * 88)
    return 0


def _run_stretch_hedge_sweep() -> int:
    print("\n" + "=" * 88)
    print("STRETCH + CRASH HEDGE SYNTHESIS SWEEP")
    print("=" * 88)
    SWEEP_RELEASES = [-0.03, -0.05, -0.08, -0.10, -0.12]
    TRIGGER_FIXED = CRASH_HEDGE_DD_TRIGGER  # -0.15
    TRAIN_MONTHS = 24
    MIN_OOS_YEAR = 2016
    MIN_FOLD_DAYS = 150
    print(f"  base = Stretch-only (slot_weights_override)")
    print(f"  hedge trigger fixed at {TRIGGER_FIXED*100:+.0f}% DD; "
          f"sweeping release ∈ {[f'{r*100:+.0f}%' for r in SWEEP_RELEASES]}")
    print(f"  basket: {CRASH_HEDGE_BASKET}")

    stretch_slot = None
    for name in ENSEMBLE_SLOT_NAMES:
        if "Stretch" in name:
            stretch_slot = name
            break
    if not stretch_slot:
        print("[stretch-hedge] no Stretch slot found; aborting.")
        return 1
    stretch_override = {stretch_slot: 1.0}

    _sh_tickers = [c for c in prices.columns if c != "PortfolioValue"]
    for tkr in CRASH_HEDGE_BASKET.keys():
        if tkr not in _sh_tickers:
            _sh_tickers.append(tkr)
    print(f"[stretch-hedge] universe: {len(_sh_tickers)} tickers, "
          f"OOS folds: year ≥ {MIN_OOS_YEAR}")
    print(f"[stretch-hedge] downloading full history (max)...")

    t0 = time.perf_counter()
    raw = yf.download(_sh_tickers, period="max", interval="1d",
                      auto_adjust=True, threads=False, progress=False)
    px = _normalize_yfinance_close(raw)
    px.index = pd.to_datetime(px.index).tz_localize(None)
    px = px.sort_index().ffill().bfill()
    fx_raw = yf.download("USDAUD=X", period="max", interval="1d",
                         auto_adjust=True, threads=False, progress=False)
    fx = fx_raw["Close"] if isinstance(fx_raw, pd.DataFrame) else fx_raw
    if isinstance(fx, pd.DataFrame):
        fx = fx.iloc[:, 0]
    fx = pd.to_numeric(fx, errors="coerce").reindex(px.index).ffill().bfill()
    usd_cols = [c for c in px.columns
                if not str(c).endswith(".AX") and not str(c).startswith("^")]
    px_aud = px.copy()
    if usd_cols:
        px_aud.update(px.loc[:, usd_cols].mul(fx, axis=0))
    px_aud = px_aud.ffill().bfill().dropna(how="all")
    print(f"[stretch-hedge] data ready ({px_aud.shape[0]} days × {px_aud.shape[1]} tickers) "
          f"in {time.perf_counter()-t0:.1f}s")

    spy_ret_full = (px_aud["SPY"].pct_change().dropna()
                    if "SPY" in px_aud.columns else pd.Series(dtype=float))

    def _run(stretch_only: bool, hedge: bool, release: float | None):
        t1 = time.perf_counter()
        out = run_oos_ensemble_walk_forward(
            px_aud,
            train_window_months=TRAIN_MONTHS,
            rebalance=REBALANCE_FREQ,
            benchmark_ticker="SPY",
            score_lookback_days=252,
            lambda_temp=3.0,
            starting_nav_aud=1_000_000.0,
            crash_hedge=hedge,
            crash_hedge_dd_release=release,
            slot_weights_override=(stretch_override if stretch_only else None),
        )
        elapsed = time.perf_counter() - t1
        strat_rets = out["blended_returns"]
        tax_ser = out.get("rebalance_taxes", pd.Series(dtype=float))
        info = {
            "elapsed_sec": elapsed,
            "n_triggers": int(out.get("hedge_n_triggers", 0)),
            "n_active_rebals": int(out.get("hedge_active_rebals", 0)),
        }
        if not tax_ser.empty and not strat_rets.empty:
            years = max(len(strat_rets) / ANNUAL_TRADING_DAYS, 1e-6)
            info["cgt_bps_per_year"] = float(tax_ser.sum() / years * 10_000)
        else:
            info["cgt_bps_per_year"] = 0.0
        return strat_rets, info

    def _fold_metrics(rets: pd.Series) -> list[dict]:
        if rets.empty:
            return []
        folds = []
        for yr in sorted(set(rets.index.year)):
            if yr < MIN_OOS_YEAR:
                continue
            mask = (rets.index.year == yr)
            chunk = rets.loc[mask]
            if len(chunk) < MIN_FOLD_DAYS:
                continue
            chunk_nav = (1.0 + chunk).cumprod()
            yic = len(chunk) / ANNUAL_TRADING_DAYS
            ann_ret = float(chunk_nav.iloc[-1] ** (1.0 / yic) - 1.0)
            vol = float(chunk.std() * np.sqrt(ANNUAL_TRADING_DAYS))
            sharpe = ann_ret / vol if vol > 0 else 0.0
            dd = float((chunk_nav / chunk_nav.cummax() - 1.0).min())
            spy_chunk = spy_ret_full.reindex(chunk.index).fillna(0.0)
            spy_nav = (1.0 + spy_chunk).cumprod()
            spy_ann = float(spy_nav.iloc[-1] ** (1.0 / yic) - 1.0)
            folds.append({
                "year": int(yr),
                "ann_return": ann_ret,
                "sharpe": sharpe,
                "max_drawdown": dd,
                "alpha_vs_spy": ann_ret - spy_ann,
            })
        return folds

    def _aggregate(folds: list[dict]) -> dict:
        sh = np.array([r["sharpe"] for r in folds])
        al = np.array([r["alpha_vs_spy"] for r in folds])
        dd = np.array([r["max_drawdown"] for r in folds])
        rt = np.array([r["ann_return"] for r in folds])
        return {
            "mean_sharpe": float(sh.mean()),
            "std_sharpe": float(sh.std(ddof=1)),
            "mean_alpha": float(al.mean()),
            "mean_ann_return": float(rt.mean()),
            "mean_max_drawdown": float(dd.mean()),
            "worst_max_drawdown": float(dd.min()),
        }

    # === Baselines ===
    print(f"\n[stretch-hedge] running baseline #1: 5-slot blend (no hedge)...")
    bl5_rets, bl5_info = _run(stretch_only=False, hedge=False, release=None)
    bl5_folds = _fold_metrics(bl5_rets)
    bl5_agg = _aggregate(bl5_folds) if bl5_folds else {}
    print(f"[stretch-hedge] 5-slot done in {bl5_info['elapsed_sec']:.1f}s")

    print(f"\n[stretch-hedge] running baseline #2: Stretch-only (no hedge)...")
    bls_rets, bls_info = _run(stretch_only=True, hedge=False, release=None)
    bls_folds = _fold_metrics(bls_rets)
    bls_agg = _aggregate(bls_folds) if bls_folds else {}
    print(f"[stretch-hedge] Stretch-only done in {bls_info['elapsed_sec']:.1f}s")

    # === Stretch + hedge sweep ===
    treatments: list[dict] = []
    for rel in SWEEP_RELEASES:
        print(f"\n[stretch-hedge] running Stretch + hedge release={rel*100:+.0f}%...")
        rets, info = _run(stretch_only=True, hedge=True, release=rel)
        folds = _fold_metrics(rets)
        if not folds:
            continue
        agg = _aggregate(folds)
        treatments.append({
            "release": rel,
            "folds": folds,
            "agg": agg,
            "info": info,
        })
        print(f"[stretch-hedge] release={rel*100:+.0f}%: "
              f"Sharpe {agg['mean_sharpe']:+.2f}±{agg['std_sharpe']:.2f}  "
              f"α {agg['mean_alpha']*100:+.2f}%  "
              f"MaxDD {agg['mean_max_drawdown']*100:+.2f}%  "
              f"triggers={info['n_triggers']}  "
              f"({info['elapsed_sec']:.1f}s)")

    # === Aggregate table ===
    print("\n" + "=" * 88)
    print("AGGREGATE RESULTS — all configs vs both baselines")
    print(f"({len(bls_folds)} modern folds, 2016-2025)")
    print("=" * 88)
    print(f"  {'Config':>26}  {'Sharpe':>14}  {'α vs SPY':>12}  {'Ann Ret':>9}  "
          f"{'MaxDD':>9}  {'Trigs':>6}")
    print(f"  {'-'*26}  {'-'*14}  {'-'*12}  {'-'*9}  {'-'*9}  {'-'*6}")
    _bl5_sh_cell = f"{bl5_agg['mean_sharpe']:+.2f}±{bl5_agg['std_sharpe']:.2f}"
    _bl5_al_cell = f"{bl5_agg['mean_alpha']*100:+.2f}%"
    _bl5_rt_cell = f"{bl5_agg['mean_ann_return']*100:+.2f}%"
    _bl5_dd_cell = f"{bl5_agg['mean_max_drawdown']*100:+.2f}%"
    print(f"  {'5-slot blend (current)':>26}  {_bl5_sh_cell:>14}  {_bl5_al_cell:>12}  "
          f"{_bl5_rt_cell:>9}  {_bl5_dd_cell:>9}  {'-':>6}")
    # Print Stretch-only baseline
    _bls_sh = f"{bls_agg['mean_sharpe']:+.2f}±{bls_agg['std_sharpe']:.2f}"
    _bls_al = f"{bls_agg['mean_alpha']*100:+.2f}%"
    _bls_rt = f"{bls_agg['mean_ann_return']*100:+.2f}%"
    _bls_dd = f"{bls_agg['mean_max_drawdown']*100:+.2f}%"
    print(f"  {'Stretch-only (no hedge)':>26}  {_bls_sh:>14}  {_bls_al:>12}  "
          f"{_bls_rt:>9}  {_bls_dd:>9}  {'-':>6}")
    # Print each treatment
    for tr in treatments:
        rel = tr["release"]
        a = tr["agg"]
        label = f"Stretch + hedge rel={rel*100:+.0f}%"
        sh_cell = f"{a['mean_sharpe']:+.2f}±{a['std_sharpe']:.2f}"
        al_cell = f"{a['mean_alpha']*100:+.2f}%"
        rt_cell = f"{a['mean_ann_return']*100:+.2f}%"
        dd_cell = f"{a['mean_max_drawdown']*100:+.2f}%"
        print(f"  {label:>26}  {sh_cell:>14}  {al_cell:>12}  "
              f"{rt_cell:>9}  {dd_cell:>9}  {tr['info']['n_triggers']:>6}")

    # === Winner pick ===
    if treatments:
        winner = max(treatments, key=lambda t: (round(t["agg"]["mean_sharpe"], 3),
                                                t["agg"]["mean_alpha"]))
        print(f"\n[stretch-hedge] BEST hedge config: release={winner['release']*100:+.0f}%  "
              f"Sharpe {winner['agg']['mean_sharpe']:+.2f}")
        # Compare to both baselines
        d_vs_5slot = winner["agg"]["mean_sharpe"] - bl5_agg.get("mean_sharpe", 0.0)
        d_vs_stretch = winner["agg"]["mean_sharpe"] - bls_agg.get("mean_sharpe", 0.0)
        print(f"  vs 5-slot blend:    ΔSharpe {d_vs_5slot:+.2f}")
        print(f"  vs Stretch-only:    ΔSharpe {d_vs_stretch:+.2f}")

    # === Verdict (honest 4-dim check: best hedge config vs Stretch-only) ===
    print()
    if treatments and winner:
        _verdict = _evaluate_sweep_result(
            baseline={
                "sharpe": float(bls_agg.get("mean_sharpe", 0.0)),
                "max_drawdown": float(bls_agg.get("mean_max_drawdown", 0.0)),
                "alpha_vs_spy": float(bls_agg.get("mean_alpha", 0.0)),
                "ann_return": float(bls_agg.get("mean_ann_return", 0.0)),
            },
            treatment={
                "sharpe": float(winner["agg"]["mean_sharpe"]),
                "max_drawdown": float(winner["agg"]["mean_max_drawdown"]),
                "alpha_vs_spy": float(winner["agg"]["mean_alpha"]),
                "ann_return": float(winner["agg"]["mean_ann_return"]),
            },
            label_baseline="Stretch-only (no hedge)",
            label_treatment=f"Stretch + hedge release={winner['release']*100:+.0f}%",
        )
        _print_sweep_verdict(_verdict)
        print(f"\n  Note: ALL configs in this window have no GFC-class crash to test "
              f"tail protection. Stress test (--stress-test) remains the only "
              f"true-tail evidence we have.")

    # Save JSON
    try:
        summary = {
            "run_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            "trigger_dd": TRIGGER_FIXED,
            "basket": CRASH_HEDGE_BASKET,
            "baselines": {
                "5_slot_no_hedge": {"folds": bl5_folds, "agg": bl5_agg},
                "stretch_only_no_hedge": {"folds": bls_folds, "agg": bls_agg},
            },
            "treatments": [
                {"release": tr["release"], "agg": tr["agg"], "info": tr["info"],
                 "folds": tr["folds"]}
                for tr in treatments
            ],
            "winner_release": (winner["release"] if treatments else None),
        }
        json_path = APP_DIR / "stretch_hedge_sweep_summary.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"\n[stretch-hedge] summary JSON → {json_path}")
    except Exception as e:
        print(f"[stretch-hedge] summary JSON save failed: {e}")

    print("\n" + "=" * 88)
    print("STRETCH + HEDGE SWEEP COMPLETE")
    print("=" * 88)
    return 0


def _run_tilted_ensemble_test() -> int:
    print("\n" + "=" * 88)
    print("AUTO-TILTED ENSEMBLE A/B TEST — walk-forward CV (tilts ON vs OFF)")
    print("=" * 88)
    print(f"  tilt lookback:   {FACTOR_TILT_LOOKBACK_DAYS} days (3M)")
    print(f"  magnitude scale: Sharpe × {FACTOR_TILT_SHARPE_TO_MAG:.2f}, "
          f"clipped to ±{FACTOR_TILT_MAX_MAGNITUDE:.2f}")
    print(f"  tilt band:       ±0.10 (soft slack in solver)")
    print(f"  region:          US (factor data)")

    TRAIN_MONTHS = 24
    MIN_OOS_YEAR = 2016
    MIN_FOLD_DAYS = 150

    _te_tickers = [c for c in prices.columns if c != "PortfolioValue"]
    print(f"[tilted-ens] universe: {len(_te_tickers)} tickers, "
          f"OOS folds: year ≥ {MIN_OOS_YEAR}")
    print(f"[tilted-ens] downloading full history (max)...")

    t0 = time.perf_counter()
    raw = yf.download(_te_tickers, period="max", interval="1d",
                      auto_adjust=True, threads=False, progress=False)
    px = _normalize_yfinance_close(raw)
    px.index = pd.to_datetime(px.index).tz_localize(None)
    px = px.sort_index().ffill().bfill()
    fx_raw = yf.download("USDAUD=X", period="max", interval="1d",
                         auto_adjust=True, threads=False, progress=False)
    fx = fx_raw["Close"] if isinstance(fx_raw, pd.DataFrame) else fx_raw
    if isinstance(fx, pd.DataFrame):
        fx = fx.iloc[:, 0]
    fx = pd.to_numeric(fx, errors="coerce").reindex(px.index).ffill().bfill()
    usd_cols = [c for c in px.columns
                if not str(c).endswith(".AX") and not str(c).startswith("^")]
    px_aud = px.copy()
    if usd_cols:
        px_aud.update(px.loc[:, usd_cols].mul(fx, axis=0))
    px_aud = px_aud.ffill().bfill().dropna(how="all")
    print(f"[tilted-ens] data ready ({px_aud.shape[0]} days × "
          f"{px_aud.shape[1]} tickers) in {time.perf_counter()-t0:.1f}s")

    # FF5+MOM factor data (US region — most relevant given universe).
    print(f"[tilted-ens] loading FF5+MOM factor data...")
    try:
        ff = _apply_data_lockbox(get_ff5_mom_daily(region="US"))
        ff.index = pd.to_datetime(ff.index).tz_localize(None)
    except Exception as e:
        print(f"[tilted-ens] failed to load FF5+MOM data: {e}")
        return 1

    # B matrix: per-asset factor loadings via the existing Dimson-corrected
    # OLS routine. Computed ONCE on the full universe's history (factor
    # loadings are slow-moving so per-rebal rebuilding adds compute for
    # little signal change).
    print(f"[tilted-ens] computing factor betas (Dimson OLS, may take ~30s)...")
    _ret_wide = px_aud.pct_change().dropna(how="all")
    _bt0 = time.perf_counter()
    try:
        B, _alpha, _resvar = compute_ff5_betas(
            df_returns_wide=_ret_wide,
            ff5_returns=ff,
            min_obs=120,
            n_lags=1,
        )
    except Exception as e:
        print(f"[tilted-ens] factor beta computation failed: {e}")
        return 1
    if B is None or B.empty:
        print(f"[tilted-ens] beta matrix empty; aborting.")
        return 1
    B = B.dropna(how="all")  # drop assets that couldn't be regressed
    print(f"[tilted-ens] B matrix ready ({B.shape[0]} assets × {B.shape[1]} factors) "
          f"in {time.perf_counter()-_bt0:.1f}s")

    spy_ret_full = (px_aud["SPY"].pct_change().dropna()
                    if "SPY" in px_aud.columns else pd.Series(dtype=float))

    def _run(tilts_on: bool):
        t1 = time.perf_counter()
        out = run_oos_ensemble_walk_forward(
            px_aud,
            train_window_months=TRAIN_MONTHS,
            rebalance=REBALANCE_FREQ,
            benchmark_ticker="SPY",
            score_lookback_days=252,
            lambda_temp=3.0,
            starting_nav_aud=1_000_000.0,
            auto_factor_tilts=tilts_on,
            ff_factors=ff if tilts_on else None,
            factor_betas=B if tilts_on else None,
            factor_tilt_lookback_days=FACTOR_TILT_LOOKBACK_DAYS,
            factor_tilt_band=0.10,
        )
        return out, time.perf_counter() - t1

    def _fold_metrics(rets):
        if rets is None or rets.empty:
            return [], None
        folds = []
        for yr in sorted(set(rets.index.year)):
            if yr < MIN_OOS_YEAR:
                continue
            mask = (rets.index.year == yr)
            chunk = rets.loc[mask]
            if len(chunk) < MIN_FOLD_DAYS:
                continue
            chunk_nav = (1.0 + chunk).cumprod()
            yic = len(chunk) / ANNUAL_TRADING_DAYS
            ann_ret = float(chunk_nav.iloc[-1] ** (1.0 / yic) - 1.0)
            vol = float(chunk.std() * np.sqrt(ANNUAL_TRADING_DAYS))
            sharpe = ann_ret / vol if vol > 0 else 0.0
            dd = float((chunk_nav / chunk_nav.cummax() - 1.0).min())
            spy_chunk = spy_ret_full.reindex(chunk.index).fillna(0.0)
            spy_nav = (1.0 + spy_chunk).cumprod()
            spy_ann = float(spy_nav.iloc[-1] ** (1.0 / yic) - 1.0)
            folds.append({
                "year": int(yr),
                "ann_return": ann_ret,
                "sharpe": sharpe,
                "max_drawdown": dd,
                "alpha_vs_spy": ann_ret - spy_ann,
            })
        # FULL-PERIOD peak-to-trough (the metric that matters per 2026-06-19)
        modern_mask = rets.index >= pd.Timestamp(f"{MIN_OOS_YEAR}-01-01")
        modern_rets = rets.loc[modern_mask]
        full_dd = None
        if not modern_rets.empty:
            nav = (1.0 + modern_rets).cumprod()
            full_dd = float((nav / nav.cummax() - 1.0).min())
        return folds, full_dd

    # === Baseline: tilts OFF ===
    print(f"\n[tilted-ens] running BASELINE (tilts OFF)...")
    base_out, base_t = _run(tilts_on=False)
    base_folds, base_full_dd = _fold_metrics(base_out.get("blended_returns"))
    print(f"[tilted-ens] baseline done in {base_t:.1f}s")

    # === Treatment: tilts ON ===
    print(f"\n[tilted-ens] running TREATMENT (auto factor tilts ON)...")
    tilt_out, tilt_t = _run(tilts_on=True)
    tilt_folds, tilt_full_dd = _fold_metrics(tilt_out.get("blended_returns"))
    print(f"[tilted-ens] treatment done in {tilt_t:.1f}s")

    if len(base_folds) < 3 or len(tilt_folds) < 3:
        print("[tilted-ens] insufficient folds; aborting.")
        return 1

    base_by = {r["year"]: r for r in base_folds}
    tilt_by = {r["year"]: r for r in tilt_folds}
    common_years = sorted(set(base_by.keys()) & set(tilt_by.keys()))

    # === Per-fold table ===
    print("\n" + "=" * 88)
    print(f"PER-FOLD COMPARISON ({len(common_years)} years)")
    print("=" * 88)
    print(f"  {'Year':>5}   {'Baseline (no tilts)':>22}     {'Auto tilts ON':>22}     {'Δ':>15}")
    print(f"  {'':>5}   {'Sharpe / α / MaxDD':>22}     {'Sharpe / α / MaxDD':>22}     "
          f"{'ΔSharpe Δα':>15}")
    print(f"  {'-'*5}   {'-'*22}     {'-'*22}     {'-'*15}")
    for yr in common_years:
        b = base_by[yr]
        s = tilt_by[yr]
        d_sh = s["sharpe"] - b["sharpe"]
        d_al = s["alpha_vs_spy"] - b["alpha_vs_spy"]
        print(f"  {yr:>5}   "
              f"{b['sharpe']:>+5.2f} / {b['alpha_vs_spy']*100:>+6.1f}% / {b['max_drawdown']*100:>+5.1f}%     "
              f"{s['sharpe']:>+5.2f} / {s['alpha_vs_spy']*100:>+6.1f}% / {s['max_drawdown']*100:>+5.1f}%     "
              f"{d_sh:>+5.2f}   {d_al*100:>+6.1f}%")

    # === Aggregate (with FULL-PERIOD MaxDD) ===
    base_sh = np.array([base_by[y]["sharpe"] for y in common_years])
    tilt_sh = np.array([tilt_by[y]["sharpe"] for y in common_years])
    base_al = np.array([base_by[y]["alpha_vs_spy"] for y in common_years])
    tilt_al = np.array([tilt_by[y]["alpha_vs_spy"] for y in common_years])
    base_rt = np.array([base_by[y]["ann_return"] for y in common_years])
    tilt_rt = np.array([tilt_by[y]["ann_return"] for y in common_years])
    base_yr_dd = np.array([base_by[y]["max_drawdown"] for y in common_years])
    tilt_yr_dd = np.array([tilt_by[y]["max_drawdown"] for y in common_years])

    print("\n" + "=" * 88)
    print("AGGREGATE — mean across folds + FULL-PERIOD peak-to-trough MaxDD")
    print("=" * 88)
    print(f"  Sharpe (mean):           baseline {base_sh.mean():+.2f}±{base_sh.std(ddof=1):.2f}   "
          f"tilts {tilt_sh.mean():+.2f}±{tilt_sh.std(ddof=1):.2f}   "
          f"Δ {tilt_sh.mean() - base_sh.mean():+.2f}")
    print(f"  Ann return (mean):       baseline {base_rt.mean()*100:+.2f}%   "
          f"tilts {tilt_rt.mean()*100:+.2f}%   "
          f"Δ {(tilt_rt.mean() - base_rt.mean())*100:+.2f}%")
    print(f"  α vs SPY (mean):         baseline {base_al.mean()*100:+.2f}%   "
          f"tilts {tilt_al.mean()*100:+.2f}%   "
          f"Δ {(tilt_al.mean() - base_al.mean())*100:+.2f}%")
    print(f"  Worst fold MaxDD:        baseline {base_yr_dd.min()*100:+.2f}%   "
          f"tilts {tilt_yr_dd.min()*100:+.2f}%   "
          f"Δ {(tilt_yr_dd.min() - base_yr_dd.min())*100:+.2f}%")
    print(f"  FULL-PERIOD MaxDD:       baseline {base_full_dd*100:+.2f}%   "
          f"tilts {tilt_full_dd*100:+.2f}%   "
          f"Δ {(tilt_full_dd - base_full_dd)*100:+.2f}%   ← key metric")

    # === Verdict (honest 4-dimension check via central helper) ===
    print()
    _verdict = _evaluate_sweep_result(
        baseline={
            "sharpe": float(base_sh.mean()),
            "max_drawdown": float(base_full_dd),
            "alpha_vs_spy": float(base_al.mean()),
            "ann_return": float(base_rt.mean()),
        },
        treatment={
            "sharpe": float(tilt_sh.mean()),
            "max_drawdown": float(tilt_full_dd),
            "alpha_vs_spy": float(tilt_al.mean()),
            "ann_return": float(tilt_rt.mean()),
        },
        label_baseline="no tilts",
        label_treatment="auto tilts ON",
    )
    _print_sweep_verdict(_verdict)
    if _verdict["verdict"] == "SHIP":
        print(f"\n  Next: live-pipeline sanity run, then set "
              f"PRODUCTION_AUTO_FACTOR_TILTS = True if metrics_history confirms.")
    elif _verdict["verdict"] == "REVERT":
        print(f"\n  Next: KEEP PRODUCTION_AUTO_FACTOR_TILTS = False. Engine ceiling holds.")
    print()

    # Save JSON
    try:
        summary = {
            "run_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            "config": {
                "lookback_days": FACTOR_TILT_LOOKBACK_DAYS,
                "sharpe_to_mag": FACTOR_TILT_SHARPE_TO_MAG,
                "max_magnitude": FACTOR_TILT_MAX_MAGNITUDE,
                "tilt_band": 0.10,
            },
            "baseline": {
                "folds": base_folds,
                "mean_sharpe": float(base_sh.mean()),
                "mean_alpha": float(base_al.mean()),
                "full_period_max_drawdown": float(base_full_dd),
            },
            "tilted": {
                "folds": tilt_folds,
                "mean_sharpe": float(tilt_sh.mean()),
                "mean_alpha": float(tilt_al.mean()),
                "full_period_max_drawdown": float(tilt_full_dd),
            },
            "uplift": {
                "sharpe_delta_mean": float(d_sh_mean),
                "full_period_max_drawdown_delta": float(d_full_dd),
            },
        }
        json_path = APP_DIR / "tilted_ensemble_test_summary.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"[tilted-ens] summary JSON → {json_path}")
    except Exception as e:
        print(f"[tilted-ens] summary JSON save failed: {e}")

    print("\n" + "=" * 88)
    print("TILTED ENSEMBLE TEST COMPLETE")
    print("=" * 88)
    return 0
