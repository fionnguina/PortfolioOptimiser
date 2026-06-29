"""JSONL log writers + readers for the engine's persistence layer.

Three append-only JSONL streams + their loaders, extracted from
Portfolio_Optimiser.py for testability + module-split prep:

  trade_recommendation_log.jsonl   what the engine recommended each run
  live_nav_history.jsonl           one row per run (date, NAV in AUD)
  cash_ledger.jsonl                one row per run with brokerage/CGT snapshot

All functions are pure I/O: they take paths + structured arguments and
return data or None. No module-level globals or engine state are read.
This makes them safe to call from any context (live run, paper exec,
post-hoc reconciliation tools).

Schema notes:
  trade_recommendation_log: see ARCHITECTURE.md §5.v1 for the field list.
                            tlh_swaps array added 2026-06-22 for live TLH.
  live_nav_history:         {date: "YYYY-MM-DD", nav_aud: float}.
                            Idempotent within a day — re-append on same
                            date overwrites the prior entry.
  cash_ledger:              {date, run_at (ISO timestamp), portfolio_value_aud,
                            net_invested_aud, cash_balance_aud,
                            brokerage_this_run_aud, cgt_this_run_aud,
                            loss_carry_forward_tax_aud, selected_mode, broker}.
                            Pure append; dedup only on exact run_at match.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


def append_trade_recommendation_log(
    log_path,
    *,
    selected_mode: str,
    trade_df: pd.DataFrame,
    w_target: pd.Series,
    current_units: pd.Series,
    portfolio_value_aud: float,
    regime_mix: pd.Series,
    expected_brokerage_aud: float,
    expected_cgt_aud: float,
    broker_name: str,
    cgt_mtr: float,
    universe_size: int,
    tlh_events: list[dict] | None = None,
) -> None:
    """Append one JSONL entry recording the engine's current recommendation.

    Foundation for the live vs backtest drift tracker (Tier-1 #3). Each run
    appends one line. Once trading starts, a separate sheet of actual fills
    will be joined against this log to compute slippage + adherence.

    tlh_events (optional): list of TLH swap dicts from _run_tlh_pass on the
    live lot book. When non-empty, the rebalance delta in recommended_trades
    has already been computed against POST-TLH units (so executing all
    recommended_trades realises the harvest swap implicitly). tlh_events
    are also recorded as their own array so downstream consumers can render
    a TLH-specific view (PPT slide 3 footer, IBKR exec annotation).
    """
    entry = {
        "run_at": pd.Timestamp.now().isoformat(timespec="seconds"),
        "selected_mode": str(selected_mode),
        "broker": str(broker_name),
        "cgt_mtr": float(cgt_mtr),
        "portfolio_value_aud": float(portfolio_value_aud),
        "universe_size": int(universe_size),
        "regime_mix": {
            str(k): float(v) for k, v in regime_mix.items()
        } if regime_mix is not None and not regime_mix.empty else {},
        "target_weights": {
            str(k): round(float(v), 6)
            for k, v in w_target.items() if abs(float(v)) > 1e-6
        },
        "current_units": {
            str(k): int(v)
            for k, v in current_units.items() if int(v) != 0
        },
        "expected_brokerage_aud": round(float(expected_brokerage_aud), 2),
        "expected_cgt_aud": round(float(expected_cgt_aud), 2),
        "recommended_trades": [],
        "tlh_swaps": [
            {
                "ticker_sold":          str(ev.get("ticker_sold", "")),
                "ticker_bought":        str(ev.get("ticker_bought", "")),
                "units_sold":           int(round(float(ev.get("units_sold", 0)))),
                "units_bought":         int(round(float(ev.get("units_bought", 0)))),
                "sale_price":           round(float(ev.get("sale_price", 0)), 4),
                "buy_price":            round(float(ev.get("buy_price", 0)), 4),
                "loss_aud":             round(float(ev.get("loss_aud", 0)), 2),
                "swap_value_aud":       round(float(ev.get("swap_value_aud", 0)), 2),
                "hold_days":            int(ev.get("hold_days", 0)),
                "lot_date":             str(ev.get("lot_date", "")),
            }
            for ev in (tlh_events or [])
        ],
    }
    delta_col = "Delta Units" if "Delta Units" in trade_df.columns else None
    if delta_col is not None:
        for sec, row in trade_df.iterrows():
            try:
                delta = int(pd.to_numeric(row.get(delta_col, 0), errors="coerce"))
            except Exception:
                continue
            if delta == 0:
                continue
            px_aud = float(pd.to_numeric(row.get("Last Px (AUD)", 0), errors="coerce") or 0)
            broke = float(pd.to_numeric(row.get("Brokerage (AUD)", 0), errors="coerce") or 0)
            ticker = str(row.get("Security", sec)) if "Security" in trade_df.columns else str(sec)
            entry["recommended_trades"].append({
                "ticker": ticker,
                "side": "buy" if delta > 0 else "sell",
                "delta_units": delta,
                "px_aud": round(px_aud, 4),
                "delta_value_aud": round(delta * px_aud, 2),
                "brokerage_aud": round(broke, 2),
            })
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
        print(f"[drift] logged recommendation → {Path(log_path).name} "
              f"({len(entry['recommended_trades'])} trades, "
              f"NAV AUD {portfolio_value_aud:,.0f}, mode={selected_mode})")
    except Exception as e:
        print(f"[drift] failed to write recommendation log: {e}")


def _load_recommendation_log(log_path) -> list[dict]:
    """Load the recommendation JSONL. Returns [] if missing/empty."""
    p = Path(log_path)
    if not p.exists():
        return []
    out: list[dict] = []
    try:
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"[drift] could not load recommendation log: {e}")
    return out


def append_live_nav_history(nav_path, nav_aud: float, as_of_date=None) -> None:
    """Append today's NAV to live_nav_history.jsonl. Idempotent within a day —
    if the most recent entry is already today's date, it's overwritten."""
    if not np.isfinite(nav_aud) or nav_aud <= 0:
        return
    p = Path(nav_path)
    date = pd.Timestamp(as_of_date or pd.Timestamp.today().normalize()).strftime("%Y-%m-%d")
    new_entry = {"date": date, "nav_aud": float(nav_aud)}
    try:
        existing: list[dict] = []
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        existing.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        existing = [e for e in existing if e.get("date") != date]
        existing.append(new_entry)
        existing.sort(key=lambda e: e.get("date", ""))
        with open(p, "w", encoding="utf-8") as f:
            for e in existing:
                f.write(json.dumps(e) + "\n")
    except Exception as e:
        print(f"[drift] could not append NAV history: {e}")


def _load_live_nav_series(nav_path) -> pd.Series:
    """Load live_nav_history.jsonl into a date-indexed Series."""
    p = Path(nav_path)
    if not p.exists():
        return pd.Series(dtype=float)
    rows: list[dict] = []
    try:
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except Exception:
        return pd.Series(dtype=float)
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows)
    if "date" not in df.columns or "nav_aud" not in df.columns:
        return pd.Series(dtype=float)
    s = pd.Series(
        pd.to_numeric(df["nav_aud"], errors="coerce").values,
        index=pd.to_datetime(df["date"], errors="coerce"),
    ).dropna().sort_index()
    s = s[~s.index.duplicated(keep="last")]
    return s


def append_cash_ledger(
    ledger_path,
    *,
    portfolio_value_aud: float,
    net_invested_aud: float,
    cash_balance_aud: float,
    brokerage_this_run_aud: float,
    cgt_this_run_aud: float,
    loss_cf_tax_aud: float,
    selected_mode: str,
    broker_name: str,
    as_of_date=None,
) -> None:
    """Append one snapshot to cash_ledger.jsonl. Pure append — every run is
    recorded as its own row so we can see what's happening across re-runs
    (price drift, brokerage cost, etc.). Dedup by exact run_at timestamp
    only, to guard against pathological double-invocations within the
    same second."""
    p = Path(ledger_path)
    iso_date = pd.Timestamp(as_of_date or pd.Timestamp.now()).strftime("%Y-%m-%d")
    run_at = pd.Timestamp.now().isoformat(timespec="seconds")
    entry = {
        "date": iso_date,
        "run_at": run_at,
        "portfolio_value_aud": round(float(portfolio_value_aud), 2),
        "net_invested_aud": round(float(net_invested_aud), 2),
        "cash_balance_aud": round(float(cash_balance_aud), 2),
        "brokerage_this_run_aud": round(float(brokerage_this_run_aud), 2),
        "cgt_this_run_aud": round(float(cgt_this_run_aud), 2),
        "loss_carry_forward_tax_aud": round(float(loss_cf_tax_aud), 2),
        "selected_mode": str(selected_mode),
        "broker": str(broker_name),
    }
    try:
        existing: list[dict] = []
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        existing.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        if not any(e.get("run_at") == run_at for e in existing):
            existing.append(entry)
        existing.sort(key=lambda e: e.get("run_at", e.get("date", "")))
        with open(p, "w", encoding="utf-8") as f:
            for e in existing:
                f.write(json.dumps(e) + "\n")
    except Exception as e:
        print(f"[cash] could not append cash ledger: {e}")
