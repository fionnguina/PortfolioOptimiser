"""Lot-book construction + expansion (module split #18, 2026-07-09).

Pure functions — no engine globals. Reconstruct the FIFO/HIFO lot book from
its authoritative sources and expand trades into per-lot CGT rows:
  _read_lots_from_path        Read a Lots sheet (Excel) into a normalised frame.
  _build_lots_from_fills_log  Rebuild lots from ibkr_fills_log.jsonl (broker truth).
  _build_lots_from_holdings   Seed lots from current Holdings units + last prices.
  expand_with_lots            Expand a trade frame into per-lot sale rows (FIFO/HIFO).

Engine re-exports these from its namespace for backward compat. The deprecated
_update_lots_after_trades stays in the engine (it reads the LOT_MATCH_METHOD global).
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


def _read_lots_from_path(xl_path, sheet="Lots") -> pd.DataFrame:
    """
    Lots sheet expected schema:
      Security | AcqDate | Units | CostBaseAUD
    """
    base_cols = ["Security", "AcqDate", "Units", "CostBaseAUD"]
    try:
        df = pd.read_excel(xl_path, sheet_name=sheet)
    except Exception:
        return pd.DataFrame(columns=base_cols)

    if df.empty:
        return pd.DataFrame(columns=base_cols)

    df = df.rename(columns={c: str(c).strip() for c in df.columns})

    # Handle mild schema variants.
    rename_map = {
        "Cost Base AUD": "CostBaseAUD",
        "CostBase": "CostBaseAUD",
        "AcquisitionDate": "AcqDate",
        "Qty": "Units",
    }
    for old, new in rename_map.items():
        if old in df.columns and new not in df.columns:
            df[new] = df[old]

    missing = [c for c in base_cols if c not in df.columns]
    if missing:
        return pd.DataFrame(columns=base_cols)

    df["AcqDate"] = pd.to_datetime(df["AcqDate"], errors="coerce")
    df["Units"] = pd.to_numeric(df["Units"], errors="coerce")
    df["CostBaseAUD"] = pd.to_numeric(df["CostBaseAUD"], errors="coerce")

    df = df.dropna(subset=base_cols)
    df["Security"] = df["Security"].astype(str).str.strip()
    df = df[df["Units"] > 0]
    return df[base_cols].copy()


def _build_lots_from_fills_log(
    fills_path,
    fx_map: "pd.Series | dict | None" = None,
    lot_match_method: str = "HIFO",
    seed_path = None,
) -> pd.DataFrame:
    """Reconstruct the lot book from `ibkr_fills_log.jsonl`.

    The fills log is the authoritative record of what transacted. Only
    rows with `qty_filled > 0` contribute. BUYs add a new lot at
    `avg_fill_price_local` converted to AUD; SELLs decrement matched
    lots using HIFO/FIFO so the resulting book reflects the live
    position basis at any point in time.

    Optional `seed_path` — a JSON list of lot dicts (Security, AcqDate,
    Units, CostBaseAUD) representing positions that existed BEFORE the
    fills log started. Required when migrating an existing portfolio
    onto the engine; the triage script writes this file. Fills are
    replayed on top of the seed, so a sell after the seed correctly
    decrements the seed lot.

    Cost basis AUD priority per BUY row:
      1. `avg_fill_price_local * fx_map[ticker]` if both present
      2. `rec_px_aud` (the engine's planned AUD price) as fallback
      3. row skipped if neither produces a positive cost basis

    Returns an empty Lots-format DataFrame if no seed AND no filled
    rows are available.
    """
    base_cols = ["Security", "AcqDate", "Units", "CostBaseAUD"]
    p = Path(fills_path) if not hasattr(fills_path, "exists") else fills_path

    seed_lots: list[dict] = []
    if seed_path is not None:
        sp = Path(seed_path) if not hasattr(seed_path, "exists") else seed_path
        if sp.exists():
            try:
                seed_data = json.loads(sp.read_text(encoding="utf-8"))
                for item in seed_data:
                    try:
                        u = int(round(float(item.get("Units") or 0)))
                        cb = float(item.get("CostBaseAUD") or 0)
                        if u <= 0 or cb <= 0:
                            continue
                        seed_lots.append({
                            "Security": str(item.get("Security", "")).strip(),
                            "AcqDate": pd.Timestamp(item.get("AcqDate")),
                            "Units": u,
                            "CostBaseAUD": cb,
                        })
                    except Exception:
                        continue
            except Exception:
                pass

    if not p.exists() and not seed_lots:
        return pd.DataFrame(columns=base_cols)

    if isinstance(fx_map, pd.Series):
        fx_lookup = {str(k).strip(): float(v) for k, v in fx_map.items()
                     if pd.notna(v) and float(v) > 0}
    elif isinstance(fx_map, dict):
        fx_lookup = {str(k).strip(): float(v) for k, v in fx_map.items()
                     if v is not None and float(v) > 0}
    else:
        fx_lookup = {}

    rows = []
    if p.exists():
        try:
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        r = json.loads(line.replace("NaN", "null"))
                    except Exception:
                        continue
                    qf = float(r.get("qty_filled") or 0)
                    if qf <= 0:
                        continue
                    rows.append(r)
        except Exception:
            rows = []

    if not rows and not seed_lots:
        return pd.DataFrame(columns=base_cols)

    rows.sort(key=lambda r: r.get("exec_timestamp", ""))
    lots: list[dict] = list(seed_lots)

    for r in rows:
        sec = str(r.get("ticker", "")).strip()
        if not sec:
            continue
        qty = float(r.get("qty_filled") or 0)
        side = str(r.get("side", "")).upper()
        ts = r.get("exec_timestamp") or r.get("rec_log_run_at")
        try:
            acq = pd.Timestamp(ts)
        except Exception:
            acq = pd.Timestamp.utcnow()

        if side == "BUY":
            local_px = r.get("avg_fill_price_local")
            try:
                local_px = float(local_px) if local_px is not None else None
            except Exception:
                local_px = None
            fx = fx_lookup.get(sec, 1.0 if sec.endswith(".AX") else None)
            if local_px is not None and fx is not None and fx > 0:
                cost_aud = local_px * fx
            else:
                cost_aud = float(r.get("rec_px_aud") or 0.0)
            if cost_aud <= 0:
                continue
            lots.append({
                "Security": sec,
                "AcqDate": acq,
                "Units": int(round(qty)),
                "CostBaseAUD": cost_aud,
            })
        elif side == "SELL":
            block_idx = [i for i, lt in enumerate(lots) if lt["Security"] == sec]
            if not block_idx:
                continue
            if str(lot_match_method).upper() == "HIFO":
                block_idx.sort(key=lambda i: (-lots[i]["CostBaseAUD"], lots[i]["AcqDate"]))
            else:
                block_idx.sort(key=lambda i: lots[i]["AcqDate"])
            remaining = qty
            for i in block_idx:
                if remaining <= 0:
                    break
                have = float(lots[i]["Units"])
                take = min(remaining, have)
                lots[i]["Units"] = have - take
                remaining -= take
            lots = [lt for lt in lots if lt["Units"] > 0]

    if not lots:
        return pd.DataFrame(columns=base_cols)
    return pd.DataFrame(lots, columns=base_cols)


def _build_lots_from_holdings(
    units: "pd.Series | dict",
    last_px_aud: "pd.Series | dict",
    today: "pd.Timestamp | None" = None,
) -> pd.DataFrame:
    """CGT-naive baseline: single lot per held ticker at today's AUD price.

    Plumbed-but-not-wired alternative for users who don't have a fills
    history (e.g. moving off IBKR to a broker without per-fill export).
    Loses any real cost-basis history — every existing position is
    treated as freshly acquired at the current price.
    """
    base_cols = ["Security", "AcqDate", "Units", "CostBaseAUD"]
    if today is None:
        today = pd.Timestamp.utcnow().normalize()

    u = pd.Series(units, dtype=float) if not isinstance(units, pd.Series) else units.astype(float)
    px = pd.Series(last_px_aud, dtype=float) if not isinstance(last_px_aud, pd.Series) else last_px_aud.astype(float)

    rows = []
    for tk, qty in u.items():
        try:
            qty_i = int(round(float(qty)))
        except Exception:
            continue
        if qty_i <= 0:
            continue
        p = float(px.get(tk, 0.0) or 0.0)
        if p <= 0:
            continue
        rows.append({
            "Security": str(tk),
            "AcqDate": pd.Timestamp(today),
            "Units": qty_i,
            "CostBaseAUD": p,
        })
    if not rows:
        return pd.DataFrame(columns=base_cols)
    return pd.DataFrame(rows, columns=base_cols)


def expand_with_lots(trade_df, lots_df, sale_date, method="FIFO"):
    """
    Expand sell trades by matching parcels from lots_df.

    Returns rows with:
      Security, AcqDate, UnitsSold, AcqPrice, CostBase,
      Last Px (AUD), Cash Flow (AUD), Brokerage (AUD), RealisedGain
    """
    if trade_df is None or trade_df.empty:
        return pd.DataFrame()

    delta_col = _trade_delta_col(trade_df)
    if delta_col is None:
        return pd.DataFrame()

    lots = lots_df.copy() if lots_df is not None else pd.DataFrame(columns=["Security", "AcqDate", "Units", "CostBaseAUD"])
    if lots.empty:
        lots = pd.DataFrame(columns=["Security", "AcqDate", "Units", "CostBaseAUD"])

    lots = lots.dropna(subset=["Security", "Units", "CostBaseAUD"], how="any")
    if not lots.empty:
        lots["Units"] = pd.to_numeric(lots["Units"], errors="coerce").fillna(0.0).astype(int)
        lots["CostBaseAUD"] = pd.to_numeric(lots["CostBaseAUD"], errors="coerce").fillna(0.0).astype(float)
        lots["AcqDate"] = pd.to_datetime(lots["AcqDate"], errors="coerce")

    out_rows = []

    for idx, row in trade_df.iterrows():
        sec = _security_from_row(idx, row)
        delta = int(pd.to_numeric(row.get(delta_col, 0), errors="coerce") or 0)

        if delta >= 0:
            continue

        units_to_sell = -delta
        sec_lots = lots[lots["Security"] == sec].copy()

        if sec_lots.empty:
            out_rows.append(
                {
                    "Security": sec,
                    "AcqDate": pd.NaT,
                    "UnitsSold": units_to_sell,
                    "AcqPrice": np.nan,
                    "CostBase": np.nan,
                    "Last Px (AUD)": row.get("Last Px (AUD)", np.nan),
                    "Cash Flow (AUD)": row.get("Cash Flow (AUD)", np.nan),
                    "Brokerage (AUD)": row.get("Brokerage (AUD)", 0.0),
                    "RealisedGain": np.nan,
                }
            )
            continue

        if str(method).upper() == "FIFO":
            sec_lots = sec_lots.sort_values("AcqDate")
        elif str(method).upper() == "HIFO":
            sec_lots = sec_lots.sort_values("CostBaseAUD", ascending=False)

        for _, lot in sec_lots.iterrows():
            if units_to_sell <= 0:
                break

            take = min(units_to_sell, int(lot["Units"]))
            units_to_sell -= take

            acq_price = float(lot["CostBaseAUD"])
            cost_base = take * acq_price
            proceeds = take * float(pd.to_numeric(row.get("Last Px (AUD)", 0.0), errors="coerce") or 0.0)
            realised = proceeds - cost_base

            out_rows.append(
                {
                    "Security": sec,
                    "AcqDate": lot["AcqDate"],
                    "UnitsSold": int(take),
                    "AcqPrice": acq_price,
                    "CostBase": cost_base,
                    "Last Px (AUD)": row.get("Last Px (AUD)", np.nan),
                    "Cash Flow (AUD)": row.get("Cash Flow (AUD)", np.nan),
                    "Brokerage (AUD)": row.get("Brokerage (AUD)", 0.0),
                    "RealisedGain": realised,
                }
            )

        if units_to_sell > 0:
            out_rows.append(
                {
                    "Security": sec,
                    "AcqDate": pd.NaT,
                    "UnitsSold": int(units_to_sell),
                    "AcqPrice": np.nan,
                    "CostBase": np.nan,
                    "Last Px (AUD)": row.get("Last Px (AUD)", np.nan),
                    "Cash Flow (AUD)": row.get("Cash Flow (AUD)", np.nan),
                    "Brokerage (AUD)": row.get("Brokerage (AUD)", 0.0),
                    "RealisedGain": np.nan,
                }
            )

    return pd.DataFrame(out_rows)
