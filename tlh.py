"""Tax-Loss Harvesting (TLH) pass + helpers.

Extracted from Portfolio_Optimiser.py for testability + module-split prep.

At each rebalance, the OOS engine + live pipeline scan current lots for
unrealised losses ≥ TLH_MIN_LOSS_PCT. For each loss lot whose ticker has
a substitute defined in tlh_pairs.json, we sell the loss lot (realises
the loss into the FY bucket → offsets gains) and buy equivalent dollar
value of the substitute. Cooldown prevents immediate swap-back (ATO
anti-avoidance under TR 2008/1).

Constants (canonical here; engine imports them back):
  TLH_ENABLED            Master switch; False fully disables the pass.
  TLH_MIN_LOSS_PCT       Threshold for a lot to be swap-eligible.
  TLH_COOLDOWN_DAYS      Min days between a ticker being sold OUT and
                         re-bought as a substitute.
  TLH_MIN_LOSS_AUD       Skip lots where |loss| is below brokerage floor.

Functions:
  _load_tlh_pairs(path)         Load substitute mapping from JSON. Falls
                                back to built-in default if missing/malformed.
  _run_tlh_pass(...)            One pass; mutates lot_book + cooldown_state.
                                pairs is REQUIRED (no implicit fallback).
  _build_lot_book_from_df(df)   Convert Lots sheet DataFrame → LotBook.
  _load_tlh_cooldown_state(p)   Read persisted cooldown ({tkr: Timestamp}).
  _save_tlh_cooldown_state(p,s) Write cooldown JSON.

Cross-module deps:
  cgt.LotBook (for _build_lot_book_from_df + _run_tlh_pass type hints)
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from cgt import LotBook


# === TLH config (canonical) ==================================================
TLH_ENABLED              = True
TLH_MIN_LOSS_PCT         = -0.05   # only harvest lots in ≥5% loss
TLH_COOLDOWN_DAYS        = 31      # ≥30d = comfortably outside US wash-sale and OK under TR 2008/1
TLH_MIN_LOSS_AUD         = 100.0   # don't bother for absolute losses < $100 (brokerage floor)


def _load_tlh_pairs(pairs_path) -> dict[str, str]:
    """Load TLH substitute pairs from a JSON file. Falls back to a small
    conservative default if file missing/malformed so the engine still runs.

    pairs_path is required so callers control where the JSON lives (engine
    uses APP_DIR / "tlh_pairs.json"; tests can point elsewhere).
    """
    default = {
        "IVV": "SPY", "SPY": "IVV",
        "QQQ": "NDQ.AX", "NDQ.AX": "QQQ",
        "VAS.AX": "A200.AX", "A200.AX": "VAS.AX",
    }
    try:
        pairs_path = Path(pairs_path)
        if not pairs_path.exists():
            print(f"[tlh] tlh_pairs.json not found at {pairs_path}; using built-in defaults")
            return default
        with pairs_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        raw = data.get("pairs", data) if isinstance(data, dict) else {}
        pairs = {str(k): str(v) for k, v in raw.items() if k != "_doc"}
        return pairs if pairs else default
    except Exception as e:
        print(f"[tlh] Failed to load tlh_pairs.json ({e}); using built-in defaults")
        return default


def _run_tlh_pass(
    lot_book: LotBook,
    price_snapshot,
    as_of,
    cooldown_state: dict,
    pairs: dict,
    min_loss_pct: float | None = None,
    min_loss_aud: float | None = None,
    cooldown_days: int | None = None,
    cfg: dict | None = None,
    nav_aud: float | None = None,
) -> dict:
    """One pass of tax-loss harvesting. Mutates `lot_book` and `cooldown_state`.

    For each lot in unrealised loss ≥ min_loss_pct (and ≥ min_loss_aud absolute):
      1. Look up its substitute in `pairs`. Skip if no pair, substitute price
         unavailable, or substitute is itself within the cooldown window
         (recently sold via TLH → buying it back here is a wash-swap).
      2. Sell the specific loss lot (realises loss into the FY bucket).
      3. Buy equivalent dollar value of the substitute at current price.
      4. Record the ticker we sold in cooldown_state[ticker] = as_of so it
         can't be the substitute of another TLH swap for cooldown_days.

    Returns dict with:
      events           List of event dicts (date, ticker_sold, lot_date,
                       units_sold, sale_price, cost_basis_per_unit, loss_aud,
                       ticker_bought, units_bought, buy_price, holding).
      realised         {st_gain, lt_gain, st_loss, lt_loss} totals — caller
                       folds into FY accumulator.
      total_loss_aud   Sum of |loss| realised this pass (positive).
      n_events         Count.
    """
    min_loss_pct = TLH_MIN_LOSS_PCT if min_loss_pct is None else min_loss_pct
    min_loss_aud = TLH_MIN_LOSS_AUD if min_loss_aud is None else min_loss_aud
    cooldown_days = TLH_COOLDOWN_DAYS if cooldown_days is None else cooldown_days
    as_of_ts = pd.Timestamp(as_of)

    result = {
        "events": [],
        "realised": {"st_gain": 0.0, "lt_gain": 0.0, "st_loss": 0.0, "lt_loss": 0.0},
        "total_loss_aud": 0.0,
        "n_events": 0,
    }
    if not TLH_ENABLED or not pairs:
        return result

    # Take a SNAPSHOT of loss lots first — we mutate the book during the loop
    # (sell_lot deletes/edits entries), so iterating live would be unsafe.
    losses = lot_book.unrealised_losses(
        price_snapshot=price_snapshot,
        as_of=as_of_ts,
        min_loss_pct=min_loss_pct,
        min_loss_aud=min_loss_aud,
    )
    # Re-fetch lot_idx by (ticker, lot_date, cost_basis) at sell time because
    # earlier sells may shift indices within the same ticker.
    def _find_lot_idx(tkr: str, lot_date, cost: float) -> int:
        for i, l in enumerate(lot_book.lots.get(tkr, [])):
            if l["date"] == lot_date and abs(l["cost_basis_per_unit"] - cost) < 1e-9:
                return i
        return -1

    for lossrec in losses:
        tkr = lossrec["ticker"]
        sub = pairs.get(tkr)
        if not sub:
            continue
        # Substitute price must exist + be positive.
        try:
            if hasattr(price_snapshot, "get"):
                p_sub = float(price_snapshot.get(sub, np.nan))
            else:
                p_sub = float(price_snapshot[sub]) if sub in price_snapshot else float("nan")
        except Exception:
            continue
        if not np.isfinite(p_sub) or p_sub <= 0:
            continue
        # Cooldown: substitute can't be a ticker we recently TLH-sold.
        if sub in cooldown_state:
            since = (as_of_ts - pd.Timestamp(cooldown_state[sub])).days
            if since < cooldown_days:
                continue
        # Re-locate the lot (indices may have shifted from earlier sells).
        idx = _find_lot_idx(tkr, lossrec["lot_date"], lossrec["cost_basis_per_unit"])
        if idx < 0:
            continue
        units = lossrec["units"]
        sale_price = lossrec["current_price"]
        sale_value = units * sale_price

        realised = lot_book.sell_lot(
            ticker=tkr, lot_idx=idx, units=units,
            date=as_of_ts, price=sale_price, cfg=cfg,
        )
        for k in result["realised"]:
            result["realised"][k] += realised[k]

        units_bought = sale_value / p_sub
        lot_book.buy(sub, units_bought, as_of_ts, p_sub)
        cooldown_state[tkr] = as_of_ts

        loss_real = float(realised.get("st_loss", 0.0) + realised.get("lt_loss", 0.0))
        result["total_loss_aud"] += loss_real
        result["events"].append({
            "date": as_of_ts,
            "ticker_sold": tkr,
            "lot_date": lossrec["lot_date"],
            "units_sold": float(units),
            "sale_price": float(sale_price),
            "cost_basis_per_unit": float(lossrec["cost_basis_per_unit"]),
            "loss_aud": loss_real,
            "loss_pct": float(lossrec["loss_pct"]),
            "hold_days": int(lossrec["hold_days"]),
            "ticker_bought": sub,
            "units_bought": float(units_bought),
            "buy_price": float(p_sub),
            "swap_value_aud": float(sale_value),
            "nav_aud": float(nav_aud) if nav_aud else None,
        })
        result["n_events"] += 1
    return result


def _build_lot_book_from_df(lots_df) -> LotBook:
    """Convert the Lots sheet DataFrame into an in-memory LotBook for TLH/CGT
    queries. Rows with missing/invalid ticker, units, or cost basis are
    skipped silently. Cost basis convention: per-unit AUD (engine standard,
    see _allocate_sale_to_lots)."""
    lb = LotBook()
    if lots_df is None or (hasattr(lots_df, "empty") and lots_df.empty):
        return lb
    for _, r in lots_df.iterrows():
        try:
            tk = str(r.get("Security", "")).strip()
            if not tk or tk.lower() in ("nan", "none"):
                continue
            units = float(r.get("Units", 0) or 0)
            if units <= 0 or not np.isfinite(units):
                continue
            date = pd.Timestamp(r.get("AcqDate"))
            cost_basis = float(r.get("CostBaseAUD", 0) or 0)
            if not np.isfinite(cost_basis) or cost_basis <= 0:
                continue
            lb.buy(tk, units, date, cost_basis)
        except Exception:
            continue
    return lb


def _load_tlh_cooldown_state(path) -> dict:
    """Load persisted TLH cooldown state. Empty dict if file missing/corrupt.
    Stored as {ticker: ISO timestamp string}, returned as {ticker: Timestamp}."""
    try:
        path = Path(path)
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return {str(k): pd.Timestamp(v) for k, v in raw.items()}
    except Exception:
        return {}


def _save_tlh_cooldown_state(path, state: dict) -> None:
    """Persist TLH cooldown state to JSON. Failures are logged, not raised —
    the cooldown is an optimisation; a corrupt/missing file just means the
    next run is more cautious (treats no cooldown as active)."""
    try:
        with open(Path(path), "w", encoding="utf-8") as f:
            json.dump({str(k): pd.Timestamp(v).isoformat()
                        for k, v in state.items()}, f, indent=2)
    except Exception as e:
        print(f"[tlh-live] cooldown state save failed: {e}")
