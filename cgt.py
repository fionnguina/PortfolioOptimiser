"""Australian CGT helpers — FY-netted, 12mo discount, FIFO/HIFO matching.

Extracted from Portfolio_Optimiser.py for testability + module-split prep.

Contains:
  * Pure tax helpers (compute_cgt_tax + supports) — take trade/lot DataFrames,
    return tax+breakdown. No globals.
  * The CGT_PROFILES / ACTIVE_CGT_PROFILE / CGT_CONFIG block — canonical here.
  * LotBook — stateful FIFO/HIFO lot accounting used by the live OOS engine
    and TLH pass. Default cfg falls back to CGT_CONFIG.
  * _effective_cgt_rate + compute_cgt_for_rebalance — single-rebalance tax
    calculators (used by OOS walk-forward and live trade plan).

See ARCHITECTURE.md §4 "CGT model in detail" for the design rationale
(FY netting, 12mo LT discount, loss carry-forward).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta


# === CGT profile config ======================================================
# Switch profile via ACTIVE_CGT_PROFILE. CGT_CONFIG is the working copy.
# Re-export both so the engine's config snapshot logger + PPT subtitles can
# read the same values without duplication.
CGT_PROFILES = {
    "personal_30pc": {
        "marginal_tax_rate":   0.30,
        "medicare_levy":       0.02,
        "include_medicare":    True,
        "lt_discount_rate":    0.50,
        "lt_holding_days":     365,
        "description":         "Personal name, 30% MTR + 2% Medicare (user's current bracket)",
    },
    "personal_45pc": {
        "marginal_tax_rate":   0.45,
        "medicare_levy":       0.02,
        "include_medicare":    True,
        "lt_discount_rate":    0.50,
        "lt_holding_days":     365,
        "description":         "Personal name, top AU bracket + Medicare",
    },
    "trust_30pc": {
        "marginal_tax_rate":   0.30,
        "medicare_levy":       0.02,
        "include_medicare":    True,
        "lt_discount_rate":    0.50,
        "lt_holding_days":     365,
        "description":         "Family trust, distributed to single 30% bracket beneficiary",
    },
    "trust_split": {
        # Assumes optimal distribution across multiple lower-bracket beneficiaries
        # (e.g. spouse on 19%, kids on 0% up to threshold). Effective avg ~20%.
        "marginal_tax_rate":   0.20,
        "medicare_levy":       0.02,
        "include_medicare":    True,
        "lt_discount_rate":    0.50,
        "lt_holding_days":     365,
        "description":         "Family trust, optimally split across beneficiaries (~20% avg MTR)",
    },
}

ACTIVE_CGT_PROFILE = "personal_30pc"
CGT_CONFIG = CGT_PROFILES[ACTIVE_CGT_PROFILE].copy()


# Column-name variants used to find the trade delta column across spreadsheet
# import paths (the curly-d Δ unicode character sometimes gets mangled to
# the latin-1 mojibake "ÃŽâ€".
TRADE_DELTA_CANDIDATES = ("Delta Units", "ÃŽâ€ Units")


def _trade_delta_col(trade_df: pd.DataFrame) -> str | None:
    for c in TRADE_DELTA_CANDIDATES:
        if c in trade_df.columns:
            return c
    return None


def _security_from_row(idx, row: pd.Series) -> str:
    if "Security" in row.index:
        return str(row["Security"])
    return str(idx)


def _is_long_term_au(acq_date: pd.Timestamp, sale_date: pd.Timestamp) -> bool:
    """AU CGT discount eligibility: held at least 12 months."""
    if pd.isna(acq_date) or pd.isna(sale_date):
        return False
    return pd.Timestamp(sale_date) >= (pd.Timestamp(acq_date) + relativedelta(years=1))


def _allocate_sale_to_lots(
    lots: pd.DataFrame,
    sell_units: float,
    sale_price_aud: float,
    sale_date: pd.Timestamp,
    method: str = "HIFO",
):
    """
    Consume lot units to satisfy a sale.
    Returns list of dicts with:
      qty, acq_date, proceed, cost_base, gain, long_term
    """
    if lots is None or lots.empty or sell_units <= 0:
        return []

    lots = lots.copy()
    lots["AcqDate"] = pd.to_datetime(lots["AcqDate"], errors="coerce")

    if str(method).upper() == "HIFO":
        lots = lots.sort_values(by=["CostBaseAUD", "AcqDate"], ascending=[False, True])
    else:
        lots = lots.sort_values(by=["AcqDate"], ascending=True)

    out = []
    remaining = float(sell_units)

    for _, L in lots.iterrows():
        if remaining <= 0:
            break

        have = float(pd.to_numeric(L.get("Units", 0.0), errors="coerce") or 0.0)
        if have <= 0:
            continue

        qty = min(remaining, have)
        cb_unit = float(pd.to_numeric(L.get("CostBaseAUD", 0.0), errors="coerce") or 0.0)
        acq = pd.Timestamp(L.get("AcqDate"))

        proceed = float(sale_price_aud) * qty
        cost_base = cb_unit * qty
        gain = proceed - cost_base

        out.append(
            {
                "qty": qty,
                "acq_date": acq,
                "proceed": proceed,
                "cost_base": cost_base,
                "gain": gain,
                "long_term": bool(_is_long_term_au(acq, sale_date)),
            }
        )
        remaining -= qty

    return out


def compute_cgt_tax(
    trade_df: pd.DataFrame,
    lots_df: pd.DataFrame,
    sale_date: pd.Timestamp,
    marginal_rate: float,
    carry_forward_loss: float = 0.0,
    method: str = "HIFO",
) -> tuple[float, dict]:
    """
    Returns (tax_AUD, breakdown_dict) with per-lot audit table.
    """
    empty_result = {
        "st_gain": 0.0,
        "lt_gain": 0.0,
        "losses": 0.0,
        "discounted_lt_after_losses": 0.0,
        "taxable": 0.0,
        "audit": pd.DataFrame(),
    }

    if trade_df is None or trade_df.empty:
        return 0.0, empty_result

    delta_col = _trade_delta_col(trade_df)
    if delta_col is None:
        return 0.0, empty_result

    lots_df = lots_df.copy() if lots_df is not None else pd.DataFrame(columns=["Security", "AcqDate", "Units", "CostBaseAUD"])
    if "AcqDate" in lots_df.columns:
        lots_df["AcqDate"] = pd.to_datetime(lots_df["AcqDate"], errors="coerce")

    lots_by_sec = {s: g.copy() for s, g in lots_df.groupby("Security")} if not lots_df.empty else {}

    audit_rows = []
    st_gain = 0.0
    lt_gain = 0.0
    losses = 0.0

    for i, r in trade_df.iterrows():
        dU = int(pd.to_numeric(r.get(delta_col, 0), errors="coerce") or 0)
        if dU >= 0:
            continue

        sec = _security_from_row(i, r)
        px_aud = float(pd.to_numeric(r.get("Last Px (AUD)", 0.0), errors="coerce") or 0.0)
        sell_qty = abs(dU)

        ledger = _allocate_sale_to_lots(
            lots_by_sec.get(sec, pd.DataFrame(columns=["Security", "AcqDate", "Units", "CostBaseAUD"])),
            sell_qty,
            px_aud,
            sale_date,
            method=method,
        )

        sold = 0.0
        for row in ledger:
            sold += row["qty"]
            g = float(row["gain"])

            audit_rows.append(
                {
                    "Security": sec,
                    "Qty": row["qty"],
                    "AcqDate": row["acq_date"],
                    "SaleDate": pd.Timestamp(sale_date),
                    "Proceeds": row["proceed"],
                    "CostBase": row["cost_base"],
                    "Gain": g,
                    "LongTermEligible": bool(row["long_term"]),
                }
            )

            if g >= 0:
                if row["long_term"]:
                    lt_gain += g
                else:
                    st_gain += g
            else:
                losses += -g

        # conservative: unmatched sells contribute zero gain
        _unused = max(0.0, sell_qty - sold)

    rem_losses = float(carry_forward_loss) + float(losses)
    st_off = min(rem_losses, st_gain)
    st_gain -= st_off
    rem_losses -= st_off

    lt_off = min(rem_losses, lt_gain)
    lt_gain -= lt_off
    rem_losses -= lt_off

    discounted_lt = 0.5 * max(0.0, lt_gain)
    taxable = max(0.0, st_gain + discounted_lt)
    tax = float(marginal_rate) * float(taxable)

    bkd = {
        "st_gain": float(st_gain),
        "lt_gain": float(lt_gain),
        "losses": float(losses + carry_forward_loss),
        "discounted_lt_after_losses": float(discounted_lt),
        "taxable": float(taxable),
        # Losses left after offsetting ST + LT gains. Carries to next FY and
        # becomes the "tax saved" side of the Deferred Tax callout.
        "loss_carry_forward": float(max(0.0, rem_losses)),
        "audit": pd.DataFrame(audit_rows),
    }
    return float(tax), bkd


def au_financial_year(d) -> str:
    """AU FY label for a date: FY2026-27 runs 2026-07-01 → 2027-06-30."""
    d = pd.Timestamp(d)
    start_year = d.year if d.month >= 7 else d.year - 1
    return f"FY{start_year}-{str(start_year + 1)[-2:]}"


def compute_fy_tax_ledger(
    fills_path,
    seed_path=None,
    fx_map=None,
    cfg: dict | None = None,
    lot_match_method: str = "HIFO",
) -> pd.DataFrame:
    """Per-financial-year CGT ledger from the ACTUAL fills log.

    Replays seed lots + executed fills chronologically (same conventions
    as _build_lots_from_fills_log: per-unit CostBaseAUD, HIFO/FIFO match,
    qty_filled > 0 only), computes per-lot realised gains on every SELL,
    buckets them into AU financial years, then applies the same netting
    as compute_cgt_tax per FY: losses offset ST gains first, then LT,
    remaining LT gets the 50% discount, tax at MTR (+Medicare). Loss
    carry-forward chains across FYs.

    Purpose: reconcile live NAV against the backtest's assumption that
    CGT is paid (and TLH savings reinvested) inside the portfolio. In
    reality tax settles at lodgement in the user's bank account — this
    ledger tells the user each FY what that settlement is, so they can
    contribute the refund back (or know the bill) and keep the live
    NAV comparable to the simulated one.

    Stateless by design — derived fresh from the fills log every run,
    like the lot-book rebuild (no state file to corrupt).

    Returns a DataFrame with one row per FY (chronological); the last
    row is the open (current) FY.
    """
    import json as _json
    from pathlib import Path as _Path

    cfg = dict(cfg or CGT_CONFIG)
    mtr = float(cfg.get("marginal_tax_rate", 0.30))
    if cfg.get("include_medicare", True):
        mtr += float(cfg.get("medicare_levy", 0.02))
    lt_discount = float(cfg.get("lt_discount_rate", 0.50))

    if isinstance(fx_map, pd.Series):
        fx_lookup = {str(k).strip(): float(v) for k, v in fx_map.items()
                     if pd.notna(v) and float(v) > 0}
    elif isinstance(fx_map, dict):
        fx_lookup = {str(k).strip(): float(v) for k, v in fx_map.items()
                     if v is not None and float(v) > 0}
    else:
        fx_lookup = {}

    # --- Load seed lots ----------------------------------------------------
    lots: list[dict] = []
    sp = _Path(seed_path) if seed_path is not None and not hasattr(seed_path, "exists") else seed_path
    if sp is not None and sp.exists():
        try:
            for item in _json.loads(sp.read_text(encoding="utf-8")):
                try:
                    u = int(round(float(item.get("Units") or 0)))
                    cb = float(item.get("CostBaseAUD") or 0)
                    if u <= 0 or cb <= 0:
                        continue
                    lots.append({
                        "Security": str(item.get("Security", "")).strip(),
                        "AcqDate": pd.Timestamp(item.get("AcqDate")),
                        "Units": float(u),
                        "CostBaseAUD": cb,
                    })
                except Exception:
                    continue
        except Exception:
            pass

    # --- Load fills --------------------------------------------------------
    fills: list[dict] = []
    fp = _Path(fills_path) if fills_path is not None and not hasattr(fills_path, "exists") else fills_path
    if fp is not None and fp.exists():
        try:
            with open(fp, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        r = _json.loads(line.replace("NaN", "null"))
                    except Exception:
                        continue
                    if float(r.get("qty_filled") or 0) <= 0:
                        continue
                    fills.append(r)
        except Exception:
            fills = []
    fills.sort(key=lambda r: r.get("exec_timestamp", ""))

    # --- Replay: buys append lots, sells realise per-lot gains --------------
    fy_events: dict[str, dict] = {}

    def _fy_bucket(fy: str) -> dict:
        return fy_events.setdefault(fy, {
            "sells": 0, "proceeds": 0.0,
            "st_gain": 0.0, "lt_gain": 0.0, "losses": 0.0,
        })

    for r in fills:
        sec = str(r.get("ticker", "")).strip()
        if not sec:
            continue
        qty = float(r.get("qty_filled") or 0)
        side = str(r.get("side", "")).upper()
        ts = r.get("exec_timestamp") or r.get("rec_log_run_at")
        try:
            when = pd.Timestamp(ts)
        except Exception:
            continue

        local_px = r.get("avg_fill_price_local")
        try:
            local_px = float(local_px) if local_px is not None else None
        except Exception:
            local_px = None
        fx = fx_lookup.get(sec, 1.0 if sec.endswith(".AX") else None)
        if local_px is not None and fx is not None and fx > 0:
            px_aud = local_px * fx
        else:
            px_aud = float(r.get("rec_px_aud") or 0.0)
        if px_aud <= 0:
            continue

        if side == "BUY":
            lots.append({
                "Security": sec,
                "AcqDate": when,
                "Units": qty,
                "CostBaseAUD": px_aud,
            })
        elif side == "SELL":
            block = pd.DataFrame([lt for lt in lots if lt["Security"] == sec])
            ledger = _allocate_sale_to_lots(
                block, qty, px_aud, when, method=lot_match_method,
            )
            b = _fy_bucket(au_financial_year(when))
            b["sells"] += 1
            for row in ledger:
                b["proceeds"] += float(row["proceed"])
                g = float(row["gain"])
                if g >= 0:
                    if row["long_term"]:
                        b["lt_gain"] += g
                    else:
                        b["st_gain"] += g
                else:
                    b["losses"] += -g
            # Decrement the master lot list to mirror the allocation.
            remaining = qty
            block_idx = [i for i, lt in enumerate(lots) if lt["Security"] == sec]
            if str(lot_match_method).upper() == "HIFO":
                block_idx.sort(key=lambda i: (-lots[i]["CostBaseAUD"], lots[i]["AcqDate"]))
            else:
                block_idx.sort(key=lambda i: lots[i]["AcqDate"])
            for i in block_idx:
                if remaining <= 0:
                    break
                have = float(lots[i]["Units"])
                take = min(remaining, have)
                lots[i]["Units"] = have - take
                remaining -= take
            lots = [lt for lt in lots if lt["Units"] > 0]

    # --- FY netting with carry-forward chain --------------------------------
    cols = ["FY", "Sell Events", "Proceeds (AUD)", "ST Gains", "LT Gains",
            "Losses Realised", "Carry-Fwd In", "Net Taxable",
            "CGT at Lodgement (AUD)", "Carry-Fwd Out", "Status"]
    if not fy_events:
        return pd.DataFrame(columns=cols)

    current_fy = au_financial_year(pd.Timestamp.now())
    out_rows = []
    carry = 0.0
    for fy in sorted(fy_events.keys()):
        b = fy_events[fy]
        st, lt, losses = b["st_gain"], b["lt_gain"], b["losses"]
        rem = carry + losses
        st_off = min(rem, st)
        rem -= st_off
        lt_off = min(rem, lt)
        rem -= lt_off
        taxable = max(0.0, (st - st_off) + (1.0 - lt_discount) * max(0.0, lt - lt_off))
        tax = mtr * taxable
        out_rows.append({
            "FY": fy,
            "Sell Events": int(b["sells"]),
            "Proceeds (AUD)": round(b["proceeds"], 2),
            "ST Gains": round(st, 2),
            "LT Gains": round(lt, 2),
            "Losses Realised": round(losses, 2),
            "Carry-Fwd In": round(carry, 2),
            "Net Taxable": round(taxable, 2),
            "CGT at Lodgement (AUD)": round(tax, 2),
            "Carry-Fwd Out": round(rem, 2),
            "Status": "open (to date)" if fy == current_fy else "closed",
        })
        carry = rem

    return pd.DataFrame(out_rows, columns=cols)


# === Stateful CGT (live + OOS walk-forward) ==================================

def _effective_cgt_rate(short_term: bool = True, cfg: dict | None = None) -> float:
    """Effective tax rate on a $1 of capital gain.
    Short-term: full MTR (+ medicare if enabled).
    Long-term:  full rate × (1 - discount).
    """
    if cfg is None:
        cfg = CGT_CONFIG
    base = float(cfg["marginal_tax_rate"])
    if cfg.get("include_medicare", True):
        base += float(cfg["medicare_levy"])
    if short_term:
        return base
    return base * (1.0 - float(cfg["lt_discount_rate"]))


class LotBook:
    """Tracks FIFO lots per ticker for CGT calculation.

    Each lot stores: acquisition date, units, cost basis per unit.
    On sell: matches oldest lots first (FIFO), classifies each parcel as
    short-term (< 365 days) or long-term, and returns realised gains/losses
    broken down by category. On buy: appends a new lot.
    """
    def __init__(self):
        self.lots: dict[str, list[dict]] = {}

    def buy(self, ticker: str, units: float, date, price: float) -> None:
        if units <= 0 or not np.isfinite(units):
            return
        self.lots.setdefault(ticker, []).append({
            "date": pd.Timestamp(date),
            "units": float(units),
            "cost_basis_per_unit": float(price),
        })

    def sell(self, ticker: str, units: float, date, price: float,
             cfg: dict | None = None, protect=None) -> dict:
        """FIFO sale. Returns dict with ST/LT realised gain & loss components.

        `protect`: optional callable(lot: dict, hold_days: int) -> bool.
        Lots for which it returns True are SKIPPED by the allocation
        (left intact, FIFO continues to the next lot). Used by the
        LT-discount deferral rule to shield near-long-term gain lots
        from short-term realisation.
        """
        if cfg is None:
            cfg = CGT_CONFIG
        lt_threshold = int(cfg["lt_holding_days"])
        out = {"st_gain": 0.0, "lt_gain": 0.0, "st_loss": 0.0, "lt_loss": 0.0}
        if ticker not in self.lots or not self.lots[ticker] or units <= 0:
            return out

        sale_date = pd.Timestamp(date)
        remaining = float(units)
        new_lots = []
        for lot in self.lots[ticker]:
            if remaining <= 1e-9:
                new_lots.append(lot)
                continue
            if protect is not None:
                try:
                    _hd = (sale_date - lot["date"]).days
                    if protect(lot, _hd):
                        new_lots.append(lot)
                        continue
                except Exception:
                    pass
            qty = min(lot["units"], remaining)
            proceeds = qty * float(price)
            cost_base = qty * lot["cost_basis_per_unit"]
            gain = proceeds - cost_base
            hold_days = (sale_date - lot["date"]).days
            is_lt = hold_days >= lt_threshold

            if gain >= 0:
                if is_lt:
                    out["lt_gain"] += gain
                else:
                    out["st_gain"] += gain
            else:
                if is_lt:
                    out["lt_loss"] += -gain
                else:
                    out["st_loss"] += -gain

            remaining -= qty
            if qty < lot["units"]:
                new_lots.append({
                    "date": lot["date"],
                    "units": lot["units"] - qty,
                    "cost_basis_per_unit": lot["cost_basis_per_unit"],
                })

        self.lots[ticker] = new_lots
        return out

    def units(self, ticker: str) -> float:
        """Current units held."""
        return float(sum(lot["units"] for lot in self.lots.get(ticker, [])))

    def near_lt_gain_units(self, ticker: str, price: float, as_of,
                           window_days: int,
                           cfg: dict | None = None) -> float:
        """Units in lots that are (a) at a GAIN at `price` and (b) within
        `window_days` of long-term discount eligibility (12mo in AU).

        These are the lots the LT-deferral rule shields: selling them now
        realises a short-term gain taxed at full MTR, while waiting ≤
        `window_days` halves the tax via the CGT discount. Loss lots are
        never shielded (realising losses is beneficial — TLH territory),
        and already-LT lots sell freely at the discounted rate.
        """
        if cfg is None:
            cfg = CGT_CONFIG
        lt_threshold = int(cfg["lt_holding_days"])
        if window_days <= 0 or not np.isfinite(price) or price <= 0:
            return 0.0
        ref = pd.Timestamp(as_of)
        total = 0.0
        for lot in self.lots.get(ticker, []):
            if lot["units"] <= 0:
                continue
            if price <= float(lot["cost_basis_per_unit"]):
                continue  # loss or flat — no shield
            hold_days = (ref - lot["date"]).days
            if (lt_threshold - int(window_days)) <= hold_days < lt_threshold:
                total += float(lot["units"])
        return total

    def unrealised_losses(self, price_snapshot, as_of,
                          min_loss_pct: float = -0.05,
                          min_loss_aud: float = 100.0) -> list[dict]:
        """Return lot-level unrealised losses ≤ min_loss_pct AND ≤ -min_loss_aud.

        Each entry: ticker, lot_idx (within-ticker), units, cost_basis_per_unit,
        current_price, market_value_aud, loss_aud (positive number), loss_pct
        (negative fraction), hold_days. Sorted by largest loss_aud first so the
        TLH pass can prioritise high-value harvests.
        """
        out: list[dict] = []
        ref_date = pd.Timestamp(as_of)
        for tkr, lot_list in self.lots.items():
            try:
                if hasattr(price_snapshot, "get"):
                    p = float(price_snapshot.get(tkr, np.nan))
                else:
                    p = float(price_snapshot[tkr]) if tkr in price_snapshot else float("nan")
            except Exception:
                continue
            if not np.isfinite(p) or p <= 0:
                continue
            for idx, lot in enumerate(lot_list):
                if lot["units"] <= 0:
                    continue
                cost = float(lot["cost_basis_per_unit"])
                mkt = lot["units"] * p
                loss = (cost - p) * lot["units"]  # positive = loss
                if cost <= 0:
                    continue
                loss_pct = (p - cost) / cost     # negative for loss
                if loss_pct > min_loss_pct:      # not loss enough
                    continue
                if loss < min_loss_aud:          # below absolute floor
                    continue
                out.append({
                    "ticker": tkr,
                    "lot_idx": idx,
                    "units": float(lot["units"]),
                    "cost_basis_per_unit": cost,
                    "current_price": p,
                    "market_value_aud": float(mkt),
                    "loss_aud": float(loss),
                    "loss_pct": float(loss_pct),
                    "hold_days": int((ref_date - lot["date"]).days),
                    "lot_date": lot["date"],
                })
        out.sort(key=lambda r: r["loss_aud"], reverse=True)
        return out

    def sell_lot(self, ticker: str, lot_idx: int, units: float, date,
                 price: float, cfg: dict | None = None) -> dict:
        """Sell up to `units` from a SPECIFIC lot (overrides FIFO). Used by the
        TLH pass to target loss lots without disturbing the rest of the book.
        Returns the same realised-bucket dict as `sell()` so callers can fold
        the result straight into the FY accumulators.
        """
        if cfg is None:
            cfg = CGT_CONFIG
        out = {"st_gain": 0.0, "lt_gain": 0.0, "st_loss": 0.0, "lt_loss": 0.0}
        if ticker not in self.lots or units <= 0:
            return out
        lot_list = self.lots[ticker]
        if not (0 <= lot_idx < len(lot_list)):
            return out
        lot = lot_list[lot_idx]
        qty = min(float(lot["units"]), float(units))
        if qty <= 0:
            return out
        lt_threshold = int(cfg["lt_holding_days"])
        proceeds = qty * float(price)
        cost_base = qty * float(lot["cost_basis_per_unit"])
        gain = proceeds - cost_base
        is_lt = (pd.Timestamp(date) - lot["date"]).days >= lt_threshold
        if gain >= 0:
            out["lt_gain" if is_lt else "st_gain"] += gain
        else:
            out["lt_loss" if is_lt else "st_loss"] += -gain
        if qty >= float(lot["units"]) - 1e-9:
            # Removed entirely
            del lot_list[lot_idx]
        else:
            lot["units"] = float(lot["units"]) - qty
        return out


def compute_cgt_for_rebalance(realised: dict, cfg: dict | None = None) -> float:
    """Tax owed on a single rebalance's realised gains, with within-rebalance
    loss offset. Long-term gains discounted before tax. Returns AUD tax.
    """
    if cfg is None:
        cfg = CGT_CONFIG
    st_gain = float(realised.get("st_gain", 0.0))
    lt_gain = float(realised.get("lt_gain", 0.0))
    st_loss = float(realised.get("st_loss", 0.0))
    lt_loss = float(realised.get("lt_loss", 0.0))

    # 1) Net within each category
    st_net = st_gain - st_loss   # may be negative
    lt_net = lt_gain - lt_loss

    # 2) Cross-offset: if one side is negative (net loss), it can reduce the
    #    other side's positive gain. This is the AU rule for the same FY.
    if st_net < 0 and lt_net > 0:
        offset = min(lt_net, -st_net)
        lt_net -= offset
        st_net += offset
    if lt_net < 0 and st_net > 0:
        offset = min(st_net, -lt_net)
        st_net -= offset
        lt_net += offset

    # 3) Apply rates to remaining positive net gains
    tax = 0.0
    if st_net > 0:
        tax += st_net * _effective_cgt_rate(short_term=True, cfg=cfg)
    if lt_net > 0:
        tax += lt_net * _effective_cgt_rate(short_term=False, cfg=cfg)
    return float(tax)
