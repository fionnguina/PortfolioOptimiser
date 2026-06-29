"""Australian CGT helpers — FY-netted, 12mo discount, FIFO/HIFO matching.

Extracted from Portfolio_Optimiser.py for testability + module-split prep.
Pure functions taking trade/lot DataFrames and returning tax+breakdown
tuples. No module-level globals, no I/O.

See ARCHITECTURE.md §4 "CGT model in detail" for the design rationale
(FY netting, 12mo LT discount, loss carry-forward).

NOTE: The LotBook class (live OOS walk-forward lot accounting) remains in
Portfolio_Optimiser.py for now — it consumes engine globals (CGT_CONFIG)
and is heavily entangled with the OOS engine. Future extraction once
config moves to its own module.
"""
from __future__ import annotations

import pandas as pd
from dateutil.relativedelta import relativedelta


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
