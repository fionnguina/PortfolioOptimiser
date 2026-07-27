"""Excel/PPT sheet writers + formatting utilities (module split #18, 2026-07-09).

xlwings sheet writers + generic openpyxl/pptx helpers, split out of the monolith.
Take a workbook/table + a DataFrame; no engine state beyond TARGET_PORTFOLIO_VALUE_AUD
(synced by the engine after import). _write_holdings_sheet stays in the engine (it
needs the FX helpers) and calls these utilities via the engine's re-export.
  get_or_clear_sheet / set_number_formats / set_truefalse_validation  xlwings helpers.
  _ensure_actual_fills_sheet / _read_actual_fills                     Actual_Fills sheet.
  _write_drift_sheets / _write_cash_ledger_sheet / _write_tilts_sheet Sheet renderers.
  _autofit_table_width                                                PPT table column fit.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pptx.util import Cm

from fx import get_usd_aud_fx, fx_to_aud_for_tickers

# Synced by the engine after import (cash-ledger "drift vs target" anchor).
TARGET_PORTFOLIO_VALUE_AUD = 0.0


def get_or_clear_sheet(wb, name):
    """Return sheet `name` (creating after the last sheet if absent), with contents cleared."""
    try:
        sht = wb.sheets[name]
        try:
            sht.used_range.clear_contents()
        except Exception:
            pass
    except Exception:
        sht = wb.sheets.add(name, after=wb.sheets[-1])
    return sht


def set_truefalse_validation(sht, a1_range):
    """Apply TRUE/FALSE data validation to the given A1-style range; silent on failure."""
    try:
        val_rng = sht.range(a1_range).api
        val_rng.Validation.Delete()
        val_rng.Validation.Add(3, 1, 1, "TRUE,FALSE")
    except Exception:
        pass


def set_number_formats(sht, fmt_by_range):
    """Apply Excel NumberFormat strings to multiple ranges in one go. fmt_by_range: {a1_range: fmt}."""
    try:
        for rng, fmt in fmt_by_range.items():
            sht.range(rng).api.NumberFormat = fmt
    except Exception:
        pass


def _ensure_actual_fills_sheet(wb) -> None:
    """Create Actual_Fills sheet with column headers if missing. NEVER touches
    existing user data (user enters fills by hand)."""
    try:
        existing = [s.name for s in wb.sheets]
    except Exception:
        return
    if "Actual_Fills" in existing:
        return
    try:
        sht = wb.sheets.add("Actual_Fills", after=wb.sheets[-1])
        headers = ["Fill Date", "Ticker", "Side", "Units", "Px AUD",
                   "Fees AUD", "Notes"]
        sht.range("A1").value = headers
        sht.range("A2").value = "(enter fills below — date YYYY-MM-DD, units signed +/-)"
    except Exception as e:
        print(f"[drift] could not create Actual_Fills sheet: {e}")


def _read_actual_fills(wb) -> pd.DataFrame:
    """Read the Actual_Fills ledger into the schema drift's fill-adherence join
    expects (Fill Date / Ticker / Units).

    The sheet has a summary BANNER (rows 1-5: title, source, batch counts, note)
    above the ledger table (header at ~row 7 with columns Exec TS / Ticker /
    Side / Qty Filled / ...). The old reader assumed row 1 was the header and
    A1.expand() grabbed the irregular banner -> "(4,1) vs (4,53)" shape error, so
    fill-adherence never got any fills. Now we scan for the ledger header (the
    row containing "Ticker") and map the ledger columns:
      Fill Date <- Exec TS ; Units <- Qty Filled signed by Side ;
      Px AUD <- Px AUD ; Fees AUD <- Fees AUD (both fx-converted by the writer).
    Px AUD / Fees AUD are optional — older ledgers written before the writer
    emitted them are left as NaN, so drift skips slippage/fee-delta for those.
    Returns empty on any problem; fill-adherence is non-fatal."""
    try:
        if "Actual_Fills" not in [s.name for s in wb.sheets]:
            return pd.DataFrame()
        raw = wb.sheets["Actual_Fills"].used_range.value
        if not isinstance(raw, list) or not raw:
            return pd.DataFrame()
        raw = [r if isinstance(r, (list, tuple)) else [r] for r in raw]
        hdr_i = next((i for i, r in enumerate(raw)
                      if any(str(c).strip() == "Ticker" for c in r if c is not None)),
                     None)
        if hdr_i is None:
            return pd.DataFrame()
        hdr = [str(h).strip() for h in raw[hdr_i]]
        ncol = len(hdr)
        body = []
        for r in raw[hdr_i + 1:]:
            r = list(r)[:ncol] + [None] * (ncol - len(r))
            if any(c is not None for c in r):
                body.append(r)
        if not body:
            return pd.DataFrame()
        led = pd.DataFrame(body, columns=hdr)
        if not {"Exec TS", "Ticker", "Qty Filled"}.issubset(led.columns):
            return pd.DataFrame()
        out = pd.DataFrame()
        out["Fill Date"] = pd.to_datetime(led["Exec TS"], errors="coerce")
        out["Ticker"] = led["Ticker"].astype(str).str.strip()
        qty = pd.to_numeric(led["Qty Filled"], errors="coerce")
        side = led.get("Side", "").astype(str).str.upper() if "Side" in led.columns \
            else pd.Series("", index=led.index)
        out["Units"] = qty * side.map(lambda s: -1.0 if str(s).startswith("SELL") else 1.0)
        # Optional AUD-converted price/fees — only present in ledgers written by
        # the post-2026-07 writer. Absent => NaN => drift skips slippage/fee-delta.
        if "Px AUD" in led.columns:
            out["Px AUD"] = pd.to_numeric(led["Px AUD"], errors="coerce")
        if "Fees AUD" in led.columns:
            out["Fees AUD"] = pd.to_numeric(led["Fees AUD"], errors="coerce")
        out = out.dropna(subset=["Fill Date", "Ticker", "Units"])
        out = out[out["Units"] != 0]
        return out.reset_index(drop=True)
    except Exception as e:
        print(f"[drift] Actual_Fills read skipped ({type(e).__name__})")
        return pd.DataFrame()


def _write_drift_sheets(
    wb,
    fills_drift_df: pd.DataFrame,
    nav_drift_df: pd.DataFrame,
    live_nav_series: pd.Series,
    live_dd: float,
) -> None:
    """Write Drift_Fills + Drift_NAV sheets. Overwrites each run."""
    try:
        sht = get_or_clear_sheet(wb, "Drift_Fills")
        if fills_drift_df is not None and not fills_drift_df.empty:
            sht.range("A1").options(index=False).value = fills_drift_df
        else:
            sht.range("A1").value = "(no fills in Actual_Fills yet — enter fills to populate)"
    except Exception as e:
        print(f"[drift] could not write Drift_Fills sheet: {e}")
    try:
        sht = get_or_clear_sheet(wb, "Drift_NAV")
        # Summary header
        sht.range("A1").value = "Live MaxDD (current from peak)"
        sht.range("B1").value = round(float(live_dd) * 100, 2)
        sht.range("C1").value = "%"
        sht.range("A2").value = "Live NAV samples"
        sht.range("B2").value = int(live_nav_series.size)
        if nav_drift_df is not None and not nav_drift_df.empty:
            sht.range("A4").options(index=False).value = nav_drift_df
        else:
            sht.range("A4").value = ("(monthly drift table inactive — set "
                                     "LIVE_TRADING_START_DATE in config to enable)")
    except Exception as e:
        print(f"[drift] could not write Drift_NAV sheet: {e}")


def _write_cash_ledger_sheet(wb, ledger_df: pd.DataFrame) -> None:
    """Render the cash ledger to an Excel sheet. Overwrites each run."""
    try:
        sht = get_or_clear_sheet(wb, "Cash_Ledger")
        if ledger_df is None or ledger_df.empty:
            sht.range("A1").value = "(cash ledger empty — first run after this update populates it)"
            return
        # Summary band across the top.
        latest = ledger_df.iloc[-1]
        n_runs = len(ledger_df)
        sht.range("A1").value = "Target Portfolio (AUD)"
        sht.range("B1").value = round(TARGET_PORTFOLIO_VALUE_AUD, 2)
        sht.range("A2").value = "Latest Portfolio (AUD)"
        sht.range("B2").value = float(latest["portfolio_value_aud"])
        sht.range("A3").value = "Total Drift vs Target"
        sht.range("B3").value = float(latest["drift_vs_target_aud"])
        sht.range("A4").value = "Total Drift vs Start (run 1)"
        sht.range("B4").value = float(latest["drift_vs_start_aud"])
        sht.range("A5").value = "Cum. Brokerage (all runs)"
        sht.range("B5").value = float(latest["cum_brokerage_aud"])
        sht.range("A6").value = "Cum. CGT (all runs)"
        sht.range("B6").value = float(latest["cum_cgt_aud"])
        sht.range("A7").value = "Total Cost (Brokerage + CGT)"
        sht.range("B7").value = float(latest["cum_brokerage_aud"] + latest["cum_cgt_aud"])
        sht.range("A8").value = "Runs recorded"
        sht.range("B8").value = int(n_runs)
        # Per-run detail starts below.
        display_cols = [
            "date", "selected_mode",
            "portfolio_value_aud", "net_invested_aud", "cash_balance_aud",
            "delta_vs_prev_aud",
            "brokerage_this_run_aud", "cgt_this_run_aud",
            "loss_carry_forward_tax_aud",
            "cum_brokerage_aud", "cum_cgt_aud",
            "drift_vs_start_aud", "drift_vs_target_aud",
            "unexplained_delta_aud",
        ]
        present = [c for c in display_cols if c in ledger_df.columns]
        out_df = ledger_df[present].copy()
        out_df.rename(columns={
            "date": "Date",
            "selected_mode": "Mode",
            "portfolio_value_aud": "Portfolio (AUD)",
            "net_invested_aud": "Net Invested (AUD)",
            "cash_balance_aud": "Cash (AUD)",
            "delta_vs_prev_aud": "Δ vs Prior (AUD)",
            "brokerage_this_run_aud": "Brokerage (AUD)",
            "cgt_this_run_aud": "CGT (AUD)",
            "loss_carry_forward_tax_aud": "Tax Saved (AUD)",
            "cum_brokerage_aud": "Cum. Brokerage",
            "cum_cgt_aud": "Cum. CGT",
            "drift_vs_start_aud": "Drift vs Start",
            "drift_vs_target_aud": "Drift vs Target",
            "unexplained_delta_aud": "Unexplained Δ",
        }, inplace=True)
        sht.range("A10").options(index=False).value = out_df
    except Exception as e:
        print(f"[cash] could not write Cash_Ledger sheet: {e}")


def _write_tilts_sheet(wb, tilts_df, sheet_name="Tilts"):
    sht = get_or_clear_sheet(wb, sheet_name)

    out = tilts_df.reset_index().rename(columns={"index": "Factor"})
    out = out[["Factor","Target","Band","Use?"]]
    sht.range("A1").value = [["Factor","Target","Band","Use?"]]
    sht.range("A2").options(index=False, header=False).value = out
    last_row = 1 + len(out)
    set_number_formats(sht, {
        f"B2:B{last_row}": "0.000",
        f"C2:C{last_row}": "0.000",
    })
    set_truefalse_validation(sht, f"D2:D{last_row}")
    sht.autofit()


def _autofit_table_width(table, df, total_width_cm=12.02):
    """Auto-fit PPT table column widths from a DataFrame's content."""
    def est_width(text):
        return len(str(text)) * 0.22  # empirical avg for 9pt Calibri
    est_widths = []
    for col in df.columns:
        header_w = est_width(col)
        data_w = max(est_width(v) for v in df[col].astype(str)) if len(df) else header_w
        width = max(header_w, data_w)
        if col.lower() in ("current", "target", "change"):
            width = max(width, 1.85)
        elif col.lower() == "security":
            width = max(width, 1.778)
        est_widths.append(width)
    total_est = sum(est_widths)
    scale = total_width_cm / total_est if total_est else 1.0
    for j, est in enumerate(est_widths):
        table.columns[j].width = Cm(est * scale)


def _write_holdings_sheet(wb, prices, units, include_flags,
                          sheet_name="Holdings", fx_to_aud_map=None):
    if fx_to_aud_map is None:
        usd_aud = get_usd_aud_fx()
        fx_to_aud_map = fx_to_aud_for_tickers(prices.columns, usd_aud)

    tickers_all = [
        t for t in prices.columns
        if t != "PortfolioValue"
    ]
    last_px = prices.ffill().iloc[-1]
    rows = []
    units_s = pd.Series(units)
    include_s = pd.Series(include_flags)
    for t in tickers_all:
        # Default True: tickers added to the universe but not yet in the
        # Holdings dict should be tradable. False here previously silently
        # froze every newly-added ticker — make_trade_plan would override
        # tgt_units with cur_units (=0), producing 0 trades despite the
        # ensemble recommending them. Aligns with make_trade_plan's own
        # fillna(True) default for missing include_flags entries.
        inc = bool(include_s.get(t, True))
        rows.append({
            "Security": t,
            "Units": float(units_s.get(t, 0.0)),
            "Last Price": float(pd.Series(last_px).get(t, np.nan)),
            "FX to AUD": float(pd.Series(fx_to_aud_map).get(t, 1.0)),
            "Market Value": 0.0,
            "Weight": 0.0,
            "Include?": inc,
        })
    df = pd.DataFrame(rows)

    sht = get_or_clear_sheet(wb, sheet_name)

    sht.range('A1').value = [["Security","Units","Last Price","FX to AUD","Market Value","Weight","Include?"]]
    sht.range('A2').options(index=False, header=False).value = df
    n = len(df); last_row = 1 + n
    if n >= 1:
        sht.range('E2').formula = "=B2*C2*D2"
        if n > 1:
            sht.range(f"E2:E{last_row}").api.FillDown()
        sumif_den = f"SUMIF($G$2:$G${last_row},TRUE,$E$2:$E${last_row})"
        sht.range('F2').formula = f"=IF({sumif_den}=0,0,IF($G2,E2/{sumif_den},0))"
        if n > 1:
            sht.range(f"F2:F{last_row}").api.FillDown()
        set_truefalse_validation(sht, f"G2:G{last_row}")
        set_number_formats(sht, {
            f"C2:C{last_row}": "0.0000",
            f"D2:D{last_row}": "0.0000",
            f"E2:E{last_row}": "$0.00",
            f"F2:F{last_row}": "0.00%",
        })
    sht.autofit()
