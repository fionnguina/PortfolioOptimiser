"""Holdings & factor-tilts dialog (CustomTkinter + ttk fallback) + seed readers.

Extracted from Portfolio_Optimiser.py BLOCK 4 (module split, 2026-07-08).

Engine-injected callbacks — heavy pipeline functions set on this module by
the engine's _wire_dialogs_module() right before the dialog opens (injection
avoids a circular import; some are defined AFTER the engine's block-4
position):
    _fetch_prices_for_new_tickers(prices, new_tickers) -> DataFrame
    ask_tradeplan_portfolio_choice() -> str
    recommended_tilts_for_universe(included_tickers, factor_index) -> Series

Config mirrors — seeded by the engine pre-dialog and SYNCED BACK post-dialog
(the dialog's Save handlers write these via globals(), which now targets THIS
module's namespace):
    OPEN_EXCEL_AFTER_SAVE, OPEN_PPT_AFTER_SAVE, TRADE_PLAN_MODE, TILT_FACTORS

Cross-module deps: factors (region mapping + regions.json persistence).
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from factors import (
    FF5_REGION_URLS,
    region_for_ticker,
    _load_regions_json,
    _save_regions_json,
)

# --- engine-injected (see module docstring) ---------------------------------
_fetch_prices_for_new_tickers = None
ask_tradeplan_portfolio_choice = None
recommended_tilts_for_universe = None
OPEN_EXCEL_AFTER_SAVE = True
OPEN_PPT_AFTER_SAVE = True
TILT_FACTORS = ("Mkt-RF", "SMB", "HML", "RMW", "CMA", "MOM")
TRADE_PLAN_MODE = "ask"

# =====================================================================
# BLOCK 4 Creating the Stock Holdings Dialog Box
# =====================================================================
# -------------------------------
# 2) GUI portfolio editor (Tkinter) + helpers
# -------------------------------
import tkinter as _tk
from tkinter import ttk as _ttk, messagebox as _mb

# CustomTkinter gives the holdings/tilts editor a modern look. It is optional:
# if it is not installed the app falls back to the classic ttk dialog below.
try:
    import customtkinter as _ctk
    HAS_CTK = True
except Exception:
    _ctk = None
    HAS_CTK = False

_TRUTHY_STRINGS = {"TRUE", "1", "Y", "YES", "T"}


def _to_bool_flag(value, default=False):
    """Parse mixed truthy values commonly found in spreadsheets."""
    if pd.isna(value):
        return bool(default)
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    return str(value).strip().upper() in _TRUTHY_STRINGS


def _to_float(value, default=0.0):
    """Convert value to float with safe fallback."""
    try:
        if value is None:
            return float(default)
        txt = str(value).strip()
        if txt == "":
            return float(default)
        return float(txt)
    except Exception:
        return float(default)


# -------- File-based seed readers (no COM, reliable) --------
def _read_holdings_seed_from_path(xl_path, sheet_name="Holdings"):
    try:
        df = pd.read_excel(xl_path, sheet_name=sheet_name)
    except Exception as e:
        print(f"[seed-path] holdings: {e} -> EMPTY")
        return pd.Series(dtype=float), {}

    if not isinstance(df, pd.DataFrame) or df.empty or "Security" not in df.columns:
        print("[seed-path] holdings: empty/malformed -> EMPTY")
        return pd.Series(dtype=float), {}

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    df["Security"] = df["Security"].astype(str).str.strip()
    df = df[df["Security"] != ""]

    if "Units" not in df.columns:
        for alt in ["Curr Units", "Current Units", "Holdings", "Qty"]:
            if alt in df.columns:
                df["Units"] = df[alt]
                break

    work = pd.DataFrame(index=df.index)
    work["Security"] = df["Security"]
    work["Units"] = pd.to_numeric(df.get("Units", 0.0), errors="coerce").fillna(0.0)

    if "Include?" in df.columns:
        work["Include"] = df["Include?"].map(lambda x: _to_bool_flag(x, default=True))
    else:
        work["Include"] = True

    # Consolidate duplicate security rows from the sheet.
    agg = work.groupby("Security", as_index=True).agg({"Units": "sum", "Include": "max"})
    units = agg["Units"].astype(float)
    include = agg["Include"].astype(bool).to_dict()
    return units, include


def _read_tilts_seed_from_path(xl_path, sheet_name="Tilts"):
    factors = list(TILT_FACTORS) if "TILT_FACTORS" in globals() else ["Mkt-RF", "SMB", "HML", "RMW", "CMA"]
    default = pd.DataFrame(
        {
            "Target": [1.0] + [0.0] * (len(factors) - 1),
            "Band": [0.05] * len(factors),
            "Use?": [True] + [False] * (len(factors) - 1),
        },
        index=factors,
    )

    try:
        df = pd.read_excel(xl_path, sheet_name=sheet_name)
    except Exception as e:
        print(f"[seed-path] tilts: {e} -> DEFAULTS")
        return default

    if not isinstance(df, pd.DataFrame) or df.empty:
        print("[seed-path] tilts: empty -> DEFAULTS")
        return default

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    required = {"Factor", "Target", "Band", "Use?"}
    if not required.issubset(df.columns):
        print("[seed-path] tilts: malformed -> DEFAULTS")
        return default

    df["Factor"] = df["Factor"].astype(str).str.strip()
    df = df[df["Factor"] != ""]
    df = df.set_index("Factor").reindex(factors)

    out = default.copy()
    out.loc[df.index, "Target"] = pd.to_numeric(df["Target"], errors="coerce")
    out.loc[df.index, "Band"] = pd.to_numeric(df["Band"], errors="coerce")
    out.loc[df.index, "Use?"] = df["Use?"].map(lambda x: _to_bool_flag(x, default=False))

    out["Target"] = out["Target"].fillna(default["Target"]).astype(float)
    out["Band"] = out["Band"].fillna(default["Band"]).astype(float)
    out["Use?"] = out["Use?"].fillna(default["Use?"]).astype(bool)
    return out.reindex(factors)


# -------------------------------
# Combined dialog (one window)
# -------------------------------
def _edit_holdings_dialog_ttk(
    prices,
    exclude,
    seed_units,
    seed_include,
    seed_tilts,
    title="Edit Holdings & Factor Tilts",
):
    """Classic Tkinter/ttk holdings + factor-tilts editor (fallback when CustomTkinter is unavailable).

    Returns:
        (units_series, last_price_series, prices_df, include_flags_dict, tilts_df, portfolio_value_override)
    """
    exclude = set(exclude or [])
    tickers_all = [
        t
        for t in prices.columns
        if t != "PortfolioValue" and not str(t).startswith("^")
    ]

    if isinstance(prices, pd.DataFrame) and not prices.empty:
        last_px = prices.ffill().iloc[-1]
    else:
        last_px = pd.Series(dtype=float)

    # Factor list (use global TILT_FACTORS if available so MOM is included)
    if isinstance(seed_tilts, pd.DataFrame) and not seed_tilts.empty:
        factors = list(seed_tilts.index)
    elif "TILT_FACTORS" in globals():
        factors = list(TILT_FACTORS)
    else:
        factors = ["Mkt-RF", "SMB", "HML", "RMW", "CMA", "MOM"]

    if not isinstance(seed_tilts, pd.DataFrame) or seed_tilts.empty:
        seed_tilts = pd.DataFrame(
            {
                "Target": [1.0] + [0.0] * (len(factors) - 1),
                "Band": [0.05] * len(factors),
                "Use?": [True] + [False] * (len(factors) - 1),
            },
            index=factors,
        )

    # Normalize seed maps once
    seed_units_map = pd.Series(seed_units, dtype=float)
    seed_include_map = pd.Series(seed_include, dtype=bool)

    root = _tk.Tk()
    root.title(title)
    root.geometry("1080x720")
    root.minsize(920, 560)

    # Global toggles (Open Excel / PPT)
    open_excel_var = _tk.BooleanVar(master=root, value=bool(globals().get("OPEN_EXCEL_AFTER_SAVE", True)))
    open_ppt_var = _tk.BooleanVar(master=root, value=bool(globals().get("OPEN_PPT_AFTER_SAVE", True)))

    # Portfolio value mode
    portfolio_value_var = _tk.StringVar(master=root, value="")
    use_portfolio_value = _tk.BooleanVar(master=root, value=False)

    # Top options row for portfolio-value mode
    frm_top_opts = _ttk.Frame(root)
    frm_top_opts.pack(fill="x")

    _ttk.Checkbutton(
        frm_top_opts,
        text="Build from Portfolio Value (AUD) instead of units",
        variable=use_portfolio_value,
    ).pack(anchor="w", pady=4)

    frm_val = _ttk.Frame(frm_top_opts)
    frm_val.pack(anchor="w")
    _ttk.Label(frm_val, text="Portfolio Value (AUD):").pack(side="left")
    _ttk.Entry(frm_val, textvariable=portfolio_value_var, width=18).pack(side="left", padx=6)

    # Main layout
    frm_main = _ttk.Frame(root, padding=10)
    frm_main.pack(fill="both", expand=True)

    # Left: holdings
    frm_left = _ttk.LabelFrame(frm_main, text="Holdings", padding=10)
    frm_left.pack(side="left", fill="both", expand=True, padx=(0, 6))
    for i in range(3):
        frm_left.rowconfigure(i, weight=(1 if i == 1 else 0))
    frm_left.columnconfigure(0, weight=1)

    header = _ttk.Frame(frm_left)
    header.grid(row=0, column=0, sticky="ew")
    _ttk.Label(header, text="Inc?", width=5).grid(row=0, column=0, sticky="w")
    _ttk.Label(header, text="Del?", width=5).grid(row=0, column=1, sticky="w")
    _ttk.Label(header, text="Security", width=20).grid(row=0, column=2, sticky="w")
    _ttk.Label(header, text="Units", width=14).grid(row=0, column=3, sticky="w")
    _ttk.Label(header, text="Last Price", width=12).grid(row=0, column=4, sticky="w")

    # Scrollable list
    list_container = _ttk.Frame(frm_left)
    list_container.grid(row=1, column=0, sticky="nsew", pady=(4, 6))
    list_container.rowconfigure(0, weight=1)
    list_container.columnconfigure(0, weight=1)

    canvas = _tk.Canvas(list_container, highlightthickness=0)
    scroll_y = _ttk.Scrollbar(list_container, orient="vertical", command=canvas.yview)
    body = _ttk.Frame(canvas)
    body.bind("<Configure>", lambda _e: canvas.configure(scrollregion=canvas.bbox("all")))
    canvas.create_window((0, 0), window=body, anchor="nw")
    canvas.configure(yscrollcommand=scroll_y.set)
    canvas.grid(row=0, column=0, sticky="nsew")
    scroll_y.grid(row=0, column=1, sticky="ns")

    def _on_mousewheel(event):
        if event.delta:
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        elif getattr(event, "num", None) in (4, 5):
            canvas.yview_scroll(-1 if event.num == 4 else 1, "units")

    body.bind("<Enter>", lambda _e: canvas.bind_all("<MouseWheel>", _on_mousewheel))
    body.bind("<Leave>", lambda _e: canvas.unbind_all("<MouseWheel>"))
    canvas.bind_all("<Button-4>", _on_mousewheel)
    canvas.bind_all("<Button-5>", _on_mousewheel)

    row_vars = {}

    def _format_units_for_entry(value):
        f = _to_float(value, default=0.0)
        if abs(f - round(f)) < 1e-9:
            return str(int(round(f)))
        return str(f)

    def _add_row(ticker, units_default=0.0, include_default=True, disabled=False):
        r = len(row_vars) + 1
        v_inc = _tk.BooleanVar(value=(False if disabled else bool(include_default)))
        v_del = _tk.BooleanVar(value=False)
        v_units = _tk.StringVar(value=("0" if disabled else _format_units_for_entry(units_default)))

        row_frame = _ttk.Frame(body)

        def _on_delete_toggle():
            if bool(v_del.get()):
                row_frame.grid_remove()
                v_inc.set(False)
            else:
                row_frame.grid()

        chk_inc = _ttk.Checkbutton(row_frame, variable=v_inc)
        chk_del = _ttk.Checkbutton(row_frame, variable=v_del, command=_on_delete_toggle)
        ent_units = _ttk.Entry(row_frame, textvariable=v_units, width=16)
        lbl_t = _ttk.Label(row_frame, text=str(ticker), width=20)

        last_px_value = _to_float(last_px.get(ticker, np.nan), default=np.nan)
        last_px_str = f"{last_px_value:.4f}" if np.isfinite(last_px_value) else "nan"
        lbl_px = _ttk.Label(row_frame, text=last_px_str, width=12)

        if disabled:
            chk_inc.state(["disabled"])
            ent_units.state(["disabled"])
            lbl_t.configure(foreground="#888")

        row_frame.grid(row=r, column=0, columnspan=5, sticky="ew", pady=2)
        chk_inc.grid(row=0, column=0, sticky="w", padx=(0, 6))
        chk_del.grid(row=0, column=1, sticky="w", padx=(0, 6))
        lbl_t.grid(row=0, column=2, sticky="w", padx=(0, 6))
        ent_units.grid(row=0, column=3, sticky="w", padx=(0, 6))
        lbl_px.grid(row=0, column=4, sticky="w", padx=(0, 6))

        row_vars[ticker] = {
            "inc": v_inc,
            "del": v_del,
            "units": v_units,
            "disabled": bool(disabled),
            "lbl_px": lbl_px,
            "row_frame": row_frame,
            "ent_units": ent_units,
        }

    def _sync_units_state(*_args):
        lock = bool(use_portfolio_value.get())
        for _t, vs in row_vars.items():
            ent = vs.get("ent_units")
            if ent is None:
                continue
            if lock and not vs.get("disabled", False):
                ent.state(["disabled"])
            elif not vs.get("disabled", False):
                ent.state(["!disabled"])

    use_portfolio_value.trace_add("write", _sync_units_state)

    # Prefill rows
    for t in tickers_all:
        disabled = t in exclude
        inc_default = bool(seed_include_map.get(t, True)) and not disabled
        units_default = float(seed_units_map.get(t, 0.0))
        _add_row(t, units_default=units_default, include_default=inc_default, disabled=disabled)

    _sync_units_state()

    # Add-holding box
    add_box = _ttk.LabelFrame(frm_left, text="Add holding", padding=10)
    add_box.grid(row=2, column=0, sticky="ew")

    _ttk.Label(add_box, text="Ticker").grid(row=0, column=0, sticky="w")
    ent_new_ticker = _ttk.Entry(add_box, width=18)
    ent_new_ticker.grid(row=0, column=1, sticky="w", padx=(4, 12))

    _ttk.Label(add_box, text="Units").grid(row=0, column=2, sticky="w")
    ent_new_units = _ttk.Entry(add_box, width=14)
    ent_new_units.grid(row=0, column=3, sticky="w", padx=(4, 12))

    _btn_add = _ttk.Button(add_box, text="Add")
    _btn_add.grid(row=0, column=4, sticky="w")

    added_tickers = []

    def _do_add():
        t = ent_new_ticker.get().strip().upper()
        if not t:
            _mb.showwarning("Add holding", "Please enter a ticker.")
            return

        if t in row_vars:
            vs = row_vars[t]
            if bool(vs["del"].get()):
                vs["del"].set(False)
                vs["inc"].set(True)
                vs["row_frame"].grid()
                _mb.showinfo("Add holding", f"{t} restored.")
            else:
                _mb.showinfo("Add holding", f"{t} already listed.")
            return

        txt_units = ent_new_units.get().strip()
        try:
            u = float(txt_units) if txt_units else 0.0
        except ValueError:
            _mb.showwarning("Add holding", "Units must be numeric.")
            return

        _add_row(t, units_default=u, include_default=True, disabled=(t in exclude))
        _sync_units_state()

        added_tickers.append(t)
        ent_new_ticker.delete(0, _tk.END)
        ent_new_units.delete(0, _tk.END)

    _btn_add.configure(command=_do_add)

    # Right panel - Factor Tilts
    frm_right = _ttk.LabelFrame(frm_main, text="Factor Tilts", padding=10)
    frm_right.pack(side="right", fill="y", padx=(6, 0))

    _ttk.Label(frm_right, text="Use?", width=5).grid(row=0, column=0, sticky="w")
    _ttk.Label(frm_right, text="Factor", width=12).grid(row=0, column=1, sticky="w")
    _ttk.Label(frm_right, text="Target beta", width=10).grid(row=0, column=2, sticky="w")
    _ttk.Label(frm_right, text="Band", width=10).grid(row=0, column=3, sticky="w")

    tilt_vars = {}
    for i, f in enumerate(factors, start=1):
        use_default = bool(seed_tilts.loc[f, "Use?"]) if f in seed_tilts.index else False
        tgt_default = float(seed_tilts.loc[f, "Target"]) if f in seed_tilts.index else 0.0
        band_default = float(seed_tilts.loc[f, "Band"]) if f in seed_tilts.index else 0.05

        v_use = _tk.BooleanVar(value=use_default)
        v_tgt = _tk.StringVar(value=f"{tgt_default:.3f}")
        v_bnd = _tk.StringVar(value=f"{band_default:.3f}")

        _ttk.Checkbutton(frm_right, variable=v_use).grid(row=i, column=0, sticky="w", pady=2)
        _ttk.Label(frm_right, text=f, width=12).grid(row=i, column=1, sticky="w", pady=2)
        _ttk.Entry(frm_right, textvariable=v_tgt, width=10).grid(row=i, column=2, sticky="w", pady=2)
        _ttk.Entry(frm_right, textvariable=v_bnd, width=10).grid(row=i, column=3, sticky="w", pady=2)
        tilt_vars[f] = (v_use, v_tgt, v_bnd)

    def _compute_recommended_tilts():
        """Achievable factor-tilt targets for included tickers (pipeline-consistent factor moments)."""
        incl = [t for t, vs in row_vars.items() if vs["inc"].get() and (not vs["del"].get())]
        return recommended_tilts_for_universe(incl, seed_tilts.index)

    def _apply_recommended_tilts():
        rec = _compute_recommended_tilts()
        for f in factors:
            v_use, v_tgt, v_bnd = tilt_vars[f]
            v_use.set(True)
            v_tgt.set(f"{float(rec.get(f, 0.0)):.3f}")
            if not v_bnd.get().strip():
                v_bnd.set("0.200")
        _mb.showinfo("Tilts", "Recommended tilts applied.\n(You can still edit before Save.)")

    btn_reco = _ttk.Button(frm_right, text="Auto-recommend tilts", command=_apply_recommended_tilts)
    btn_reco.grid(row=len(factors) + 2, column=0, columnspan=4, sticky="ew", pady=(12, 0))

    # Bottom options + buttons
    frm_bottom_opts = _ttk.Frame(root, padding=(10, 0, 10, 0))
    frm_bottom_opts.pack(fill="x")
    _ttk.Checkbutton(frm_bottom_opts, text="Open Excel after Save", variable=open_excel_var).pack(anchor="w")
    _ttk.Checkbutton(frm_bottom_opts, text="Open PowerPoint after Save", variable=open_ppt_var).pack(anchor="w")

    frm_btns = _ttk.Frame(root, padding=(10, 0, 10, 10))
    frm_btns.pack(fill="x")

    def _reset_to_seed_units():
        for t, vs in row_vars.items():
            if vs.get("disabled"):
                continue
            vs["units"].set(_format_units_for_entry(seed_units_map.get(t, 0.0)))
        _mb.showinfo("Holdings", "Units reset to values loaded from Excel at run start.")

    _ttk.Button(frm_btns, text="Reset to Seed", command=_reset_to_seed_units).pack(side="left", padx=6)
    _ttk.Button(frm_btns, text="Cancel", command=root.destroy).pack(side="right", padx=6)

    def _on_save():
        nonlocal prices

        if added_tickers:
            prices = _fetch_prices_for_new_tickers(added_tickers, prices)

        to_delete = []
        units_out = {}
        include_flags = {}

        if not isinstance(prices, pd.DataFrame):
            prices = pd.DataFrame()

        for t, vs in row_vars.items():
            mark_delete = bool(vs["del"].get())
            disabled = bool(vs["disabled"])
            inc = bool(vs["inc"].get()) and (not disabled) and (not mark_delete)
            include_flags[t] = inc

            if mark_delete:
                to_delete.append(t)
                continue

            if not disabled:
                units_out[t] = _to_float(vs["units"].get(), default=0.0)

            # Refresh last-price label if data is available.
            if not prices.empty and t in prices.columns:
                try:
                    lp = float(prices.ffill().iloc[-1].get(t, np.nan))
                    if np.isfinite(lp):
                        vs["lbl_px"].configure(text=f"{lp:.4f}")
                except Exception:
                    pass

        if to_delete and not prices.empty:
            keep_cols = [c for c in prices.columns if c not in set(to_delete)]
            prices = prices.reindex(columns=keep_cols)

        units_ser = pd.Series(units_out, dtype=float)
        if not prices.empty and len(units_ser.index) > 0:
            last_price_ser = prices.ffill().iloc[-1].reindex(units_ser.index)
        else:
            last_price_ser = pd.Series(index=units_ser.index, dtype=float)

        out_rows = []
        for f, (v_use, v_tgt, v_bnd) in tilt_vars.items():
            tgt = _to_float(v_tgt.get(), default=0.0)
            bnd = _to_float(v_bnd.get(), default=0.05)
            out_rows.append({"Factor": f, "Target": tgt, "Band": bnd, "Use?": bool(v_use.get())})
        tilts_df = pd.DataFrame(out_rows).set_index("Factor").reindex(factors)

        globals()["OPEN_EXCEL_AFTER_SAVE"] = bool(open_excel_var.get())
        globals()["OPEN_PPT_AFTER_SAVE"] = bool(open_ppt_var.get())

        # Trade-plan portfolio choice. Respect an existing "auto" override
        # (set at file top) — that means the user wants the engine to decide
        # via Sharpe each run, not to be re-prompted here.
        _existing_mode = str(globals().get("TRADE_PLAN_MODE", "")).lower().strip()
        if _existing_mode == "auto":
            pass  # leave as-is; downstream auto-picker handles selection
        elif "ipykernel" in sys.modules:
            # Jupyter: modal messageboxes can freeze some kernels — use safe default.
            globals()["TRADE_PLAN_MODE"] = "ensemble"
        else:
            if "ask_tradeplan_portfolio_choice" in globals():
                globals()["TRADE_PLAN_MODE"] = ask_tradeplan_portfolio_choice()
            else:
                globals()["TRADE_PLAN_MODE"] = "ensemble"

        portfolio_value_override = None
        if bool(use_portfolio_value.get()):
            raw = (
                str(portfolio_value_var.get())
                .replace(",", "")
                .replace("$", "")
                .replace("AUD", "")
                .strip()
            )
            portfolio_value_override = _to_float(raw, default=np.nan)
            if not np.isfinite(portfolio_value_override):
                portfolio_value_override = None

        _edit_holdings_dialog_ttk.result = (
            units_ser,
            last_price_ser,
            prices,
            include_flags,
            tilts_df,
            portfolio_value_override,
        )
        root.destroy()

    _ttk.Button(frm_btns, text="Save", command=_on_save).pack(side="right", padx=6)

    root.protocol("WM_DELETE_WINDOW", root.destroy)
    root.mainloop()
    return getattr(_edit_holdings_dialog_ttk, "result", None)


def _edit_holdings_dialog_ctk(
    prices,
    exclude,
    seed_units,
    seed_include,
    seed_tilts,
    title="Edit Holdings & Factor Tilts",
):
    """Modern CustomTkinter holdings + factor-tilts editor.

    Behaviourally identical to the classic dialog and returns the same 6-tuple:
        (units_series, last_price_series, prices_df, include_flags_dict, tilts_df, portfolio_value_override)
    """
    exclude = set(exclude or [])
    tickers_all = [
        t for t in prices.columns
        if t != "PortfolioValue" and not str(t).startswith("^")
    ]

    if isinstance(prices, pd.DataFrame) and not prices.empty:
        last_px = prices.ffill().iloc[-1]
    else:
        last_px = pd.Series(dtype=float)

    if isinstance(seed_tilts, pd.DataFrame) and not seed_tilts.empty:
        factors = list(seed_tilts.index)
    elif "TILT_FACTORS" in globals():
        factors = list(TILT_FACTORS)
    else:
        factors = ["Mkt-RF", "SMB", "HML", "RMW", "CMA", "MOM"]

    if not isinstance(seed_tilts, pd.DataFrame) or seed_tilts.empty:
        seed_tilts = pd.DataFrame(
            {
                "Target": [1.0] + [0.0] * (len(factors) - 1),
                "Band": [0.05] * len(factors),
                "Use?": [True] + [False] * (len(factors) - 1),
            },
            index=factors,
        )

    seed_units_map = pd.Series(seed_units, dtype=float)
    seed_include_map = pd.Series(seed_include, dtype=bool)

    _ctk.set_appearance_mode("system")
    _ctk.set_default_color_theme("blue")

    root = _ctk.CTk()
    root.title(title)
    root.geometry("1120x760")
    root.minsize(960, 600)

    _result = {"value": None}
    added_tickers = []

    open_excel_var = _tk.BooleanVar(master=root, value=bool(globals().get("OPEN_EXCEL_AFTER_SAVE", True)))
    open_ppt_var = _tk.BooleanVar(master=root, value=bool(globals().get("OPEN_PPT_AFTER_SAVE", True)))
    portfolio_value_var = _tk.StringVar(master=root, value="")
    use_portfolio_value = _tk.BooleanVar(master=root, value=False)

    title_font = _ctk.CTkFont(size=15, weight="bold")

    # --- Top bar: portfolio-value mode ---
    top = _ctk.CTkFrame(root)
    top.pack(fill="x", padx=12, pady=(12, 6))
    _ctk.CTkCheckBox(
        top,
        text="Build from Portfolio Value (AUD) instead of units",
        variable=use_portfolio_value,
    ).pack(side="left", padx=12, pady=10)
    _ctk.CTkLabel(top, text="Portfolio Value (AUD):").pack(side="left", padx=(20, 6))
    _ctk.CTkEntry(top, textvariable=portfolio_value_var, width=160).pack(side="left", padx=(0, 12))

    # --- Main split: holdings (left) + tilts (right) ---
    main = _ctk.CTkFrame(root, fg_color="transparent")
    main.pack(fill="both", expand=True, padx=12, pady=6)

    left = _ctk.CTkFrame(main)
    left.pack(side="left", fill="both", expand=True, padx=(0, 6))
    _ctk.CTkLabel(left, text="Holdings", font=title_font).pack(anchor="w", padx=14, pady=(12, 2))

    hdr = _ctk.CTkFrame(left, fg_color="transparent")
    hdr.pack(fill="x", padx=14, pady=(4, 0))
    for col, (txt, w) in enumerate(
        [("Inc?", 44), ("Del?", 44), ("Security", 150), ("Units", 120), ("Last Price", 100)]
    ):
        _ctk.CTkLabel(hdr, text=txt, width=w, anchor="w").grid(row=0, column=col, padx=4, sticky="w")

    rows_frame = _ctk.CTkScrollableFrame(left, fg_color="transparent")
    rows_frame.pack(fill="both", expand=True, padx=10, pady=6)
    rows_frame.grid_columnconfigure(0, weight=1)

    row_vars = {}

    def _format_units_for_entry(value):
        f = _to_float(value, default=0.0)
        if abs(f - round(f)) < 1e-9:
            return str(int(round(f)))
        return str(f)

    def _add_row(ticker, units_default=0.0, include_default=True, disabled=False):
        r = len(row_vars)
        v_inc = _tk.BooleanVar(master=root, value=(False if disabled else bool(include_default)))
        v_del = _tk.BooleanVar(master=root, value=False)
        v_units = _tk.StringVar(master=root, value=("0" if disabled else _format_units_for_entry(units_default)))

        rf = _ctk.CTkFrame(rows_frame, fg_color="transparent")

        def _on_delete_toggle():
            if bool(v_del.get()):
                rf.grid_remove()
                v_inc.set(False)
            else:
                rf.grid()

        chk_inc = _ctk.CTkCheckBox(rf, text="", width=44, variable=v_inc)
        chk_del = _ctk.CTkCheckBox(rf, text="", width=44, variable=v_del, command=_on_delete_toggle)
        lbl_t = _ctk.CTkLabel(rf, text=str(ticker), width=150, anchor="w")
        ent_units = _ctk.CTkEntry(rf, textvariable=v_units, width=120)

        last_px_value = _to_float(last_px.get(ticker, np.nan), default=np.nan)
        last_px_str = f"{last_px_value:.4f}" if np.isfinite(last_px_value) else "nan"
        lbl_px = _ctk.CTkLabel(rf, text=last_px_str, width=100, anchor="w")

        if disabled:
            chk_inc.configure(state="disabled")
            ent_units.configure(state="disabled")
            lbl_t.configure(text_color="gray")

        rf.grid(row=r, column=0, sticky="ew", pady=2)
        chk_inc.grid(row=0, column=0, padx=4, sticky="w")
        chk_del.grid(row=0, column=1, padx=4, sticky="w")
        lbl_t.grid(row=0, column=2, padx=4, sticky="w")
        ent_units.grid(row=0, column=3, padx=4, sticky="w")
        lbl_px.grid(row=0, column=4, padx=4, sticky="w")

        row_vars[ticker] = {
            "inc": v_inc, "del": v_del, "units": v_units,
            "disabled": bool(disabled), "lbl_px": lbl_px,
            "row_frame": rf, "ent_units": ent_units,
        }

    def _sync_units_state(*_args):
        lock = bool(use_portfolio_value.get())
        for _t, vs in row_vars.items():
            ent = vs.get("ent_units")
            if ent is None:
                continue
            if lock and not vs.get("disabled", False):
                ent.configure(state="disabled")
            elif not vs.get("disabled", False):
                ent.configure(state="normal")

    use_portfolio_value.trace_add("write", _sync_units_state)

    for t in tickers_all:
        disabled = t in exclude
        inc_default = bool(seed_include_map.get(t, True)) and not disabled
        units_default = float(seed_units_map.get(t, 0.0))
        _add_row(t, units_default=units_default, include_default=inc_default, disabled=disabled)

    _sync_units_state()

    # Add-holding box (includes factor-region selector so the user picks the
    # correct Ken French region at add-time — IEU.AX style ASX-listed-but-
    # foreign-tracking cases can't be inferred from the suffix).
    add_box = _ctk.CTkFrame(left)
    add_box.pack(fill="x", padx=14, pady=(4, 12))
    _ctk.CTkLabel(add_box, text="Add holding", font=title_font).grid(
        row=0, column=0, columnspan=6, sticky="w", padx=8, pady=(8, 4)
    )
    _ctk.CTkLabel(add_box, text="Ticker").grid(row=1, column=0, sticky="w", padx=(8, 4), pady=(0, 8))
    ent_new_ticker = _ctk.CTkEntry(add_box, width=140)
    ent_new_ticker.grid(row=1, column=1, sticky="w", padx=(0, 12), pady=(0, 8))
    _ctk.CTkLabel(add_box, text="Units").grid(row=1, column=2, sticky="w", padx=(0, 4), pady=(0, 8))
    ent_new_units = _ctk.CTkEntry(add_box, width=110)
    ent_new_units.grid(row=1, column=3, sticky="w", padx=(0, 12), pady=(0, 8))
    _ctk.CTkLabel(add_box, text="Region").grid(row=1, column=4, sticky="w", padx=(0, 4), pady=(0, 8))
    region_options = list(FF5_REGION_URLS.keys())
    region_var = _tk.StringVar(master=root, value=region_options[0])
    region_menu = _ctk.CTkOptionMenu(add_box, values=region_options, variable=region_var, width=140)
    region_menu.grid(row=1, column=5, sticky="w", padx=(0, 12), pady=(0, 8))
    btn_add = _ctk.CTkButton(add_box, text="Add", width=80)
    btn_add.grid(row=1, column=6, sticky="w", padx=(0, 8), pady=(0, 8))

    def _auto_region_from_ticker(_event=None):
        """Update the Region dropdown to the heuristic guess as the user types."""
        try:
            t = ent_new_ticker.get().strip().upper()
            if t:
                region_var.set(region_for_ticker(t))
        except Exception:
            pass

    ent_new_ticker.bind("<KeyRelease>", _auto_region_from_ticker)
    ent_new_ticker.bind("<FocusOut>", _auto_region_from_ticker)

    def _do_add():
        t = ent_new_ticker.get().strip().upper()
        if not t:
            _mb.showwarning("Add holding", "Please enter a ticker.")
            return
        if t in row_vars:
            vs = row_vars[t]
            if bool(vs["del"].get()):
                vs["del"].set(False)
                vs["inc"].set(True)
                vs["row_frame"].grid()
                _mb.showinfo("Add holding", f"{t} restored.")
            else:
                _mb.showinfo("Add holding", f"{t} already listed.")
            return
        txt_units = ent_new_units.get().strip()
        try:
            u = float(txt_units) if txt_units else 0.0
        except ValueError:
            _mb.showwarning("Add holding", "Units must be numeric.")
            return
        # Persist the region choice into regions.json so the next pipeline run
        # uses it. Skips if the user-selected region matches what region_for_ticker
        # would have guessed anyway (no override needed for the heuristic case).
        try:
            picked = region_var.get().strip().upper()
            heuristic = region_for_ticker(t)
            if picked and picked in FF5_REGION_URLS and picked != heuristic:
                current = _load_regions_json()
                current[t] = picked
                _save_regions_json(current)
                USER_REGION_OVERRIDES[t] = picked
                print(f"[region] Saved override: {t} -> {picked}")
        except Exception as _e_save:
            print(f"[region] Could not persist region choice for {t}: {_e_save}")

        _add_row(t, units_default=u, include_default=True, disabled=(t in exclude))
        _sync_units_state()
        added_tickers.append(t)
        ent_new_ticker.delete(0, "end")
        ent_new_units.delete(0, "end")
        # Reset region picker to default
        region_var.set(region_options[0])

    btn_add.configure(command=_do_add)

    # Right panel: Factor Tilts
    right = _ctk.CTkFrame(main)
    right.pack(side="right", fill="y", padx=(6, 0))
    _ctk.CTkLabel(right, text="Factor Tilts", font=title_font).grid(
        row=0, column=0, columnspan=4, sticky="w", padx=12, pady=(12, 6)
    )
    for col, (txt, w) in enumerate(
        [("Use?", 44), ("Factor", 110), ("Target beta", 90), ("Band", 90)]
    ):
        _ctk.CTkLabel(right, text=txt, width=w, anchor="w").grid(row=1, column=col, padx=6, sticky="w")

    tilt_vars = {}
    for i, f in enumerate(factors, start=2):
        use_default = bool(seed_tilts.loc[f, "Use?"]) if f in seed_tilts.index else False
        tgt_default = float(seed_tilts.loc[f, "Target"]) if f in seed_tilts.index else 0.0
        band_default = float(seed_tilts.loc[f, "Band"]) if f in seed_tilts.index else 0.05

        v_use = _tk.BooleanVar(master=root, value=use_default)
        v_tgt = _tk.StringVar(master=root, value=f"{tgt_default:.3f}")
        v_bnd = _tk.StringVar(master=root, value=f"{band_default:.3f}")

        _ctk.CTkCheckBox(right, text="", width=44, variable=v_use).grid(row=i, column=0, padx=6, pady=3, sticky="w")
        _ctk.CTkLabel(right, text=f, width=110, anchor="w").grid(row=i, column=1, padx=6, pady=3, sticky="w")
        _ctk.CTkEntry(right, textvariable=v_tgt, width=90).grid(row=i, column=2, padx=6, pady=3, sticky="w")
        _ctk.CTkEntry(right, textvariable=v_bnd, width=90).grid(row=i, column=3, padx=6, pady=3, sticky="w")
        tilt_vars[f] = (v_use, v_tgt, v_bnd)

    def _compute_recommended_tilts():
        """Achievable factor-tilt targets for included tickers (pipeline-consistent factor moments)."""
        incl = [t for t, vs in row_vars.items() if vs["inc"].get() and (not vs["del"].get())]
        return recommended_tilts_for_universe(incl, seed_tilts.index)

    def _apply_recommended_tilts():
        rec = _compute_recommended_tilts()
        for f in factors:
            v_use, v_tgt, v_bnd = tilt_vars[f]
            v_use.set(True)
            v_tgt.set(f"{float(rec.get(f, 0.0)):.3f}")
            if not v_bnd.get().strip():
                v_bnd.set("0.200")
        _mb.showinfo("Tilts", "Recommended tilts applied.\n(You can still edit before Save.)")

    _ctk.CTkButton(right, text="Auto-recommend tilts", command=_apply_recommended_tilts).grid(
        row=len(factors) + 3, column=0, columnspan=4, sticky="ew", padx=12, pady=(14, 12)
    )

    # --- Bottom: output toggles + action buttons ---
    bottom = _ctk.CTkFrame(root, fg_color="transparent")
    bottom.pack(fill="x", padx=12, pady=(0, 12))

    opts = _ctk.CTkFrame(bottom, fg_color="transparent")
    opts.pack(side="left", anchor="w")
    _ctk.CTkCheckBox(opts, text="Open Excel after Save", variable=open_excel_var).pack(anchor="w", pady=2)
    _ctk.CTkCheckBox(opts, text="Open PowerPoint after Save", variable=open_ppt_var).pack(anchor="w", pady=2)

    def _reset_to_seed_units():
        for t, vs in row_vars.items():
            if vs.get("disabled"):
                continue
            vs["units"].set(_format_units_for_entry(seed_units_map.get(t, 0.0)))
        _mb.showinfo("Holdings", "Units reset to values loaded from Excel at run start.")

    def _on_save():
        nonlocal prices

        if added_tickers:
            prices = _fetch_prices_for_new_tickers(added_tickers, prices)

        to_delete = []
        units_out = {}
        include_flags = {}

        if not isinstance(prices, pd.DataFrame):
            prices = pd.DataFrame()

        for t, vs in row_vars.items():
            mark_delete = bool(vs["del"].get())
            disabled = bool(vs["disabled"])
            inc = bool(vs["inc"].get()) and (not disabled) and (not mark_delete)
            include_flags[t] = inc

            if mark_delete:
                to_delete.append(t)
                continue

            if not disabled:
                units_out[t] = _to_float(vs["units"].get(), default=0.0)

            if not prices.empty and t in prices.columns:
                try:
                    lp = float(prices.ffill().iloc[-1].get(t, np.nan))
                    if np.isfinite(lp):
                        vs["lbl_px"].configure(text=f"{lp:.4f}")
                except Exception:
                    pass

        if to_delete and not prices.empty:
            keep_cols = [c for c in prices.columns if c not in set(to_delete)]
            prices = prices.reindex(columns=keep_cols)

        units_ser = pd.Series(units_out, dtype=float)
        if not prices.empty and len(units_ser.index) > 0:
            last_price_ser = prices.ffill().iloc[-1].reindex(units_ser.index)
        else:
            last_price_ser = pd.Series(index=units_ser.index, dtype=float)

        out_rows = []
        for f, (v_use, v_tgt, v_bnd) in tilt_vars.items():
            tgt = _to_float(v_tgt.get(), default=0.0)
            bnd = _to_float(v_bnd.get(), default=0.05)
            out_rows.append({"Factor": f, "Target": tgt, "Band": bnd, "Use?": bool(v_use.get())})
        tilts_df = pd.DataFrame(out_rows).set_index("Factor").reindex(factors)

        globals()["OPEN_EXCEL_AFTER_SAVE"] = bool(open_excel_var.get())
        globals()["OPEN_PPT_AFTER_SAVE"] = bool(open_ppt_var.get())

        # Respect "auto" override — same logic as the other save handler.
        _existing_mode = str(globals().get("TRADE_PLAN_MODE", "")).lower().strip()
        if _existing_mode == "auto":
            pass
        elif "ipykernel" in sys.modules:
            globals()["TRADE_PLAN_MODE"] = "ensemble"
        else:
            if "ask_tradeplan_portfolio_choice" in globals():
                globals()["TRADE_PLAN_MODE"] = ask_tradeplan_portfolio_choice()
            else:
                globals()["TRADE_PLAN_MODE"] = "ensemble"

        portfolio_value_override = None
        if bool(use_portfolio_value.get()):
            raw = (
                str(portfolio_value_var.get())
                .replace(",", "").replace("$", "").replace("AUD", "").strip()
            )
            portfolio_value_override = _to_float(raw, default=np.nan)
            if not np.isfinite(portfolio_value_override):
                portfolio_value_override = None

        _result["value"] = (
            units_ser,
            last_price_ser,
            prices,
            include_flags,
            tilts_df,
            portfolio_value_override,
        )
        root.destroy()

    btns = _ctk.CTkFrame(bottom, fg_color="transparent")
    btns.pack(side="right", anchor="e")
    _ctk.CTkButton(btns, text="Reset to Seed", width=120, fg_color="gray40",
                   hover_color="gray30", command=_reset_to_seed_units).pack(side="left", padx=6)
    _ctk.CTkButton(btns, text="Cancel", width=100, fg_color="gray40",
                   hover_color="gray30", command=root.destroy).pack(side="left", padx=6)
    _ctk.CTkButton(btns, text="Save", width=120, command=_on_save).pack(side="left", padx=6)

    root.protocol("WM_DELETE_WINDOW", root.destroy)
    root.mainloop()
    return _result["value"]


def edit_holdings_and_tilts_dialog(
    prices,
    exclude,
    seed_units,
    seed_include,
    seed_tilts,
    title="Edit Holdings & Factor Tilts",
):
    """Open the holdings + factor-tilts editor.

    Uses the modern CustomTkinter dialog when available, otherwise falls back to
    the classic ttk dialog. Both return the same 6-tuple (or None if cancelled).
    """
    if HAS_CTK:
        try:
            return _edit_holdings_dialog_ctk(prices, exclude, seed_units, seed_include, seed_tilts, title=title)
        except Exception as e:
            print(f"[ui] CustomTkinter dialog failed ({e}); falling back to classic dialog.")
    return _edit_holdings_dialog_ttk(prices, exclude, seed_units, seed_include, seed_tilts, title=title)


