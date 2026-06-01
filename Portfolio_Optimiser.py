"""
Grabbing Data From Yahoo Finance For Stock Build last updated 19/03/2026

Portfolio Optimiser - single-file application.

Pipeline (executes top to bottom):
   1. Imports
   2. Config, paths, trade-plan + validation helpers
   3. Data download: prices, Fama-French 5 + momentum factors, FX, benchmarks
   4. Holdings / tilts input dialog
   5. Optimisation engine: factor betas, tilt recommendations, efficient frontier
   6. Transaction costs & CGT
   7. Excel workbook writer
   8. Excel launcher
   9. PowerPoint report generator
  10. PowerPoint launcher

Run top-to-bottom as __main__ (via Main.py); bundled into the .exe by build_helper.py.
"""


# =====================================================================
# BLOCK 1 imports
# =====================================================================
# Standard library imports
import datetime
import hashlib
import io
import json
import math
import multiprocessing as mp
import os
import pathlib
import re
import shutil
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path

# Third-party imports
import cvxpy as cp
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import numpy.linalg as la
import openpyxl
from openpyxl import Workbook, load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import pandas as pd
import pptx
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import MSO_AUTO_SIZE, PP_ALIGN
from pptx.oxml.ns import qn
from pptx.oxml.xmlchemy import OxmlElement
from pptx.util import Cm, Inches, Pt
import requests
import scipy.optimize
from scipy.optimize import minimize
from numpy.linalg import pinv
import statsmodels.api as sm
import xlwings as xw
import yfinance as yf
from dateutil.relativedelta import relativedelta

# Optional Windows-specific import
try:
    import win32com.client as win32
    HAS_WIN32COM = True
except ImportError:
    HAS_WIN32COM = False

# Debug: Print Python executable path
print(sys.executable)


# =====================================================================
# BLOCK 2 Global codes and Data Retrieval from the web
# =====================================================================
# ---------------------------------------------------------------------
# Central base directory
# ---------------------------------------------------------------------
def _app_dir() -> Path:
    """
    Determine the application directory dynamically:
      - When frozen (PyInstaller): use the exe folder
      - When run as a script: use the script's folder
      - When interactive (Jupyter/IPython): use cwd
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    if "__file__" in globals():
        return Path(__file__).resolve().parent
    return Path(os.getcwd())


# Absolute path to your central config root (for dev use)
_DEV_BASE = Path.home() / "Portfolio_Optimiser"

# Use the dev folder if it exists, otherwise fall back to dynamic app dir
APP_DIR = _DEV_BASE if _DEV_BASE.exists() else _app_dir()

# ---------------------------------------------------------------------
# Config file and Excel workbook paths
# ---------------------------------------------------------------------
def _default_excel_path() -> str:
    """Return full path to the default Excel workbook."""
    app_dir = Path(APP_DIR)  # APP_DIR might be a string in the notebook
    return str((app_dir / "Stock Analysis.xlsm").resolve())

CONFIG_PATH = APP_DIR / "config.json"

# ---------------------------------------------------------------------
# Export directory (for generated reports)
# ---------------------------------------------------------------------
EXPORT_DIR = APP_DIR / "Reports"
EXPORT_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------
# Default configuration values
# ---------------------------------------------------------------------
_DEFAULTS = {
    "excel_path": _default_excel_path(),
    "marginal_tax_rate": 0.37,
    "carry_forward_losses": 0.0,
    "lot_match_method": "HIFO",
    "open_after_save": True,
    "use_xlwings": True,
    "open_excel_after_save": True,
    "open_ppt_after_save": True,
}

# ---------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------
def load_config() -> dict:
    """Load configuration from config.json, falling back to defaults."""
    cfg = _DEFAULTS.copy()
    try:
        if CONFIG_PATH.exists():
            with CONFIG_PATH.open("r", encoding="utf-8") as f:
                user_cfg = json.load(f)
            cfg.update({k: v for k, v in user_cfg.items() if k in cfg})
    except Exception as e:
        print(f"[config] Using defaults (error reading config.json): {e}")

    # Ensure workbook directory exists
    try:
        Path(cfg["excel_path"]).parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    return cfg

# ---------------------------------------------------------------------
# PPTX handling
# ---------------------------------------------------------------------
CFG = load_config()

def open_ppt_if_enabled(pptx_path: str) -> None:
    """Open PPTX file if enabled in config."""
    if not CFG.get("open_ppt_after_save", True):
        return

    pptx_path = os.path.abspath(pptx_path)

    # Kill any orphaned PowerPoint processes from prior notebook runs (Windows only)
    _kill_orphan_powerpoint()

    # Always use OS open to launch the user's normal PowerPoint UI instance
    _os_open(pptx_path)


def _os_open(path: str) -> None:
    """Open file with default OS application."""
    try:
        os.startfile(path)  # Windows
    except AttributeError:
        import subprocess
        if sys.platform == "darwin":
            subprocess.run(["open", path])
        else:
            subprocess.run(["xdg-open", path])


def _kill_orphan_powerpoint() -> None:
    """Kill orphaned PowerPoint processes (Windows only)."""
    try:
        import subprocess
        subprocess.run(
            ["taskkill", "/F", "/IM", "POWERPNT.EXE"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


user_opts = {}

# ---------------------------------------------------------------------
# Validation Code
# ---------------------------------------------------------------------

# Constants
TRADE_PLAN_MODE = "ask"  # Options: "ask", "auto", "no_tilts", "with_tilts"
VALIDATION_LOOKBACK_DAYS = 252  # 1 year of daily data
ANNUAL_TRADING_DAYS = 252  # For Sharpe calculation


def ask_tradeplan_portfolio_choice() -> str:
    """
    Prompt user to choose between 'with_tilts' or 'no_tilts' portfolio.

    Returns:
        str: "with_tilts" or "no_tilts"
    """
    try:
        import tkinter as tk
        from tkinter import messagebox as mb
        root = tk.Tk()
        root.withdraw()
        use_tilts = mb.askyesno(
            "Trade Plan Portfolio",
            "Use WITH TILTS portfolio for the trade plan?\n\n"
            "Yes = With Tilts\n"
            "No  = Optimised (No Tilts)"
        )
        root.destroy()
        return "with_tilts" if use_tilts else "no_tilts"
    except Exception:
        return "no_tilts"  # Safe fallback


def build_trade_plan_from_units(
    current_units: pd.Series,
    last_prices: pd.Series,
    target_weights: pd.Series,
    cash_buffer: float = 0.0,
    min_trade_aud: float = 0.0,
    round_to_whole_units: bool = True,
    portfolio_value_override: float | None = None
) -> pd.DataFrame:
    """
    Build a rebalance trade list from current units to target weights.

    Args:
        current_units: Current holdings (ticker-indexed).
        last_prices: Latest prices (ticker-indexed).
        target_weights: Target weights (ticker-indexed, sum to 1).
        cash_buffer: Fraction to hold as cash (0-1).
        min_trade_aud: Minimum trade size in AUD.
        round_to_whole_units: Whether to round to whole units.
        portfolio_value_override: Override portfolio value.

    Returns:
        DataFrame with trade plan details.
    """
    # Validate inputs
    if len(target_weights.index) and isinstance(target_weights.index[0], (int, np.integer)):
        raise ValueError("target_weights must be ticker-indexed Series.")

    # Align and clean data
    universe = sorted(set(current_units.index) | set(last_prices.index) | set(target_weights.index))
    u = current_units.reindex(universe).fillna(0.0).astype(float)
    p = last_prices.reindex(universe).fillna(np.nan).astype(float)

    # Filter tradable assets
    tradable = p.notna() & (p > 0) & np.isfinite(p)
    u = u.loc[tradable]
    p = p.loc[tradable]

    w_tgt = target_weights.reindex(u.index).fillna(0.0)

    # Normalize target weights
    w_sum = w_tgt.sum()
    if w_sum > 0:
        w_tgt /= w_sum
    w_tgt *= (1.0 - cash_buffer)

    curr_value = u * p
    port_value = curr_value.sum()

    if portfolio_value_override is not None and np.isfinite(portfolio_value_override) and portfolio_value_override > 0:
        port_value = portfolio_value_override

    if port_value <= 0:
        return pd.DataFrame({
            "Security": u.index,
            "Price": p.values,
            "CurrUnits": u.values,
            "CurrValue": curr_value.values,
            "CurrW": np.zeros(len(u)),
            "TgtW": w_tgt.values,
            "TgtValue": np.zeros(len(u)),
            "DeltaValue": np.zeros(len(u)),
            "TradeUnits": np.zeros(len(u)),
            "Side": ["HOLD"] * len(u),
        })

    curr_w = curr_value / port_value
    tgt_value = w_tgt * port_value
    delta_value = tgt_value - curr_value
    trade_units = delta_value / p

    if round_to_whole_units:
        trade_units = np.where(trade_units >= 0, np.floor(trade_units), np.ceil(trade_units))

    # Apply min trade filter
    notional = np.abs(trade_units * p)
    trade_units = np.where(notional >= min_trade_aud, trade_units, 0.0)

    side = np.where(trade_units > 0, "BUY", np.where(trade_units < 0, "SELL", "HOLD"))

    out = pd.DataFrame({
        "Security": u.index,
        "Price": p.values,
        "CurrUnits": u.values,
        "CurrValue": curr_value.values,
        "CurrW": curr_w.values,
        "TgtW": w_tgt.values,
        "TgtValue": tgt_value.values,
        "DeltaValue": delta_value.values,
        "TradeUnits": trade_units,
        "Side": side
    })

    # Sort: sells first, then buys
    side_rank = {"SELL": 0, "HOLD": 1, "BUY": 2}
    out["SideRank"] = out["Side"].map(side_rank).fillna(1).astype(int)
    out = out.sort_values(["SideRank", "Security"]).drop(columns=["SideRank"]).reset_index(drop=True)

    return out


def _annualized_sharpe(returns: pd.Series, rf_annual: float) -> float:
    """Calculate annualized Sharpe ratio."""
    r = pd.to_numeric(returns, errors="coerce").dropna()
    if r.empty:
        return np.nan
    rf_daily = (1.0 + rf_annual) ** (1.0 / ANNUAL_TRADING_DAYS) - 1.0
    excess = r - rf_daily
    vol = excess.std(ddof=1)
    if vol <= 0 or not np.isfinite(vol):
        return np.nan
    return excess.mean() / vol * np.sqrt(ANNUAL_TRADING_DAYS)


def choose_portfolio_for_tradeplan(
    returns_df: pd.DataFrame,
    w_no_tilts: pd.Series,
    w_with_tilts: pd.Series,
    rf_annual: float,
    lookback_days: int = VALIDATION_LOOKBACK_DAYS
) -> tuple[str, pd.Series, dict]:
    """
    Choose portfolio based on Sharpe ratio over lookback period.

    Returns:
        (choice, weights, diagnostics)
    """
    r = returns_df.tail(lookback_days).replace([np.inf, -np.inf], np.nan).dropna(how="all")

    w0 = w_no_tilts.reindex(r.columns).fillna(0.0)
    w1 = w_with_tilts.reindex(r.columns).fillna(0.0)

    # Normalize weights
    for w in [w0, w1]:
        w_sum = w.sum()
        if w_sum != 0:
            w /= w_sum

    p0 = (r @ w0).dropna()
    p1 = (r @ w1).dropna()

    sh0 = _annualized_sharpe(p0, rf_annual)
    sh1 = _annualized_sharpe(p1, rf_annual)

    diag = {"sharpe_no_tilts": sh0, "sharpe_with_tilts": sh1}

    if np.isfinite(sh1) and (not np.isfinite(sh0) or sh1 > sh0):
        return "with_tilts", w1, diag
    return "no_tilts", w0, diag

# ---------------------------------------------------------------------
# Main Configuration Binding
# ---------------------------------------------------------------------
TILT_FACTORS = ["Mkt-RF", "SMB", "HML", "RMW", "CMA", "MOM"]

# Bind config to globals
filename = CFG["excel_path"]
MARGINAL_TAX_RATE = CFG["marginal_tax_rate"]
CAPITAL_LOSS_CARRY_FWD = CFG["carry_forward_losses"]
LOT_MATCH_METHOD = CFG["lot_match_method"].upper()
OPEN_AFTER_SAVE = CFG.get("open_after_save", True)
USE_XLWINGS = CFG.get("use_xlwings", True)

# ---------------------------------------------------------------------
# Risk-Free Rate (AU): RBA Cash Rate
# ---------------------------------------------------------------------
def get_rba_cash_rate_target_current(default: float = 0.04) -> float:
    """
    Get latest RBA cash rate target as decimal.

    Tries HTML table first, then CSV fallback.
    """
    url_html = "https://www.rba.gov.au/statistics/cash-rate/"
    url_csv = "https://www.rba.gov.au/statistics/tables/csv/f1.1-data.csv"

    # Try HTML
    try:
        dfs = pd.read_html(url_html)
        for df in dfs:
            df.columns = [c.strip().lower() for c in df.columns]
            candidates = [c for c in df.columns if "cash" in c and "target" in c]
            if candidates:
                val = pd.to_numeric(df[candidates[0]], errors="coerce").dropna().iloc[0]
                return val / 100.0
    except Exception:
        pass

    # Try CSV
    try:
        df = pd.read_csv(url_csv)
        df.columns = [c.strip().lower() for c in df.columns]
        candidates = [c for c in df.columns if "cash" in c and "target" in c]
        if candidates:
            vals = pd.to_numeric(df[candidates[0]], errors="coerce").dropna()
            if not vals.empty:
                return vals.iloc[-1] / 100.0
    except Exception:
        pass

    return default

# ---------------------------------------------------------------------
# Caching for FF5 + MOM Data
# ---------------------------------------------------------------------
_CACHE_DIR = Path.home() / ".portfolio_optimiser_cache"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _cache_path(url: str) -> Path:
    key = hashlib.md5(url.encode("utf-8")).hexdigest()
    return _CACHE_DIR / f"{key}.csv"

def _cached_read(url: str, build_df_fn, ttl_days: int = 7) -> pd.DataFrame:
    """Load from cache if recent, else build and cache."""
    p = _cache_path(url)
    try:
        if p.exists() and (time.time() - p.stat().st_mtime) <= ttl_days * 86400:
            df = pd.read_csv(p, index_col=0, parse_dates=[0])
            df.index = pd.to_datetime(df.index)
            return df.sort_index()
    except Exception as e:
        print(f"[cache] Read miss: {e}")

    df = build_df_fn()
    try:
        df.to_csv(p)
    except Exception as e:
        print(f"[cache] Write skipped: {e}")
    return df

# ---------------------------------------------------------------------
# FF5 + Momentum Data Loaders
# ---------------------------------------------------------------------
FF5_DAILY_ZIP = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_daily_CSV.zip"
MOM_DAILY_ZIP = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_daily_CSV.zip"

def get_mom_daily() -> pd.DataFrame:
    """Get daily momentum factor data."""
    def _builder() -> pd.DataFrame:
        r = requests.get(MOM_DAILY_ZIP, timeout=60)
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
        csv_file = next(n for n in z.namelist() if n.lower().endswith(".csv"))
        raw = z.read(csv_file).decode("latin1", errors="ignore").splitlines()
        num_rx = re.compile(r"^\s*\d{6,8}\s*[,\s]")
        first = next(i for i, ln in enumerate(raw) if num_rx.match(ln))
        header = "Date,MOM"
        data = [header] + [ln.strip() for ln in raw[first:] if num_rx.match(ln)]
        df = pd.read_csv(io.StringIO("\n".join(data)), sep=r"\s*,\s*", engine="python")
        df["Date"] = pd.to_datetime(df["Date"], format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["Date"]).set_index("Date").sort_index()
        df["MOM"] = pd.to_numeric(df["MOM"], errors="coerce") / 100.0
        return df[["MOM"]]

    df = _cached_read(MOM_DAILY_ZIP, _builder, ttl_days=7)
    df = df.copy()
    if "MOM" not in df.columns:
        df["MOM"] = pd.to_numeric(df.iloc[:, 0], errors="coerce")
        df = df[["MOM"]]
    df.index = pd.to_datetime(df.index)
    return df.sort_index()

def get_ff5_daily(cache_csv_path: str | None = None) -> pd.DataFrame:
    """
    Get Fama-French 5 factors daily data.

    Args:
        cache_csv_path: Optional path to save cache CSV.
    """
    def _builder() -> pd.DataFrame:
        resp = requests.get(FF5_DAILY_ZIP, timeout=60)
        resp.raise_for_status()
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        csv_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))

        raw = zf.read(csv_name).decode("latin1", errors="ignore")
        lines = raw.splitlines()

        num_rx = re.compile(r"^\s*\d{6,8}\s*[,\s]")
        first_data_idx = next(i for i, ln in enumerate(lines) if num_rx.match(ln))

        header_idx = None
        for i in range(max(0, first_data_idx - 5), first_data_idx + 1):
            if re.search(r"\bdate\b", lines[i], flags=re.I) and "mkt" in lines[i].lower():
                header_idx = i
                break

        header = lines[header_idx].strip() if header_idx is not None else "Date,Mkt-RF,SMB,HML,RMW,CMA,RF"
        data_lines = [header]
        for ln in lines[first_data_idx:]:
            if not num_rx.match(ln):
                break
            data_lines.append(ln.strip())

        df = pd.read_csv(io.StringIO("\n".join(data_lines)), sep=r"\s*,\s*", engine="python")
        df.columns = [c.strip() for c in df.columns]
        col_map = {c.lower().replace(" ", ""): c for c in df.columns}
        ren = {}
        for want in ["Date", "Mkt-RF", "SMB", "HML", "RMW", "CMA", "RF"]:
            key = want.lower().replace(" ", "")
            if key in col_map:
                ren[col_map[key]] = want
        df = df.rename(columns=ren)

        df["Date"] = pd.to_datetime(df["Date"], format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["Date"]).set_index("Date").sort_index()
        factor_cols = ["Mkt-RF", "SMB", "HML", "RMW", "CMA", "RF"]
        df[factor_cols] = df[factor_cols].apply(pd.to_numeric, errors="coerce") / 100.0
        return df.dropna(subset=factor_cols)

    df = _cached_read(FF5_DAILY_ZIP, _builder, ttl_days=7)

    if cache_csv_path:
        try:
            df.to_csv(cache_csv_path, index=True)
        except Exception as e:
            print(f"[ff5] Could not write cache_csv_path: {e}")

    return df

def get_ff5_mom_daily() -> pd.DataFrame:
    """Get combined FF5 + MOM daily factors."""
    ff5 = get_ff5_daily()
    mom = get_mom_daily()
    out = ff5.join(mom, how="inner").sort_index()
    return out[["Mkt-RF", "SMB", "HML", "RMW", "CMA", "MOM", "RF"]]

# ---------------------------------------------------------------------
# Foreign Exchange
# ---------------------------------------------------------------------
def _last_numeric(series: pd.Series) -> float:
    """Get last numeric value from series."""
    v = series.iloc[-1]
    if isinstance(v, pd.Series):
        v = v.iloc[0]
    return float(v)

def get_usd_aud_fx(default: float = 1.50) -> float:
    """Get latest USD/AUD FX rate from global 'fx' series."""
    try:
        if "fx" in globals():
            series = globals()["fx"]
            if isinstance(series, pd.DataFrame):
                s = series.iloc[:, 0]
            else:
                s = series
            s = pd.to_numeric(s, errors="coerce").dropna()
            if not s.empty:
                last = _last_numeric(s)
                if last > 0:
                    return last
    except Exception:
        pass
    return default

def fx_to_aud_for_tickers(tickers, usd_aud_rate: float) -> pd.Series:
    """Map tickers to FX rates (1.0 for AUD, usd_aud_rate for USD)."""
    out = {}
    for t in map(str, tickers):
        out[t] = 1.0 if t.startswith("^") or t.endswith(".AX") else usd_aud_rate
    return pd.Series(out, name="FX to AUD")
# Runtime overrides to enforce 3-month tilt recommendation horizon in notebook runs.
TILT_RECOMMENDATION_LOOKBACK_DAYS = 63
FF5_LOOKBACK_DAYS = 63

# In notebook mode we default to validation-based trade-plan choice.
if str(globals().get("TRADE_PLAN_MODE", "ask")).lower().strip() == "ask":
    TRADE_PLAN_MODE = "auto"

print("[override] TILT_RECOMMENDATION_LOOKBACK_DAYS=63, TRADE_PLAN_MODE=", TRADE_PLAN_MODE)


# =====================================================================
# BLOCK 3 Downloading Prices
# =====================================================================
# =====================================================================
# BLOCK 7: DATA DOWNLOAD â€” Prices, Factors, FX, Benchmarks
# =====================================================================

# Constants for data processing
PRICE_DOWNLOAD_PERIOD = "2y"
FF5_BETA_WINDOW_DAYS = 504  # ~2 years of business days
FX_CACHE_PERIOD = "5y"
BENCHMARK_PERIOD = "6y"
BENCHMARK_INDICES = ["^AORD", "^GSPC", "^IXIC"]  # ASX, S&P500, NASDAQ
STATIC_STARTERS = ["^AORD"]
EXCLUDE_FROM_OPT = {"^AORD"}

# Initialize data storage
data_dict = {}

# =====================================================================
# 1) Configuration & Universe Setup
# =====================================================================
_XL_PATH = globals().get("filename", _default_excel_path())
rf_annual = get_rba_cash_rate_target_current()
rf_label = f"{rf_annual * 100:.2f}%"


def _extract_tickers_from_holdings(xl_path: str, sheet: str = "Holdings") -> list[str]:
    """Extract unique tickers from Holdings sheet Security column."""
    try:
        df = pd.read_excel(xl_path, sheet_name=sheet)
        if not isinstance(df, pd.DataFrame) or df.empty or "Security" not in df.columns:
            return []
        tickers = df["Security"].dropna().astype(str).str.strip()
        return list(dict.fromkeys([t for t in tickers if t]))
    except Exception:
        return []


def _build_ticker_universe(sheet_tickers: list[str], starters: list[str]) -> list[str]:
    """Build deduped ticker universe with mandatory benchmark."""
    universe = list(dict.fromkeys(sheet_tickers + starters))
    if "^AORD" not in universe:
        universe.insert(0, "^AORD")
    return universe


# Build universe
tickers_from_sheet = _extract_tickers_from_holdings(_XL_PATH, sheet="Holdings")
tickers = _build_ticker_universe(tickers_from_sheet, STATIC_STARTERS)
print(f"XL_PATH = {_XL_PATH}")
print(f"Tickers loaded from sheet: {tickers_from_sheet}")

# =====================================================================
# 2) Download Prices
# =====================================================================
def _normalize_yfinance_close(dl) -> pd.DataFrame:
    """Handle yfinance output (may be Series or DataFrame) and return DataFrame."""
    if isinstance(dl, pd.DataFrame) and "Close" in dl.columns:
        return dl["Close"]
    # Single ticker returns Series
    if isinstance(dl, pd.Series):
        return dl.to_frame()
    return pd.DataFrame()


dl = yf.download(
    tickers,
    period=PRICE_DOWNLOAD_PERIOD,
    auto_adjust=True,
    threads=False,
    progress=False
)
prices = _normalize_yfinance_close(dl)

# Clean: ensure datetime index, fill gaps, dedupe columns
prices.index = pd.to_datetime(prices.index)
idx = pd.date_range(start=prices.index.min(), end=prices.index.max(), freq="B")
prices = prices.reindex(idx).ffill().bfill()
prices.index.name = "Date"
prices = prices.loc[:, ~prices.columns.duplicated()]

# =====================================================================
# 3) Fama-French 5 Factors + Momentum
# =====================================================================
ff5_raw = get_ff5_mom_daily().loc[:, ~get_ff5_mom_daily().columns.duplicated()].copy()

# Ensure expected columns
expected_cols = ["Mkt-RF", "SMB", "HML", "RMW", "CMA", "MOM", "RF"]
ff5_raw = ff5_raw.reindex(columns=expected_cols)
ff5_win_for_betas = ff5_raw.tail(FF5_BETA_WINDOW_DAYS)

# =====================================================================
# 4) FX Rates (AUD/USD for factor adjustment & USD/AUD for holdings)
# =====================================================================
def _download_fx_series(ticker: str, period: str = FX_CACHE_PERIOD) -> pd.Series:
    """Download FX rate and return as Series."""
    try:
        dl = yf.download(
            ticker,
            period=period,
            interval="1d",
            auto_adjust=True,
            progress=False,
            threads=False
        )
        fx = dl["Close"] if isinstance(dl, pd.DataFrame) else dl
        return pd.to_numeric(fx, errors="coerce")
    except Exception as e:
        print(f"Warning: Failed to download {ticker}: {e}")
        return pd.Series(dtype=float)


# AUD/USD for factor conversion (align to factor index)
fx_audusd = _download_fx_series("AUDUSD=X", period=BENCHMARK_PERIOD)
if fx_audusd.empty:
    print("Warning: AUDUSD=X unavailable; using flat FX=1.0")
    fx_audusd = pd.Series(1.0, index=ff5_raw.index)
else:
    fx_audusd = fx_audusd.reindex(ff5_raw.index).ffill()

fx_ret = fx_audusd.pct_change().fillna(0.0)

# USD/AUD for holdings conversion (align to price index)
fx_usdaud = _download_fx_series("USDAUD=X", period=FX_CACHE_PERIOD)
if fx_usdaud.empty:
    fx_usdaud = pd.Series(1.5, index=prices.index)  # fallback
else:
    fx_usdaud = fx_usdaud.reindex(prices.index).ffill()

# =====================================================================
# 5) AUD-Adjusted Returns
# =====================================================================
# Identify USD-priced tickers (no .AX suffix, not an index)
usd_tickers = [str(c) for c in prices.columns 
               if not str(c).endswith(".AX") and not str(c).startswith("^")]

# Convert USD prices to AUD for return calculation
prices_aud = prices.copy()
if usd_tickers:
    prices_aud[usd_tickers] = prices[usd_tickers].mul(fx_usdaud, axis=0)

# Compute returns
df_returns = (
    prices_aud.reset_index()
    .melt(id_vars="Date", var_name="Security", value_name="Close")
    .sort_values(["Security", "Date"])
)
df_returns["Return"] = df_returns.groupby("Security", sort=False)["Close"].pct_change()
df_returns = df_returns.dropna()

# FX map for holdings sheet last-price conversion
usd_aud = get_usd_aud_fx()
fx_map_all = fx_to_aud_for_tickers(prices.columns, usd_aud)

# =====================================================================
# 6) Download Benchmark Data
# =====================================================================
def _download_benchmarks(
    tickers: list[str],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp
) -> pd.DataFrame:
    """Download benchmark data and return as DataFrame."""
    try:
        data = yf.download(
            tickers,
            start=start_date,
            end=end_date,
            progress=False,
            auto_adjust=True,
            threads=False
        )
        # Extract Close prices
        if isinstance(data.columns, pd.MultiIndex):
            data = data["Close"]
        return data.ffill().bfill()
    except Exception as e:
        print(f"[data] Benchmark download failed: {e}")
        return pd.DataFrame()


benchmark_data = _download_benchmarks(
    BENCHMARK_INDICES,
    prices.index[0],
    prices.index[-1]
)
if not benchmark_data.empty:
    print(f"[data] Benchmarks downloaded: {list(benchmark_data.columns)}")

data_dict["benchmark_data"] = benchmark_data

# =====================================================================
# 7) Helper: Fetch New Tickers
# =====================================================================
def _fetch_prices_for_new_tickers(
    new_tickers: list[str],
    base_prices: pd.DataFrame,
    period: str = FX_CACHE_PERIOD
) -> pd.DataFrame:
    """Download prices for new tickers and merge into base_prices."""
    if base_prices is None or not isinstance(base_prices, pd.DataFrame):
        base_prices = pd.DataFrame()

    # Filter to genuinely new tickers
    new_only = [str(t) for t in new_tickers if str(t) not in base_prices.columns]
    if not new_only:
        return base_prices

    # Download
    dl = yf.download(
        new_only,
        period=period,
        auto_adjust=True,
        threads=False,
        progress=False
    )
    new_px = _normalize_yfinance_close(dl)

    if new_px is None or new_px.empty:
        return base_prices

    # Align to existing index if present
    new_px.index = pd.to_datetime(new_px.index).sort_values()
    new_px = new_px.loc[:, ~new_px.columns.duplicated()]
    
    if not base_prices.empty:
        new_px = new_px.reindex(index=base_prices.index).ffill()

    # Merge
    if base_prices.empty:
        return new_px

    combined = base_prices.copy()
    for col in new_px.columns:
        if col not in combined.columns:
            combined[col] = new_px[col]
    return combined


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

        # Trade-plan portfolio choice.
        # In Jupyter, modal messageboxes can freeze some kernels, so use deterministic fallback.
        if "ipykernel" in sys.modules:
            globals()["TRADE_PLAN_MODE"] = "with_tilts"
        else:
            if "ask_tradeplan_portfolio_choice" in globals():
                globals()["TRADE_PLAN_MODE"] = ask_tradeplan_portfolio_choice()
            else:
                globals()["TRADE_PLAN_MODE"] = "with_tilts"

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

    # Add-holding box
    add_box = _ctk.CTkFrame(left)
    add_box.pack(fill="x", padx=14, pady=(4, 12))
    _ctk.CTkLabel(add_box, text="Add holding", font=title_font).grid(
        row=0, column=0, columnspan=5, sticky="w", padx=8, pady=(8, 4)
    )
    _ctk.CTkLabel(add_box, text="Ticker").grid(row=1, column=0, sticky="w", padx=(8, 4), pady=(0, 8))
    ent_new_ticker = _ctk.CTkEntry(add_box, width=140)
    ent_new_ticker.grid(row=1, column=1, sticky="w", padx=(0, 12), pady=(0, 8))
    _ctk.CTkLabel(add_box, text="Units").grid(row=1, column=2, sticky="w", padx=(0, 4), pady=(0, 8))
    ent_new_units = _ctk.CTkEntry(add_box, width=110)
    ent_new_units.grid(row=1, column=3, sticky="w", padx=(0, 12), pady=(0, 8))
    btn_add = _ctk.CTkButton(add_box, text="Add", width=80)
    btn_add.grid(row=1, column=4, sticky="w", padx=(0, 8), pady=(0, 8))

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
        ent_new_ticker.delete(0, "end")
        ent_new_units.delete(0, "end")

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

        if "ipykernel" in sys.modules:
            globals()["TRADE_PLAN_MODE"] = "with_tilts"
        else:
            if "ask_tradeplan_portfolio_choice" in globals():
                globals()["TRADE_PLAN_MODE"] = ask_tradeplan_portfolio_choice()
            else:
                globals()["TRADE_PLAN_MODE"] = "with_tilts"

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


# =====================================================================
# BLOCK 5 Creating the Covariance Matrix and the Rest of the OPT
# =====================================================================
# === Analytics helpers (moved from Block 4) ===================================
gamma_cgt = 1.0
rf_label = f"{rf_annual*100:.2f}%"
chart_title = "Efficient Frontier"
TRADING_DAYS = 252


def holdings_portfolio_returns(prices: pd.DataFrame, units: pd.Series) -> pd.Series:
    units = pd.Series(units).reindex(prices.columns).fillna(0.0)
    if units.abs().sum() == 0:
        return pd.Series(dtype=float)
    px = prices.reindex(columns=units.index).ffill()
    port_val = (px * units.values).sum(axis=1)
    return port_val.pct_change(fill_method=None).dropna()


def current_holdings_weights(
    units: pd.Series,
    last_prices: pd.Series,
    investable: list[str],
    fx_to_aud: pd.Series | float | None = None,
) -> pd.Series:
    if isinstance(fx_to_aud, pd.Series):
        fx = fx_to_aud.reindex(units.index).fillna(1.0)
    else:
        fx = float(fx_to_aud) if isinstance(fx_to_aud, (int, float)) else 1.0

    mv = pd.Series(units, dtype=float) * pd.Series(last_prices, dtype=float) * fx
    mv = mv.reindex(investable).fillna(0.0)
    den = float(mv.sum())
    return (mv / den) if den > 0 else mv


# ------------------------------------------------------------
# 3) COVARIANCE MATRIX (daily)
# ------------------------------------------------------------
# Backward-compatible aliases (Block 7 now emits df_returns/prices_aud).
if "df_melt" not in globals():
    if "df_returns" in globals():
        df_melt = df_returns.copy()
    else:
        raise NameError("df_melt (or df_returns) is not defined; run Block 3 first.")

if "prices_aud_for_returns" not in globals():
    if "prices_aud" in globals():
        prices_aud_for_returns = prices_aud.copy()
    elif "prices" in globals():
        prices_aud_for_returns = prices.copy()
    else:
        raise NameError("prices_aud_for_returns (or prices_aud/prices) is not defined; run Block 3 first.")

df_cov_wide = (
    df_melt[["Date", "Security", "Return"]]
    .pivot(index="Date", columns="Security", values="Return")
)
Sigma_daily = df_cov_wide.cov()

# Optional sanity check that Sigma was built from AUD-converted prices
try:
    Sigma_from_aud = (
        pd.melt(
            prices_aud_for_returns.reset_index(),
            id_vars="Date",
            var_name="Security",
            value_name="Close",
        )
        .sort_values(["Security", "Date"])
        .assign(Return=lambda d: d.groupby("Security")["Close"].pct_change(fill_method=None))
        .pivot(index="Date", columns="Security", values="Return")
        .cov()
    ).reindex(index=Sigma_daily.index, columns=Sigma_daily.columns)

    diff = (Sigma_daily - Sigma_from_aud).abs().to_numpy()
    max_abs_diff = float(np.nanmax(diff)) if diff.size else np.nan
    using_fx = np.allclose(
        Sigma_daily.to_numpy(),
        Sigma_from_aud.to_numpy(),
        rtol=0,
        atol=1e-12,
        equal_nan=True,
    )
    print(f"Using FX-adjusted returns for Sigma?: {using_fx} (max |diff|={max_abs_diff:.2e})")
except Exception as e:
    print(f"[sigma-check] skipped: {e}")


# ------------------------------------------------------------
# 4) GEOMETRIC EXPECTED RETURNS (annual)
# ------------------------------------------------------------
df_melt["LogRet"] = np.log1p(df_melt["Return"])
mu_log_ann = df_melt.groupby("Security")["LogRet"].mean() * TRADING_DAYS
mu_ann_geo = np.expm1(mu_log_ann)

securities_all = [s for s in Sigma_daily.columns if s != "PortfolioValue"]
Sigma_daily = Sigma_daily.loc[securities_all, securities_all]
mu_vec_all = mu_ann_geo.reindex(securities_all)

valid_all = [
    s
    for s in securities_all
    if pd.notna(mu_vec_all.get(s, np.nan)) and pd.notna(Sigma_daily.loc[s, s])
]
Sigma_daily = Sigma_daily.loc[valid_all, valid_all]
mu_vec_all = mu_vec_all.reindex(valid_all)


# ------------------------------------------------------------
# 5) FF5 helper functions
# ------------------------------------------------------------
def compute_ff5_betas(df_returns_wide: pd.DataFrame, ff5_returns: pd.DataFrame, min_obs: int = 120, n_lags: int = 1):
    """
    Estimate FF5+MOM betas per security via OLS with a Dimson (1979) correction for
    non-synchronous trading.

    Each factor enters contemporaneously PLUS n_lags lagged terms, and the reported
    beta is the SUM of the contemporaneous and lagged coefficients. This recovers the
    true exposure of assets that trade in a different timezone from the US factors:
    ASX-listed ETFs close ~16h before the US market, so their same-day beta to US
    factors is spuriously near zero while the lagged term carries the real loading.

    Returns: B (Dimson betas), alpha_daily, resid_var
    """
    joined = df_returns_wide.join(ff5_returns, how="inner").dropna(how="any").sort_index()
    if joined.empty:
        return None, None, None

    securities = list(df_returns_wide.columns)
    factors = [c for c in ff5_returns.columns if c != "RF"]
    n_lags = max(0, int(n_lags))

    # Design matrix: contemporaneous factors + lagged copies for the Dimson sum.
    design = {f: pd.to_numeric(joined[f], errors="coerce") for f in factors}
    lag_cols = {f: [] for f in factors}
    for L in range(1, n_lags + 1):
        for f in factors:
            col = f"{f}__lag{L}"
            design[col] = pd.to_numeric(joined[f], errors="coerce").shift(L)
            lag_cols[f].append(col)
    X_all = pd.DataFrame(design, index=joined.index)
    all_factor_cols = list(X_all.columns)

    B = pd.DataFrame(index=securities, columns=factors, dtype=float)
    alpha_daily = pd.Series(index=securities, dtype=float)
    resid_var = pd.Series(index=securities, dtype=float)

    for sec in securities:
        y = pd.to_numeric(joined[sec], errors="coerce")
        reg_df = pd.concat([y.rename("y"), X_all], axis=1).dropna(how="any")
        if len(reg_df) < min_obs:
            continue

        X_reg = sm.add_constant(reg_df[all_factor_cols])
        try:
            model = sm.OLS(reg_df["y"], X_reg, missing="drop").fit()
        except Exception:
            continue

        alpha_daily.loc[sec] = model.params.get("const", np.nan)
        resid_var.loc[sec] = float(np.nanvar(model.resid, ddof=1))

        for f in factors:
            # Dimson beta = contemporaneous coefficient + sum of lagged coefficients.
            beta_f = float(model.params.get(f, np.nan))
            for col in lag_cols[f]:
                beta_f += float(model.params.get(col, 0.0))
            B.loc[sec, f] = beta_f

    return B, alpha_daily, resid_var


def compute_factor_feasible_ranges(
    B: pd.DataFrame,
    include_flags: dict,
    factor_order: list[str] | None = None,
) -> pd.DataFrame:
    """
    Under long-only and sum(w)=1, feasible factor beta range is [min(beta_i), max(beta_i)]
    over included securities.
    """
    if B is None or B.empty:
        return pd.DataFrame(columns=["Min beta", "Max beta"])

    tickers = [t for t in B.index if include_flags.get(t, False)]
    if not tickers:
        return pd.DataFrame(columns=["Min beta", "Max beta"])

    B_sub = B.loc[tickers]
    factors = factor_order if factor_order else list(B_sub.columns)

    out = pd.DataFrame(index=factors, columns=["Min beta", "Max beta"], dtype=float)
    for f in factors:
        if f in B_sub.columns:
            col = pd.to_numeric(B_sub[f], errors="coerce")
            out.loc[f, "Min beta"] = float(col.min())
            out.loc[f, "Max beta"] = float(col.max())
    return out


def get_ff5_mom_aud(ff_factors: pd.DataFrame, fx_ret_series: pd.Series) -> pd.DataFrame:
    ff = ff_factors.copy()
    ff = ff.loc[:, ~ff.columns.duplicated()]
    fx_series = pd.to_numeric(fx_ret_series.reindex(ff.index), errors="coerce").fillna(0.0)

    for col in ff.columns:
        if col != "RF":
            ff[col] = pd.to_numeric(ff[col], errors="coerce").fillna(0.0) + fx_series
    return ff


def recommend_factor_tilts(f_mean_ann: pd.Series, Fcov_daily: pd.DataFrame, normalise: bool = True) -> pd.Series:
    """Theoretical unconstrained tilt recommendation: t = Sigma^-1 * mu."""
    fac = list(f_mean_ann.index)
    mu = f_mean_ann.to_numpy(dtype=float)
    Sigma = Fcov_daily.loc[fac, fac].to_numpy(dtype=float) * TRADING_DAYS
    t_opt = np.linalg.pinv(Sigma) @ mu

    if normalise and "Mkt-RF" in fac:
        i_mkt = fac.index("Mkt-RF")
        if abs(t_opt[i_mkt]) > 1e-12:
            # Scale by the MAGNITUDE of the market tilt so every factor's sign is
            # preserved. Dividing by the signed value flips all signs whenever the
            # market tilt is negative (e.g. a down-market estimate window).
            t_opt = t_opt / abs(t_opt[i_mkt])

    return pd.Series(t_opt, index=fac, name="Recommended beta")


def recommend_factor_tilts_achievable(B: pd.DataFrame, f_mean_ann: pd.Series, Fcov_daily: pd.DataFrame):
    """
    Project theoretical factor tilts into the achievable space from investable betas B.
    Returns: (achievable_tilts, theoretical_tilts)
    """
    fac = list(f_mean_ann.index)
    theoretical = recommend_factor_tilts(f_mean_ann, Fcov_daily, normalise=False)

    if B is None or B.empty:
        return theoretical.copy(), theoretical

    B_use = B.reindex(columns=fac).dropna(how="any")
    if B_use.empty:
        return theoretical.copy(), theoretical

    Bmat = B_use.to_numpy(dtype=float)
    n = len(B_use)

    def obj(w):
        return float(np.sum((Bmat.T @ w - theoretical.values) ** 2))

    cons = ({"type": "eq", "fun": lambda w: np.sum(w) - 1.0},)
    bnds = [(0.0, 1.0)] * n
    w0 = np.full(n, 1.0 / n)

    try:
        sol = minimize(obj, w0, method="SLSQP", bounds=bnds, constraints=cons)
        if sol.success:
            w = np.asarray(sol.x, dtype=float)
            achievable = pd.Series(Bmat.T @ w, index=fac)
        else:
            achievable = theoretical.copy()
    except Exception:
        achievable = theoretical.copy()

    try:
        mins = pd.to_numeric(B_use[fac].min(axis=0), errors="coerce")
        maxs = pd.to_numeric(B_use[fac].max(axis=0), errors="coerce")
        achievable = achievable.clip(lower=mins, upper=maxs)
    except Exception:
        pass

    return achievable, theoretical


def max_sharpe_long_only(mu, Sigma, rf: float = 0.0) -> pd.Series:
    """Long-only maximum-Sharpe weights via the standard kappa transform
    (min y'Sigma y s.t. (mu-rf)'y = 1, y >= 0; then normalise). Falls back to the
    minimum-variance long-only portfolio if the Sharpe problem is infeasible
    (e.g. no positive excess returns). Returns weights indexed like the inputs.
    """
    mu = pd.to_numeric(pd.Series(mu), errors="coerce")
    Sigma = pd.DataFrame(Sigma)
    idx = [c for c in Sigma.index if c in Sigma.columns and c in mu.index]
    mu = mu.reindex(idx)
    Sig = Sigma.loc[idx, idx]
    good = mu.notna() & ~Sig.isna().any(axis=1)
    idx = [i for i in idx if bool(good.get(i, False))]
    if not idx:
        return pd.Series(dtype=float)

    mu_v = mu.reindex(idx).to_numpy(dtype=float)
    S_v = Sig.loc[idx, idx].to_numpy(dtype=float)
    S_v = S_v + 1e-10 * np.eye(len(idx))
    n = len(idx)
    excess = mu_v - float(rf)

    w = None
    if np.any(excess > 0):
        y = cp.Variable(n, nonneg=True)
        try:
            prob = cp.Problem(cp.Minimize(cp.quad_form(y, S_v)), [excess @ y == 1])
            prob.solve(solver=cp.OSQP, verbose=False)
            if y.value is None:
                prob.solve(solver=cp.ECOS, verbose=False)
        except Exception:
            pass
        if y.value is not None and float(np.nansum(y.value)) > 1e-12:
            w = np.clip(np.asarray(y.value, dtype=float), 0.0, None)
            w = w / w.sum()

    if w is None:  # fallback: minimum-variance long-only
        wv = cp.Variable(n, nonneg=True)
        try:
            cp.Problem(cp.Minimize(cp.quad_form(wv, S_v)), [cp.sum(wv) == 1]).solve(solver=cp.OSQP, verbose=False)
            if wv.value is None:
                cp.Problem(cp.Minimize(cp.quad_form(wv, S_v)), [cp.sum(wv) == 1]).solve(solver=cp.ECOS, verbose=False)
        except Exception:
            pass
        if wv.value is None:
            return pd.Series(np.full(n, 1.0 / n), index=idx)
        w = np.clip(np.asarray(wv.value, dtype=float), 0.0, None)
        w = (w / w.sum()) if w.sum() > 0 else np.full(n, 1.0 / n)

    return pd.Series(w, index=idx)


def compute_achieved_tilts(
    B: pd.DataFrame,
    w: pd.Series,
    factors=None,
    renormalise_missing: bool = True,
) -> pd.Series:
    """Factor betas of a portfolio: B.T @ w (long-only weights renormalised by default)."""
    if B is None or B.empty:
        return pd.Series(dtype=float)

    w_all = pd.Series(w).reindex(B.index).fillna(0.0)
    if renormalise_missing and float(w_all.sum()) > 0:
        w_use = w_all / float(w_all.sum())
    else:
        w_use = w_all

    out = (B.T @ w_use).rename("Achieved beta")
    return out.reindex(factors) if factors is not None else out


def optimal_portfolio_tilts(B, mu_assets, Sigma_assets, factor_index, included=None, rf: float = 0.0) -> pd.Series:
    """Recommended factor tilts = factor betas of the long-only max-Sharpe portfolio.

    Risk-aware and achievable by construction, so it never collapses to the extreme
    feasible corners the way the old direction-matching projection did. Filters out
    securities with NaN factor betas (e.g. Dimson regression failed for them) so
    they can't masquerade as "risk-free safe assets" to the solver.
    """
    zero = pd.Series(0.0, index=list(factor_index), dtype=float)
    try:
        if B is None or getattr(B, "empty", True):
            return zero
        factors = list(factor_index)
        usable_factor_cols = [f for f in factors if f in B.columns]
        if not usable_factor_cols:
            return zero
        B_clean = B.dropna(subset=usable_factor_cols)
        if B_clean.empty:
            return zero
        mu_raw = pd.to_numeric(pd.Series(mu_assets), errors="coerce").dropna()
        Sig_raw = pd.DataFrame(Sigma_assets)
        candidates = list(B_clean.index)
        if included:
            candidates = [t for t in included if t in candidates]
        candidates = [t for t in candidates
                      if t in mu_raw.index and t in Sig_raw.index and t in Sig_raw.columns]
        if not candidates:
            return zero
        Sig_sub = Sig_raw.loc[candidates, candidates]
        keep = [t for t in candidates if not Sig_sub.loc[t].isna().any()]
        if not keep:
            return zero
        mu_s = mu_raw.reindex(keep)
        Sig = Sig_sub.loc[keep, keep]
        w = max_sharpe_long_only(mu_s, Sig, rf=rf)
        if w is None or w.empty:
            return zero
        return compute_achieved_tilts(B_clean, w, factors=factors).reindex(factors).fillna(0.0)
    except Exception as e:
        print(f"[optimal_portfolio_tilts] {type(e).__name__}: {e}")
        return zero


def recommended_tilts_for_universe(included_tickers, factor_index):
    """Auto-recommend handler for the dialog: factor betas of the risk-optimal
    (max-Sharpe) long-only portfolio for the currently included tickers.
    """
    zero = pd.Series(0.0, index=list(factor_index), dtype=float)
    try:
        B_g = globals().get("B")
        mu_g = globals().get("mu_vec_opt")
        Sig_g = globals().get("Sigma_opt")
        if B_g is None or mu_g is None or Sig_g is None or getattr(B_g, "empty", True):
            return zero
        rf = float(globals().get("rf_annual", 0.0))
        return optimal_portfolio_tilts(
            B_g, mu_g, Sig_g, factor_index, included=included_tickers, rf=rf
        )
    except Exception:
        return zero


# ------------------------------------------------------------
# 6) Build optimiser moments (FF5 model or sample moments)
# ------------------------------------------------------------
WINDOW = 504

# Blended factor expected returns: a long-run premium anchor plus a small, capped
# recent tilt. This stops one quarter's regime (e.g. a market dip) from dictating
# the factor tilts, while still letting recent data nudge them. Short-term momentum
# is captured separately by the MOM factor.
FACTOR_MU_LONG_DAYS = 252 * 5      # ~5yr long-run premium anchor
FACTOR_MU_RECENT_DAYS = 252        # ~1yr recent window
FACTOR_MU_RECENT_WEIGHT = 0.20     # weight on the recent window (0..1)

USE_FF5 = True

B, alpha_daily, resid_var = compute_ff5_betas(
    df_cov_wide,
    ff5_win_for_betas,
    min_obs=120,
)

f_mean_ann = pd.Series(dtype=float)
Fcov_daily = pd.DataFrame()

if USE_FF5 and (B is not None) and (not B.empty):
    ff_aud = get_ff5_mom_aud(ff5_raw, fx_ret)
    ff5_win = ff_aud.tail(WINDOW)
    fac_cols = [c for c in ff5_win.columns if c != "RF"]

    Fcov_daily = ff5_win[fac_cols].cov()
    # Blended expected returns (long-run anchor + small capped recent tilt).
    _mu_long = ff_aud[fac_cols].tail(FACTOR_MU_LONG_DAYS).mean() * TRADING_DAYS
    _mu_recent = ff_aud[fac_cols].tail(FACTOR_MU_RECENT_DAYS).mean() * TRADING_DAYS
    f_mean_ann = (1.0 - FACTOR_MU_RECENT_WEIGHT) * _mu_long + FACTOR_MU_RECENT_WEIGHT * _mu_recent

    alpha_ann = pd.to_numeric(alpha_daily, errors="coerce").fillna(0.0) * TRADING_DAYS

    B_aligned = B.reindex(columns=fac_cols)
    mu_ff_ann = alpha_ann.reindex(B_aligned.index).fillna(0.0) + (B_aligned @ f_mean_ann).fillna(0.0) + float(rf_annual)

    securities_opt = [t for t in B_aligned.index if t not in EXCLUDE_FROM_OPT]

    F = Fcov_daily.to_numpy(dtype=float)
    Bmat = B_aligned.fillna(0.0).to_numpy(dtype=float)
    resid_diag = np.diag(pd.to_numeric(resid_var.reindex(B_aligned.index), errors="coerce").clip(lower=0).fillna(0.0).to_numpy(dtype=float))
    Sigma_ff_daily_np = Bmat @ F @ Bmat.T + resid_diag

    Sigma_ff_daily = pd.DataFrame(Sigma_ff_daily_np, index=B_aligned.index, columns=B_aligned.index)
    Sigma_opt = Sigma_ff_daily.loc[securities_opt, securities_opt].copy()
    mu_vec_opt = mu_ff_ann.reindex(securities_opt).copy()

    exp_ret_label = "Expected Return (annual, FF5 AUD-adjusted)"
else:
    securities_opt = [s for s in valid_all if s not in EXCLUDE_FROM_OPT]
    Sigma_opt = Sigma_daily.loc[securities_opt, securities_opt].copy()
    mu_vec_opt = mu_vec_all.reindex(securities_opt).copy()
    exp_ret_label = "Expected Return (ann., geom)"

# Guardrail: PortfolioValue never belongs in optimisation inputs
if "PortfolioValue" in Sigma_opt.index:
    Sigma_opt = Sigma_opt.drop(index="PortfolioValue", columns="PortfolioValue", errors="ignore")
if "PortfolioValue" in mu_vec_opt.index:
    mu_vec_opt = mu_vec_opt.drop(index="PortfolioValue", errors="ignore")

Sigma_frontier = Sigma_opt.copy()
mu_frontier = mu_vec_opt.copy()
mu_plus = mu_vec_opt.copy()
cov_plus = Sigma_opt.copy()

# Recommended tilts (if factor inputs are available)
tilt_reco_achievable = pd.Series(dtype=float)
w_tilt = None
tilt_reco = pd.Series(dtype=float)

if (B is not None) and (not B.empty) and (not f_mean_ann.empty) and (not Fcov_daily.empty):
    try:
        tilt_reco_achievable = optimal_portfolio_tilts(B, mu_vec_opt, Sigma_opt, TILT_FACTORS, rf=rf_annual)
        tilt_reco = recommend_factor_tilts(f_mean_ann, Fcov_daily)
        print("\nRecommended factor tilts (betas of the risk-optimal long-only portfolio):")
        print(tilt_reco_achievable.round(3))
    except Exception as e:
        print(f"[tilts] recommendation skipped: {e}")


# Display tables (once mu / Sigma are final)
n_opt = len(Sigma_opt.index)
cov_plus = pd.DataFrame(0.0, index=list(Sigma_opt.index) + ["w"], columns=list(Sigma_opt.index) + ["w"])
cov_plus.iloc[:n_opt, :n_opt] = Sigma_opt.values
exp_ret_df = mu_vec_opt.rename(exp_ret_label).to_frame()


# ------------------------------------------------------------
# 8) OPTIMISATION UTILITIES (unconstrained + tilt-constrained)
# ------------------------------------------------------------
def solve_frontier_point_cvxpy(
    mu: pd.Series,
    Sigma: pd.DataFrame,
    target_return: float,
    *,
    use_inequality: bool = True,
    B: pd.DataFrame | None = None,
    tilt_targets: pd.Series | dict | None = None,
    tilt_bands: pd.Series | dict | None = None,
    use_mask: dict | None = None,
    tilt_mode: str = "soft",
    tilt_penalty: float = 1e4,
) -> tuple[np.ndarray, bool, str]:
    """
    Long-only Markowitz with optional factor tilt constraints.
    """
    mu = pd.Series(mu).reindex(Sigma.index)
    mu = pd.to_numeric(mu, errors="coerce")

    keep = mu.index[mu.notna()]
    Sigma_use = Sigma.loc[keep, keep].copy()
    mu_use = mu.loc[keep].astype(float)

    # Drop assets with any NaN covariance row/col
    good = ~(Sigma_use.isna().any(axis=1) | Sigma_use.isna().any(axis=0))
    Sigma_use = Sigma_use.loc[good, good]
    mu_use = mu_use.reindex(Sigma_use.index)

    if len(mu_use) == 0:
        return np.array([]), False, "No valid assets"

    S = Sigma_use.to_numpy(dtype=float)
    S = S + 1e-10 * np.eye(len(S))

    n = len(mu_use)
    w = cp.Variable(n)

    constraints = [cp.sum(w) == 1, w >= 0]
    if use_inequality:
        constraints.append(mu_use.to_numpy(dtype=float) @ w >= float(target_return))
    else:
        constraints.append(mu_use.to_numpy(dtype=float) @ w == float(target_return))

    slack_terms = []
    if B is not None and tilt_targets is not None and tilt_bands is not None:
        B_use = B.reindex(mu_use.index)

        if isinstance(tilt_targets, dict):
            tilt_targets = pd.Series(tilt_targets)
        if isinstance(tilt_bands, dict):
            tilt_bands = pd.Series(tilt_bands)
        if use_mask is None:
            use_mask = {}

        tilt_targets = pd.to_numeric(tilt_targets, errors="coerce")
        tilt_bands = pd.to_numeric(tilt_bands, errors="coerce")

        for f in tilt_targets.index:
            if not bool(use_mask.get(f, False)):
                continue
            if f not in B_use.columns:
                continue

            t = float(tilt_targets.get(f, 0.0))
            b = float(tilt_bands.get(f, 0.0))
            v = pd.to_numeric(B_use[f], errors="coerce").fillna(0.0).to_numpy(dtype=float)

            if tilt_mode.lower() == "hard":
                constraints.append(v @ w <= t + b)
                constraints.append(v @ w >= t - b)
            else:
                s_pos = cp.Variable(nonneg=True)
                s_neg = cp.Variable(nonneg=True)
                constraints.append(v @ w <= (t + b) + s_pos)
                constraints.append(v @ w >= (t - b) - s_neg)
                slack_terms.extend([s_pos, s_neg])

    objective = cp.quad_form(w, S)
    if slack_terms and tilt_mode.lower() == "soft":
        objective = objective + float(tilt_penalty) * cp.sum(cp.hstack(slack_terms))

    prob = cp.Problem(cp.Minimize(objective), constraints)

    try:
        prob.solve(solver=cp.OSQP, verbose=False)
        if w.value is None:
            prob.solve(solver=cp.ECOS, verbose=False)
    except Exception as e:
        return np.full(len(Sigma.index), np.nan), False, f"Solver error: {e}"

    if w.value is None:
        return np.full(len(Sigma.index), np.nan), False, "Infeasible"

    w_sub = np.asarray(w.value).reshape(-1)
    w_full = pd.Series(0.0, index=Sigma.index)
    w_full.loc[mu_use.index] = w_sub

    note = "CVXPY success"
    if slack_terms and tilt_mode.lower() == "soft":
        note = "CVXPY success (soft tilts)"

    return w_full.to_numpy(dtype=float), True, note


def solve_frontier_point_cvxpy_with_tilts(
    mu: pd.Series,
    Sigma: pd.DataFrame,
    target_return: float,
    B: pd.DataFrame,
    tilt_targets: pd.Series,
    tilt_bands: pd.Series,
    use_mask: dict,
    *,
    use_inequality: bool = True,
):
    return solve_frontier_point_cvxpy(
        mu,
        Sigma,
        target_return,
        use_inequality=use_inequality,
        B=B,
        tilt_targets=tilt_targets,
        tilt_bands=tilt_bands,
        use_mask=use_mask,
        tilt_mode="hard",
    )


def optimise_unconstrained_analytic(mu, Sigma, target_return):
    mu = np.asarray(mu, dtype=float)
    Sigma = np.asarray(Sigma, dtype=float)

    n = len(mu)
    ones = np.ones(n)
    Sigma_inv = np.linalg.pinv(Sigma)

    A = ones @ Sigma_inv @ ones
    Bv = ones @ Sigma_inv @ mu
    C = mu @ Sigma_inv @ mu

    M = np.array([[A, Bv], [Bv, C]])
    rhs = np.array([1.0, float(target_return)])

    try:
        alpha, beta = np.linalg.solve(M, rhs)
        w = Sigma_inv @ (alpha * ones + beta * mu)
        return w, "Analytic solution."
    except np.linalg.LinAlgError:
        return np.full(n, np.nan), "Analytic solver failed (singular)."


def optimise_long_only_with_tilts(mu, Sigma, target_return, B, tilt_targets, tilt_bands, use_mask):
    mu_arr = np.asarray(mu, dtype=float)
    Sigma_arr = np.asarray(Sigma, dtype=float)
    n = len(mu_arr)

    def obj(w):
        return float(w @ Sigma_arr @ w)

    constraints = [
        {"type": "eq", "fun": lambda w: np.sum(w) - 1.0},
        {"type": "eq", "fun": lambda w: float(mu_arr @ w) - float(target_return)},
    ]

    factors = list(tilt_targets.keys()) if hasattr(tilt_targets, "keys") else []
    for f in factors:
        if not use_mask.get(f, True):
            continue
        if hasattr(B, "columns") and f not in B.columns:
            continue

        t = float(tilt_targets.get(f, 0.0))
        b = float(tilt_bands.get(f, 0.05))
        v = np.asarray(pd.Series(B[f]).reindex(range(n)).fillna(0.0), dtype=float)

        constraints.append({"type": "ineq", "fun": lambda w, v=v, t=t, b=b: (t + b) - float(v @ w)})
        constraints.append({"type": "ineq", "fun": lambda w, v=v, t=t, b=b: float(v @ w) - (t - b)})

    x0 = np.full(n, 1.0 / n)
    bounds = [(0.0, 1.0)] * n

    try:
        sol = minimize(obj, x0, method="SLSQP", bounds=bounds, constraints=constraints)
        if not sol.success:
            return np.full(n, np.nan), f"SLSQP failed: {sol.message}"
        return np.asarray(sol.x, dtype=float), "SLSQP success"
    except Exception as e:
        return np.full(n, np.nan), f"SLSQP error: {e}"


def _build_frontier(
    mu_vec_opt: pd.Series,
    Sigma_opt: pd.DataFrame,
    target_returns: list[float] | None = None,
    *,
    n_points: int = 18,
    max_excess_return: float = 0.40,
) -> tuple[pd.DataFrame, pd.DataFrame, float, float]:
    """
    Build long-only efficient frontier on realistic target returns.
    """
    mu = pd.to_numeric(mu_vec_opt, errors="coerce").reindex(Sigma_opt.index)
    keep = mu.index[mu.notna()]

    Sigma_clean = Sigma_opt.loc[keep, keep].copy()
    good = ~(Sigma_clean.isna().any(axis=1) | Sigma_clean.isna().any(axis=0))
    Sigma_clean = Sigma_clean.loc[good, good]
    mu_clean = mu.reindex(Sigma_clean.index).astype(float)

    if len(mu_clean) == 0:
        raise ValueError("No valid assets after cleaning mu/Sigma")

    assets = list(Sigma_clean.index)
    S = Sigma_clean.to_numpy(dtype=float)
    S = S + 1e-10 * np.eye(len(S))
    mu_arr = mu_clean.to_numpy(dtype=float)

    # Global minimum-variance portfolio
    n = len(assets)
    w_var = cp.Variable(n)
    prob_mvp = cp.Problem(cp.Minimize(cp.quad_form(w_var, S)), [cp.sum(w_var) == 1, w_var >= 0])
    try:
        prob_mvp.solve(solver=cp.OSQP, verbose=False)
        if w_var.value is None:
            prob_mvp.solve(solver=cp.ECOS, verbose=False)
    except Exception:
        pass

    if w_var.value is None:
        w_mvp = np.full(n, 1.0 / n)
    else:
        w_mvp = np.asarray(w_var.value).reshape(-1)

    R_mvp_ann = float(w_mvp @ mu_arr)
    mu_max = float(np.nanmax(mu_arr))

    low_floor = float(rf_annual)
    max_high_allowed = min(mu_max, low_floor + max_excess_return)

    span_down = max(R_mvp_ann - low_floor, 0.0)
    span_up = max(max_high_allowed - R_mvp_ann, 0.0)
    span = min(span_down, span_up)

    if span <= 0:
        low = low_floor
        high = max_high_allowed
    else:
        low = R_mvp_ann - span
        high = R_mvp_ann + span

    if high <= low + 0.01:
        high = low + 0.06

    if target_returns is None:
        target_returns = np.linspace(low, high, n_points).tolist()

    print(f"[frontier] target_returns from {target_returns[0]:.4%} to {target_returns[-1]:.4%}")

    weights_dict = {}
    stats_rows = []

    for R in target_returns:
        w_full, ok, note = solve_frontier_point_cvxpy(
            mu_clean,
            Sigma_clean,
            R,
            use_inequality=True,
        )

        # w_full returned on Sigma_clean.index length
        if len(w_full) != len(assets):
            w_series = pd.Series(0.0, index=assets)
        else:
            w_series = pd.Series(w_full, index=assets)

        weights_dict[R] = w_series.to_numpy(dtype=float)

        if ok and np.isfinite(w_series.to_numpy(dtype=float)).all():
            wv = w_series.to_numpy(dtype=float)
            vol_ann = float(np.sqrt(max(wv @ S @ wv, 0.0)) * np.sqrt(TRADING_DAYS))
            achieved = float(mu_arr @ wv)
        else:
            vol_ann = np.nan
            achieved = np.nan

        sharpe = (
            (achieved - float(rf_annual)) / vol_ann
            if (pd.notna(vol_ann) and vol_ann > 0 and pd.notna(achieved))
            else np.nan
        )

        stats_rows.append(
            {
                "Target Return": float(R),
                "Achieved Return": achieved,
                "Volatility (ann.)": vol_ann,
                "Sharpe": sharpe,
                "Method": "Frontier CVXPY",
                "Note": note,
            }
        )

    target_returns = [r for r in target_returns if np.isfinite(r)]
    cols = [
        f"{r*100:.1f}%" if (r * 100) % 1 != 0 else f"{int(r*100):d}%"
        for r in target_returns
    ]

    W = pd.DataFrame(
        {c: weights_dict[R] for c, R in zip(cols, target_returns)},
        index=assets,
    )

    stats_df = pd.DataFrame(stats_rows)
    stats_df.insert(0, "Target (%)", cols)
    stats_df = stats_df.drop(columns=["Target Return"])

    sh = pd.to_numeric(stats_df["Sharpe"], errors="coerce")
    if sh.notna().any():
        best_idx = int(sh.idxmax())
    else:
        vol_series = pd.to_numeric(stats_df["Volatility (ann.)"], errors="coerce")
        best_idx = int(vol_series.idxmin()) if vol_series.notna().any() else 0

    tan_ret = float(pd.to_numeric(stats_df.loc[best_idx, "Achieved Return"], errors="coerce"))
    tan_vol = float(pd.to_numeric(stats_df.loc[best_idx, "Volatility (ann.)"], errors="coerce"))

    if not np.isfinite(tan_ret) or not np.isfinite(tan_vol):
        tan_ret, tan_vol = np.nan, np.nan

    print(f"[frontier] tangency ret ~ {tan_ret:.4f}, vol ~ {tan_vol:.4f}")
    return W, stats_df, tan_ret, tan_vol


# ------------------------------------------------------------
# 9) FRONTIER: MVP-centred long-only
# ------------------------------------------------------------
mu_frontier = mu_vec_opt.copy()
Sigma_frontier = Sigma_opt.copy()

W, stats_df, tan_ret, tan_vol = _build_frontier(
    mu_frontier,
    Sigma_frontier,
    target_returns=None,
    n_points=20,
    max_excess_return=0.95,
)


# ------------------------------------------------------------
# 10) PREPARE A TRADE PLAN
# ------------------------------------------------------------
cov_plus = cov_plus.fillna(0.0)
exp_ret_df = mu_vec_opt.rename(exp_ret_label).to_frame()


def make_trade_plan(
    units_cur,
    last_px,
    fx_map,
    w_target,
    include_flags,
    include_zero_lines: bool = False,
    portfolio_value_override=None,
):
    """
    Return (trade_df, residual_cash) to move from current units to target weights (AUD).
    """
    tickers = pd.Index(w_target.index, name="Security")

    lp = pd.to_numeric(last_px, errors="coerce").reindex(tickers).fillna(0.0)
    fx = pd.Series(1.0, index=tickers)
    if isinstance(fx_map, (dict, pd.Series)):
        fx = pd.to_numeric(pd.Series(fx_map), errors="coerce").reindex(tickers).fillna(1.0)

    px_aud = (lp * fx).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    cur_units = pd.to_numeric(units_cur, errors="coerce").reindex(tickers).fillna(0).astype(int)

    cur_val = float((cur_units * px_aud).sum())
    if portfolio_value_override is not None and np.isfinite(portfolio_value_override) and portfolio_value_override > 0:
        cur_val = float(portfolio_value_override)

    tgt_val = pd.to_numeric(w_target, errors="coerce").reindex(tickers).fillna(0.0) * cur_val
    tgt_units = (tgt_val / px_aud.replace(0.0, np.nan)).fillna(0.0).round().astype(int)

    if isinstance(include_flags, dict):
        inc = pd.Series(include_flags).reindex(tickers).fillna(True).astype(bool)
        tgt_units.loc[~inc] = cur_units.loc[~inc]

    delta = (tgt_units - cur_units).astype(int)
    cash_flow = (-delta * px_aud).astype(float)

    df = pd.DataFrame(
        {
            "Security": tickers,
            "Curr Units": cur_units.values,
            "Target Units": tgt_units.values,
            "Delta Units": delta.values,
            "Last Px (AUD)": px_aud.values,
            "Cash Flow (AUD)": cash_flow.values,
        }
    ).set_index("Security")

    if not include_zero_lines:
        df = df.loc[df["Delta Units"] != 0]

    residual_cash = float(df["Cash Flow (AUD)"].sum())
    return df, residual_cash


def compute_target_units_for_holdings(
    units_cur,
    last_px,
    fx_map,
    w_target,
    include_flags,
    portfolio_value_override=None,
):
    tickers = list(pd.Index(w_target.index))

    inc = pd.Series(include_flags).reindex(tickers).fillna(True).astype(bool)
    tickers = [t for t in tickers if inc.get(t, True)]

    lp_aud = (
        pd.Series(last_px).reindex(tickers).astype(float)
        * pd.Series(fx_map).reindex(tickers).fillna(1.0).astype(float)
    )
    cur_units = pd.Series(units_cur).reindex(tickers).fillna(0.0).astype(float)

    cur_val = float((cur_units * lp_aud).sum())
    if portfolio_value_override is not None and np.isfinite(portfolio_value_override) and portfolio_value_override > 0:
        cur_val = float(portfolio_value_override)

    if cur_val <= 0:
        return pd.Series(0, index=w_target.index, dtype=int)

    tgt_val = pd.Series(w_target).reindex(tickers).fillna(0.0) * cur_val
    tgt_units_float = (tgt_val / lp_aud).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    tgt_units_int = tgt_units_float.round().astype(int)
    return tgt_units_int.reindex(w_target.index).fillna(0).astype(int)


def generate_targets_mvp_centric(mu_vec, Sigma, span_vol: float = 0.20, n_points: int = 20):
    """
    Compute MVP and return targets around MVP Â± span_vol * MVP_vol.
    """
    mu_arr = np.asarray(mu_vec, dtype=float)
    S = np.asarray(Sigma, dtype=float)
    n = len(mu_arr)

    x0 = np.full(n, 1.0 / n)
    cons = ({"type": "eq", "fun": lambda w: np.sum(w) - 1.0},)
    bnds = [(0.0, 1.0)] * n

    def vol_objective(w):
        return float(np.sqrt(max(w @ S @ w, 0.0)))

    sol = minimize(vol_objective, x0, bounds=bnds, constraints=cons, method="SLSQP")
    if not sol.success:
        raise RuntimeError("Could not compute MVP")

    w_mvp = np.asarray(sol.x, dtype=float)
    mu_mvp = float(w_mvp @ mu_arr)
    vol_mvp = float(np.sqrt(max(w_mvp @ S @ w_mvp, 0.0)))

    lo = mu_mvp - span_vol * vol_mvp
    hi = mu_mvp + span_vol * vol_mvp
    targets = np.linspace(lo, hi, n_points)
    return targets, mu_mvp, vol_mvp


print("\n--- DEBUG CHECK: Sigma_opt / mu_vec_opt ---")
print("Any NaN in Sigma_opt:", bool(Sigma_opt.isna().any().any()))
print("Any NaN in mu_vec_opt:", bool(mu_vec_opt.isna().any()))
print("Min variance:", float(np.nanmin(np.diag(Sigma_opt))))
print("Number of assets:", len(Sigma_opt))
print(Sigma_opt.head())
print(mu_vec_opt.head())
print("OPT TICKERS:", list(securities_opt))
print("mu:", mu_vec_opt.describe())
print("Sigma diag min/max:", float(np.nanmin(Sigma_opt.values.diagonal())), float(np.nanmax(Sigma_opt.values.diagonal())))
if not f_mean_ann.empty:
    print(f_mean_ann)


# =====================================================================
# BLOCK 6 Transaction costs
# =====================================================================
# --- Brokerage & CGT config (edit these to suit) ---
BROKERAGE = {
    "ASX": {"first_buy_free_threshold": 1000.0, "min_fee": 11.0, "rate": 0.001},
    "US": {"min_fee": 0.0, "rate": 0.0},
}

MIN_TRADE_VALUE = 11.0
TRADE_DELTA_CANDIDATES = ("Delta Units", "ÃŽâ€ Units")


def _market_of(ticker: str) -> str:
    t = str(ticker)
    if t.startswith("^"):
        return "INDEX"
    if t.endswith(".AX"):
        return "ASX"
    return "US"


def _trade_delta_col(trade_df: pd.DataFrame) -> str | None:
    for c in TRADE_DELTA_CANDIDATES:
        if c in trade_df.columns:
            return c
    return None


def _security_from_row(idx, row: pd.Series) -> str:
    if "Security" in row.index:
        return str(row["Security"])
    return str(idx)


def suppress_small_trades_by_value(
    trade_df: pd.DataFrame,
    min_trade_value_aud: float = MIN_TRADE_VALUE,
) -> pd.DataFrame:
    """
    Suppress trades where abs(delta_units) * Last Px (AUD) <= threshold.

    Recomputes Cash Flow (AUD) using suppressed units with convention:
      cash flow > 0 for sells, < 0 for buys.
    """
    if trade_df is None or trade_df.empty:
        return trade_df

    out = trade_df.copy()
    delta_col = _trade_delta_col(out)
    if delta_col is None or "Last Px (AUD)" not in out.columns:
        return out

    du = pd.to_numeric(out[delta_col], errors="coerce").fillna(0.0)
    px = pd.to_numeric(out["Last Px (AUD)"], errors="coerce").fillna(0.0)

    trade_val = (du.abs() * px).astype(float)
    suppressed = trade_val <= float(min_trade_value_aud)

    du_adj = du.where(~suppressed, 0.0).round().astype(int)

    out["Trade Value (AUD)"] = trade_val
    out["Suppressed"] = suppressed.astype(bool)
    out[delta_col] = du_adj
    out["Cash Flow (AUD)"] = (-du_adj * px).astype(float)
    return out


def compute_brokerage(trade_df: pd.DataFrame) -> tuple[float, pd.Series]:
    """Return (total_brokerage_AUD, per_row_series)."""
    if trade_df is None or trade_df.empty:
        return 0.0, pd.Series(dtype=float)

    delta_col = _trade_delta_col(trade_df)
    if delta_col is None:
        return 0.0, pd.Series(0.0, index=trade_df.index, name="Brokerage (AUD)")

    fees = []
    asx_buy_candidates = []  # (row_idx, trade_value)

    for i, r in trade_df.iterrows():
        units = float(pd.to_numeric(r.get(delta_col, 0.0), errors="coerce") or 0.0)
        if abs(units) < 1e-12:
            fees.append(0.0)
            continue

        sec = _security_from_row(i, r)
        mkt = _market_of(sec)
        px = float(pd.to_numeric(r.get("Last Px (AUD)", 0.0), errors="coerce") or 0.0)
        trade_val = abs(units) * px

        if mkt == "US":
            fee = 0.0
        elif mkt == "ASX":
            fee = max(BROKERAGE["ASX"]["min_fee"], BROKERAGE["ASX"]["rate"] * trade_val)
            if units > 0 and trade_val <= BROKERAGE["ASX"]["first_buy_free_threshold"] + 1e-9:
                asx_buy_candidates.append((i, trade_val))
        else:
            fee = 0.0

        fees.append(float(fee))

    fees = pd.Series(fees, index=trade_df.index, name="Brokerage (AUD)")

    # First ASX buy <= threshold can be brokerage-free for one row.
    if asx_buy_candidates:
        idx0 = sorted(asx_buy_candidates, key=lambda x: x[1])[0][0]
        fees.loc[idx0] = 0.0

    return float(fees.sum()), fees


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
        "audit": pd.DataFrame(audit_rows),
    }
    return float(tax), bkd


def evaluate_transaction_costs(
    trade_df: pd.DataFrame,
    lots_df: pd.DataFrame,
    sale_date: pd.Timestamp,
    marginal_rate: float,
    carry_forward_loss: float = 0.0,
    method: str = "HIFO",
) -> dict:
    brokerage_total, brokerage_per_row = compute_brokerage(trade_df)
    tax_total, tax_bkd = compute_cgt_tax(
        trade_df,
        lots_df,
        sale_date,
        marginal_rate=float(marginal_rate),
        carry_forward_loss=float(carry_forward_loss),
        method=str(method),
    )
    return {
        "brokerage": brokerage_total,
        "cgt_tax": tax_total,
        "total_cost": brokerage_total + tax_total,
        "breakdown": tax_bkd,
        "per_row_brokerage": brokerage_per_row,
    }


def _update_lots_after_trades(
    lots_df: pd.DataFrame,
    trade_df: pd.DataFrame,
    sale_date: pd.Timestamp,
    fx_map: pd.Series | dict,
):
    """
    Apply executed trades to lots table.
      - Sells: decrement matched lots using LOT_MATCH_METHOD.
      - Buys: append a new lot at current Last Px (AUD).
    Returns a new lots DataFrame.
    """
    base_cols = ["Security", "AcqDate", "Units", "CostBaseAUD"]
    out = lots_df.copy() if lots_df is not None else pd.DataFrame(columns=base_cols)

    for c in base_cols:
        if c not in out.columns:
            out[c] = np.nan

    out["AcqDate"] = pd.to_datetime(out["AcqDate"], errors="coerce")
    out["Units"] = pd.to_numeric(out["Units"], errors="coerce").fillna(0.0)
    out["CostBaseAUD"] = pd.to_numeric(out["CostBaseAUD"], errors="coerce").fillna(0.0)

    if trade_df is None or trade_df.empty:
        return out[base_cols].copy()

    delta_col = _trade_delta_col(trade_df)
    if delta_col is None:
        return out[base_cols].copy()

    for i, tr in trade_df.iterrows():
        sec = _security_from_row(i, tr)
        dU = int(pd.to_numeric(tr.get(delta_col, 0), errors="coerce") or 0)
        px_aud = float(pd.to_numeric(tr.get("Last Px (AUD)", 0.0), errors="coerce") or 0.0)

        if dU < 0:
            lot_block = out[out["Security"] == sec].copy()
            if lot_block.empty:
                continue

            if str(LOT_MATCH_METHOD).upper() == "HIFO":
                lot_block = lot_block.sort_values(by=["CostBaseAUD", "AcqDate"], ascending=[False, True])
            else:
                lot_block = lot_block.sort_values(by=["AcqDate"], ascending=True)

            remaining = abs(dU)
            for lot_idx in lot_block.index:
                if remaining <= 0:
                    break
                have = float(out.at[lot_idx, "Units"])
                take = min(remaining, have)
                out.at[lot_idx, "Units"] = have - take
                remaining -= take

            out = out[out["Units"] > 0.0].copy()

        elif dU > 0:
            new_lot = pd.DataFrame(
                [
                    {
                        "Security": sec,
                        "AcqDate": pd.Timestamp(sale_date),
                        "Units": int(dU),
                        "CostBaseAUD": px_aud,
                    }
                ]
            )
            out = pd.concat([out, new_lot], ignore_index=True)

    return out[base_cols].copy()


# ------------------------------
# BLOCK 5 CODE GRAVEYARD
# ------------------------------
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


# Load parcels once (if sheet missing, returns empty table)
lots_df = _read_lots_from_path(filename, "Lots")


# =====================================================================
# Block 7 Writing into the excel (i.e. formatting and building the actual sheet)
# =====================================================================
# ------------------------------------------------------------
# 10) WRITE TO EXCEL 
# ------------------------------------------------------------
def ensure_workbook(path):
    if os.path.exists(path):
        return
    with xw.App(visible=False, add_book=True) as app:
        wb = app.books.add()
        for nm in ["Holdings","Tilts","OPT","Input","Cov","FF5F","Lots"]:
            try: wb.sheets[nm]
            except: wb.sheets.add(nm)
        # Minimal headers
        wb.sheets["Holdings"].range("A1").value = [["Security","Units","Last Price","FX to AUD","Market Value","Weight","Include?"]]
        wb.sheets["Tilts"].range("A1").value = [["Factor","Target","Band","Use?"]]
        wb.sheets["Tilts"].range("A2").value = [[f, (1.0 if i==0 else 0.0), 0.20, (i==0)] for i,f in enumerate(TILT_FACTORS)]
        wb.sheets["Lots"].range("A1").value = [["Security","AcqDate","Units","CostBaseAUD"]]
        wb.save(path); wb.close()

# Call it right before Block 7 seed reads:
ensure_workbook(filename)
print("[cfg] excel_path:", filename)

# Define path for saving portfolio state if not already defined
state_path = os.path.join(os.path.dirname(filename), "portfolio_state.json")
global results

OPEN_EXCEL_AFTER_SAVE = bool(globals().get("OPEN_EXCEL_AFTER_SAVE", CFG.get("open_excel_after_save", True)))
OPEN_PPT_AFTER_SAVE = bool(globals().get("OPEN_PPT_AFTER_SAVE", CFG.get("open_ppt_after_save", True)))

# -------------------------------
# Writers (used by Block 7)
# -------------------------------
def _write_tilts_sheet(wb, tilts_df, sheet_name="Tilts"):
    try:
        sht = wb.sheets[sheet_name]
    except Exception:
        sht = wb.sheets.add(sheet_name, after=wb.sheets[-1])
    try:
        sht.used_range.clear_contents()
    except Exception:
        pass

    out = tilts_df.reset_index().rename(columns={"index": "Factor"})
    out = out[["Factor","Target","Band","Use?"]]
    sht.range("A1").value = [["Factor","Target","Band","Use?"]]
    sht.range("A2").options(index=False, header=False).value = out
    last_row = 1 + len(out)
    try:
        sht.range(f"B2:B{last_row}").api.NumberFormat = "0.000"
        sht.range(f"C2:C{last_row}").api.NumberFormat = "0.000"
        val_rng = sht.range(f"D2:D{last_row}").api
        val_rng.Validation.Delete()
        val_rng.Validation.Add(3, 1, 1, "TRUE,FALSE")
    except Exception:
        pass
    sht.autofit()


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
        inc = bool(include_s.get(t, False))
        rows.append({
            "Security": t,
            "Units": float(units_s.get(t, 0.0)),
            "Last Price": float(pd.Series(last_px).get(t, np.nan)),
            "FX to AUD": float(pd.Series(fx_to_aud_map).get(t, 1.0)),
            "Market Value": 0.0,
            "Weight": 0.0,
            "Include?": inc
        })
    df = pd.DataFrame(rows)

    try:
        sht = wb.sheets[sheet_name]
    except Exception:
        sht = wb.sheets.add(sheet_name, after=wb.sheets[-1])
    try:
        sht.used_range.clear_contents()
    except Exception:
        pass

    sht.range('A1').value = [["Security","Units","Last Price","FX to AUD","Market Value","Weight","Include?"]]
    sht.range('A2').options(index=False, header=False).value = df
    n = len(df); last_row = 1 + n
    last_row = 1 + len(df)
    if n >= 1:
        sht.range('E2').formula = "=B2*C2*D2"
        if n > 1:
            sht.range(f"E2:E{last_row}").api.FillDown()
        sumif_den = f"SUMIF($G$2:$G${last_row},TRUE,$E$2:$E${last_row})"
        sht.range('F2').formula = f"=IF({sumif_den}=0,0,IF($G2,E2/{sumif_den},0))"
        if n > 1:
            sht.range(f"F2:F{last_row}").api.FillDown()
        try:
            val_rng = sht.range(f"G2:G{last_row}").api
            val_rng.Validation.Delete()
            val_rng.Validation.Add(3, 1, 1, "TRUE,FALSE")
        except Exception:
            pass
        try:
            sht.range(f"C2:C{last_row}").api.NumberFormat = "0.0000"
            sht.range(f"D2:D{last_row}").api.NumberFormat = "0.0000"
            sht.range(f"E2:E{last_row}").api.NumberFormat = "$0.00"
            sht.range(f"F2:F{last_row}").api.NumberFormat = "0.00%"
        except Exception:
            pass
    sht.autofit()

def update_efficient_frontier_chart(
    opt_sheet, stats_df, start_s_row, rf_annual,
    tan_ret, tan_vol, current_point,
    title_text, target_point=None,
    previous_point=None, factor_point=None,
    no_tilt_point=None,
    tilt_point=None
):
    """Safe no-crash chart updater. Creates/refreshes a single scatter chart named 'Efficient Frontier' on OPT."""
    try:
        co = opt_sheet.api.ChartObjects()

        # stats_df was written at A{start_s_row+1} with a header row,
        # so first numeric data row is start_s_row+2
        first_row = start_s_row + 2
        last_row  = first_row + len(stats_df) - 1

        # Build x/y ranges up front (every time)
        x_rng = opt_sheet.range(f"C{first_row}:C{last_row}").api  # Volatility (ann.)
        y_rng = opt_sheet.range(f"B{first_row}:B{last_row}").api  # Achieved Return

        # Find existing chart by title
        the_chart = None
        for i in range(1, co.Count + 1):
            ch_i = co.Item(i).Chart
            if ch_i.HasTitle and "Efficient Frontier" in str(getattr(ch_i.ChartTitle, "Text", "")):
                the_chart = co.Item(i)
                break

        # Create if missing
        if the_chart is None:
            the_chart = opt_sheet.api.ChartObjects().Add(10, 10, 600, 360)  # x,y,w,h
            the_chart.Chart.ChartType = 74  # xlXYScatterSmoothNoMarkers
            the_chart.Chart.HasTitle = True
            the_chart.Chart.ChartTitle.Text = "Efficient Frontier"

        ch = the_chart.Chart

        # Clear series
        try:
            while ch.SeriesCollection().Count > 0:
                ch.SeriesCollection(1).Delete()
        except Exception:
            pass

        # Efficient Frontier (smooth line, no markers)
        s1 = ch.SeriesCollection().NewSeries()
        s1.Name = "Efficient Frontier"
        s1.XValues = x_rng
        s1.Values  = y_rng
        try:
            s1.ChartType = 74          # xlXYScatterSmoothNoMarkers
            s1.MarkerStyle = -4142     # xlMarkerStyleNone
            s1.Smooth = True
        except Exception:
            pass

        # Current portfolio marker
        if current_point:
            s3 = ch.SeriesCollection().NewSeries()
            s3.Name = "Current"
            s3.XValues = [float(current_point[0])]
            s3.Values  = [float(current_point[1])]
            try:
                s3.ChartType = -4169
                s3.MarkerStyle = 8
                s3.MarkerSize = 8
            except Exception:
                pass

        # Previous portfolio marker
        if previous_point:
            sp = ch.SeriesCollection().NewSeries()
            sp.Name = "Previous"
            sp.XValues = [float(previous_point[0])]
            sp.Values  = [float(previous_point[1])]
            try:
                sp.ChartType = -4169
                sp.MarkerStyle = 9
                sp.MarkerSize = 9
            except Exception:
                pass

        # Factor-effected marker
        if factor_point:
            sf = ch.SeriesCollection().NewSeries()
            sf.Name = "Factor-effected"
            sf.XValues = [float(factor_point[0])]
            sf.Values  = [float(factor_point[1])]
            try:
                sf.ChartType = -4169
                sf.MarkerStyle = 4
                sf.MarkerSize = 10
            except Exception:
                pass

        # Target portfolio marker
        if target_point:
            s4 = ch.SeriesCollection().NewSeries()
            s4.Name = "Target"
            s4.XValues = [float(target_point[0])]
            s4.Values  = [float(target_point[1])]
            try:
                s4.ChartType = -4169
                s4.MarkerStyle = 2
                s4.MarkerSize = 10
            except Exception:
                pass

        # Target (No Tilts) point (if available)
        if no_tilt_point:
            s_nt = ch.SeriesCollection().NewSeries()
            s_nt.Name = "Target (No Tilts)"
            s_nt.XValues = [float(no_tilt_point[0])]
            s_nt.Values  = [float(no_tilt_point[1])]
            try:
                s_nt.ChartType = -4169      # markers only
                s_nt.MarkerStyle = 3        # triangle
                s_nt.MarkerSize = 9
            except Exception:
                pass
        # Target (With Tilts) point (if available)
        if tilt_point:
            s_tilt = ch.SeriesCollection().NewSeries()
            s_tilt.Name = "Target (With Tilts)"
            s_tilt.XValues = [float(tilt_point[0])]
            s_tilt.Values  = [float(tilt_point[1])]
            try:
                s_tilt.ChartType = -4169
                s_tilt.MarkerStyle = 8   # circle
                s_tilt.MarkerSize = 9
            except Exception:
                pass

        
        # Title
        ch.ChartTitle.Text = title_text if title_text else "Efficient Frontier"

    except Exception as e:
        print(f"[chart] Skipping chart update (safe wrapper): {e}")



# ---- 10A) Read seeds (no COM; avoids UsedRange issues) ----

seed_units, seed_include = _read_holdings_seed_from_path(filename, sheet_name="Holdings")
tilt_seed = _read_tilts_seed_from_path(filename, sheet_name="Tilts")

# Ensure MOM exists in the seed and rows are in the canonical order
if not isinstance(tilt_seed, pd.DataFrame) or tilt_seed.empty:
    tilt_seed = pd.DataFrame(
        {"Target":[1.0] + [0.0]*(len(TILT_FACTORS)-1),
         "Band":[0.20]*len(TILT_FACTORS),
         "Use?":[True] + [False]*(len(TILT_FACTORS)-1)},
        index=TILT_FACTORS
    )
else:
    for f in TILT_FACTORS:
        if f not in tilt_seed.index:
            tilt_seed.loc[f] = {"Target":0.0, "Band":0.20, "Use?":False}
    tilt_seed = tilt_seed.reindex(TILT_FACTORS)

# Seed the tilt TARGETS from the current portfolio's own factor exposures, so the
# dialog opens at "where you are now" rather than arbitrary hardcoded defaults.
try:
    if "B" in globals() and isinstance(B, pd.DataFrame) and not B.empty:
        _last_px_seed = (
            prices.ffill().iloc[-1]
            if isinstance(prices, pd.DataFrame) and not prices.empty
            else pd.Series(dtype=float)
        )
        _w_cur_seed = current_holdings_weights(
            seed_units,
            _last_px_seed,
            list(B.index),
            fx_map_all if "fx_map_all" in globals() else 1.0,
        )
        _cur_tilts = compute_achieved_tilts(B, _w_cur_seed, factors=TILT_FACTORS)
        if _cur_tilts is not None and float(pd.Series(_w_cur_seed).abs().sum()) > 0 and not _cur_tilts.dropna().empty:
            tilt_seed["Target"] = (
                pd.to_numeric(_cur_tilts.reindex(TILT_FACTORS), errors="coerce").fillna(0.0).round(3)
            )
            print("[tilts] Seeded dialog targets from current portfolio factor exposures.")
except Exception as _e_seed:
    print(f"[tilts] Could not seed from current portfolio; using sheet/defaults: {_e_seed}")

# ---- 10B) Combined dialog (holdings + tilts) ----
res = edit_holdings_and_tilts_dialog(
    prices=prices,
    exclude=EXCLUDE_FROM_OPT,
    seed_units=current_holdings_units if 'current_holdings_units' in globals() and current_holdings_units is not None else seed_units,
    seed_include=seed_include,
    seed_tilts=tilt_seed
)
if res is None:
    units = seed_units.copy()
    include_flags = seed_include.copy()
    last_px_hold = prices.ffill().iloc[-1].reindex(units.index)
    tilt_df = tilt_seed.copy()
else:
    if len(res) == 6:
        units, last_px_hold, prices, include_flags, tilt_df, portfolio_value_override = res
    else:
        units, last_px_hold, prices, include_flags, tilt_df = res
        portfolio_value_override = None
            
    current_holdings_units = units.copy() 

# ---- Make optimiser globals available ----
current_holdings_units = units
securities_opt = list(units.index)
lots_df = lots_df  # already loaded earlier in block 7
gamma_cgt = 0.005        # soft penalty weight for CGT (tune as desired)
beta_brokerage = 0.25    # soft penalty weight for brokerage (tune as desired)

# --- helper: rebuild analytics from (possibly updated) prices ---
def _rebuild_core_from_prices(prices, fx_ticker="USDAUD=X", period="5y"):
    fx_raw = yf.download(fx_ticker, period=period, interval="1d",
                         auto_adjust=True, threads=False, progress=False)
    fx = fx_raw["Close"] if isinstance(fx_raw, pd.DataFrame) else fx_raw
    if isinstance(fx, pd.DataFrame):
        fx = fx.iloc[:, 0]
    fx = pd.to_numeric(fx, errors="coerce").reindex(prices.index).ffill()

    usd_cols = [c for c in prices.columns
                if not str(c).endswith(".AX") and not str(c).startswith("^")]

    prices_aud = prices.copy()
    prices_aud = prices_aud.drop(columns=[c for c in ["PortfolioValue"] if c in prices_aud.columns], errors="ignore")

    if usd_cols:
        prices_aud.update(prices.loc[:, usd_cols].mul(fx, axis=0))

    # FIX: fill missing values AFTER FX conversion but BEFORE returns
    prices_aud = prices_aud.ffill().bfill()

    # Melt into long format
    d = (prices_aud.reset_index()
         .melt(id_vars="Date", var_name="Security", value_name="Close")
         .sort_values(["Security", "Date"]))

    d["Return"] = d.groupby("Security", sort=False)["Close"].pct_change(fill_method=None)
    d = d.dropna()

    df_cov_wide = d.pivot(index="Date", columns="Security", values="Return").sort_index()
    rets_opt = df_cov_wide.dropna(how="any")
    Sigma_daily = df_cov_wide.cov()

    d["LogRet"] = np.log1p(d["Return"])
    mu_log_ann = d.groupby("Security")["LogRet"].mean() * 252.0
    mu_ann_geo = np.expm1(mu_log_ann)

    return prices_aud, d, df_cov_wide, Sigma_daily, mu_ann_geo


# === Rebuild core analytics ===
prices_aud_for_returns, df_melt, df_cov_wide, Sigma_daily, mu_ann_geo = _rebuild_core_from_prices(prices)
globals()["returns_wide_df"] = df_cov_wide.copy()

# Tables used later
n_opt = len(securities_opt)
cov_plus = Sigma_opt.copy()
cov_plus.loc[:, 'w'] = 0.0
cov_plus.loc['w', :] = 0.0
cov_plus.loc['w', 'w'] = 0.0
exp_ret_df = mu_vec_opt.rename(exp_ret_label).to_frame()

# FX map used by Holdings + trade plan
usd_aud    = get_usd_aud_fx()
fx_map_all = fx_to_aud_for_tickers(prices.columns, usd_aud)

# ---- 10D) Reopen Excel and WRITE everything, then close ----
if USE_XLWINGS:
    try:
        with xw.App(visible=False, add_book=False) as app:
            filename = os.path.abspath(filename)
            wb = app.books.open(filename, update_links=False, read_only=False)      
            
            if bool(wb.api.ReadOnly):
                # If Excel forces read-only (usually file is already open/locked), write to a new file instead of CSV fallback
                base, ext = os.path.splitext(filename)
                alt = base + "_AUTO" + ext
                shutil.copy2(filename, alt)
                print(f"[warn] Workbook opened read-only. Will write to: {alt}")
                wb.close()
                wb = app.books.open(alt, update_links=False, read_only=False)
            
            wb.activate()
            app.display_alerts = False
            app.screen_updating = False
            try: app.api.EnableEvents = False
            except Exception: pass
            time.sleep(0.2)

            # Pick the max-Sharpe portfolio column once for reuse
            sh = pd.to_numeric(stats_df['Sharpe'], errors='coerce').fillna(-1)
            best_idx = int(sh.values.argmax()) if len(sh) else 0
            w_star = W.iloc[:, best_idx].reindex(W.index).fillna(0.0)

            w_star_no_tilts = pd.to_numeric(w_star, errors="coerce").reindex(Sigma_opt.index).fillna(0.0)
            if float(w_star_no_tilts.sum()) != 0:
                w_star_no_tilts = w_star_no_tilts / float(w_star_no_tilts.sum())
            
            R_star = float(stats_df.loc[best_idx, "Achieved Return"])

            if "w_tilt" in locals():
                print("[debug] len(Sigma_opt.index) =", len(Sigma_opt.index), "| type(w_tilt) =", type(w_tilt), "| len(w_tilt) =", (len(w_tilt) if w_tilt is not None else None))
            else:
                print("[debug] len(Sigma_opt.index) =", len(Sigma_opt.index), "| w_tilt not in locals()")

            # --- Target WITHOUT tilts ---
            use_mask_no_tilts = {f: False for f in tilt_df.index}
            w_nt_raw, _, _ = solve_frontier_point_cvxpy_with_tilts(
                mu_vec_opt,
                Sigma_opt,
                R_star,
                B,
                tilt_df["Target"],
                tilt_df["Band"],
                use_mask_no_tilts
            )
            
            # Convert solver output back to ticker-indexed Series
            w_nt_raw = np.asarray(w_nt_raw, dtype=float).reshape(-1)
            w_nt_raw = w_nt_raw[:len(Sigma_opt.index)]  # safety if solver returns an extra element
            w_star_no_tilts = pd.Series(w_nt_raw, index=Sigma_opt.index).fillna(0.0)
            
            # Normalise
            s = float(w_star_no_tilts.sum())
            if s != 0.0:
                w_star_no_tilts = w_star_no_tilts / s
            
            # --- Target WITH tilts ---
            use_mask_with_tilts = tilt_df["Use?"].astype(bool).to_dict()
            w_wt_raw, _, _ = solve_frontier_point_cvxpy_with_tilts(
                mu_vec_opt,
                Sigma_opt,
                R_star,
                B,
                tilt_df["Target"],
                tilt_df["Band"],
                use_mask_with_tilts
            )
            
            # Convert solver output back to ticker-indexed Series
            w_wt_raw = np.asarray(w_wt_raw, dtype=float).reshape(-1)
            w_wt_raw = w_wt_raw[:len(Sigma_opt.index)]  # safety if solver returns an extra element
            w_star_with_tilts = pd.Series(w_wt_raw, index=Sigma_opt.index).fillna(0.0)
            
            # Normalise (use the WITH-TILTS sum, not the no-tilts sum)
            s_wt = float(w_star_with_tilts.sum())
            if s_wt != 0.0:
                w_star_with_tilts = w_star_with_tilts / s_wt

            # Always publish the series so the PPT performance chart can plot it
            globals()["W_WITH_TILTS_SER"] = w_star_with_tilts.copy()


            # 1) Cov sheet
            try:
                cov = wb.sheets['Cov']; cov.used_range.clear_contents()
            except Exception:
                cov = wb.sheets.add('Cov', after=wb.sheets[-1])
            cov.range('A1').options(pd.DataFrame, index=True, header=True).value = Sigma_opt

            # 2) Input sheet
            try:
                inp = wb.sheets['Input']; inp.used_range.clear_contents()
            except Exception:
                inp = wb.sheets.add('Input', after=wb.sheets[-1])
            inp.range('A1').options(pd.DataFrame, index=False, header=True).value = df_melt

            # 3) OPT sheet
            try:
                opt = wb.sheets['OPT']; opt.used_range.clear_contents()
            except Exception:
                opt = wb.sheets.add('OPT', after=wb.sheets[-1])

            # Header
            opt.range('A1').value = 'Optimal Portfolio Theory (long-only where possible)'
            opt.range('A2').value = f"Generated: {datetime.now():%Y-%m-%d %H:%M:%S}"
            opt.range('A3').value = 'Expected returns use geometric (log-based) annualisation.'
            opt.range('A4').value = 'Variance is daily; annual vol = sqrt(252) * stdev.'
            try:
                opt.range('A1').api.Font.Bold = True; opt.range('A1').api.Font.Size = 14
            except Exception:
                pass

            # Expected returns
            opt.range('A6').value = exp_ret_label
            opt.range('A7').options(pd.DataFrame, index=True, header=True).value = exp_ret_df
            n_rows = exp_ret_df.shape[0] + 1
            try:
                opt.range(f"B8:B{7+n_rows}").api.NumberFormat = "0.00%"
            except Exception:
                pass

            # Covariance (+ weight row/col)
            start_cov_row = 9 + n_rows
            opt.range(f"A{start_cov_row}").value = 'Covariance Matrix (daily, model) with weight row/column'
            opt.range(f"A{start_cov_row+1}").options(pd.DataFrame, index=True, header=True).value = cov_plus.fillna(0.0)

            # Weights grid
            start_w_row = start_cov_row + cov_plus.shape[0] + 4
            opt.range(f"A{start_w_row}").value = 'Optimised Weights by Target Return'
            opt.range(f"A{start_w_row+1}").options(pd.DataFrame, index=True, header=True).value = W
            # --- Dynamic format for W (optimised weights table) ---
            w_first = start_w_row + 1               # header row
            w_data_first = w_first + 1              # first data row
            w_rows = W.shape[0]
            w_cols = W.shape[1]
            
            # Percent format for all weight cells
            rng_w = opt.range(
                f"B{w_data_first}:{chr(ord('A')+w_cols)}{w_data_first + w_rows - 1}"
            )
            try:
                rng_w.api.NumberFormat = "0.00%"
            except:
                pass

            # Portfolio Statistics
            start_s_row = start_w_row + W.shape[0] + 4
            opt.range(f"A{start_s_row}").value = 'Portfolio Statistics'
            opt.range(f"A{start_s_row+1}").options(pd.DataFrame, index=False, header=True).value = stats_df
            # ==========================================================
            #  Efficient Frontier Chart
            # ==========================================================
            co_old = opt.api.ChartObjects()
            to_delete = []
            for i in range(1, co_old.Count + 1):
                o = co_old.Item(i)
                try:
                    title_text = o.Chart.ChartTitle.Text
                    if "Efficient Frontier" in str(title_text):
                        to_delete.append(o)
                except:
                    pass
            
            for o in to_delete:
                o.Delete()

            co = opt.api.ChartObjects()
            chart_obj = None
            
            # Find existing chart by the *dynamic* title
            for i in range(1, co.Count + 1):
                o = co.Item(i)
                try:
                    if o.Chart.HasTitle and "Efficient Frontier" in str(o.Chart.ChartTitle.Text):
                        chart_obj = o
                        break
                except:
                    pass
            
            # If not found, create it
            if chart_obj is None:
                left = opt.range("I1").api.Left       # right of the stats table
                top  = opt.range(f"A{start_s_row+1}").api.Top
                width = 480
                height = 245
            
                chart_obj = co.Add(left, top, width, height)
                ch = chart_obj.Chart
                ch.ChartType = -4169       # XY scatter
                ch.HasTitle = True
                ch.ChartTitle.Text = chart_title
            else:
                ch = chart_obj.Chart
            
            # Reposition every run
            chart_obj.Left   = opt.range("I1").api.Left
            chart_obj.Top    = opt.range(f"A{start_s_row+1}").api.Top
            chart_obj.Width  = 480
            chart_obj.Height = 245

            # ----------------------------------------------------------
            # Format Portfolio Statistics
            # ----------------------------------------------------------
            
            stat_rows = stats_df.shape[0]
            if stat_rows > 0:
                header_row = start_s_row + 1
                data_first = header_row + 1
            
                for col_name, fmt in {
                    "Achieved Return": "0.00%",
                    "Volatility (ann.)": "0.00%",
                    "Sharpe": "0.00"
                }.items():
                    if col_name in stats_df.columns:
                        col_idx = list(stats_df.columns).index(col_name)
                        col_letter = chr(ord("A") + col_idx)
                        try:
                            opt.range(
                                f"{col_letter}{data_first}:{col_letter}{data_first + stat_rows - 1}"
                            ).api.NumberFormat = fmt
                        except:
                            pass

            # ================= Efficient Frontier chart updater =================
            def _col_letter(idx0: int) -> str:
                n = idx0 + 1  # A=1
                letters = ""
                while n:
                    n, rem = divmod(n - 1, 26)
                    letters = chr(65 + rem) + letters
                return letters
            
            def _get_chart_by_title(opt_sheet, title_text: str):
                # --- Hard reset: delete all previous Efficient Frontier charts ---
                co = opt_sheet.api.ChartObjects()
                delete_list = []
                for i in range(1, co.Count + 1):
                    o = co.Item(i)
                    try:
                        t = o.Chart.ChartTitle.Text
                        if "Efficient Frontier" in str(t):
                            delete_list.append(o)
                    except Exception:
                        pass
                
                for o in delete_list:
                    o.Delete()

                """Return the COM Chart object whose Title text equals title_text (case/space-insensitive)."""
                def _norm(s): return " ".join(str(s).split()).casefold()
                co = opt_sheet.api.ChartObjects()
                want = _norm(title_text)
                for i in range(1, co.Count + 1):
                    o = co.Item(i)
                    try:
                        ch = o.Chart
                        if ch.HasTitle and _norm(ch.ChartTitle.Text) == want:
                            return ch
                    except Exception:
                        pass
                return None
                            
           
            # -------- Example usage (fits your existing variables) --------
            # Compute current portfolio point if you want it plotted; otherwise pass current_point=None.
            current_point = None
            target_point = None
            previous_point = None
            factor_point = None
            
            try:
                mu_use = mu_vec_opt.reindex(Sigma_opt.index).fillna(0.0).values
                S_use = Sigma_opt.values
            
                # --- Current (from current holdings / current weights) ---
                curr_w = current_holdings_weights(
                    units=current_holdings_units if 'current_holdings_units' in globals() else units_ser,
                    last_prices=last_px_hold,
                    investable=list(Sigma_opt.index),
                    fx_to_aud=fx_map_all
                ).reindex(Sigma_opt.index).fillna(0.0)
            
                wv0 = curr_w.values
                curr_ret = float(mu_use @ wv0)
                curr_vol = float(np.sqrt(wv0 @ S_use @ wv0) * np.sqrt(252.0))
                current_point = (curr_vol, curr_ret)
            
                # --- Previous (seed / last-saved holdings) ---
                try:
                    if seed_units is not None and isinstance(seed_units, pd.Series) and not seed_units.empty:
                        prev_w = current_holdings_weights(
                            units=seed_units,
                            last_prices=last_px_hold,
                            investable=list(Sigma_opt.index),
                            fx_to_aud=fx_map_all
                        ).reindex(Sigma_opt.index).fillna(0.0)
            
                        wp = prev_w.values
                        prev_ret = float(mu_use @ wp)
                        prev_vol = float(np.sqrt(wp @ S_use @ wp) * np.sqrt(252.0))
                        previous_point = (prev_vol, prev_ret)
                except Exception:
                    previous_point = None
            
                # --- Factor-effected point (achievable tilt weights) ---
                try:
                    if "B" in globals() and "f_mean_ann" in globals() and "Fcov_daily" in globals():
                        B_sub = B.reindex(Sigma_opt.index).dropna(how="any")
                        if not B_sub.empty:
                            w_fac = max_sharpe_long_only(mu_vec_opt, Sigma_opt, rf=float(globals().get("rf_annual", 0.0))).reindex(Sigma_opt.index).fillna(0.0)
            
                            if float(w_fac.sum()) != 0:
                                w_fac = w_fac / float(w_fac.sum())
            
                            wf = w_fac.values
                            fac_ret = float(mu_use @ wf)
                            fac_vol = float(np.sqrt(wf @ S_use @ wf) * np.sqrt(252.0))
                            factor_point = (fac_vol, fac_ret)
                            factor_vol, factor_ret = float(factor_point[0]), float(factor_point[1])
                except Exception:
                    factor_point = None
            
                # --- Target (post-trade) using optimiser weights ---
                w1 = pd.to_numeric(w_star, errors="coerce").reindex(Sigma_opt.index).fillna(0.0)
                if float(w1.sum()) != 0:
                    w1 = w1 / float(w1.sum())
            
                wv1 = w1.values
                tgt_ret = float(mu_use @ wv1)
                tgt_vol = float(np.sqrt(wv1 @ S_use @ wv1) * np.sqrt(252.0))
                target_point = (tgt_vol, tgt_ret)
                
                # --- Target (With Tilts) point (soft tilts; closest feasible) ---
                tilt_point = None
                try:
                    B_sub = B.reindex(Sigma_opt.index) if ("B" in globals() and isinstance(B, pd.DataFrame)) else None
                    tilt_targets = pd.to_numeric(tilt_df["Target"], errors="coerce").fillna(0.0)
                    tilt_bands   = pd.to_numeric(tilt_df["Band"],   errors="coerce").fillna(0.0)
                    use_mask     = tilt_df["Use?"].astype(bool).to_dict()
                
                    w_tilt, ok_tilt, note_tilt = solve_frontier_point_cvxpy(
                        mu_vec_opt,
                        Sigma_opt,
                        float(tgt_ret),
                        B=B_sub,
                        tilt_targets=tilt_targets,
                        tilt_bands=tilt_bands,
                        use_mask=use_mask,
                        tilt_mode="soft",
                        tilt_penalty=1e4
                    )


                    # Defensive alignment: if solver returns a shorter vector, pad with zeros to match Sigma_opt
                    try:
                        w_tilt = np.asarray(w_tilt, dtype=float).reshape(-1)
                        n_expected = len(Sigma_opt.index)
                        if len(w_tilt) != n_expected:
                            w_tmp = np.zeros(n_expected, dtype=float)
                            # Best-effort: fill from the front (assumes solver used the same ordering)
                            w_tmp[:min(len(w_tilt), n_expected)] = w_tilt[:min(len(w_tilt), n_expected)]
                            w_tilt = w_tmp
                    except Exception:
                        pass
                    if ok_tilt and np.all(np.isfinite(w_tilt)):
                        w_tilt = w_tilt / float(np.sum(w_tilt))
                        tr = float(mu_use @ w_tilt)
                        tv = float(np.sqrt(w_tilt @ S_use @ w_tilt) * np.sqrt(252.0))
                        tilt_point = (tv, tr)
                except Exception as e:
                    print(f"[chart] Tilt (soft) point error: {e}")
                print(f"[debug] tilt_point={tilt_point}, factor_point={factor_point}")

                # --- Target (No Tilts) ---
                no_tilt_point = None
                try:
                    w_nt = pd.Series(w_star_no_tilts, index=Sigma_opt.index)
                    if float(w_nt.sum()) != 0:
                        w_nt = w_nt / float(w_nt.sum())
                
                    wv_nt = w_nt.values
                    nt_ret = float(mu_use @ wv_nt)
                    nt_vol = float(np.sqrt(wv_nt @ S_use @ wv_nt) * np.sqrt(252.0))
                    no_tilt_point = (nt_vol, nt_ret)
                except Exception as e:
                    print(f"[chart] No-tilt point error: {e}")

            except Exception as e:
                print(f"[chart] Point compute error: {e}")
                current_point = None
                target_point = None
                previous_point = None
                factor_point = None


            # --- Build Efficient Frontier PNG for PowerPoint (optional) ---
            charts = globals().get("charts", {}) or {}

            try:
                APP_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
                eff_path = str((EXPORT_DIR / "efficient_frontier.png").resolve())
            
                _x = pd.to_numeric(stats_df["Volatility (ann.)"], errors="coerce")
                _y = pd.to_numeric(stats_df["Achieved Return"], errors="coerce")
            
                fig, ax = plt.subplots(figsize=(7.5, 4.8))
                ax.plot(_x, _y, linewidth=2.0)
                ax.set_title(chart_title)
                ax.set_xlabel("Volatility (ann.)")
                ax.set_ylabel("Return (ann.)")
                ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
                ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
                
                # Points
                with_tilts_point = tilt_point if tilt_point else factor_point
                if current_point:
                    ax.scatter([float(current_point[0])], [float(current_point[1])], s=60, marker="s", label="Current")
                if previous_point:
                    ax.scatter([float(previous_point[0])], [float(previous_point[1])], s=60, marker="D", label="Previous")
                if with_tilts_point:
                    wt_vol = float(with_tilts_point[0])
                    wt_ret = float(with_tilts_point[1])
                    eps_v, eps_r = 0.0005, 0.0005
                
                    ax.scatter(
                        [wt_vol + eps_v],
                        [wt_ret + eps_r],
                        marker="D",
                        s=70,
                        label="With Tilts",
                        zorder=6
                    )
                
                    ax.annotate(
                        "With Tilts",
                        (wt_vol + eps_v, wt_ret + eps_r),
                        xytext=(6, 6),
                        textcoords="offset points",
                        fontsize=9
                    )
                if target_point:
                    ax.scatter([float(target_point[0])], [float(target_point[1])], s=70, marker="+", label="Target")
            
                ax.legend()
                fig.savefig(eff_path, bbox_inches="tight")
                plt.close(fig)
            
                charts["efficient_frontier_path"] = str(eff_path)
                charts["frontier_points"] = {
                    "Current": current_point,
                    "Previous": previous_point,
                    "Optimised": no_tilt_point if no_tilt_point else factor_point,
                    "With Tilts": with_tilts_point,
                    "Target": target_point,
                }
                globals()["charts"] = charts
            except Exception as _e_eff_png:
                print(f"[pptx] Efficient frontier PNG build skipped: {_e_eff_png}")
            
            # ---- Store achieved tilts for PPT Slide 5 (With Tilts + Without Tilts) ----
            try:
                if (B is None) or B.empty:
                    raise ValueError("B is None or empty")
            
                factor_order = ["Mkt-RF", "SMB", "HML", "RMW", "CMA", "MOM"]
            
                def _norm_w(w, idx):
                    s = pd.Series(np.asarray(w, dtype=float).reshape(-1), index=idx).fillna(0.0)
                    tot = float(s.sum())
                    return (s / tot) if tot != 0 else s
            
                # --- With Tilts achieved betas (use optimiser w_star_with_tilts) ---
                w_with = _norm_w(w_star_with_tilts, Sigma_opt.index).reindex(B.index).fillna(0.0)
                with_beta = (B.T @ w_with)
            
                # --- Without Tilts achieved betas (use optimiser w_star) ---
                w_without = _norm_w(w_star, Sigma_opt.index).reindex(B.index).fillna(0.0)
                without_beta = (B.T @ w_without)
            
                out = pd.DataFrame(index=[f for f in factor_order if f in with_beta.index])
                out["With Tilts"] = with_beta.reindex(out.index).astype(float)
                out["Without Tilts"] = without_beta.reindex(out.index).astype(float)
            
                # Targets (from Tilts sheet)
                if isinstance(tilt_df, pd.DataFrame) and (not tilt_df.empty) and ("Target" in tilt_df.columns):
                    tgt = tilt_df.reindex(out.index)
                    out["Target"] = pd.to_numeric(tgt["Target"], errors="coerce")
                    print("[debug] tilt targets used:", out["Target"].to_dict())

                    # Optional: filter to Use? if present
                    if "Use?" in tgt.columns:
                        use_mask = tgt["Use?"].astype(bool)
                        out = out.loc[use_mask.reindex(out.index).fillna(False)]
            
                charts["tilts_comparison_rows"] = (
                    out.reset_index()
                    .rename(columns={"index": "Factor"})
                    .to_dict("records")
                )
                
            except Exception as _e_ppt_front:
                print(f"[pptx] Tilt table storage skipped: {_e_ppt_front}")
            
            globals()["charts"] = charts
            
            print("[debug] tilts_comparison_rows sample:", (charts.get("tilts_comparison_rows") or [])[:2])

            
            # Finally, update the existing chart on 'OPT'
            # --- Efficient Frontier Chart Update (safe version) ---
            try:
                update_efficient_frontier_chart(
                    opt_sheet=opt,
                    stats_df=stats_df,
                    start_s_row=start_s_row,
                    rf_annual=float(rf_annual),
                    tan_ret=float(tan_ret),
                    tan_vol=float(tan_vol),
                    current_point=current_point,
                    title_text=chart_title,
                    target_point=target_point,
                    previous_point=previous_point,
                    factor_point=factor_point,
                    no_tilt_point=no_tilt_point,
                    tilt_point=tilt_point
                )

            except Exception as e:
                print(f"[chart] Skipping chart update: {e}")
 
            co = opt.api.ChartObjects()
            for i in range(1, co.Count + 1):
                o = co.Item(i)
                title = ""
                try:
                    if o.Chart.HasTitle:
                        title = o.Chart.ChartTitle.Text
                except Exception:
                    pass
                print(i, "name:", o.Name, "| title:", title)

          
            # ---- Build trade plan & costs - writing Trade Plan/Costs/Tilts ----
            _tp_mode = str(globals().get("TRADE_PLAN_MODE", "ask")).lower().strip()
            
            # Decide which portfolio drives the ACTIVE trade plan
            if _tp_mode == "ask":
                _tp_mode = ask_tradeplan_portfolio_choice()
            
            elif _tp_mode == "auto":
                # Validation-based choice (Sharpe over lookback)
                _rwide = globals().get("returns_wide_df", None)
                if isinstance(_rwide, pd.DataFrame) and not _rwide.empty:
                    choice_label, w_chosen, diag = choose_portfolio_for_tradeplan(
                        returns_df=_rwide,
                        w_no_tilts=pd.Series(w_star, index=Sigma_opt.index),
                        w_with_tilts=pd.Series(w_star_with_tilts, index=Sigma_opt.index),
                        rf_annual=float(rf_annual),
                        lookback_days=int(globals().get("VALIDATION_LOOKBACK_DAYS", 252)),
                    )
                    _tp_mode = choice_label
                else:
                    _tp_mode = "no_tilts"
            
            # Final mapping to weights
            w_tradeplan = w_star_with_tilts if _tp_mode == "with_tilts" else w_star
            w_tradeplan = pd.Series(np.asarray(w_tradeplan, dtype=float), index=Sigma_opt.index)            
            # Build BOTH trade plans
            trade_no, resid_no = make_trade_plan(
                units, last_px_hold, fx_map_all, w_star,
                include_zero_lines=True, include_flags=include_flags,
                portfolio_value_override=portfolio_value_override
            )
            
            trade_with, resid_with = make_trade_plan(
                units, last_px_hold, fx_map_all, w_star_with_tilts,
                include_zero_lines=True, include_flags=include_flags,
                portfolio_value_override=portfolio_value_override
            )
            
            # Select which one is the "active" plan (the rest of your pipeline uses trade_rec)
            use_with = (_tp_mode == "with_tilts")
            trade_rec  = trade_with if use_with else trade_no
            resid_rec  = resid_with if use_with else resid_no
            
            # Persist labels/weights for PPT + achieved-tilts table
            globals()["TRADEPLAN_LABEL"] = "with_tilts" if use_with else "no_tilts"
            globals()["TRADEPLAN_WEIGHTS_SER"] = pd.Series(np.asarray(w_tradeplan, dtype=float), index=Sigma_opt.index).copy()
            
            # Keep the other plan available for Excel writing
            globals()["TRADEPLAN_DF_NO_TILTS"] = trade_no.copy()
            globals()["TRADEPLAN_DF_WITH_TILTS"] = trade_with.copy()
            
            # --- Ensure 'Security' is a proper column BEFORE any downstream functions ---
            trade_rec = trade_rec.copy()
            trade_rec.columns = [str(c).strip() for c in trade_rec.columns]
            trade_rec.index.name = "Security"
            if "Security" not in trade_rec.columns:
                trade_rec = trade_rec.reset_index()
            
            # --- Now it is safe to compute costs (some code expects trade_rec["Security"]) ---
            costs_rec = evaluate_transaction_costs(
                trade_rec, lots_df, pd.Timestamp(prices.index[-1]), MARGINAL_TAX_RATE
            )
            
            # --- Add per-row brokerage (keep your existing logic) ---
            row_b = costs_rec.get("per_row_brokerage", pd.Series(0.0, index=trade_rec.index))
            row_b = pd.to_numeric(row_b, errors="coerce").reindex(trade_rec.index).fillna(0.0)
            
            # No brokerage where trade delta is zero (support both Delta Units and ÃŽâ€ Units)
            _delta_col = (_trade_delta_col(trade_rec) if "_trade_delta_col" in globals() else ("Delta Units" if "Delta Units" in trade_rec.columns else "ÃŽâ€ Units"))
            _delta_vals = pd.to_numeric(trade_rec.get(_delta_col, 0), errors="coerce").fillna(0).astype(int)
            row_b = np.where(_delta_vals == 0, 0.0, row_b)
            trade_rec["Brokerage (AUD)"] = pd.Series(row_b, index=trade_rec.index).round(2)
            
            trade_rec.drop(
                columns=[c for c in trade_rec.columns if str(c).lower().startswith("promo")],
                errors="ignore",
                inplace=True
            )
            
            # --- Lot expansion (safe now that 'Security' exists as a column) ---
            lot_expanded = expand_with_lots(
                trade_rec,
                lots_df,
                sale_date=pd.Timestamp(prices.index[-1]),
                method="FIFO"
            )
            print("\n=== LOT-EXPANDED TABLE ===")
            print(lot_expanded.head(20))


            if "Security" not in trade_rec.columns and trade_rec.index.name == "Security":
                trade_rec = trade_rec.reset_index()
            if "Security" not in trade_rec.columns:
                trade_rec.insert(0, "Security", trade_rec.index.astype(str))
            
            if isinstance(lot_expanded, pd.DataFrame):
                if "Security" not in lot_expanded.columns and lot_expanded.index.name == "Security":
                    lot_expanded = lot_expanded.reset_index()

            
            # === Build CGT audit table (parcel-level) ===
            try:
                tax_bkd = costs_rec.get("breakdown", {})
                audit_df = tax_bkd.get("audit", pd.DataFrame()).copy()

                if not audit_df.empty:
                    # Ensure proper dtypes
                    audit_df["AcqDate"] = pd.to_datetime(audit_df["AcqDate"], errors="coerce")
                    audit_df["SaleDate"] = pd.to_datetime(audit_df["SaleDate"], errors="coerce")
                    audit_df["Qty"]      = pd.to_numeric(audit_df["Qty"], errors="coerce")
                    audit_df["Proceeds"] = pd.to_numeric(audit_df["Proceeds"], errors="coerce")
                    audit_df["CostBase"] = pd.to_numeric(audit_df["CostBase"], errors="coerce")
                    audit_df["Gain"]     = pd.to_numeric(audit_df["Gain"], errors="coerce")

                    # Holding period & discount flag (12-month rule)
                    audit_df["HoldingDays"] = (audit_df["SaleDate"] - audit_df["AcqDate"]).dt.days
                    audit_df["LongTermEligible"] = audit_df["LongTermEligible"].astype(bool)

                    # 50% discount only for positive gains that are LT eligible
                    audit_df["DiscountRate"] = 0.0
                    audit_df.loc[(audit_df["Gain"] > 0) & (audit_df["LongTermEligible"]), "DiscountRate"] = 0.5

                    audit_df["DiscountedGainIllustrative"] = audit_df["Gain"]
                    mask_disc = (audit_df["Gain"] > 0) & (audit_df["LongTermEligible"])
                    audit_df.loc[mask_disc, "DiscountedGainIllustrative"] = (
                        audit_df.loc[mask_disc, "Gain"] * 0.5
                    )

                    # === Write parcel-level audit sheet ===
                    try:
                        try:
                            sht_cgt = wb.sheets["CGT_Audit"]
                        except Exception:
                            sht_cgt = wb.sheets.add("CGT_Audit", after=wb.sheets[-1])

                        sht_cgt.used_range.clear_contents()
                        sht_cgt.range("A1").value = [[
                            "Security",
                            "Qty",
                            "AcqDate",
                            "SaleDate",
                            "Proceeds",
                            "CostBase",
                            "Gain",
                            "LongTermEligible",
                            "HoldingDays",
                            "DiscountRate",
                            "DiscountedGainIllustrative",
                        ]]
                        sht_cgt.range("A2").options(index=False, header=False).value = audit_df[
                            [
                                "Security",
                                "Qty",
                                "AcqDate",
                                "SaleDate",
                                "Proceeds",
                                "CostBase",
                                "Gain",
                                "LongTermEligible",
                                "HoldingDays",
                                "DiscountRate",
                                "DiscountedGainIllustrative",
                            ]
                        ]
                    except Exception as e_cgt_sheet:
                        print(f"[cgt] could not write CGT_Audit sheet: {e_cgt_sheet}")

                    # === Optional security-level summary ===
                    try:
                        sec_grp = audit_df.groupby("Security", as_index=False).agg(
                            ProceedsTotal=("Proceeds", "sum"),
                            CostBaseTotal=("CostBase", "sum"),
                            GainTotal=("Gain", "sum"),
                        )

                        lt_mask = audit_df["LongTermEligible"]
                        st_mask = ~audit_df["LongTermEligible"]

                        lt_sum = (
                            audit_df.loc[lt_mask]
                            .groupby("Security")["Gain"]
                            .sum()
                            .rename("LongTermGain")
                        )
                        st_sum = (
                            audit_df.loc[st_mask]
                            .groupby("Security")["Gain"]
                            .sum()
                            .rename("ShortTermGain")
                        )

                        sec_summary = (
                            sec_grp
                            .merge(lt_sum, on="Security", how="left")
                            .merge(st_sum, on="Security", how="left")
                            .fillna(0.0)
                        )

                        sht_cgt.range("L1").value = [[
                            "Security",
                            "ProceedsTotal",
                            "CostBaseTotal",
                            "GainTotal",
                            "LongTermGain",
                            "ShortTermGain",
                        ]]
                        sht_cgt.range("L2").options(index=False, header=False).value = sec_summary[
                            [
                                "Security",
                                "ProceedsTotal",
                                "CostBaseTotal",
                                "GainTotal",
                                "LongTermGain",
                                "ShortTermGain",
                            ]
                        ]
                    except Exception as e_cgt_summary:
                        print(f"[cgt] could not write CGT summary: {e_cgt_summary}")

                else:
                    print("[cgt] audit_df is empty (no CGT-relevant sells).")

            except Exception as e_cgt:
                print(f"[cgt] error building CGT audit table: {e_cgt}")

            
            # ---- Achieved factor tilts table (use implemented target holdings if available) ----
            tilts_out = None
            if (B is not None) and (not B.empty):
                factor_order = ["Mkt-RF","SMB","HML","RMW","CMA","MOM"]
            
                achieved_series = None
            
                # 1) Prefer implemented portfolio from trade plan target units
                try:
                    if isinstance(trade_rec, pd.DataFrame) and (not trade_rec.empty):
                        tr = trade_rec.copy()
                        if "Security" not in tr.columns and tr.index.name == "Security":
                            tr = tr.reset_index()
            
                        if ("Security" in tr.columns) and ("Target Units" in tr.columns) and ("Last Px (AUD)" in tr.columns):
                            tgt_u = pd.to_numeric(tr["Target Units"], errors="coerce").fillna(0.0)
                            px_aud = pd.to_numeric(tr["Last Px (AUD)"], errors="coerce").fillna(0.0)
                            val = tgt_u * px_aud
            
                            w_impl = pd.Series(val.values, index=tr["Security"].astype(str))
                            w_impl = w_impl.reindex(B.index).fillna(0.0)
            
                            s = float(w_impl.sum())
                            if s > 0:
                                w_impl = w_impl / s
                                achieved_series = (B.T @ w_impl).reindex(factor_order)
                except Exception as _e_tilt_impl:
                    achieved_series = None
            
                # 2) Fallback to model weights if needed
                if achieved_series is None:
                    w_use = None
                
                    # Prefer w_tilt if available (and align safely)
                    if ("w_tilt" in locals()) and (w_tilt is not None):
                        if isinstance(w_tilt, pd.Series):
                            w_use = pd.to_numeric(w_tilt, errors="coerce").reindex(Sigma_opt.index).fillna(0.0)
                        else:
                            wt = np.asarray(w_tilt, dtype=float).reshape(-1)
                            if wt.shape[0] == len(Sigma_opt.index):
                                w_use = pd.Series(wt, index=Sigma_opt.index).fillna(0.0)
                            elif wt.shape[0] == len(Sigma_opt.index) + 1:
                                w_use = pd.Series(wt[:len(Sigma_opt.index)], index=Sigma_opt.index).fillna(0.0)
                            else:
                                raise ValueError(f"w_tilt length {wt.shape[0]} does not match Sigma_opt universe {len(Sigma_opt.index)}")
                
                    # If no w_tilt, fall back to w_star
                    if w_use is None:
                        w_use = pd.Series(w_star, index=Sigma_opt.index).reindex(Sigma_opt.index).fillna(0.0)
                
                    # If trade plan weights were stored, use those; otherwise fall back to the with-tilts optimiser weights
                    w_use = globals().get("TRADEPLAN_WEIGHTS_SER", None)
                    if w_use is None:
                        w_use = w_star_with_tilts
                    
                    w_use = pd.Series(w_use).reindex(B.index).fillna(0.0)
                    
                    s = float(w_use.sum())
                    if s > 0:
                        w_use = w_use / s
                    
                    achieved_series = (B.T @ w_use).reindex(factor_order)

            
                # Build output table
                if isinstance(tilt_df, pd.DataFrame) and not tilt_df.empty:
                    tgt = tilt_df.reindex(factor_order)
                    tilts_out = pd.DataFrame({
                        "Use?": tgt["Use?"].astype(str).str.upper().isin(["TRUE","1","Y","YES","T"]).map({True:"Yes", False:"No"}),
                        "Target Beta": pd.to_numeric(tgt["Target"], errors="coerce"),
                        "Band": pd.to_numeric(tgt["Band"], errors="coerce"),
                        "Achieved Beta": achieved_series,
                    })
                    tilts_out["Diff"] = tilts_out["Achieved Beta"] - tilts_out["Target Beta"]
                    tilts_out["Within Band?"] = (tilts_out["Diff"].abs() <= tilts_out["Band"]).map({True: "Yes", False: "No"})
                else:
                    tilts_out = achieved_series.to_frame()

            # ---------- Layout anchors (avoid overlaps) ----------
            anchor_row = start_s_row + stats_df.shape[0] + 4
            TP_COL, COST_COL, TILT_COL = "A", "J", "M"

            # ---------- LEFT: Trade Plan ----------
            opt.range(f"{TP_COL}{anchor_row}").value = "Trade Plan (rounded units)"
            # --- Write an Alternative Trade Plan block (full + aligned + with summaries) ---
            try:
                # Identify which DF is the alternative one
                alt_df = globals().get("TRADEPLAN_DF_NO_TILTS", None)
                if str(globals().get("TRADEPLAN_LABEL", "")).lower().strip() == "no_tilts":
                    alt_df = globals().get("TRADEPLAN_DF_WITH_TILTS", None)
            
                if isinstance(alt_df, pd.DataFrame) and not alt_df.empty:
                    # Force same columns as the main trade plan (prevents "missing columns")
                    alt_df = alt_df.copy()
                    alt_df = alt_df.copy()
                    if "Security" not in alt_df.columns:
                        alt_df.insert(0, "Security", alt_df.index.astype(str))
                    
                    alt_df = alt_df.reindex(columns=trade_rec.columns)            
                    # Place BELOW the main trade plan summary (so nothing gets overwritten)
                    alt_anchor = summary_row + 4
            
                    opt.range(f"{TP_COL}{alt_anchor}").value = "Alternative Trade Plan (rounded units)"
                    opt.range(f"{TP_COL}{alt_anchor+1}").options(pd.DataFrame, index=False, header=True).value = alt_df
            
                    # Compute alt costs (so you can show brokerage/CGT/total for the alt plan too)
                    alt_for_costs = alt_df.copy()
                    alt_for_costs.columns = [str(c).strip() for c in alt_for_costs.columns]
                    alt_for_costs.index.name = "Security"
                    if "Security" not in alt_for_costs.columns:
                        alt_for_costs = alt_for_costs.reset_index()
            
                    alt_costs = evaluate_transaction_costs(
                        alt_for_costs, lots_df, pd.Timestamp(prices.index[-1]), MARGINAL_TAX_RATE
                    )
                    alt_total_brokerage = float(alt_costs.get("brokerage", 0.0))
            
                    # Alt portfolio value + cash summary
                    alt_net_invested = 0.0
                    alt_cash_balance = 0.0
                    alt_total_portfolio = 0.0
            
                    if not alt_df.empty:
                        alt_tgt_units = pd.to_numeric(alt_df["Target Units"], errors="coerce").fillna(0.0)
                        alt_last_px = pd.to_numeric(alt_df["Last Px (AUD)"], errors="coerce").fillna(0.0)
                        alt_net_invested = float((alt_tgt_units * alt_last_px).sum())
            
                        if portfolio_value_override is not None and np.isfinite(portfolio_value_override) and float(portfolio_value_override) > 0:
                            alt_total_portfolio = float(portfolio_value_override)
                            alt_cash_balance = alt_total_portfolio - alt_net_invested - alt_total_brokerage
                        else:
                            alt_cash_balance = float(pd.to_numeric(alt_df["Cash Flow (AUD)"], errors="coerce").fillna(0.0).sum())
                            alt_total_portfolio = alt_net_invested + alt_cash_balance
            
                    alt_summary_row = alt_anchor + alt_df.shape[0] + 4
                    opt.range(f"{TP_COL}{alt_summary_row}").value = [
                        ["Portfolio Value (Holdings)", alt_net_invested],
                        ["Cash", alt_cash_balance],
                        ["Total Portfolio", alt_total_portfolio],
                    ]
                    try:
                        rng_labels2 = opt.range(f"{TP_COL}{alt_summary_row}:{TP_COL}{alt_summary_row+2}").api
                        rng_labels2.Font.Bold = True
                        rng_vals2 = opt.range(f"{TP_COL}{alt_summary_row}:{TP_COL}{alt_summary_row+2}").offset(0, 1).api
                        rng_vals2.NumberFormat = "$0.00"
                    except Exception:
                        pass
            
                    # Alt costs summary (middle column block)
                    opt.range(f"{COST_COL}{alt_anchor}").value = "Transaction Costs (AUD) - Alternative"
                    opt.range(f"{COST_COL}{alt_anchor+1}").value = [
                        ["Brokerage", "CGT Tax", "Total"],
                        [alt_costs.get("brokerage", 0.0), alt_costs.get("cgt_tax", 0.0), alt_costs.get("total_cost", 0.0)],
                    ]
                    try:
                        opt.range(f"{COST_COL}{alt_anchor+2}").api.NumberFormat = "0.00"
                        opt.range(f"{COST_COL}{alt_anchor+2}").offset(0,1).api.NumberFormat = "0.00"
                        opt.range(f"{COST_COL}{alt_anchor+2}").offset(0,2).api.NumberFormat = "0.00"
                    except Exception:
                        pass
            
            except Exception as _e_alt_plan:
                print(f"[excel] Alternative trade plan write skipped: {_e_alt_plan}")


            # --- Add portfolio value & cash summary underneath the Trade Plan ---
            net_invested = 0.0
            cash_balance = 0.0
        
            if not trade_rec.empty:
                # Value of target holdings
                tgt_units = pd.to_numeric(trade_rec["Target Units"], errors="coerce").fillna(0.0)
                last_px_aud = pd.to_numeric(trade_rec["Last Px (AUD)"], errors="coerce").fillna(0.0)
                net_invested = float((tgt_units * last_px_aud).sum())
        
                # Cash handling:
                # - If portfolio_value_override is provided, treat it as TOTAL portfolio value (holdings + cash),
                #   and compute cash as the residual after funding the TARGET holdings.
                # - Otherwise, fall back to deriving cash from the net trade cashflows.
                if portfolio_value_override is not None and np.isfinite(portfolio_value_override) and float(portfolio_value_override) > 0:
                    total_portfolio = float(portfolio_value_override)
                    cash_balance = total_portfolio - net_invested - total_brokerage
                else:
                    # Net cash after trades (positive = cash released, negative = extra cash needed)
                    cash_balance = float(pd.to_numeric(trade_rec["Cash Flow (AUD)"], errors="coerce").fillna(0.0).sum())
                    total_portfolio = net_invested + cash_balance

        
            summary_row = anchor_row + trade_rec.shape[0] + 4
            opt.range(f"{TP_COL}{summary_row}").value = [
                ["Portfolio Value (Holdings)", net_invested],
                ["Cash",                      cash_balance],
                ["Total Portfolio",           total_portfolio],
            ]
            try:
                # Bold the three summary labels and format the numbers as currency
                rng_labels = opt.range(f"{TP_COL}{summary_row}:{TP_COL}{summary_row+2}").api
                rng_labels.Font.Bold = True
                rng_vals = opt.range(f"{TP_COL}{summary_row}:{TP_COL}{summary_row+2}").offset(0, 1).api
                rng_vals.NumberFormat = "$0.00"
            except Exception:
                pass

            # ---------- MIDDLE: Transaction Costs summary ----------
            opt.range(f"{COST_COL}{anchor_row}").value = "Transaction Costs (AUD)"
            opt.range(f"{COST_COL}{anchor_row+1}").value = [
                ["Brokerage", "CGT Tax", "Total"],
                [costs_rec["brokerage"], costs_rec["cgt_tax"], costs_rec["total_cost"]],
            ]
            try:
                opt.range(f"{COST_COL}{anchor_row+2}").api.NumberFormat = "0.00"
                opt.range(f"{COST_COL}{anchor_row+2}").offset(0,1).api.NumberFormat = "0.00"
                opt.range(f"{COST_COL}{anchor_row+2}").offset(0,2).api.NumberFormat = "0.00"
            except Exception:
                pass

            # ---------- RIGHT: Achieved Factor Tilts ----------
            if tilts_out is not None:
                opt.range(f"{TILT_COL}{anchor_row}").value = "Achieved Factor Tilts vs Targets"
                opt.range(f"{TILT_COL}{anchor_row+1}").options(pd.DataFrame, index=True, header=True).value = tilts_out
                t_rows = tilts_out.shape[0] + 1
                t_first = anchor_row + 1
                t_data_first = t_first + 1
                try:
                    for col_name in ["Target Beta","Band","Achieved Beta","Diff"]:
                        if col_name in tilts_out.columns:
                            idx = list(tilts_out.columns).index(col_name)
                            col_letter = chr(ord(TILT_COL) + 1 + idx)  # after index column
                            opt.range(f"{col_letter}{t_data_first}:{col_letter}{t_first+t_rows}").api.NumberFormat = "0.000"
                except Exception:
                    pass
            # ---------- BELOW RIGHT: Factor Feasible Ranges (long-only, sum=1) ----------
            if (B is not None) and (not B.empty):
                factor_order = ["Mkt-RF","SMB","HML","RMW","CMA","MOM"]
                rng_df = compute_factor_feasible_ranges(B, include_flags=include_flags, factor_order=factor_order)
            
                # Optional: show your target & achieved alongside the ranges
                if isinstance(tilts_out, pd.DataFrame):
                    # pull Target and Achieved columns safely
                    tgt = pd.to_numeric(tilts_out.get("Target Beta", np.nan), errors="coerce")
                    ach = pd.to_numeric(tilts_out.get("Achieved Beta", np.nan), errors="coerce")
                    rng_df = rng_df.join(tgt.rename("Target Beta")).join(ach.rename("Achieved Beta"))
                    min_col = "Min Beta" if "Min Beta" in rng_df.columns else ("Min Î²" if "Min Î²" in rng_df.columns else "Min beta")
                    max_col = "Max Beta" if "Max Beta" in rng_df.columns else ("Max Î²" if "Max Î²" in rng_df.columns else "Max beta")
                    if ("Target Beta" in rng_df.columns) and (min_col in rng_df.columns) and (max_col in rng_df.columns):
                        rng_df["Within Range?"] = (rng_df["Target Beta"] >= rng_df[min_col]) & (rng_df["Target Beta"] <= rng_df[max_col])
                    else:
                        rng_df["Within Range?"] = np.nan
            
                # place a few rows *below* the achieved-tilts table to avoid overlap
                tilt_rows = (tilts_out.shape[0] + 2) if isinstance(tilts_out, pd.DataFrame) else 3
                ranges_anchor = anchor_row + tilt_rows + 2
            
                opt.range(f"{TILT_COL}{ranges_anchor}").value = "Factor Feasible Ranges (long-only, sum=1)"
                opt.range(f"{TILT_COL}{ranges_anchor+1}").options(pd.DataFrame, index=True, header=True).value = rng_df
            
                # number formats
                rr = ranges_anchor + 1
                rr_rows = rng_df.shape[0] + 1
                try:
                    # format numeric columns to 3 decimals if present
                    for col_name in ["Min Beta","Max Beta","Min beta","Max beta","Target Beta","Achieved Beta"]:
                        if col_name in rng_df.columns:
                            idx = list(rng_df.columns).index(col_name)
                            # first data column is one to the right of TILT_COL
                            col_letter = chr(ord(TILT_COL) + 1 + idx)
                            opt.range(f"{col_letter}{rr+1}:{col_letter}{rr+rr_rows}").api.NumberFormat = "0.000"
                except Exception:
                    pass

            # Final tidy
            try: opt.autofit()
            except Exception: pass

            # 4) FF5F sheet (optional transparency)
            try:
                ff5s = wb.sheets['FF5F']; ff5s.used_range.clear_contents()
            except Exception:
                ff5s = wb.sheets.add('FF5F', after=wb.sheets[-1])
            ff5s.range('A1').options(pd.DataFrame, index=True, header=True).value = ff_aud

            # ---- Update Lots and overwrite Holdings with target units (for next run) ----
            UPDATED_LOTS = _update_lots_after_trades(lots_df, trade_rec, pd.Timestamp(prices.index[-1]), fx_map_all)
            try:
                sht_lots = wb.sheets['Lots']
            except Exception:
                sht_lots = wb.sheets.add('Lots', after=wb.sheets[-1])
            sht_lots.used_range.clear_contents()
            sht_lots.range("A1").value = [["Security","AcqDate","Units","CostBaseAUD"]]
            sht_lots.range("A2").options(index=False, header=False).value = UPDATED_LOTS
            
            tgt_units_full = compute_target_units_for_holdings(units, last_px_hold, fx_map_all, w_star, include_flags, portfolio_value_override=portfolio_value_override)

            _write_holdings_sheet(wb, prices, tgt_units_full, include_flags, sheet_name="Holdings", fx_to_aud_map=fx_map_all)

            # --- Step 1: Compute current portfolio values ---
            if not trade_rec.empty:
                trade_rec["Target Units"] = pd.to_numeric(trade_rec["Target Units"], errors="coerce").fillna(0.0)
                trade_rec["Last Px (AUD)"] = pd.to_numeric(trade_rec["Last Px (AUD)"], errors="coerce").fillna(0.0)
                trade_rec["Value"] = trade_rec["Target Units"] * trade_rec["Last Px (AUD)"]
                net_invested = float(trade_rec["Value"].sum())
                # Net cash after trades (already net of brokerage)
                cash_balance = float(pd.to_numeric(trade_rec["Cash Flow (AUD)"], errors="coerce").fillna(0.0).sum())
            else:
                net_invested = 0.0
                cash_balance = 0.0
        
            # Brokerage (for reporting)
            total_brokerage = float(costs_rec.get("brokerage", 0.0))
            
            # Total portfolio + cash:
            # If the user provided a portfolio value override, treat it as TOTAL portfolio value.
            # Cash becomes the residual AFTER funding target holdings AND paying brokerage.
            pvo = None
            try:
                pvo = float(portfolio_value_override) if portfolio_value_override is not None else None
            except Exception:
                pvo = None
            
            if pvo is not None and np.isfinite(pvo) and pvo > 0:
                total_portfolio = float(pvo)
                cash_balance = total_portfolio - float(net_invested) - float(total_brokerage)
            else:
                # Fall back to "cash from trade cashflows" (your current behaviour)
                # NOTE: if your Cash Flow already includes brokerage, then total_portfolio should be:
                # holdings + cash
                total_portfolio = float(net_invested) + float(cash_balance)
            
            print(f"[debug] Current totals â†’ Portfolio: {total_portfolio:.2f}, Net Invested: {net_invested:.2f}")
            
            # --- Step 2: Load previous run data (AFTER calculating current totals) ---
            if os.path.exists(state_path):
                with open(state_path, "r") as f:
                    prev_state = json.load(f)
                previous_portfolio = prev_state.get("portfolio_value", 0.0)
                previous_invested = prev_state.get("net_invested", 0.0)
                print(f"[debug] Previous totals â†’ Portfolio: {previous_portfolio:.2f}, Net Invested: {previous_invested:.2f}")
            else:
                previous_portfolio = 0.0
                previous_invested = 0.0
                print("[info] No previous state file found â€” starting fresh deltas at 0.")
            
            # --- Step 3: Compute deltas for PowerPoint ---
            results = {
                "total_brokerage": total_brokerage,
                "net_invested": net_invested,
                "total_portfolio_value": total_portfolio,
                "portfolio_change": total_portfolio - previous_portfolio,
                "net_invested_change": net_invested - previous_invested,
                "cash_balance": cash_balance,
            }
            
            # --- Step 4: Save current state for next comparison ---
            with open(state_path, "w") as f:
                json.dump(
                    {"portfolio_value": total_portfolio, "net_invested": net_invested},
                    f,
                    indent=2
                )
            print(f"[debug] Saved new state â†’ Portfolio: {total_portfolio:.2f}, Net Invested: {net_invested:.2f}")
            
            # --- Step 5: Generate PowerPoint summary ---
            trades = trade_rec.copy()
            # --- Label which portfolio this trade plan represents (used later by PPT) ---
            globals()["TRADE_PLAN_PORTFOLIO_LABEL"] = str(globals().get("choice_label", globals().get("TRADE_PLAN_MODE", "unknown")))
            globals()["TRADE_PLAN_SOURCE"] = "trade_rec"
           
            charts = dict(globals().get("charts", {}) or {})
            if ("tilts_comparison_rows" not in charts) or (not charts.get("tilts_comparison_rows")):
                try:
                    if "out" in locals() and isinstance(out, pd.DataFrame) and (not out.empty):
                        charts["tilts_comparison_rows"] = (
                            out.reset_index()
                            .rename(columns={"index": "Factor"})
                            .to_dict("records")
                        )
                except Exception:
                    pass
            charts.pop("tilts_comparison_rows", None)
            charts.pop("with_tilts_achieved_tilts", None)     

            # Persist report payload for downstream launcher cells
            globals()["results"] = results
            globals()["trades"] = trades
            globals()["charts"] = charts

            # --- Step 6: Compute PortfolioValue for PowerPoint charts (no Excel readback) ---
            try:
                # Use the in-memory target units you already computed
                units_ser = pd.to_numeric(pd.Series(tgt_units_full), errors="coerce").fillna(0.0)
                valid_tickers = [t for t in units_ser.index.astype(str) if t in prices.columns]
            
                if not valid_tickers:
                    raise ValueError("No valid tickers found in prices for target holdings.")
            
                port_prices = prices[valid_tickers].copy().ffill().bfill()
                u = units_ser.reindex(valid_tickers).astype(float).values
                portfolio_value_series = (port_prices * u).sum(axis=1).ffill().bfill()
                portfolio_value_series = portfolio_value_series.copy()
                
                print(f"[pptx prep] PortfolioValue series computed for {len(valid_tickers)} securities.")
                # Diagnostic: confirm the date range that downstream slides will anchor to.
                # If this end date is much earlier than today, something upstream is
                # truncating `prices` (yfinance cache, dropna, reindex, etc.).
                try:
                    _pv_idx = portfolio_value_series.dropna().index
                    print(f"[pptx prep] portfolio_value_series spans "
                          f"{_pv_idx.min().date()} -> {_pv_idx.max().date()} "
                          f"({len(_pv_idx)} rows); prices.index.max()={prices.index.max().date()}")
                except Exception:
                    pass
            except Exception as e:
                print(f"[pptx prep] Could not compute PortfolioValue: {e}")
         
                # Rebuild tilts rows if missing (prevents Slide 5 table disappearing)
                if ("tilts_comparison_rows" not in charts) or (not charts.get("tilts_comparison_rows")):
                    charts["tilts_comparison_rows"] = (globals().get("charts", {}) or {}).get("tilts_comparison_rows", [])
                    print("[pptx prep] tilts_comparison_rows length:", len(charts.get("tilts_comparison_rows") or []))

                try:
                    ppt_path = export_to_ppt(results, trades, charts)
                except Exception as e:
                    print(f"[pptx] Skipped PowerPoint generation: {e}")

            wb.save()
            wb.close()

    except Exception as e:
        print(f"[Excel fallback] xlwings/COM error â†’ exporting CSVs instead: {e}")
        export_dir = os.path.join(os.path.dirname(filename), "Exports")
        try: os.makedirs(export_dir, exist_ok=True)
        except Exception: pass
        try: exp_ret_df.to_csv(os.path.join(export_dir, "expected_returns.csv"))
        except Exception as ee: print(f"[export] expected_returns.csv: {ee}")
        try: cov_plus.to_csv(os.path.join(export_dir, "covariance_plus.csv"))
        except Exception as ee: print(f"[export] covariance_plus.csv: {ee}")
        try: W.to_csv(os.path.join(export_dir, "weights_grid.csv"))
        except Exception as ee: print(f"[export] weights_grid.csv: {ee}")
        try: stats_df.to_csv(os.path.join(export_dir, "portfolio_stats.csv"), index=False)
        except Exception as ee: print(f"[export] portfolio_stats.csv: {ee}")
        try: tilt_df.to_csv(os.path.join(export_dir, "tilts.csv"))
        except Exception as ee: print(f"[export] tilts.csv: {ee}")
        try: df_melt.to_csv(os.path.join(export_dir, "returns_long.csv"), index=False)
        except Exception as ee: print(f"[export] returns_long.csv: {ee}")
else:
    # ---------- Headless fallback: write key outputs as CSVs ----------
    export_dir = os.path.join(os.path.dirname(filename), "Exports")
    try: os.makedirs(export_dir, exist_ok=True)
    except Exception: pass
    try: exp_ret_df.to_csv(os.path.join(export_dir, "expected_returns.csv"))
    except Exception as e: print(f"[export] expected_returns.csv: {e}")
    try: cov_plus.to_csv(os.path.join(export_dir, "covariance_plus.csv"))
    except Exception as e: print(f"[export] covariance_plus.csv: {e}")
    try: W.to_csv(os.path.join(export_dir, "weights_grid.csv"))
    except Exception as e: print(f"[export] weights_grid.csv: {e}")
    try: stats_df.to_csv(os.path.join(export_dir, "portfolio_stats.csv"), index=False)
    except Exception as e: print(f"[export] portfolio_stats.csv: {e}")
    try: tilt_df.to_csv(os.path.join(export_dir, "tilts.csv"))
    except Exception as e: print(f"[export] tilts.csv: {e}")
    try: df_melt.to_csv(os.path.join(export_dir, "returns_long.csv"), index=False)
    except Exception as e: print(f"[export] returns_long.csv: {e}")

# Ensure report payload exists even if Excel/COM path was skipped or failed
if "results" not in globals():
    _net_invested = 0.0
    _cash_balance = 0.0
    if "trade_rec" in globals() and isinstance(trade_rec, pd.DataFrame) and not trade_rec.empty:
        _tgt = pd.to_numeric(trade_rec.get("Target Units"), errors="coerce").fillna(0.0)
        _px = pd.to_numeric(trade_rec.get("Last Px (AUD)"), errors="coerce").fillna(0.0)
        _net_invested = float((_tgt * _px).sum())
        _cash_balance = float(pd.to_numeric(trade_rec.get("Cash Flow (AUD)"), errors="coerce").fillna(0.0).sum())
    _total_brokerage = float(costs_rec.get("brokerage", 0.0)) if "costs_rec" in globals() else 0.0
    _total_portfolio = float(_net_invested + _cash_balance)
    results = {
        "total_brokerage": _total_brokerage,
        "net_invested": _net_invested,
        "total_portfolio_value": _total_portfolio,
        "portfolio_change": 0.0,
        "net_invested_change": 0.0,
        "cash_balance": _cash_balance,
    }
if "trades" not in globals():
    trades = trade_rec.copy() if "trade_rec" in globals() and isinstance(trade_rec, pd.DataFrame) else pd.DataFrame()
if "charts" not in globals() or not isinstance(charts, dict):
    charts = dict(globals().get("charts", {}) or {})
if isinstance(charts, dict) and ("portfolio_value_series" not in charts or charts.get("portfolio_value_series") is None):
    try:
        _pv_series = None
        if "prices" in globals() and isinstance(prices, pd.DataFrame) and not prices.empty and isinstance(trades, pd.DataFrame) and not trades.empty:
            _sec = trades.get("Security")
            _tgt = pd.to_numeric(trades.get("Target Units"), errors="coerce").fillna(0.0)
            if _sec is not None:
                _u = pd.Series(_tgt.values, index=_sec.astype(str)).groupby(level=0).sum()
                _valid = [t for t in _u.index if t in prices.columns]
                if _valid:
                    _px = prices[_valid].copy().ffill().bfill()
                    _vals = _u.reindex(_valid).astype(float).values
                    _pv_series = (_px * _vals).sum(axis=1).ffill().bfill()
        if _pv_series is not None and len(_pv_series) > 0:
            charts["portfolio_value_series"] = _pv_series
            globals()["portfolio_value_series"] = _pv_series
    except Exception:
        pass
globals()["results"] = results
globals()["trades"] = trades
globals()["charts"] = charts

print("Workbook Successfully Updated")

# --- Create a Desktop shortcut (optional, safe in any context) ---
try:
    if HAS_WIN32COM:
        shortcut_path = str(Path.home() / "Desktop" / "Portfolio Optimiser.lnk")

        # Prefer the exe if it exists; otherwise point at the script weâ€™re running.
        # Works when frozen, when run as .py, and in Jupyter (falls back to .py name in APP_DIR).
        if getattr(sys, "frozen", False):
            target = Path(sys.executable)
        else:
            # Try the current file if available; else fall back to a known script name in this folder
            if "__file__" in globals():
                target = Path(__file__).resolve()
            else:
                # Adjust the name if your launcher script is 'Main.py' instead
                # (You have both Main.py and Portfolio_Optimiser3110.py in your screenshot.)
                candidate = APP_DIR / "Portfolio_Optimiser1411.py"
                target = candidate if candidate.exists() else (APP_DIR / "Main.py")

        shell = win32.Dispatch("WScript.Shell")
        sc = shell.CreateShortCut(shortcut_path)
        sc.WindowStyle = 1  # normal window
        sc.Arguments = ""   # no extra args      
        sc.Targetpath = str(target)
        sc.WorkingDirectory = str(target.parent)
        # Use icon.ico if present; otherwise the target itself
        icon_path = APP_DIR / "icon.ico"
        sc.IconLocation = str(icon_path if icon_path.exists() else target)
        sc.save()
    else:
        print("[shortcut] pywin32 not available; skipping Desktop shortcut.")
except Exception as e:
    print(f"[shortcut] skipped due to error: {e}")
print("=== MU VEC (sorted) ===")
print(mu_vec_opt.sort_values())
print("\nMin mu:", mu_vec_opt.min())
print("Max mu:", mu_vec_opt.max())
print("Mean mu:", mu_vec_opt.mean())
print("Top 10 assets by expected return:")
print(mu_vec_opt.sort_values().tail(10))
print(ff5_raw.head())
# Post-write OPT annotation + alternative plan placement + shortcut repair.
from datetime import datetime
from pathlib import Path

try:
    _xl = str(globals().get("filename", "")).strip()
    _diag = dict(globals().get("TRADEPLAN_VALIDATION_DIAG", {}) or {})
    _mode = str(_diag.get("mode", globals().get("TRADE_PLAN_MODE", ""))).strip().lower()
    _selected = str(_diag.get("selected", globals().get("TRADEPLAN_LABEL", ""))).strip()
    _lookback = _diag.get("lookback_days", globals().get("VALIDATION_LOOKBACK_DAYS", 252))
    _sh0 = _diag.get("sharpe_no_tilts", np.nan)
    _sh1 = _diag.get("sharpe_with_tilts", np.nan)

    if not _xl or not os.path.exists(_xl):
        print("[post] Skipped OPT post-write fixes: workbook path not available.")
    else:
        _rows = [
            ("Mode", _mode),
            ("Lookback (days)", _lookback),
            ("Selected Portfolio", _selected),
            ("Sharpe (Optimised)", _sh0),
            ("Sharpe (With Tilts)", _sh1),
        ]

        _written = False

        # First try xlwings so it can write when workbook is already open in Excel.
        try:
            _book = xw.Book(_xl)
            _ws = _book.sheets["OPT"] if "OPT" in [s.name for s in _book.sheets] else _book.sheets.add("OPT")

            _ws.range("W2").value = "Trade Plan Validation"
            _ws.range("W3").value = _rows
            _ws.range("X6:X7").api.NumberFormat = "0.000"

            # Re-write the alternative trade plan block reliably below the main plan.
            _alt = globals().get("TRADEPLAN_DF_NO_TILTS", None)
            if str(globals().get("TRADEPLAN_LABEL", "")).lower().strip() == "no_tilts":
                _alt = globals().get("TRADEPLAN_DF_WITH_TILTS", None)

            _main = globals().get("trade_rec", None)
            if isinstance(_alt, pd.DataFrame) and not _alt.empty and isinstance(_main, pd.DataFrame) and not _main.empty:
                _alt_df = _alt.copy()
                if "Security" not in _alt_df.columns:
                    _alt_df.insert(0, "Security", _alt_df.index.astype(str))
                _alt_df = _alt_df.reindex(columns=_main.columns)

                _start_s_row = int(globals().get("start_s_row", 1))
                _stats_df = globals().get("stats_df", pd.DataFrame())
                _stats_rows = int(_stats_df.shape[0]) if isinstance(_stats_df, pd.DataFrame) else 0
                _anchor = _start_s_row + _stats_rows + 4
                _alt_anchor = _anchor + _main.shape[0] + 8

                _ws.range(f"A{_alt_anchor}").value = "Alternative Trade Plan (rounded units)"
                _ws.range(f"A{_alt_anchor+1}").options(index=False, header=True).value = _alt_df

            _book.save()
            _written = True
            print(f"[post] Wrote OPT validation + layout fixes to: {_xl}")
        except Exception as _e_xlw:
            print(f"[post] xlwings post-write fallback triggered: {_e_xlw}")

        # Fallback to openpyxl; if locked, save a timestamped copy.
        if not _written:
            _wbx = load_workbook(_xl, keep_vba=True)
            _ws = _wbx["OPT"] if "OPT" in _wbx.sheetnames else _wbx.create_sheet("OPT")
            _r0, _c0 = 2, 23  # W2
            _ws.cell(_r0, _c0, "Trade Plan Validation")
            for i, (k, v) in enumerate(_rows, start=1):
                _ws.cell(_r0 + i, _c0, k)
                _ws.cell(_r0 + i, _c0 + 1, v)

            try:
                _wbx.save(_xl)
                print(f"[post] Wrote Trade Plan Validation block to OPT sheet in: {_xl}")
            except PermissionError:
                _stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                _copy = _xl.replace(".xlsm", f"_validated_{_stamp}.xlsm")
                _wbx.save(_copy)
                print(f"[post] Workbook locked. Saved validated copy instead: {_copy}")

    # Repair desktop shortcut safely.
    try:
        if globals().get("HAS_WIN32COM", False):
            _shortcut = str(Path.home() / "Desktop" / "Portfolio Optimiser.lnk")
            _app_dir = Path(globals().get("APP_DIR", os.getcwd()))
            if getattr(sys, "frozen", False):
                _target = Path(sys.executable)
            elif "__file__" in globals():
                _target = Path(__file__).resolve()
            else:
                _cand = _app_dir / "Portfolio_Optimiser1411.py"
                _target = _cand if _cand.exists() else (_app_dir / "Main.py")

            _shell = win32.Dispatch("WScript.Shell")
            _sc = _shell.CreateShortCut(_shortcut)
            _sc.WindowStyle = 1
            _sc.Arguments = ""
            _sc.Targetpath = str(_target)
            _sc.WorkingDirectory = str(_target.parent)
            _icon = _app_dir / "icon.ico"
            _sc.IconLocation = str(_icon if _icon.exists() else _target)
            _sc.save()
            print(f"[post] Desktop shortcut refreshed: {_shortcut}")
    except Exception as _e_sc:
        print(f"[post] Shortcut refresh skipped: {_e_sc}")

except Exception as _e_post_val:
    print(f"[post] OPT validation annotation skipped: {_e_post_val}")


# =====================================================================
# --- Block 8: Finishers / Launchers Excel and Code ---
# =====================================================================
# --- Optional: auto-open outputs after save ---
OPEN_EXCEL_AFTER_SAVE = bool(globals().get("OPEN_EXCEL_AFTER_SAVE", CFG.get("open_excel_after_save", True)))
OPEN_PPT_AFTER_SAVE = bool(globals().get("OPEN_PPT_AFTER_SAVE", CFG.get("open_ppt_after_save", True)))

if bool(globals().get("OPEN_AFTER_SAVE", True)) and OPEN_EXCEL_AFTER_SAVE:
    _excel_path = str(filename) if "filename" in globals() else ""
    if _excel_path and os.path.exists(_excel_path):
        try:
            _os_open(_excel_path)
        except Exception as exc:
            print(f"[open] Could not open Excel workbook: {exc}")
    else:
        print(f"[open] Workbook not found, skipping open: {_excel_path}")


# =====================================================================
# --- BLOCK 9: PowerPoint Report Generator ---
# =====================================================================
# --- BLOCK 9: PowerPoint Report Generator ---

# -----------------------------------------------------------------
# PPTX helpers (module level so they're not redefined per export call)
# -----------------------------------------------------------------
def _nearest_on_or_before(idx, dt):
    """Return the largest value in `idx` that is <= dt (best-effort)."""
    if len(idx) == 0:
        return None
    dt = pd.to_datetime(dt)
    pos = idx.searchsorted(dt, side="right") - 1
    if pos < 0:
        return idx[0]
    return idx[min(pos, len(idx) - 1)]


def _period_total_return(px, end_dt, months=None, years=None):
    """Price-based total return over the lookback window ending at end_dt."""
    s = pd.to_numeric(pd.Series(px), errors="coerce").dropna()
    if s.empty:
        return np.nan
    end_dt = _nearest_on_or_before(s.index, end_dt)
    if end_dt is None:
        return np.nan
    start_target = pd.to_datetime(end_dt)
    if years:
        start_target = start_target - relativedelta(years=int(years))
    if months:
        start_target = start_target - relativedelta(months=int(months))
    start_dt = _nearest_on_or_before(s.index, start_target)
    if start_dt is None:
        return np.nan
    v0 = float(s.loc[start_dt])
    v1 = float(s.loc[end_dt])
    if not np.isfinite(v0) or not np.isfinite(v1) or v0 == 0:
        return np.nan
    return (v1 / v0) - 1.0


def _window_compound_total(r, end_dt, months=None, years=None):
    """Returns-based compounded total return over the lookback window ending at end_dt."""
    r = pd.to_numeric(pd.Series(r), errors="coerce").dropna()
    if r.empty:
        return np.nan
    start_target = end_dt
    if years:
        start_target = start_target - relativedelta(years=years)
    if months:
        start_target = start_target - relativedelta(months=months)
    start_dt = _nearest_on_or_before(r.index, start_target)
    end_dt2 = _nearest_on_or_before(r.index, end_dt)
    if start_dt is None or end_dt2 is None or start_dt >= end_dt2:
        return np.nan
    rr = r.loc[start_dt:end_dt2]
    if rr.empty:
        return np.nan
    return float((1.0 + rr).prod() - 1.0)


def _ppt_anchor(slide, layout, name, fb_left_cm, fb_top_cm, fb_w_cm, fb_h_cm):
    """
    Look up a named shape on the slide or its layout and return its
    (left, top, width, height) in EMU. Falls back to the hardcoded
    cm values when no shape with that name exists.

    To customise positions: in PowerPoint, add a no-fill placeholder shape
    on the slide layout (e.g. layout 20), open Selection Pane, and rename
    the shape to match `name` (e.g. "chart_main", "table_perf").
    """
    for src in (slide, layout):
        if src is None:
            continue
        try:
            for shp in src.shapes:
                if getattr(shp, "name", "") == name:
                    return (shp.left, shp.top, shp.width, shp.height)
        except Exception:
            continue
    return (Cm(fb_left_cm), Cm(fb_top_cm), Cm(fb_w_cm), Cm(fb_h_cm))


def _autofit_table_width(table, df, total_width_cm=12.02):
    """Auto-fit PPT table column widths from a DataFrame's content."""
    def est_width(text):
        return len(str(text)) * 0.22  # empirical avg for 9pt Calibri
    est_widths = []
    for col in df.columns:
        header_w = est_width(col)
        data_w = max(est_width(v) for v in df[col].astype(str)) if len(df) else header_w
        width = max(header_w, data_w)
        if col.lower() in ("target", "change"):
            width = max(width, 1.85)
        elif col.lower() == "security":
            width = max(width, 1.778)
        est_widths.append(width)
    total_est = sum(est_widths)
    scale = total_width_cm / total_est if total_est else 1.0
    for j, est in enumerate(est_widths):
        table.columns[j].width = Cm(est * scale)


def _format_perf_value(v, fmt="pct2"):
    """Format a numeric value for a perf-style table cell."""
    if pd.isna(v):
        return ""
    try:
        fv = float(v)
    except (TypeError, ValueError):
        return ""
    if not np.isfinite(fv):
        return ""
    if fmt == "pct2":
        return f"{fv*100:.2f}%"
    if fmt == "dec3":
        return f"{fv:.3f}"
    return str(fv)


def _add_perf_table(slide, df_metrics, left, top, width, height,
                    title=None, value_fmt="pct2", font_pt=11):
    """Add a formatted PPT table from a DataFrame. Values formatted per `value_fmt`."""
    rows = df_metrics.shape[0] + 1
    cols = df_metrics.shape[1] + 1  # include row label column
    shp = slide.shapes.add_table(rows, cols, left, top, width, height)
    tbl = shp.table
    tbl.cell(0, 0).text = str(title) if title else ""
    for j, c in enumerate(df_metrics.columns, start=1):
        tbl.cell(0, j).text = str(c)
    for i, (idx, row) in enumerate(df_metrics.iterrows(), start=1):
        tbl.cell(i, 0).text = str(idx)
        for j, c in enumerate(df_metrics.columns, start=1):
            tbl.cell(i, j).text = _format_perf_value(row[c], fmt=value_fmt)
    for r in range(rows):
        for c in range(cols):
            for p in tbl.cell(r, c).text_frame.paragraphs:
                p.font.size = Pt(font_pt)
                p.font.bold = True
                p.alignment = PP_ALIGN.CENTER
    return tbl


def _add_change_run(paragraph, val, font_pt=14):
    """Add a coloured (+/-) change run after a number in a summary line."""
    run = paragraph.add_run()
    if val == 0:
        run.text = ""
        return
    sign = "+" if val > 0 else ""
    run.text = f" ({sign}{val:,.2f})"
    run.font.size = Pt(font_pt)
    if val > 0:
        run.font.color.rgb = RGBColor(0, 128, 0)
    elif val < 0:
        run.font.color.rgb = RGBColor(192, 0, 0)
    else:
        run.font.color.rgb = RGBColor(80, 80, 80)


def add_header_footer(slide, title_text: str, footer_text: str = ""):
    """Adds a consistent header and footer banner with text."""
    # Header banner
    header = slide.shapes.add_shape(
        1,  # mso_shape.rectangle
        Cm(0), Cm(0),
        Cm(25.4), Cm(2.54)
    )
    fill = header.fill
    fill.solid()
    fill.fore_color.rgb = RGBColor(0, 51, 102)  # dark navy
    header.line.fill.background()  # no border

    # Header text
    tf = header.text_frame
    tf.text = title_text
    p = tf.paragraphs[0]
    p.font.bold = True
    p.font.size = Pt(32)
    p.font.color.rgb = RGBColor(255, 255, 255)
    p.alignment = 1  # centre

    # Footer banner
    footer = slide.shapes.add_shape(
        1, Cm(0), Cm(17.78), Cm(25.4), Cm(1.016)
    )
    fill = footer.fill
    fill.solid()
    fill.fore_color.rgb = RGBColor(230, 230, 230)
    footer.line.fill.background()

    # Footer text
    tf = footer.text_frame
    tf.text = footer_text or "Generated by Portfolio Optimiser"
    p = tf.paragraphs[0]
    p.font.size = Pt(12)
    p.font.color.rgb = RGBColor(80, 80, 80)
    p.alignment = 1  # centre

def export_to_ppt(results, trades, charts=None):
    """
    Generates a professional PowerPoint summary based on your custom template.
    """
    APP_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()

    # Template file (design only, never edited)
    template_path = os.path.join(APP_DIR, "PowerPoint_Template.pptx")

    # Output path â€” always overwrite this file
    ppt_path = str(EXPORT_DIR / "Portfolio_Report.pptx")

    # Load your custom template
    prs = Presentation(template_path)
   
    # --- SLIDE 1: Title and TimeStamp ---
    slide = prs.slides[0] 
    
    # --- Title text box ---  
    if slide.shapes.title:
        slide.shapes.title.text = "Portfolio Performance Overview"
    
    # --- Timestamp text box ---
    now = datetime.now()
    timestamp = now.strftime("Last updated on the %d %B %Y at %I:%M %p")
    ts_box = slide.shapes.add_textbox(Cm(2.032), Cm(15.24), Cm(22.86), Cm(1.27))
    tf2 = ts_box.text_frame
    tf2.word_wrap = False
    p2 = tf2.add_paragraph()
    p2.text = timestamp
    p2.font.size = Pt(16)
    p2.alignment = PP_ALIGN.LEFT  # uses default colour/font

    # --- SLIDE 2: Trade Plan + Brokerage ---
    slide_layout = prs.slide_layouts[20]  # clean layout from your master
    slide = prs.slides.add_slide(slide_layout)
    
    # Title
    if slide.shapes.title:
        slide.shapes.title.text = "Trade Plan and Brokerage Overview"

    # --- Portfolio identity call-out (checkbox + label) ---
    plan_label = str(globals().get("TRADEPLAN_LABEL", "Unknown")).strip()
    
    # Show both options with one checked
    pl = plan_label.lower().replace("_", " ").strip()
    is_with = (pl == "with tilts")
    box = slide.shapes.add_textbox(Cm(2.15), Cm(2.05), Cm(21.80), Cm(0.45))
    tf = box.text_frame
    tf.clear()
    tf.word_wrap = False
    tf.margin_left = 0
    tf.margin_right = 0
    tf.margin_top = 0
    tf.margin_bottom = 0
    p = tf.paragraphs[0]
    p.text = f"{'[x]' if is_with else '[ ]'} With Tilts    {'[ ]' if is_with else '[x]'} Optimised (No Tilts)    |    Trade plan: {plan_label}"
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = RGBColor(255, 255, 255)
    p.alignment = PP_ALIGN.LEFT

    # --- Draw Trade Plan table ---
    if trades is not None and not trades.empty:
        # Resolve the trade-delta column defensively (handles legacy/mis-encoded headers).
        delta_col = None
        if "_trade_delta_col" in globals():
            try:
                delta_col = _trade_delta_col(trades)
            except Exception:
                delta_col = None
        if not delta_col:
            if "Delta Units" in trades.columns:
                delta_col = "Delta Units"
            else:
                for _c in trades.columns:
                    _cs = str(_c)
                    if _cs.endswith(" Units") and "delta" in _cs.lower():
                        delta_col = _c
                        break

        # Map existing columns to desired display names.
        # IMPORTANT: only choose one brokerage source to avoid duplicate 'Brokerage' columns.
        rename_map = {
            "Curr Units": "Current",
            "Target Units": "Target",
            "Last Px (AUD)": "Last Price",
            "Cash Flow (AUD)": "Cash Flow",
        }
        if "Brokerage" in trades.columns:
            rename_map["Brokerage"] = "Brokerage"
        elif "Brokerage (AUD)" in trades.columns:
            rename_map["Brokerage (AUD)"] = "Brokerage"
        if delta_col:
            rename_map[delta_col] = "Change"
    
        # Select, copy, and rename columns
        cols_needed = ["Security"] + list(rename_map.keys())
        cols_present = [c for c in cols_needed if c in trades.columns]
        df = trades[cols_present].copy()
        if "Security" not in df.columns and trades.index.name == "Security":
            df = trades.reset_index()[cols_present]
        df.rename(columns=rename_map, inplace=True)
        # Reorder to your final order but only for those that exist
        final_order = [c for c in ["Security","Current","Target","Change","Last Price","Brokerage","Cash Flow"] if c in df.columns]
        df = df[final_order]
            
    
        # --- Clean and format values ---
        df["Security"] = df["Security"].astype(str).str.replace(".AX", "", regex=False)
        for col in ["Last Price", "Cash Flow", "Brokerage"]:
            if col in df.columns:
                df[col] = (
                    pd.to_numeric(df[col], errors="coerce")
                    .round(2)
                    .apply(lambda x: f"-${abs(x):,.2f}" if x < 0 else f"${x:,.2f}")
                )
    
        # --- Determine if we split into two tables ---
        rows, cols = df.shape
        split = rows > 15
        half = math.ceil(rows / 2) if split else rows
        table_sets = [df.iloc[:half]] if not split else [df.iloc[:half], df.iloc[half:]]
        left_positions = [Cm(1.1), Cm(11.5)] if split else [Cm(1.1)]
    
        # --- Draw tables ---
        for idx, subdf in enumerate(table_sets):
            top = Cm(4.0)
        
            table_w = 12.02
            gap = 0.8
            left_margin = 0.35
            left = Cm(left_margin + idx * (table_w + gap))
            width = Cm(table_w)
            height = Cm(6.94)
        
            table = slide.shapes.add_table(
                rows=subdf.shape[0] + 1,
                cols=subdf.shape[1],
                left=left,
                top=top,
                width=width,
                height=height
            ).table
        
            # Auto-fit widths
            _autofit_table_width(table, subdf, total_width_cm=table_w)
        
            # Disable wrapping
            for cell in table.iter_cells():
                cell.text_frame.word_wrap = False
                cell.text_frame.margin_left = 0
                cell.text_frame.margin_right = 0
                cell.text_frame.margin_top = 0
                cell.text_frame.margin_bottom = 0
        
            # Row height
            for r in range(len(table.rows)):
                table.rows[r].height = Cm(0.584)
        
            # Header
            for j, col_name in enumerate(subdf.columns):
                cell = table.cell(0, j)
                cell.text = col_name
                tf = cell.text_frame
                tf.auto_size = MSO_AUTO_SIZE.NONE
                p = tf.paragraphs[0]
                p.font.bold = True
                p.font.size = Pt(8)
                p.alignment = PP_ALIGN.CENTER
        
            # Data rows
            for i, (_, row) in enumerate(subdf.iterrows(), start=1):
                for j, val in enumerate(row):
                    cell = table.cell(i, j)
                    cell.text = str(val)
                    p = cell.text_frame.paragraphs[0]
                    p.font.size = Pt(9)
                    p.font.bold = (j == 0)
                    p.alignment = PP_ALIGN.CENTER
    
        # --- Summary bar across top ---
        left = Cm(2.5)
        top = Cm(2.5)  # just below the title
        width = Cm(20.00)
        height = Cm(1.1)
        textbox = slide.shapes.add_textbox(left, top, width, height)
        tf = textbox.text_frame
        tf.word_wrap = False
        tf.clear()
        
        # --- Fetch values ---
        total_portfolio = results.get("total_portfolio_value", 0)
        total_brokerage = results.get("total_brokerage", 0)
        net_invested = results.get("net_invested", 0)
        portfolio_change = results.get("portfolio_change", 0)
        net_invested_change = results.get("net_invested_change", 0)
        
        # --- Main summary line ---
        p = tf.add_paragraph()
        p.font.size = Pt(14)
        p.font.bold = True
        p.alignment = PP_ALIGN.CENTER

        # Text with separate runs for coloured numbers
        run1 = p.add_run()
        run1.text = f"Total Portfolio: ${total_portfolio:,.2f}"
        run1.font.size = Pt(14)
        run1.font.bold = True
        _add_change_run(p, portfolio_change)

        run2 = p.add_run()
        run2.text = f"     Total Brokerage: ${total_brokerage:,.2f}     "
        run2.font.size = Pt(14)
        run2.font.bold = True
        run2.font.color.rgb = RGBColor(0, 0, 0)

        run3 = p.add_run()
        run3.text = f"Net Invested: ${net_invested:,.2f}"
        run3.font.size = Pt(14)
        run3.font.bold = True
        _add_change_run(p, net_invested_change)

        # --- Slide 3: Portfolio vs Indices ---
        # --- Cash summary (derived from Trade Plan cash flows) ---
        try:
            cash_balance = 0.0
            if trades is not None and not trades.empty and "Cash Flow (AUD)" in trades.columns:
                cash_balance = float(results.get("cash_balance", 0.0))
        
            cash_box = slide.shapes.add_textbox(Cm(18.288), Cm(14.732), Cm(6.604), Cm(1.524))
            tfc = cash_box.text_frame
            tfc.clear()
            p = tfc.paragraphs[0]
            p.text = f"Cash: {cash_balance:,.0f} AUD"
            p.font.size = Pt(18)
            p.font.bold = True
            p.alignment = PP_ALIGN.RIGHT
        except Exception:
            pass

        slide_layout = prs.slide_layouts[20]  # clean layout from your master
        slide = prs.slides.add_slide(slide_layout)
        
        # Title
        if slide.shapes.title:
            slide.shapes.title.text = "Portfolio Performance"
        
        # --- Get 3-month portfolio + benchmarks ---
        lookback_days = 90
        
        # Use the portfolio value series as the date anchor (NOT prices)
        pval_src = globals().get("portfolio_value_series", None)
        if pval_src is None and isinstance(charts, dict):
            pval_src = charts.get("portfolio_value_series", None)
        if pval_src is None:
            raise ValueError("portfolio_value_series is missing. Run Cell 15 first.")
        pval_all = pd.to_numeric(pd.Series(pval_src), errors="coerce").dropna().copy()
        pval_all.index = pd.to_datetime(pval_all.index).tz_localize(None)
        
        end_dt = pval_all.index.max()
        start_dt = end_dt - pd.Timedelta(days=lookback_days)
        
        pval = pval_all.loc[start_dt:end_dt].copy()
        pval = pval.ffill().bfill()
        
        benchmarks = ["^AORD", "^GSPC", "^IXIC"]
        
        # yfinance end is exclusive; add a day so we actually include the last date
        bench_raw = yf.download(
            benchmarks,
            start=pval.index.min(),
            end=(pval.index.max() + pd.Timedelta(days=1)),
            progress=False,
            auto_adjust=True,
            threads=False
        )
        
        # Handle multi-index and clean
        if isinstance(bench_raw.columns, pd.MultiIndex):
            bench_raw = bench_raw["Close"]
        else:
            # Sometimes yf returns a single level; still try to use Close if present
            if "Close" in bench_raw.columns:
                bench_raw = bench_raw["Close"]
        
        bench_raw.index = pd.to_datetime(bench_raw.index).tz_localize(None)
        bench_raw = bench_raw.ffill().bfill()
        
        # Align benchmarks to portfolio dates
        bench = bench_raw.reindex(pval.index).ffill().bfill()
        
        # Returns from start of window (decimal)
        portfolio_returns = (pval / pval.iloc[0]) - 1.0
        benchmark_returns = bench.div(bench.iloc[0]).subtract(1.0)
                
        # --- Optional: add "With Tilts" synthetic performance line ---
        tilted_returns = None
        try:
            returns_wide_df = globals().get("returns_wide_df", None)
            w_with_tilts = globals().get("W_WITH_TILTS_SER", None)
        
            if isinstance(returns_wide_df, pd.DataFrame) and isinstance(w_with_tilts, (pd.Series, dict)):
                w_ser = pd.Series(w_with_tilts).astype(float)
                # Align weights to the returns matrix columns (tickers)
                common = returns_wide_df.columns.intersection(w_ser.index)
                w_ser = w_ser.reindex(common).fillna(0.0)
                if float(w_ser.sum()) != 0.0:
                    w_ser = w_ser / float(w_ser.sum())
        
                r_tilt = (returns_wide_df[common].reindex(pval.index).fillna(0.0) @ w_ser).astype(float)
                tilted_curve = (1.0 + r_tilt).cumprod()
                tilted_curve = tilted_curve / float(tilted_curve.iloc[0])
                tilted_returns = tilted_curve - 1.0
        except Exception:
            tilted_returns = None


        # Friendly labels
        benchmark_returns = benchmark_returns.rename(columns={
            "^AORD": "ASX",
            "^GSPC": "S&P 500",
            "^IXIC": "NASDAQ"
        })
        
        # --- Combine into one DataFrame ---
        series_list = [portfolio_returns.rename("Portfolio")]
        
        # Add With Tilts line if we successfully built it
        if "tilted_returns" in locals() and tilted_returns is not None:
            series_list.append(tilted_returns.rename("With Tilts"))
        
        series_list += [benchmark_returns[c].rename(c) for c in benchmark_returns.columns]
        
        perf_df = pd.concat(series_list, axis=1).dropna(how="all")
        
        if perf_df.empty or perf_df.dropna(how="all").empty:
            raise ValueError("Slide 3 perf_df is empty after alignment (portfolio vs benchmarks).")
        
        # Optional: clip extreme outliers (prevents visual spikes)
        perf_df = perf_df.clip(lower=-0.2, upper=0.5)

        # --- Plot ---
        fig, ax = plt.subplots(figsize=(7, 4.5))
        perf_df.mul(100).plot(ax=ax, linewidth=1.8)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d-%b"))
        ax.tick_params(axis="x", labelsize=8, rotation=30)
        for lbl in ax.get_xticklabels():
            lbl.set_rotation(90)
            lbl.set_ha("center")
            lbl.set_va("top")
        fig.subplots_adjust(bottom=0.25)
        
        ax.set_title("Portfolio vs ASX, S&P 500, NASDAQ (3-Month Performance)")
        ax.set_ylabel("Return (%)")
        ax.legend(loc="upper left", frameon=False)
        ax.grid(True, linestyle="--", alpha=0.4)
        
        chart_path = os.path.join(APP_DIR, "perf_vs_indices.png")
        fig.savefig(chart_path, bbox_inches="tight")
        plt.close(fig)
        
        # --- Insert chart in PowerPoint ---
        chart_left, chart_top, chart_w, chart_h = _ppt_anchor(
            slide, slide_layout, "chart_perf",
            fb_left_cm=2.032, fb_top_cm=2.95, fb_w_cm=20.828, fb_h_cm=11.176,
        )
        slide.shapes.add_picture(chart_path, chart_left, chart_top, width=chart_w, height=chart_h)

        # --- Performance table (3m / 6m / 12m / 3y) under the chart ---
        try:
            # Portfolio series for table (prefer in-memory value series)
            port_px = globals().get("portfolio_value_series", None)
            if port_px is None:
                port_px = globals().get("portfolio_value_series", None)

            
            # Benchmarks: download a longer window so 6M/12M/3Y arenâ€™t clipped to the same start date
            bench_px = {}
            if port_px is not None and not pd.to_numeric(port_px, errors="coerce").dropna().empty:
                end_dt_tbl = pd.to_datetime(pd.to_numeric(port_px, errors="coerce").dropna().index[-1])
            else:
                end_dt_tbl = pd.to_datetime(recent_prices.index[-1])
            
            start_dt_tbl = end_dt_tbl - relativedelta(years=3, months=1)
            
            benchmark_data_long = yf.download(
                benchmarks,
                start=start_dt_tbl,
                end=end_dt_tbl,
                progress=False,
                auto_adjust=True,
                threads=False
            )
            
            if isinstance(benchmark_data_long.columns, pd.MultiIndex):
                benchmark_data_long = benchmark_data_long["Close"]
            
            benchmark_data_long = benchmark_data_long.ffill().bfill()
            
            for b in benchmarks:
                if b in benchmark_data_long.columns:
                    bench_px[b] = benchmark_data_long[b]

        
            end_dt = None
            if port_px is not None and not port_px.dropna().empty:
                end_dt = port_px.dropna().index[-1]
            elif len(bench_px) > 0:
                end_dt = list(bench_px.values())[0].dropna().index[-1]
        
            metrics = ["3M", "6M", "12M", "3Y"]
            rows = {}
        
            if port_px is not None:
                rows["Portfolio"] = [
                    _period_total_return(port_px, end_dt, months=3),
                    _period_total_return(port_px, end_dt, months=6),
                    _period_total_return(port_px, end_dt, months=12),
                    _period_total_return(port_px, end_dt, years=3),
                ]
            # Add With Tilts to the table if we can build a synthetic tilted price series
            try:
                returns_wide_df = globals().get("returns_wide_df", None)
                w_with_tilts = globals().get("W_WITH_TILTS_SER", None)
            
                if isinstance(returns_wide_df, pd.DataFrame) and isinstance(w_with_tilts, (pd.Series, dict)) and port_px is not None:
                    w_ser = pd.Series(w_with_tilts).astype(float)
                    common = returns_wide_df.columns.intersection(w_ser.index)
                    w_ser = w_ser.reindex(common).fillna(0.0)
                    if float(w_ser.sum()) != 0.0:
                        w_ser = w_ser / float(w_ser.sum())
            
                    # Build a synthetic "price" series over the SAME date index as port_px (so _period_total_return works)
                    r_tilt_tbl = (returns_wide_df[common].reindex(port_px.index).fillna(0.0) @ w_ser).astype(float)
                    px_tilt_tbl = (1.0 + r_tilt_tbl).cumprod()
                    px_tilt_tbl = px_tilt_tbl * float(pd.to_numeric(port_px, errors="coerce").dropna().iloc[0])
            
                    rows["With Tilts"] = [
                        _period_total_return(px_tilt_tbl, end_dt, months=3),
                        _period_total_return(px_tilt_tbl, end_dt, months=6),
                        _period_total_return(px_tilt_tbl, end_dt, months=12),
                        _period_total_return(px_tilt_tbl, end_dt, years=3),
                    ]
            except Exception:
                pass

            for b, s in bench_px.items():
                rows[str(b)] = [
                    _period_total_return(s, end_dt, months=3),
                    _period_total_return(s, end_dt, months=6),
                    _period_total_return(s, end_dt, months=12),
                    _period_total_return(s, end_dt, years=3),
                ]
        
            perf_tbl = pd.DataFrame.from_dict(rows, orient="index", columns=metrics)
            name_map = {"^AORD": "ASX", "^GSPC": "S&P 500", "^IXIC": "NASDAQ"}
            perf_tbl = perf_tbl.rename(index=name_map)
            tbl_left, tbl_top, tbl_w, tbl_h = _ppt_anchor(
                slide, slide_layout, "table_perf",
                fb_left_cm=2.032, fb_top_cm=13.85, fb_w_cm=20.828, fb_h_cm=2.40,
            )
            _add_perf_table(
                slide, perf_tbl,
                left=tbl_left, top=tbl_top, width=tbl_w, height=tbl_h,
                title="Return Summary",
            )
        except Exception as e:
            print(f"[pptx] Slide 3 table skipped: {e}")

        
        # --- SLIDE 4: Fama French benchmarks + table (quarterly) ---
        try:
            slide_layout = prs.slide_layouts[20]
            slide4 = prs.slides.add_slide(slide_layout)
            if slide4.shapes.title:
                slide4.shapes.title.text = "Fama French Benchmarks vs Portfolio"
        
            # Pull FF factors (daily) and convert to quarterly returns
            ff = globals().get("ff5_raw", None)
            pxdf = globals().get("prices", None)
        
            if isinstance(ff, pd.DataFrame) and not ff.empty and isinstance(pxdf, pd.DataFrame) and "PortfolioValue" in pxdf.columns:
                port_px = pd.to_numeric(pxdf["PortfolioValue"], errors="coerce").dropna()
        
            # Build daily benchmark series (FF factors are daily; most recent date may lag live markets)
            ff_cols = [c for c in ["Mkt-RF","SMB","HML","RMW","CMA","MOM","RF"] if c in ff.columns]
            ffd = ff[ff_cols].dropna().copy()
            
            # Market total return proxy = (Mkt-RF + RF)
            if ("Mkt-RF" in ffd.columns) and ("RF" in ffd.columns):
                ffd["Market (Mkt-RF)"] = ffd["Mkt-RF"] + ffd["RF"]
            
            # Portfolio daily returns
            port_r = port_px.pct_change().dropna()
            
            # Use the latest common date (FF often lags)
            common_end = min(ffd.index.max(), port_r.index.max())
            
            # Table window (up to 3Y of overlap)
            window_start_tbl = common_end - relativedelta(years=3, days=10)
            ffd_tbl = ffd.loc[window_start_tbl:common_end]
            port_r_tbl = port_r.loc[window_start_tbl:common_end]
            
            # Chart window (last ~3 months of FF-available overlap)
            window_start_chart = common_end - relativedelta(months=3, days=10)
            ffd_chart = ffd.loc[window_start_chart:common_end]
            port_r_chart = port_r.loc[window_start_chart:common_end]
            
            # Choose a small set to chart (readable)
            series_to_show = []
            if "Market (Mkt-RF)" in ffd.columns:
                series_to_show.append("Market (Mkt-RF)")
            for c in ["SMB","HML","RMW","CMA","MOM"]:
                if c in ffd.columns:
                    series_to_show.append(c)
                        
            chart_df = pd.DataFrame({"Portfolio": port_r_chart}).join(ffd_chart[series_to_show], how="inner")
            tbl_df   = pd.DataFrame({"Portfolio": port_r_tbl}).join(ffd_tbl[series_to_show], how="inner")
            
            # Chart 
            APP_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
            ff_chart_path = os.path.join(APP_DIR, "ff_benchmarks.png")
            
            ret = ((1.0 + chart_df.fillna(0.0)).cumprod() - 1.0) * 100.0
            fig, ax = plt.subplots(figsize=(7.5, 4.8))
            ret.plot(ax=ax, linewidth=1.4)
            
            # Make room inside the figure on the right for the legend
            fig.subplots_adjust(right=0.78)
            ax.legend(loc="center left", bbox_to_anchor=(1.01, 0.5), frameon=False, fontsize=9)
            ax.set_title("Portfolio vs Fama French Factors (3-Month Performance)")
            ax.set_ylabel("Return (%)")
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%d-%b"))
            ax.grid(True, linestyle="--", alpha=0.4)
            ax.margins(x=0)
            if not ret.empty:
                ax.set_xlim(ret.index.min(), ret.index.max())
            fig.savefig(ff_chart_path, bbox_inches="tight")
            plt.close(fig)
            
            chart_left, chart_top, chart_w, chart_h = _ppt_anchor(
                slide4, slide_layout, "chart_ff",
                fb_left_cm=2.032, fb_top_cm=3.05, fb_w_cm=20.32, fb_h_cm=8.65,
            )
            slide4.shapes.add_picture(ff_chart_path, chart_left, chart_top, width=chart_w, height=chart_h)
            
            # Table: 3M/6M/12M/3Y (compounded) using available daily points.
            # FF factor rows are anchored to the FF data's last date (~1mo lag).
            # The Portfolio row is anchored to the *live* portfolio end date
            # (same anchor Slide 3 uses) so "3M" means the same window on both
            # slides — see Task #8 in the pending-work memory.
            end_dt_tbl = tbl_df.index.max()  # FF-data-capped end date for factor rows
            portfolio_end_dt = port_px.dropna().index.max() if not port_px.dropna().empty else end_dt_tbl
            rows = {}
            for name in tbl_df.columns:
                if name == "Portfolio":
                    rows[name] = [
                        _period_total_return(port_px, portfolio_end_dt, months=3),
                        _period_total_return(port_px, portfolio_end_dt, months=6),
                        _period_total_return(port_px, portfolio_end_dt, months=12),
                        _period_total_return(port_px, portfolio_end_dt, years=3),
                    ]
                else:
                    rr = tbl_df[name]
                    rows[name] = [
                        _window_compound_total(rr, end_dt_tbl, months=3),
                        _window_compound_total(rr, end_dt_tbl, months=6),
                        _window_compound_total(rr, end_dt_tbl, months=12),
                        _window_compound_total(rr, end_dt_tbl, years=3),
                    ]

            ff_tbl = pd.DataFrame.from_dict(rows, orient="index", columns=["3M", "6M", "12M", "3Y"])
            tbl_left, tbl_top, tbl_w, tbl_h = _ppt_anchor(
                slide4, slide_layout, "table_ff",
                fb_left_cm=2.032, fb_top_cm=11.90, fb_w_cm=20.32, fb_h_cm=2.794,
            )
            _add_perf_table(
                slide4, ff_tbl,
                left=tbl_left, top=tbl_top, width=tbl_w, height=tbl_h,
                title="Return Summary",
            )
        except Exception as e:
            print(f"[pptx] Slide 4 skipped: {e}")

        # --- SLIDE 5: Efficient Frontier (chart + points table) ---
        try:
            if isinstance(charts, dict):
                print("[pptx] Slide 5 chart keys:", list(charts.keys()))
                print("[pptx] tilts_comparison_rows present:", "tilts_comparison_rows" in charts)
                if "tilts_comparison_rows" in charts:
                    print("[pptx] tilts_comparison_rows sample:", charts["tilts_comparison_rows"][:3])
        except Exception:
            pass

        try:
            slide_layout = prs.slide_layouts[20]
            slide5 = prs.slides.add_slide(slide_layout)
            if slide5.shapes.title:
                slide5.shapes.title.text = "Efficient Frontier"
        
            eff_path = None
            if isinstance(charts, dict):
                eff_path = charts.get("efficient_frontier_path", None)
        
            # Always build rows for the points table (even if the chart image is missing)
            pts = {}
            if isinstance(charts, dict):
                pts = charts.get("frontier_points", {}) or {}
            
            rows = []
            for k in ["Current", "Previous", "Optimised", "With Tilts", "Target"]: #Change these for the names
                v = pts.get(k, None)
                if v is None:
                    continue
                try:
                    vol, ret = float(v[0]), float(v[1])
                    if np.isfinite(vol) and np.isfinite(ret):
                        rows.append({"Point": k, "Vol (ann.)": vol, "Return (ann.)": ret})
                except Exception:
                    pass
            
            # Chart is OPTIONAL: only add if we actually have a valid file path
            if eff_path and os.path.exists(eff_path):
                # chart on left
                chart_left, chart_top, chart_w, chart_h = _ppt_anchor(
                    slide5, slide_layout, "chart_frontier",
                    fb_left_cm=1.52, fb_top_cm=3.56, fb_w_cm=14.50, fb_h_cm=11.50,
                )
                slide5.shapes.add_picture(
                    eff_path, chart_left, chart_top, width=chart_w, height=chart_h,
                )
            
            # Points table (always add if we have data)
            if rows:
                df_pts = pd.DataFrame(rows).set_index("Point").rename(
                    columns={"Vol (ann.)": "Volatility", "Return (ann.)": "Return"}
                )
                tbl_left, tbl_top, tbl_w, tbl_h = _ppt_anchor(
                    slide5, slide_layout, "table_frontier_points",
                    fb_left_cm=16.50, fb_top_cm=4.06, fb_w_cm=7.72, fb_h_cm=4.32,
                )
                _add_perf_table(
                    slide5, df_pts,
                    left=tbl_left, top=tbl_top, width=tbl_w, height=tbl_h,
                    title="Portfolio",
                )

            # ---- Slide 5: Tilts table (With Tilts vs Without Tilts) ----
            try:
                tilt_rows = charts.get("tilts_comparison_rows", None) if isinstance(charts, dict) else None
                print("[pptx] Slide 5 tilt_rows raw:", tilt_rows)

                if tilt_rows:
                    df_tilts = pd.DataFrame(tilt_rows)
                    print("[pptx] Slide 5 df_tilts columns before rename:", list(df_tilts.columns))
                    print("[pptx] Slide 5 df_tilts shape before rename:", df_tilts.shape)

                    rename_map = {
                        "With Tilts": "Achieved Tilt",
                        "Target": "Target Tilt",
                    }
                    df_tilts = df_tilts.rename(columns=rename_map)

                    keep_cols = ["Factor", "Achieved Tilt", "Target Tilt"]
                    df_tilts = df_tilts[[c for c in keep_cols if c in df_tilts.columns]]
                    print("[pptx] Slide 5 df_tilts columns after filter:", list(df_tilts.columns))
                    print("[pptx] Slide 5 df_tilts preview:\n", df_tilts.head())

                    required_cols = {"Factor", "Achieved Tilt", "Target Tilt"}
                    if df_tilts.empty:
                        print("[pptx] Slide 5 tilts table skipped: df_tilts is empty after filtering")
                    elif not required_cols.issubset(df_tilts.columns):
                        print(f"[pptx] Slide 5 tilts table skipped: missing required columns. Have {list(df_tilts.columns)}")
                    else:
                        left2, top2, width2, height2 = _ppt_anchor(
                            slide5, slide_layout, "table_tilts",
                            fb_left_cm=16.50, fb_top_cm=9.60, fb_w_cm=7.72, fb_h_cm=5.20,
                        )

                        shp2 = slide5.shapes.add_table(
                            df_tilts.shape[0] + 1,
                            df_tilts.shape[1],
                            left2, top2, width2, height2,
                        )
                        tbl2 = shp2.table

                        # Headers
                        for j, col in enumerate(df_tilts.columns):
                            tbl2.cell(0, j).text = str(col)

                        # Body
                        for i, (_, r) in enumerate(df_tilts.iterrows(), start=1):
                            for j, col in enumerate(df_tilts.columns):
                                val = r[col]
                                if pd.isna(val):
                                    txt = ""
                                elif col == "Factor":
                                    txt = str(val)
                                else:
                                    txt = f"{float(val):.3f}" if np.isfinite(float(val)) else ""
                                tbl2.cell(i, j).text = txt

                        # Format (uniform 11pt to match the frontier-points table above)
                        for rr in range(df_tilts.shape[0] + 1):
                            for cc in range(df_tilts.shape[1]):
                                cell = tbl2.cell(rr, cc)
                                cell.text_frame.word_wrap = False
                                for p in cell.text_frame.paragraphs:
                                    p.font.size = Pt(11)
                                    p.font.bold = True
                                    p.alignment = PP_ALIGN.CENTER

            except Exception as _e_tilts_tbl:
                print(f"[pptx] Slide 5 tilts comparison table skipped: {_e_tilts_tbl}")

        except Exception as e:
            print(f"[pptx] Slide 5 skipped: {e}")

        tmp_path = ppt_path.replace(".pptx", ".__tmp__.pptx")
        prs.save(tmp_path)
        os.replace(tmp_path, ppt_path)
        print(f"[ppt] Report saved to: {ppt_path}")
        return ppt_path


# =====================================================================
# --- Block 10: Finishers / Launchers PPTX ---
# =====================================================================
# --- Block 10: Finishers / Launchers PPTX ---
ppt_path = None

def _wait_for_pptx_ready(path, timeout_s=10.0, stable_s=1.0, poll_s=0.2):
    """Wait for a valid, stable PPTX file on disk before opening."""
    import os
    import time
    import zipfile

    if not path:
        return False

    t0 = time.time()
    last_size = -1
    last_change = t0

    while (time.time() - t0) < timeout_s:
        try:
            if not os.path.exists(path):
                time.sleep(poll_s)
                continue

            size = os.path.getsize(path)
            if size != last_size:
                last_size = size
                last_change = time.time()

            if (time.time() - last_change) >= stable_s:
                with zipfile.ZipFile(path, "r") as zf:
                    if zf.testzip() is None:
                        return True
        except Exception:
            pass

        time.sleep(poll_s)

    return False

if CFG.get("generate_report", True):
    _results = globals().get("results")
    _trades = globals().get("trades")
    _charts = globals().get("charts")

    _missing = []
    if not isinstance(_results, dict):
        _missing.append("results")
    if _trades is None:
        _missing.append("trades")
    if _charts is None:
        _missing.append("charts")

    if _missing:
        print(f"[pptx] Skipping report generation. Missing data: {', '.join(_missing)}. Run Cell 15 first.")
        ppt_path = None
    else:
        if not isinstance(_charts, dict):
            _charts = dict(_charts or {})
            globals()["charts"] = _charts

        if isinstance(_trades, pd.DataFrame) and ("Brokerage" not in _trades.columns):
            _trades = _trades.copy()
            if "Brokerage (AUD)" in _trades.columns:
                _trades["Brokerage"] = pd.to_numeric(_trades["Brokerage (AUD)"], errors="coerce").fillna(0.0)
            else:
                _trades["Brokerage"] = 0.0
            globals()["trades"] = _trades

        try:
            _tilt_rows = _charts.get("tilts_comparison_rows") if isinstance(_charts, dict) else None

            # Rebuild from optimisation globals when charts payload is narrowed.
            if not _tilt_rows:
                try:
                    _factor_order = ["Mkt-RF", "SMB", "HML", "RMW", "CMA", "MOM"]
                    _B = globals().get("B")
                    _Sigma = globals().get("Sigma_opt")
                    _w_with = globals().get("w_star_with_tilts")
                    _w_without = globals().get("w_star")
                    _tilt_df = globals().get("tilt_df", None)

                    if isinstance(_B, pd.DataFrame) and (not _B.empty) and hasattr(_Sigma, "index") and (_w_with is not None) and (_w_without is not None):
                        def _norm_w_local(w, idx):
                            s = pd.Series(np.asarray(w, dtype=float).reshape(-1), index=idx).fillna(0.0)
                            tot = float(s.sum())
                            return (s / tot) if tot != 0 else s

                        _w_with_s = _norm_w_local(_w_with, _Sigma.index).reindex(_B.index).fillna(0.0)
                        _w_without_s = _norm_w_local(_w_without, _Sigma.index).reindex(_B.index).fillna(0.0)
                        _with_beta = (_B.T @ _w_with_s)
                        _without_beta = (_B.T @ _w_without_s)

                        _out = pd.DataFrame(index=[f for f in _factor_order if f in _with_beta.index])
                        _out["With Tilts"] = _with_beta.reindex(_out.index).astype(float)
                        _out["Without Tilts"] = _without_beta.reindex(_out.index).astype(float)

                        if isinstance(_tilt_df, pd.DataFrame) and (not _tilt_df.empty) and ("Target" in _tilt_df.columns):
                            _tgt = _tilt_df.reindex(_out.index)
                            _out["Target"] = pd.to_numeric(_tgt["Target"], errors="coerce")
                            if "Use?" in _tgt.columns:
                                _use_mask = _tgt["Use?"].astype(bool)
                                _out = _out.loc[_use_mask.reindex(_out.index).fillna(False)]

                        _tilt_rows = _out.reset_index().rename(columns={"index": "Factor"}).to_dict("records")
                        if _tilt_rows:
                            _charts["tilts_comparison_rows"] = _tilt_rows
                except Exception as _e_tilt_rebuild:
                    print(f"[pptx] Tilt-row rebuild skipped: {_e_tilt_rebuild}")

            if not _tilt_rows and isinstance(globals().get("charts"), dict):
                _tilt_rows = globals().get("charts", {}).get("tilts_comparison_rows")
                if _tilt_rows:
                    _charts["tilts_comparison_rows"] = _tilt_rows
            if not _tilt_rows:
                _keys = list(_charts.keys()) if isinstance(_charts, dict) else []
                print(f"[pptx] Warning: missing charts['tilts_comparison_rows']; Slide 5 tilt table may be skipped. Available keys: {_keys}")
            ppt_path = export_to_ppt(_results, _trades, _charts)
        except Exception as exc:
            print(f"[pptx] Report generation failed: {exc}")
            ppt_path = None

if OPEN_PPT_AFTER_SAVE and ppt_path:
    if _wait_for_pptx_ready(ppt_path):
        open_ppt_if_enabled(ppt_path)
    else:
        print(f"[pptx] Saved but not opened automatically (file not stable yet): {ppt_path}")
