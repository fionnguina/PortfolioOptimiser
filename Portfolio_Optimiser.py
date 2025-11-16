### Grabbing Data From Yahoo Finance For Stock Build last updated 1/11/2025
#!pip3 install beautifulsoup4
#!pip3 install curl_cffi
#!pip3 install frozendict
#!pip3 install multitasking
#!pip3 install numpy
#!pip3 install pandas
#!pip3 install peewee
#!pip3 install platformdirs
#!pip3 install protobuf
#!pip3 install pytz
#!pip3 install requests
#!pip3 install websockets
#!pip3 install statsmodels
#!pip3 install tkinter
#!pip3 install xlwings
#!pip3 install webdriver-manager
#!pip3 install openpyxl
#!pip3 install pyinstaller
#This is only for when we require an upgrade to software package
#!pip3 install --upgrade pandas
#!pip3 install --upgrade openpyxl
#!pip3 install --upgrade yfinance
#!pip3 install --upgrade webdriver-manager
### BLOCK 1 imports
import numpy as np
import numpy.linalg as la
import pandas as pd
import requests
import time
import yfinance as yf
import openpyxl
import pathlib
import hashlib
import shutil
import io
import sys, os
import json
import xlwings as xw
import statsmodels.api as sm
import re, zipfile
import tkinter as _tk
import multiprocessing as mp
from numpy.linalg import pinv
from pathlib import Path
from dateutil.relativedelta import relativedelta
from openpyxl import Workbook
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from datetime import datetime
from tkinter import ttk as _ttk, messagebox as _mb
from scipy.optimize import minimize
try:
    import win32com.client as win32
    HAS_WIN32COM = True
except Exception:
    HAS_WIN32COM = False
### BLOCK 2 Global codes and Data Retrieval from the web
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
    return str((APP_DIR / "Stock Analysis.xlsm").resolve())

CONFIG_PATH = APP_DIR / "config.json"

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
}

# ---------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------
def load_config() -> dict:
    cfg = dict(_DEFAULTS)
    try:
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                user_cfg = json.load(f)
            for k, v in user_cfg.items():
                if k in cfg:
                    cfg[k] = v
    except Exception as e:
        print(f"[config] using defaults (error reading config.json): {e}")

    # Ensure workbook directory exists
    try:
        os.makedirs(Path(cfg["excel_path"]).parent, exist_ok=True)
    except Exception:
        pass

    return cfg


CFG = load_config()

# ---------------------------------------------------------------------
# Actual Code
# ---------------------------------------------------------------------

TILT_FACTORS = ["Mkt-RF","SMB","HML","RMW","CMA","MOM"]

# Bind config into existing globals you already use
filename               = CFG["excel_path"]
MARGINAL_TAX_RATE      = float(CFG["marginal_tax_rate"])
CAPITAL_LOSS_CARRY_FWD = float(CFG["carry_forward_losses"])
LOT_MATCH_METHOD       = str(CFG["lot_match_method"]).upper()
OPEN_AFTER_SAVE        = bool(CFG.get("open_after_save", True))
USE_XLWINGS            = bool(CFG.get("use_xlwings", True))

def evaluate_transaction_costs(trade_df, lots_df, sale_date, tax_rate):
    # Temporary placeholder to prevent crash
    return {"brokerage": 0.0, "cgt_tax": 0.0, "total_cost": 0.0}

def _read_lots_from_path(xl_path, sheet_name="Lots"):
    try:
        return pd.read_excel(xl_path, sheet_name=sheet_name)
    except Exception:
        return pd.DataFrame(columns=["Security","AcqDate","Units","CostBaseAUD"])

# --- Brokerage & CGT config (edit these to suit) ---
BROKERAGE = {
    "ASX": {"first_buy_free_threshold": 1000.0, "min_fee": 11.0, "rate": 0.001},  # 0.10%
    "US":  {"min_fee": 0.0, "rate": 0.0},  # CMC U.S. brokerage $0
}
MARGINAL_TAX_RATE = 0.37           # your personal marginal rate (decimal)
CAPITAL_LOSS_CARRY_FWD = 0.0       # prior year carried-forward capital losses (AUD)
LOT_MATCH_METHOD = "HIFO"          # or "FIFO" (parcel-matching when selling)

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

# -------- Risk-free (AU): current RBA cash rate target --------
def get_rba_cash_rate_target_current(default=0.04):
    """
    Returns the latest RBA cash rate target as a decimal, scraped from:
    https://www.rba.gov.au/statistics/cash-rate/
    Falls back to the monthly-average CSV if needed, else `default`.
    """
    try:
        dfs = pd.read_html("https://www.rba.gov.au/statistics/cash-rate/", match="Cash rate target %")
        if dfs:
            tab = dfs[0]
            val = pd.to_numeric(tab.iloc[0]["Cash rate target %"], errors="coerce")
            if pd.notna(val):
                return float(val) / 100.0
    except Exception:
        pass
    try:
        csv = "https://www.rba.gov.au/statistics/tables/csv/f1.1-data.csv"
        df = pd.read_csv(csv)
        col = next(c for c in df.columns if c.lower().startswith("cash rate target"))
        last = pd.to_numeric(df[col], errors="coerce").dropna().iloc[-1]
        return float(last) / 100.0
    except Exception:
        return float(default)

# -------- Simple on-disk cache (7-day TTL) for FF5 + MOM --------
_CACHE_DIR = pathlib.Path(os.path.expanduser("~")) / ".portfolio_optimiser_cache"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _cache_path(url: str) -> pathlib.Path:
    key = hashlib.md5(url.encode("utf-8")).hexdigest()
    return _CACHE_DIR / f"{key}.csv"

def _cached_read(url: str, build_df_fn, ttl_days: int = 7) -> pd.DataFrame:
    """
    If we have a cached CSV newer than ttl_days, load it.
    Otherwise call build_df_fn() to construct the DataFrame, then store it.
    Assumes the DF has a DatetimeIndex.
    """
    p = _cache_path(url)
    try:
        if p.exists():
            age_sec = time.time() - p.stat().st_mtime
            if age_sec <= ttl_days * 86400:
                df = pd.read_csv(p, index_col=0, parse_dates=[0])
                # ensure sorted Date index
                df.index = pd.to_datetime(df.index)
                return df.sort_index()
    except Exception as e:
        print(f"[cache] read miss due to: {e}")

    df = build_df_fn()
    try:
        df.to_csv(p)
    except Exception as e:
        print(f"[cache] write skipped due to: {e}")
    return df


# -------- FF5F + Momentum loaders (Dartmouth) --------
FF5_DAILY_ZIP = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_daily_CSV.zip"
MOM_DAILY_ZIP = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_daily_CSV.zip"

def get_mom_daily():
    def _builder():
        r = requests.get(MOM_DAILY_ZIP, timeout=60); r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
        csv = next(n for n in z.namelist() if n.lower().endswith(".csv"))
        raw = z.read(csv).decode("latin1", errors="ignore").splitlines()
        num_rx = re.compile(r"^\s*\d{6,8}\s*[,\s]")
        first = next(i for i, ln in enumerate(raw) if num_rx.match(ln))
        header = "Date,MOM"
        data = [header] + [ln.strip() for ln in raw[first:] if num_rx.match(ln)]
        df = pd.read_csv(io.StringIO("\n".join(data)), engine="python", sep=r"\s*,\s*")
        df["Date"] = pd.to_datetime(df["Date"].astype(str), format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["Date"]).set_index("Date").sort_index()
        df["MOM"] = pd.to_numeric(df["MOM"], errors="coerce") / 100.0  # decimal
        return df[["MOM"]]
    df = _cached_read(MOM_DAILY_ZIP, _builder, ttl_days=7)
    # ensure schema exactly as expected
    df = df.copy()
    if "MOM" not in df.columns:
        df["MOM"] = pd.to_numeric(df.iloc[:, 0], errors="coerce")
        df = df[["MOM"]]
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    return df

def get_ff5_daily(cache_csv_path=None):
    """
    Fama–French 5 Factors (2x3) [Daily].
    Returns columns (decimals): ['Mkt-RF','SMB','HML','RMW','CMA','RF'] indexed by Date.
    Uses 7-day cached CSV in %USERPROFILE%\\.portfolio_optimiser_cache
    """
    def _builder():
        resp = requests.get(FF5_DAILY_ZIP, timeout=60)
        resp.raise_for_status()
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        csv_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))

        raw = zf.read(csv_name).decode("latin1", errors="ignore")
        lines = raw.splitlines()

        num_rx = re.compile(r"^\s*\d{6,8}\s*[,\s]")
        first_data_idx = next(i for i, ln in enumerate(lines) if num_rx.match(ln))

        header_idx = None
        for i in range(max(0, first_data_idx-5), first_data_idx+1):
            if re.search(r"\bdate\b", lines[i], flags=re.I) and ("mkt" in lines[i].lower()):
                header_idx = i
                break

        header = lines[header_idx].strip() if header_idx is not None else "Date,Mkt-RF,SMB,HML,RMW,CMA,RF"
        data_lines = [header]
        for ln in lines[first_data_idx:]:
            if not num_rx.match(ln):
                break
            data_lines.append(ln.strip())

        df = pd.read_csv(io.StringIO("\n".join(data_lines)), engine="python", sep=r"\s*,\s*")
        df.columns = [c.strip() for c in df.columns]
        col_map = {c.lower().replace(" ", ""): c for c in df.columns}
        ren = {}
        for want in ["Date","Mkt-RF","SMB","HML","RMW","CMA","RF"]:
            key = want.lower().replace(" ", "")
            if key in col_map:
                ren[col_map[key]] = want
        df = df.rename(columns=ren)

        df["Date"] = pd.to_datetime(df["Date"].astype(str), format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["Date"]).set_index("Date").sort_index()
        factor_cols = ["Mkt-RF","SMB","HML","RMW","CMA","RF"]
        df[factor_cols] = df[factor_cols].apply(pd.to_numeric, errors="coerce") / 100.0
        df = df.dropna(subset=factor_cols)
        return df

    df = _cached_read(FF5_DAILY_ZIP, _builder, ttl_days=7)

    # Optional external cache file output for your own debugging
    if cache_csv_path:
        try:
            df.to_csv(cache_csv_path, index=True)
        except Exception as e:
            print(f"[ff5] could not write cache_csv_path: {e}")

    return df

def get_ff5_mom_daily():
    """
    Return daily factors as decimals with columns:
    ['Mkt-RF','SMB','HML','RMW','CMA','MOM','RF'] on a common date index.
    """
    ff5_only = get_ff5_daily()   # <-- was wrongly calling get_ff5_mom_daily()
    mom = get_mom_daily()

    out = ff5_only.join(mom, how="inner").sort_index()
    # reorder defensively (only keep columns that exist)
    cols = [c for c in ["Mkt-RF","SMB","HML","RMW","CMA","MOM","RF"] if c in out.columns]
    return out[cols]


# -------- Foreign Exchange from Yahoo Finance --------
def _last_numeric(x):
    v = x.iloc[-1]
    if isinstance(v, pd.Series):
        v = v.iloc[0]
    return float(v)

def get_usd_aud_fx(default=1.50):
    try:
        px = yf.download("AUDUSD=X", period="5d", interval="1d",
                         auto_adjust=True, threads=False, progress=False)["Close"].dropna()
        last = _last_numeric(px)
        if last > 0:
            return 1.0 / last                  # AUD per 1 USD
    except Exception:
        pass
    try:
        px = yf.download("USDAUD=X", period="5d", interval="1d",
                         auto_adjust=True, threads=False, progress=False)["Close"].dropna()
        last = _last_numeric(px)
        if last > 0:
            return last                        # AUD per 1 USD
    except Exception:
        pass
    return float(default)

def fx_to_aud_for_tickers(tickers, usd_aud_rate):
    """1.0 for AUS tickers (*.AX) & indices (^...), usd_aud_rate for others (assume USD)."""
    out = {}
    for t in map(str, tickers):
        out[t] = 1.0 if (t.startswith("^") or t.endswith(".AX")) else float(usd_aud_rate)
    return pd.Series(out, name="FX to AUD")

### BLOCK 3 Downloading Prices
# ------------------------------------------------------------
# 1) DOWNLOAD PRICES  — universe comes from Holdings sheet + static starters
# ------------------------------------------------------------

# If Block 7 defines `filename`, we’ll reuse it. Otherwise set your path here.
_XL_PATH = globals().get("filename", _default_excel_path())

# Your static “starter” universe (kept as a safety net / defaults)
STATIC_STARTERS = ['^AORD']

EXCLUDE_FROM_OPT = {'^AORD'}
rf_annual = get_rba_cash_rate_target_current()

def _tickers_from_holdings(xl_path, sheet='Holdings'):
    """Extract 'Security' values using pandas (no COM)."""
    try:
        df = pd.read_excel(xl_path, sheet_name=sheet)
    except Exception:
        return []
    if not isinstance(df, pd.DataFrame) or df.empty or "Security" not in df.columns:
        return []
    sec = (df["Security"].dropna().astype(str).str.strip())
    return list(dict.fromkeys([t for t in sec if t]))

# 0) Build the universe
tickers_from_sheet = _tickers_from_holdings(_XL_PATH, sheet='Holdings')  # dynamic
tickers = list(dict.fromkeys((tickers_from_sheet or []) + STATIC_STARTERS))
if '^AORD' not in tickers:
    tickers.insert(0, '^AORD')  # always include benchmark

# 1) Download prices (robust to single/ multiple tickers)
PRICE_PERIOD = '2y'  # set '1y'/'3y' as you like
dl = yf.download(tickers, period=PRICE_PERIOD, auto_adjust=True, threads=False, progress=False)

if isinstance(dl, pd.DataFrame) and 'Close' in dl.columns:
    prices = dl['Close']
else:
    # yfinance can return a Series for a single ticker
    prices = dl if isinstance(dl, pd.Series) else pd.DataFrame()
    if isinstance(prices, pd.Series):
        prices = prices.to_frame(name=tickers[0])
prices.index = pd.to_datetime(prices.index)
prices = prices.sort_index()
prices = prices.loc[:, ~prices.columns.duplicated()]  # de-dup any duplicate tickers defensively

# ------------- FX conversion for US stocks (to AUD for returns) ------------------

# 1) AUD per 1 USD (ensure we end up with a Series)
fx_raw = yf.download("USDAUD=X", period="5y", interval="1d",
                     auto_adjust=True, threads=False, progress=False)
fx = fx_raw["Close"] if isinstance(fx_raw, pd.DataFrame) else fx_raw
if isinstance(fx, pd.DataFrame):
    fx = fx.iloc[:, 0]
fx = pd.to_numeric(fx, errors="coerce").reindex(prices.index).ffill()

# identify USD-priced tickers
usd_cols = [str(c) for c in prices.columns if not str(c).endswith(".AX") and not str(c).startswith("^")]

# build an AUD-converted copy safely
prices_aud_for_returns = prices.copy()
usd_part = prices.loc[:, usd_cols].mul(fx, axis=0)  # align by date
prices_aud_for_returns.update(usd_part)

# 4) Compute returns *from AUD-converted prices*
df = prices_aud_for_returns.reset_index()
df_melt = (
    prices_aud_for_returns.reset_index()
      .melt(id_vars='Date', var_name='Security', value_name='Close')
      .sort_values(['Security','Date'])
)
df_melt['Return'] = df_melt.groupby('Security', sort=False)['Close'].pct_change(fill_method=None)
df_melt = df_melt.dropna()

# 5) FX map for holdings (last-price conversion in the sheet)
usd_aud = get_usd_aud_fx()
fx_map_all = fx_to_aud_for_tickers(prices.columns, usd_aud)

### BLOCK 4 Creating the Stock Holdings Dialog Box
# -------------------------------
# 2) GUI portfolio editor (Tkinter) + helpers — CLEAN VERSION
# -------------------------------

def _fetch_prices_for_new_tickers(tickers_new, base_prices, period='5y'):
    add = [t for t in map(str, tickers_new) if t not in base_prices.columns]
    if not add:
        return base_prices
    try:
        dl = yf.download(add, period=period, auto_adjust=True, threads=False, progress=False)
        if isinstance(dl, pd.DataFrame) and 'Close' in dl.columns:
            dl = dl['Close']          # wide DataFrame if multiple tickers
        if isinstance(dl, pd.Series):  # single ticker case → rename to that ticker
            name = add[0]
            dl = dl.rename(name).to_frame()
        dl.index = pd.to_datetime(dl.index)
        out = base_prices.join(dl, how='outer').sort_index()
        out = out.loc[:, ~out.columns.duplicated()]
        return out
    except Exception as e:
        print(f"Warning: could not fetch some tickers {add}: {e}")
        return base_prices

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

    units = pd.to_numeric(df.get("Units", 0.0), errors="coerce").fillna(0.0)
    if "Include?" in df.columns:
        inc = df["Include?"].astype(str).str.strip().str.upper().isin({"TRUE","1","Y","YES","T"})
    else:
        inc = pd.Series(True, index=df.index)

    units = pd.Series(units.values, index=df["Security"])
    include = dict(zip(df["Security"], inc.astype(bool)))
    print(f"[seed-path] holdings: rows={len(units)}, nonzero={int((units!=0).sum())}")
    return units, include


def _read_tilts_seed_from_path(xl_path, sheet_name="Tilts"):
    # respect global factor list if you defined MOM in Block 2:
    # e.g. TILT_FACTORS = ["Mkt-RF","SMB","HML","RMW","CMA","MOM"]
    factors = list(TILT_FACTORS) if 'TILT_FACTORS' in globals() else ["Mkt-RF","SMB","HML","RMW","CMA"]
    default = pd.DataFrame(
        {"Target": [1.0] + [0.0]*(len(factors)-1),
         "Band":   [0.05]*len(factors),
         "Use?":   [True] + [False]*(len(factors)-1)},
        index=factors
    )
    try:
        df = pd.read_excel(xl_path, sheet_name=sheet_name)
    except Exception as e:
        print(f"[seed-path] tilts: {e} -> DEFAULTS"); return default

    if not isinstance(df, pd.DataFrame) or df.empty:
        print("[seed-path] tilts: empty -> DEFAULTS"); return default

    df.columns = [str(c).strip() for c in df.columns]
    need = {"Factor","Target","Band","Use?"}
    if not need.issubset(df.columns):
        print("[seed-path] tilts: malformed -> DEFAULTS"); return default

    df["Factor"] = df["Factor"].astype(str).str.strip()
    df = df.set_index("Factor").reindex(factors)
    out = default.copy()
    out.loc[df.index, "Target"] = pd.to_numeric(df["Target"], errors="coerce")
    out.loc[df.index, "Band"]   = pd.to_numeric(df["Band"],   errors="coerce")
    out.loc[df.index, "Use?"]   = df["Use?"].astype(str).str.upper().isin(["TRUE","1","Y","YES","T"])
    out["Target"] = out["Target"].fillna(default["Target"]).astype(float)
    out["Band"]   = out["Band"].fillna(default["Band"]).astype(float)
    return out.reindex(factors)

# -------------------------------
# SAFE seeds I/O used by Block 7 (readers only)
# -------------------------------
def _read_holdings_seed_from_sheet(wb, sheet_name="Holdings"):
    if wb is None:
        print("[seed] holdings: wb is None -> EMPTY seeds")
        return pd.Series(dtype=float), {}
    try:
        sht = wb.sheets[sheet_name]
    except Exception:
        print(f"[seed] holdings: sheet '{sheet_name}' not found -> EMPTY seeds")
        return pd.Series(dtype=float), {}
    try:
        ur = sht.used_range
        if ur is None:
            print("[seed] holdings: used_range is None -> EMPTY seeds")
            return pd.Series(dtype=float), {}
        vals = ur.options(ndim=2).value
        if not vals or not vals[0] or all(h is None for h in vals[0]):
            print("[seed] holdings: used_range has no headers -> EMPTY seeds")
            return pd.Series(dtype=float), {}
        headers = [str(c).strip() if c is not None else "" for c in vals[0]]
        rows = vals[1:] if len(vals) > 1 else []
        df = pd.DataFrame(rows, columns=headers)
    except Exception as e:
        print(f"[seed] holdings: failed to read table: {e}")
        return pd.Series(dtype=float), {}

    if df.empty or "Security" not in df.columns:
        print("[seed] holdings: empty/malformed -> EMPTY seeds")
        return pd.Series(dtype=float), {}

    df = df.copy()
    df["Security"] = df["Security"].astype(str).str.strip()
    units = pd.to_numeric(df.get("Units", 0.0), errors="coerce").fillna(0.0)
    if "Include?" in df.columns:
        inc = df["Include?"].astype(str).str.strip().str.upper().isin({"TRUE","1","Y","YES","T"})
    else:
        inc = pd.Series(True, index=df.index)
    units = pd.Series(units.values, index=df["Security"])
    include = dict(zip(df["Security"], inc.astype(bool)))
    print(f"[seed] holdings: loaded {int((units!=0).sum())} non-zero unit rows (of {len(units)})")
    return units, include


def _read_tilts_seed_from_sheet(wb, sheet_name="Tilts"):
    factors = list(TILT_FACTORS) if 'TILT_FACTORS' in globals() else ["Mkt-RF","SMB","HML","RMW","CMA"]
    default = pd.DataFrame(
        {"Target": [1.0] + [0.0]*(len(factors)-1),
         "Band":   [0.05]*len(factors),
         "Use?":   [True] + [False]*(len(factors)-1)},
        index=factors
    )

    if wb is None:
        print("[seed] tilts: wb is None -> DEFAULTS"); return default
    try:
        sht = wb.sheets[sheet_name]
    except Exception:
        print(f"[seed] tilts: sheet '{sheet_name}' not found -> DEFAULTS"); return default

    try:
        ur = sht.used_range
        if ur is None:
            print("[seed] tilts: used_range is None -> DEFAULTS"); return default
        vals = ur.options(ndim=2).value
        if not vals or not vals[0] or all(h is None for h in vals[0]):
            print("[seed] tilts: no data -> DEFAULTS"); return default
        headers = [str(c).strip() if c is not None else "" for c in vals[0]]
        rows = vals[1:] if len(vals) > 1 else []
        df = pd.DataFrame(rows, columns=headers)
    except Exception as e:
        print(f"[seed] tilts: failed to read table: {e} -> DEFAULTS"); return default

    need = {"Factor","Target","Band","Use?"}
    if df.empty or not need.issubset(df.columns):
        print("[seed] tilts: malformed -> DEFAULTS"); return default

    df["Factor"] = df["Factor"].astype(str).str.strip()
    df = df.set_index("Factor").reindex(factors)

    out = default.copy()
    out.loc[df.index, "Target"] = pd.to_numeric(df["Target"], errors="coerce")
    out.loc[df.index, "Band"]   = pd.to_numeric(df["Band"],   errors="coerce")
    out.loc[df.index, "Use?"]   = df["Use?"].astype(str).str.upper().isin(["TRUE","1","Y","YES","T"])
    out["Target"] = out["Target"].fillna(default["Target"]).astype(float)
    out["Band"]   = out["Band"].fillna(default["Band"]).astype(float)
    return out.reindex(factors)

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

    tickers_all = list(dict.fromkeys(list(prices.columns)))
    last_px = prices.ffill().iloc[-1]

    rows = []
    units_s = pd.Series(units)
    include_s = pd.Series(include_flags)
    for t in tickers_all:
        inc = bool(include_s.get(t, True))
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
            sht.range(f"E2:E{last_row}").api.NumberFormat = "0.00"
            sht.range(f"F2:F{last_row}").api.NumberFormat = "0.00%"
        except Exception:
            pass
    sht.autofit()

# -------------------------------
# Dialog shims (no windows here)
# -------------------------------
def edit_holdings_dialog(prices, exclude, seed_units, seed_include, title="Edit Portfolio Holdings"):
    units_ser = seed_units.copy()
    include_flags = dict(seed_include)
    last_price_ser = prices.ffill().iloc[-1].reindex(units_ser.index)
    return units_ser, last_price_ser, prices, include_flags

def edit_tilts_dialog(seed_df):
    return seed_df.copy()

# -------------------------------
# Combined dialog (one window)
# -------------------------------
def edit_holdings_and_tilts_dialog(prices, exclude, seed_units, seed_include, seed_tilts,
                                   title="Edit Holdings & Factor Tilts"):
    """
    Returns: (units_series, last_price_series, prices_df, include_flags_dict, tilts_df)
    """
    tickers_all = [t for t in prices.columns]
    exclude = set(exclude or [])
    last_px = prices.ffill().iloc[-1]

    # factor list (use global TILT_FACTORS if set, so MOM shows up)
    factors = list(seed_tilts.index) if isinstance(seed_tilts, pd.DataFrame) and not seed_tilts.empty \
              else (list(TILT_FACTORS) if 'TILT_FACTORS' in globals() else ["Mkt-RF","SMB","HML","RMW","CMA","MOM"])
    if not isinstance(seed_tilts, pd.DataFrame) or seed_tilts.empty:
        seed_tilts = pd.DataFrame(
            {"Target": [1.0] + [0.0]*(len(factors)-1),
             "Band":   [0.05]*len(factors),
             "Use?":   [True] + [False]*(len(factors)-1)},
            index=factors
        )

    root = _tk.Tk()
    root.title(title)
    root.geometry("980x640")
    root.minsize(920, 560)

    # === Main layout ===
    frm_main = _ttk.Frame(root, padding=10); frm_main.pack(fill="both", expand=True)

    # Left: holdings
    frm_left = _ttk.LabelFrame(frm_main, text="Holdings", padding=10)
    frm_left.pack(side="left", fill="both", expand=True, padx=(0, 6))
    for i in range(3):
        frm_left.rowconfigure(i, weight=(1 if i == 1 else 0))
    frm_left.columnconfigure(0, weight=1)

    # Header
    header = _ttk.Frame(frm_left); header.grid(row=0, column=0, sticky="ew")
    _ttk.Label(header, text="Inc?", width=5).grid(row=0, column=0, sticky="w")
    _ttk.Label(header, text="Del?", width=5).grid(row=0, column=1, sticky="w")
    _ttk.Label(header, text="Security", width=20).grid(row=0, column=2, sticky="w")
    _ttk.Label(header, text="Units", width=14).grid(row=0, column=3, sticky="w")
    _ttk.Label(header, text="Last Price", width=12).grid(row=0, column=4, sticky="w")

    # Scrollable list
    list_container = _ttk.Frame(frm_left); list_container.grid(row=1, column=0, sticky="nsew", pady=(4, 6))
    list_container.rowconfigure(0, weight=1); list_container.columnconfigure(0, weight=1)
    canvas = _tk.Canvas(list_container, highlightthickness=0)
    scroll_y = _ttk.Scrollbar(list_container, orient="vertical", command=canvas.yview)
    body = _ttk.Frame(canvas)
    body.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
    canvas.create_window((0, 0), window=body, anchor="nw")
    canvas.configure(yscrollcommand=scroll_y.set)
    canvas.grid(row=0, column=0, sticky="nsew"); scroll_y.grid(row=0, column=1, sticky="ns")

    def _on_mousewheel(event):
        if event.delta:
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        elif getattr(event, "num", None) in (4, 5):
            canvas.yview_scroll(-1 if event.num == 4 else 1, "units")
    body.bind("<Enter>", lambda e: canvas.bind_all("<MouseWheel>", _on_mousewheel))
    body.bind("<Leave>", lambda e: canvas.unbind_all("<MouseWheel>"))
    canvas.bind_all("<Button-4>", _on_mousewheel); canvas.bind_all("<Button-5>", _on_mousewheel)

    row_vars = {}
    def _add_row(ticker, units_default=0.0, include_default=True, disabled=False):
        r = len(row_vars) + 1
        v_inc = _tk.BooleanVar(value=(False if disabled else bool(include_default)))
        v_del = _tk.BooleanVar(value=False)
        v_units = _tk.StringVar(value=("0" if disabled else str(float(units_default))))
        chk_inc = _ttk.Checkbutton(body, variable=v_inc)
        chk_del = _ttk.Checkbutton(body, variable=v_del)
        ent = _ttk.Entry(body, textvariable=v_units, width=16)
        lbl_t = _ttk.Label(body, text=str(ticker), width=20)
        last_px_str = f"{float(last_px.get(ticker, float('nan'))):.4f}"
        lbl_px = _ttk.Label(body, text=last_px_str, width=12)
        if disabled:
            chk_inc.state(["disabled"]); ent.state(["disabled"]); lbl_t.configure(foreground="#888")
        chk_inc.grid(row=r, column=0, sticky="w", padx=(0, 6), pady=2)
        chk_del.grid(row=r, column=1, sticky="w", padx=(0, 6), pady=2)
        lbl_t.grid(row=r, column=2, sticky="w", padx=(0, 6), pady=2)
        ent.grid(row=r, column=3, sticky="w", padx=(0, 6), pady=2)
        lbl_px.grid(row=r, column=4, sticky="w", padx=(0, 6), pady=2)
        row_vars[ticker] = {"inc": v_inc, "del": v_del, "units": v_units, "disabled": disabled, "lbl_px": lbl_px}

    # Prefill rows
    for t in tickers_all:
        disabled = (t in exclude)
        inc_default = bool(pd.Series(seed_include).get(t, True)) and not disabled
        units_default = float(pd.Series(seed_units).get(t, 0.0))
        _add_row(t, units_default=units_default, include_default=inc_default, disabled=disabled)

    # Add-holding box
    add_box = _ttk.LabelFrame(frm_left, text="Add holding", padding=10)
    add_box.grid(row=2, column=0, sticky="ew")
    _ttk.Label(add_box, text="Ticker").grid(row=0, column=0, sticky="w")
    ent_new_ticker = _ttk.Entry(add_box, width=18); ent_new_ticker.grid(row=0, column=1, sticky="w", padx=(4, 12))
    _ttk.Label(add_box, text="Units").grid(row=0, column=2, sticky="w")
    ent_new_units = _ttk.Entry(add_box, width=14); ent_new_units.grid(row=0, column=3, sticky="w", padx=(4, 12))
    _btn_add = _ttk.Button(add_box, text="Add"); _btn_add.grid(row=0, column=4, sticky="w")
    added_tickers = []
    def _do_add():
        t = ent_new_ticker.get().strip()
        if not t:
            _mb.showwarning("Add holding", "Please enter a ticker."); return
        t = t.upper()
        if t in row_vars:
            _mb.showinfo("Add holding", f"{t} already listed."); return
        try:
            u = float(ent_new_units.get().strip()) if ent_new_units.get().strip() else 0.0
        except ValueError:
            _mb.showwarning("Add holding", "Units must be numeric."); return
        _add_row(t, units_default=u, include_default=True, disabled=(t in exclude))
        added_tickers.append(t)
        ent_new_ticker.delete(0, _tk.END); ent_new_units.delete(0, _tk.END)
    _btn_add.configure(command=_do_add)

    # Right panel — Factor Tilts
    frm_right = _ttk.LabelFrame(frm_main, text="Factor Tilts", padding=10)
    frm_right.pack(side="right", fill="y", padx=(6, 0))
    _ttk.Label(frm_right, text="Use?",    width=5 ).grid(row=0, column=0, sticky="w")
    _ttk.Label(frm_right, text="Factor",  width=12).grid(row=0, column=1, sticky="w")
    _ttk.Label(frm_right, text="Target β",width=10).grid(row=0, column=2, sticky="w")
    _ttk.Label(frm_right, text="Band",    width=10).grid(row=0, column=3, sticky="w")

    tilt_vars = {}
    for i, f in enumerate(factors, start=1):
        use_default  = bool(seed_tilts.loc[f, "Use?"])   if f in seed_tilts.index else False
        tgt_default  = float(seed_tilts.loc[f, "Target"]) if f in seed_tilts.index else 0.0
        band_default = float(seed_tilts.loc[f, "Band"])   if f in seed_tilts.index else 0.05
        v_use = _tk.BooleanVar(value=use_default)
        v_tgt = _tk.StringVar(value=f"{tgt_default:.3f}")
        v_bnd = _tk.StringVar(value=f"{band_default:.3f}")
        _ttk.Checkbutton(frm_right, variable=v_use).grid(row=i, column=0, sticky="w", pady=2)
        _ttk.Label(frm_right, text=f, width=12).grid(row=i, column=1, sticky="w", pady=2)
        _ttk.Entry(frm_right, textvariable=v_tgt, width=10).grid(row=i, column=2, sticky="w", pady=2)
        _ttk.Entry(frm_right, textvariable=v_bnd, width=10).grid(row=i, column=3, sticky="w", pady=2)
        tilt_vars[f] = (v_use, v_tgt, v_bnd)

    # after fac_cols/f_mean_ann are available (you have them earlier), pass them in or recompute locally.
    def _compute_recommended_tilts():
        try:
            ff = get_ff5_mom_daily().tail(FF5_LOOKBACK_DAYS)
            fac_cols = [c for c in ff.columns if c != "RF"]
            Fcov_daily = ff[fac_cols].cov()
            f_mean_ann = ff[fac_cols].mean() * 252.0
            # use the betas you already computed for the current prices window
            reco, _wtilt = recommend_factor_tilts_achievable(B, f_mean_ann, Fcov_daily)
            # ensure we return values for all factors in the dialog order
            return reco.reindex(list(seed_tilts.index)).fillna(0.0)
        except Exception:
            return pd.Series(0.0, index=list(seed_tilts.index))
    
    def _apply_recommended_tilts():
        rec = _compute_recommended_tilts()
        for f in factors:
            v_use, v_tgt, v_bnd = tilt_vars[f]
            v_use.set(True)
            v_tgt.set(f"{float(rec.get(f,0.0)):.3f}")
            # keep user band or set to a gentle default:
            if not v_bnd.get():
                v_bnd.set("0.200")
        _mb.showinfo("Tilts", "Recommended tilts applied.\n(You can still edit before Save.)")
    
    btn_reco = _ttk.Button(frm_right, text="Auto-recommend tilts", command=_apply_recommended_tilts)
    btn_reco.grid(row=len(factors)+2, column=0, columnspan=4, sticky="ew", pady=(12,0))

    # Buttons
    def _reset_to_seed_units():
        su = pd.Series(seed_units).astype(float)  # seed_units is already a parameter to this function
        for t, vs in row_vars.items():
            if vs.get("disabled"):
                continue
            vs["units"].set(str(int(round(su.get(t, 0.0)))))
        _mb.showinfo("Holdings", "Units reset to the values loaded from Excel at the start of this run.")

    
    frm_btns = _ttk.Frame(root, padding=(10, 0, 10, 10)); frm_btns.pack(fill="x")
    _ttk.Button(frm_btns, text="Reset to Seed", command=_reset_to_seed_units).pack(side="left", padx=6)
    _ttk.Button(frm_btns, text="Cancel", command=root.destroy).pack(side="right", padx=6)

    def _on_save():
        nonlocal prices
        if added_tickers:
            prices = _fetch_prices_for_new_tickers(added_tickers, prices)

        to_delete = []
        units_out, include_flags = {}, {}
        for t, vs in row_vars.items():
            mark_delete = bool(vs["del"].get())
            disabled = vs["disabled"]
            inc = bool(vs["inc"].get()) and not disabled and not mark_delete
            include_flags[t] = inc
            if mark_delete:
                to_delete.append(t)
                continue
            if not disabled:
                txt = vs["units"].get().strip()
                try:
                    val = float(txt) if txt else 0.0
                except ValueError:
                    val = 0.0
                units_out[t] = val
            # refresh last px label
            if t in prices.columns:
                try:
                    lp = float(prices.ffill().iloc[-1].get(t, float('nan')))
                    vs["lbl_px"].configure(text=f"{lp:.4f}")
                except Exception:
                    pass

        # remove deleted names from the price panel so they don’t enter μ/Σ
        if to_delete:
            keep = [c for c in prices.columns if c not in set(to_delete)]
            prices = prices.reindex(columns=keep)

        units_ser = pd.Series(units_out, dtype=float)
        last_price_ser = prices.ffill().iloc[-1].reindex(units_ser.index)

        out_rows = []
        for f, (v_use, v_tgt, v_bnd) in tilt_vars.items():
            try:  tgt = float(v_tgt.get())
            except ValueError: tgt = 0.0
            try:  bnd = float(v_bnd.get())
            except ValueError: bnd = 0.05
            out_rows.append({"Factor": f, "Target": tgt, "Band": bnd, "Use?": bool(v_use.get())})
        tilts_df = pd.DataFrame(out_rows).set_index("Factor").reindex(factors)

        edit_holdings_and_tilts_dialog.result = (units_ser, last_price_ser, prices, include_flags, tilts_df)
        root.destroy()

    _ttk.Button(frm_btns, text="Save", command=_on_save).pack(side="right", padx=6)
    root.protocol("WM_DELETE_WINDOW", root.destroy)
    root.mainloop()
    return getattr(edit_holdings_and_tilts_dialog, "result", None)
### BLOCK 5 Creating the Covariance Matrix and the Rest of the OPT 
# === Analytics helpers (moved from Block 4) ===================================
def holdings_portfolio_returns(prices: pd.DataFrame, units: pd.Series) -> pd.Series:
    """
    Build a value-weighted portfolio from 'prices' and 'units', and return daily pct-change.
    - prices: wide DataFrame of Close in AUD (or AUD-converted) with Date index.
    - units:  Series of current units held per ticker (can include zeros/missing).
    Returns a daily return Series aligned to prices' index.
    """
    units = pd.Series(units).reindex(prices.columns).fillna(0.0)
    if units.abs().sum() == 0:
        return pd.Series(dtype=float)
    px = prices.reindex(columns=units.index).ffill()
    port_val = (px * units.values).sum(axis=1)
    ret = port_val.pct_change(fill_method=None)
    return ret.dropna()

def current_holdings_weights(units: pd.Series,
                             last_prices: pd.Series,
                             investable: list[str],
                             fx_to_aud: pd.Series | float | None = None) -> pd.Series:
    """
    Compute FX-aware weights of current holdings over the investable set (long-only, renormalised).
    - units:         Series of unit counts indexed by Security
    - last_prices:   Series of last Close (native currency)
    - investable:    list of tickers included in optimisation (order matters)
    - fx_to_aud:     Series of FX-to-AUD per Security (or scalar 1.0); if None, assume 1.0
    Returns a Series of weights indexed by investable tickers (sums to 1 if MV>0).
    """
    # FX handling
    if isinstance(fx_to_aud, pd.Series):
        fx = fx_to_aud.reindex(units.index).fillna(1.0)
    else:
        fx = 1.0

    mv = (pd.Series(units, dtype=float) * pd.Series(last_prices, dtype=float) * fx)
    mv = mv.reindex(investable).fillna(0.0)
    den = mv.sum()
    return (mv / den) if den > 0 else mv

# ------------------------------------------------------------
# 3) COVARIANCE MATRIX (daily)
# ------------------------------------------------------------
df_cov_wide = (
    df_melt[['Date','Security','Return']]
    .pivot(index='Date', columns='Security', values='Return')
)

Sigma_daily = df_cov_wide.cov()  # DAILY cov (AUD returns)

# (Optional) sanity that Sigma came from AUD-converted prices
Sigma_from_aud = (
    pd.melt(prices_aud_for_returns.reset_index(), id_vars="Date",
            var_name="Security", value_name="Close")
      .sort_values(["Security","Date"])
      .assign(Return=lambda d: d.groupby("Security")["Close"].pct_change(fill_method=None))
      .pivot(index="Date", columns="Security", values="Return")
      .cov()
).reindex(index=Sigma_daily.index, columns=Sigma_daily.columns)
max_abs_diff = (Sigma_daily - Sigma_from_aud).abs().to_numpy().max()
using_fx = np.allclose(Sigma_daily.to_numpy(), Sigma_from_aud.to_numpy(), rtol=0, atol=1e-12)
print(f"Using FX-adjusted returns for Sigma?: {using_fx} (max |diff|={max_abs_diff:.2e})")

# ------------------------------------------------------------
# 4) GEOMETRIC (LOG-BASED) EXPECTED RETURNS (annual) — sample μ
# ------------------------------------------------------------
df_melt['LogRet'] = np.log1p(df_melt['Return'])
mu_log_ann = df_melt.groupby('Security')['LogRet'].mean() * 252.0
mu_ann_geo = np.expm1(mu_log_ann)  # ANNUAL (geom)

# Align
securities_all = list(Sigma_daily.columns)
Sigma_daily = Sigma_daily.loc[securities_all, securities_all]
mu_vec_all = mu_ann_geo.reindex(securities_all)

valid_all = [s for s in securities_all
             if pd.notna(mu_vec_all.get(s, np.nan)) and pd.notna(Sigma_daily.loc[s, s])]
Sigma_daily = Sigma_daily.loc[valid_all, valid_all]
mu_vec_all = mu_vec_all.reindex(valid_all)

# ------------------------------------------------------------
# 5) Build FF5 betas ONCE (so tilts are always available)
# ------------------------------------------------------------
# --- Configurable lookback (shared) ---
FF5_LOOKBACK_DAYS = globals().get("FF5_LOOKBACK_DAYS", 252*2)  # ~2 years

def compute_ff5_betas(df_cov_wide, ff, min_obs=200):
    fac = [c for c in ff.columns if c != "RF"]    # <— key change (auto includes MOM)
    B_rows, alpha, resid = [], {}, {}
    for t in df_cov_wide.columns:
        r = df_cov_wide[t].dropna()
        idx = r.index.intersection(ff.index)
        if len(idx) < min_obs:
            continue
        y = (r.loc[idx] - ff.loc[idx, "RF"]).astype(float)
        X = sm.add_constant(ff.loc[idx, fac].astype(float))
        res = sm.OLS(y, X, missing="drop").fit()
        alpha[t] = float(res.params.get("const", 0.0))
        B_rows.append((t, res.params.reindex(fac).fillna(0.0).values))
        resid[t] = float(res.resid.var(ddof=1))
    if not B_rows:
        return None, None, None
    B = pd.DataFrame([b for _, b in B_rows], index=[t for t,_ in B_rows], columns=fac)
    return B, pd.Series(alpha), pd.Series(resid)


ff5 = get_ff5_mom_daily()                     # DAILY, decimals
ff5_win = ff5.tail(FF5_LOOKBACK_DAYS)     # use same window everywhere
B, alpha_daily, resid_var = compute_ff5_betas(df_cov_wide, ff5_win, min_obs=120)

def recommend_factor_tilts_achievable(B: pd.DataFrame,
                                      f_mean_ann: pd.Series,
                                      Fcov_daily: pd.DataFrame,
                                      normalise_to_mkt: bool = True,
                                      lam_w: float = 0.0,
                                      w0: pd.Series | None = None) -> tuple[pd.Series, pd.Series]:
    """
    Project the unconstrained factor target t* = Σ_f^{-1} μ_f onto the long-only simplex:
        min_w  0.5 || B^T w - t* ||^2  + 0.5 * lam_w * ||w||^2
        s.t.   w >= 0, 1'w = 1
    Returns (tilt_reco_achievable, w_tilt).
    """
    fac = [c for c in B.columns]          # factor order
    mu = f_mean_ann.reindex(fac).astype(float).values
    Sig = Fcov_daily.loc[fac, fac].astype(float).values

    # unconstrained "ideal" target in factor space
    t_star = pinv(Sig) @ mu
    if normalise_to_mkt and "Mkt-RF" in fac:
        m_idx = fac.index("Mkt-RF")
        if abs(t_star[m_idx]) > 1e-12:
            t_star = t_star / t_star[m_idx]

    # optimise over portfolio weights
    tick = list(B.index)
    n = len(tick)
    Bt = B[fac].T.values  # shape (F, n)

    # start from current holdings weights if provided, else uniform
    if isinstance(w0, pd.Series):
        w0v = w0.reindex(tick).fillna(0.0).values
        s = w0v.sum(); w0v = (w0v / s) if s > 0 else np.full(n, 1.0/n)
    else:
        w0v = np.full(n, 1.0/n)

    def obj(w):
        diff = Bt @ w - t_star
        return 0.5 * (diff @ diff) + 0.5 * lam_w * (w @ w)

    def grad(w):
        diff = Bt @ w - t_star
        return Bt.T @ diff + lam_w * w

    cons = (
        {'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0, 'jac': lambda w: np.ones_like(w)},
    )
    bounds = [(0.0, 1.0)] * n

    res = minimize(obj, w0v, method='SLSQP', jac=grad, bounds=bounds, constraints=cons,
                   options={'maxiter': 1000, 'ftol': 1e-12, 'disp': False})
    w_hat = (res.x if res.success else w0v)
    w_hat = np.clip(w_hat, 0, 1); w_hat = w_hat / max(1e-12, w_hat.sum())

    t_ach = (Bt @ w_hat)
    tilt_reco = pd.Series(t_ach, index=fac, name="Achievable β")
    w_tilt = pd.Series(w_hat, index=tick, name="w_tilt")
    return tilt_reco, w_tilt

def compute_factor_feasible_ranges(B: pd.DataFrame,
                                   include_flags: dict[str, bool] | None = None,
                                   factor_order: list[str] | None = None) -> pd.DataFrame:
    """
    For each factor f in B.columns, solve:
        min/max   B[:, f]^T w
        s.t.      w >= 0, 1'w = 1, and (optionally) w_i = 0 if include_flags[ticker] is False.
    Returns a DataFrame with columns: ['Min β','Max β'] indexed by factor.
    """
    try:
        from scipy.optimize import linprog
    except Exception as e:
        # Graceful fallback if SciPy isn't available
        facs = list(B.columns) if factor_order is None else list(factor_order)
        return pd.DataFrame({"Min β": np.nan, "Max β": np.nan}, index=facs)

    tickers = list(B.index)
    n = len(tickers)
    if factor_order is None:
        factor_order = list(B.columns)
    factor_order = [f for f in factor_order if f in B.columns]

    # equality: sum w = 1
    A_eq = np.ones((1, n))
    b_eq = np.array([1.0])

    # bounds: 0 <= w_i <= 1, and optionally force 0 for excluded tickers
    if include_flags:
        bounds = [(0.0, 1.0 if bool(include_flags.get(t, True)) else 0.0) for t in tickers]
    else:
        bounds = [(0.0, 1.0)] * n

    mins, maxs = [], []
    for f in factor_order:
        c = B[f].astype(float).values  # objective coefficients

        # minimise c^T w
        res_min = linprog(c, A_eq=A_eq, b_eq=b_eq, bounds=bounds, method="highs")
        beta_min = float(res_min.fun) if res_min.success else np.nan

        # maximise c^T w  <=> minimise (-c)^T w
        res_max = linprog(-c, A_eq=A_eq, b_eq=b_eq, bounds=bounds, method="highs")
        beta_max = (-float(res_max.fun)) if res_max.success else np.nan

        mins.append(beta_min); maxs.append(beta_max)

    out = pd.DataFrame({"Min β": mins, "Max β": maxs}, index=factor_order)
    return out


# ------------------------------------------------------------
# 6) Choose μ and Σ source for the optimiser
#       - Sample: Sigma_daily (DAILY), mu_ann_geo (ANNUAL)
#       - FF5:    Sigma_ff (DAILY),   mu_ff_ann (ANNUAL)
# ------------------------------------------------------------
USE_FF5 = True  # flip to False to use sample moments

if USE_FF5 and (B is not None) and not B.empty:
    fac_cols = [c for c in ff5_win.columns if c != "RF"]  # auto-includes MOM if present
    Fcov_daily = ff5_win[fac_cols].cov()
    S_diag = resid_var.reindex(B.index).clip(lower=0.0).fillna(0.0)

    Sigma_ff_daily = B @ Fcov_daily @ B.T + np.diag(S_diag)
    Sigma_ff_daily = pd.DataFrame(Sigma_ff_daily, index=B.index, columns=B.index)

    f_mean_ann = ff5_win[fac_cols].mean() * 252.0        # ANNUAL premia
    
    # === RECOMMENDED FACTOR TILTS ================================================
    # Compute an achievable recommendation based on your current investable set
    tilt_reco_achievable, w_tilt = recommend_factor_tilts_achievable(B, f_mean_ann, Fcov_daily,
                                                                 normalise_to_mkt=True,
                                                                 lam_w=0.0,
                                                                 w0=None)  # or pass current weights if you prefer
    
    def recommend_factor_tilts(f_mean_ann, Fcov_daily, normalise=True):
        """
        Recommend optimal factor tilts given estimated factor premia and covariance.
        Returns a Series of recommended beta targets for each factor.
        """
        fac = f_mean_ann.index
        mu = f_mean_ann.values
        Sigma = Fcov_daily.loc[fac, fac].values
    
        # Mean–variance optimal factor exposure vector: Σ⁻¹ μ
        Sigma_inv = np.linalg.pinv(Sigma)
        t_opt = Sigma_inv @ mu
    
        # Normalise so that Market β = 1 (interpretable scaling)
        if normalise and "Mkt-RF" in fac:
            t_opt = t_opt / t_opt[list(fac).index("Mkt-RF")]
    
        return pd.Series(t_opt, index=fac, name="Recommended β")
    
    # Compute and display recommended tilts
    tilt_reco = recommend_factor_tilts(f_mean_ann, Fcov_daily)
    print("\nRecommended factor tilts (based on current factor premia):")
    print(tilt_reco.round(3))
    # ==============================================================================
    
    alpha_ann  = alpha_daily * 252.0                     # ANNUAL alpha
    mu_ff_ann  = alpha_ann.reindex(B.index).fillna(0.0) + (B @ f_mean_ann).rename(None) + rf_annual
  
    securities_opt = [t for t in Sigma_ff_daily.index if t not in EXCLUDE_FROM_OPT]
    Sigma_opt = Sigma_ff_daily.loc[securities_opt, securities_opt]   # DAILY
    mu_vec_opt = mu_ff_ann.reindex(securities_opt)                   # ANNUAL
    exp_ret_label = "Expected Return (annual, FF5)"
else:
    
    securities_opt = [s for s in valid_all if s not in EXCLUDE_FROM_OPT]
    Sigma_opt = Sigma_daily.loc[securities_opt, securities_opt]      # DAILY
    mu_vec_opt = mu_vec_all.reindex(securities_opt)                  # ANNUAL
    exp_ret_label = "Expected Return (ann., geom)"

# Display tables (once μ/Σ are final)
n_opt = len(securities_opt)
cov_plus = pd.DataFrame(0.0, index=securities_opt + ['w'], columns=securities_opt + ['w'])
cov_plus.iloc[:n_opt, :n_opt] = Sigma_opt.values
exp_ret_df = mu_vec_opt.rename(exp_ret_label).to_frame()

# ------------------------------------------------------------
# 8) OPTIMISATION UTILITIES (unconstrained + tilt-constrained)
# ------------------------------------------------------------
def optimise_long_only(mu, Sigma, target_return):
    try:
        from scipy.optimize import minimize
        n = len(mu)
        mu = np.asarray(mu, dtype=float)
        Sigma = np.asarray(Sigma, dtype=float)
        if not (mu.min() - 1e-12 <= target_return <= mu.max() + 1e-12):
            return np.full(n, np.nan), False, "Target outside long-only feasible range."
        def obj(w): return float(w @ Sigma @ w)
        cons = (
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0},
            {'type': 'eq', 'fun': lambda w: float(mu @ w) - float(target_return)},
        )
        bounds = [(0.0, 1.0)] * n
        w0 = np.full(n, 1.0/n)
        res = minimize(obj, w0, method='SLSQP', bounds=bounds, constraints=cons,
                       options={'maxiter': 1000, 'ftol': 1e-12, 'disp': False})
        if res.success and np.isfinite(res.fun):
            return res.x, True, "SLSQP success."
        return np.full(n, np.nan), False, "SLSQP failed."
    except Exception as e:
        return np.full(len(mu), np.nan), False, f"SLSQP unavailable or error: {e}"

def optimise_unconstrained_analytic(mu, Sigma, target_return):
    mu = np.asarray(mu, dtype=float)
    Sigma = np.asarray(Sigma, dtype=float)
    n = len(mu); ones = np.ones(n)
    Sigma_inv = np.linalg.pinv(Sigma)
    A = ones @ Sigma_inv @ ones
    Bv = ones @ Sigma_inv @ mu
    C = mu @ Sigma_inv @ mu
    M = np.array([[A, Bv], [Bv, C]]); rhs = np.array([1.0, float(target_return)])
    try:
        alpha, beta = np.linalg.solve(M, rhs)
        w = Sigma_inv @ (alpha * ones + beta * mu)
        return w, "Analytic solution."
    except np.linalg.LinAlgError:
        return np.full(n, np.nan), "Analytic solver failed (singular)."

def optimise_long_only_with_tilts(mu, Sigma, target_return, B, tilt_targets, tilt_bands, use_mask):
    from scipy.optimize import minimize
    mu = np.asarray(mu, dtype=float)
    Sigma = np.asarray(Sigma, dtype=float)
    n = len(mu)
    def obj(w): return float(w @ Sigma @ w)
    cons = [
        {'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0},
        {'type': 'eq', 'fun': lambda w: float(mu @ w) - float(target_return)},
    ]
    for f in ["Mkt-RF","SMB","HML","RMW","CMA"]:
        if not use_mask.get(f, True): 
            continue
        t = float(tilt_targets.get(f, 0.0))
        b = float(tilt_bands.get(f, 0.05))
        v = B[f].values
        cons.append({'type':'ineq', 'fun': (lambda v=v, t=t, b=b: lambda w: (t + b) - float(v @ w))()})
        cons.append({'type':'ineq', 'fun': (lambda v=v, t=t, b=b: lambda w:  float(v @ w) - (t - b))()})
    bounds = [(0.0, 1.0)] * n
    w0 = np.full(n, 1.0/n)
    res = minimize(obj, w0, method='SLSQP', bounds=bounds, constraints=cons,
                   options={'maxiter': 1000, 'ftol': 1e-12, 'disp': False})
    if res.success and np.isfinite(res.fun):
        return res.x, "tilt SLSQP success"
    return np.full(n, np.nan), f"tilt SLSQP failed ({getattr(res,'message','unknown')})"

# ------------------------------------------------------------
# 8) FRONTIERS: unconstrained and tilt-constrained
# ------------------------------------------------------------
target_returns = [0.01, 0.02, 0.03, 0.05, 0.075, 0.10, 0.125, 0.15, 0.175, 0.20, 0.225, 0.25, 0.275, 0.30, 0.325, 0.35, 0.375, 0.40]

# Unconstrained (your current behaviour)
weights_dict, stats_rows = {}, []
for R in target_returns:
    w, ok, note = optimise_long_only(mu_vec_opt.values, Sigma_opt.values, R)
    weights_dict[R] = w
    vol_ann = (np.sqrt(w @ Sigma_opt.values @ w) * np.sqrt(252.0)) if np.all(np.isfinite(w)) else np.nan
    achieved = float(mu_vec_opt.values @ w) if np.all(np.isfinite(w)) else np.nan
    sharpe = (achieved - rf_annual) / vol_ann if (pd.notna(vol_ann) and vol_ann > 0) else np.nan
    stats_rows.append({"Target Return": R, "Achieved Return": achieved, "Volatility (ann.)": vol_ann,
                       "Sharpe": sharpe, "Method": "Long-only SLSQP", "Note": note})
weights_cols = [f"{int(r*1000)/10:.1f}%" if (r*100)%1!=0 else f"{int(r*100):d}%" for r in target_returns]
W = pd.DataFrame({col: weights_dict[R] for col, R in zip(weights_cols, target_returns)}, index=securities_opt)
stats_df = pd.DataFrame(stats_rows)
stats_df.insert(0, "Target (%)", [f"{int(r*1000)/10:.1f}%" if (r*100)%1!=0 else f"{int(r*100):d}%"
                                   for r in target_returns])
stats_df = stats_df.drop(columns=["Target Return"])
# Robust tangency selection (handles all-NaN Sharpe)
sh = pd.to_numeric(stats_df['Sharpe'], errors='coerce')
if sh.notna().any():
    best_idx = int(sh.idxmax())
else:
    vol_series = pd.to_numeric(stats_df['Volatility (ann.)'], errors='coerce')
    best_idx = int(vol_series.idxmin()) if vol_series.notna().any() else 0

tan_ret = float(pd.to_numeric(stats_df.loc[best_idx, 'Achieved Return'], errors='coerce'))
tan_vol = float(pd.to_numeric(stats_df.loc[best_idx, 'Volatility (ann.)'], errors='coerce'))
if not np.isfinite(tan_ret) or not np.isfinite(tan_vol):
    tan_ret, tan_vol = float('nan'), float('nan')

cal_df = pd.DataFrame({"Volatility (ann.)": [0.0, tan_vol], "Return": [rf_annual, tan_ret]})


# ------------------------------------------------------------
# 9) PREPARE A Trade Plan
# ------------------------------------------------------------
cov_plus = cov_plus.fillna(0.0)

# investable assets only (matches weights grid)
exp_ret_df = mu_vec_opt.rename(exp_ret_label).to_frame()
# or, if you prefer to show all (incl. ^AORD), use mu_vec_all instead.

def make_trade_plan(units_cur, last_px, fx_map, w_target, include_flags, include_zero_lines=False):
    """Return (trade_df, residual_cash) to move from current units to target weights (AUD).
       If include_zero_lines=True, keep rows with Δ Units = 0 in the output table."""
    tickers = list(w_target.index)
    inc = pd.Series(include_flags).reindex(tickers).fillna(True).astype(bool)
    tickers = [t for t in tickers if inc.get(t, True)]

    lp_aud = (pd.Series(last_px).reindex(tickers).astype(float) *
              pd.Series(fx_map).reindex(tickers).fillna(1.0).astype(float))
    cur_units = pd.Series(units_cur).reindex(tickers).fillna(0.0).astype(float)
    cur_val = (cur_units * lp_aud).sum()
    cur_val = float(cur_val)

    if cur_val <= 0:
        out = pd.DataFrame(columns=["Security","Curr Units","Target Units","Δ Units","Last Px (AUD)","Cash Flow (AUD)"])
        return out, 0.0

    tgt_val = pd.Series(w_target).reindex(tickers).fillna(0.0) * cur_val
    tgt_units_float = (tgt_val / lp_aud).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    tgt_units_int = tgt_units_float.round().astype(int)

    delta_units = (tgt_units_int - cur_units).round().astype(int)
    cash_impact = (-delta_units * lp_aud).astype(float)
    residual = float((tgt_val - tgt_units_int * lp_aud).sum())

    out = pd.DataFrame({
        "Security": tickers,
        "Curr Units": cur_units.astype(int).values,
        "Target Units": tgt_units_int.values,
        "Δ Units": delta_units.values,
        "Last Px (AUD)": lp_aud.values,
        "Cash Flow (AUD)": cash_impact.values
    })
    if not include_zero_lines:
        out = out.loc[out["Δ Units"] != 0]
    return out.reset_index(drop=True), residual


def compute_target_units_for_holdings(units_cur, last_px, fx_map, w_target, include_flags):
    tickers = list(pd.Index(w_target.index))
    inc = pd.Series(include_flags).reindex(tickers).fillna(True).astype(bool)
    tickers = [t for t in tickers if inc.get(t, True)]

    lp_aud = (pd.Series(last_px).reindex(tickers).astype(float) *
              pd.Series(fx_map).reindex(tickers).fillna(1.0).astype(float))
    cur_units = pd.Series(units_cur).reindex(tickers).fillna(0.0).astype(float)
    cur_val = float((cur_units * lp_aud).sum())
    if cur_val <= 0:
        return pd.Series(0, index=w_target.index, dtype=int)

    tgt_val = pd.Series(w_target).reindex(tickers).fillna(0.0) * cur_val
    tgt_units_float = (tgt_val / lp_aud).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    tgt_units_int = tgt_units_float.round().astype(int)
    return tgt_units_int.reindex(w_target.index).fillna(0).astype(int)

def compute_achieved_tilts(B: pd.DataFrame, w: pd.Series, factors=None, renormalise_missing=True) -> pd.Series:
    if B is None or B.empty:
        return pd.Series(dtype=float)
    w_all = pd.Series(w).reindex(B.index).fillna(0.0)
    if renormalise_missing and w_all.sum() > 0:
        w_use = w_all / w_all.sum()
    else:
        w_use = w_all
    t = (B.T @ w_use).rename("Achieved β")
    if factors is not None:
        t = t.reindex(factors)
    return t

def _build_frontier(mu_vec_opt, Sigma_opt, target_returns=None):
    if target_returns is None:
        target_returns = [0.01, 0.02, 0.03, 0.05, 0.075, 0.10, 0.125, 0.15, 0.175, 0.20, 0.225, 0.25, 0.275, 0.30, 0.325, 0.35, 0.375, 0.40]
    weights_dict, stats_rows = {}, []
    mu = mu_vec_opt.values; S = Sigma_opt.values
    for R in target_returns:
        w, ok, note = optimise_long_only(mu, S, R)
        weights_dict[R] = w
        vol_ann = (np.sqrt(w @ S @ w) * np.sqrt(252.0)) if np.all(np.isfinite(w)) else np.nan
        achieved = float(mu @ w) if np.all(np.isfinite(w)) else np.nan
        sharpe = (achieved - rf_annual) / vol_ann if (pd.notna(vol_ann) and vol_ann > 0) else np.nan
        stats_rows.append({"Target Return": R, "Achieved Return": achieved, "Volatility (ann.)": vol_ann,
                           "Sharpe": sharpe, "Method": "Long-only SLSQP", "Note": note})
    cols = [f"{int(r*1000)/10:.1f}%" if (r*100)%1!=0 else f"{int(r*100):d}%" for r in target_returns]
    W = pd.DataFrame({c: weights_dict[R] for c, R in zip(cols, target_returns)}, index=Sigma_opt.index)
    stats_df = pd.DataFrame(stats_rows)
    stats_df.insert(0, "Target (%)", cols)
    stats_df = stats_df.drop(columns=["Target Return"])
    # Robust tangency selection (handles all-NaN Sharpe)
    sh = pd.to_numeric(stats_df['Sharpe'], errors='coerce')
    if sh.notna().any():
        best_idx = int(sh.idxmax())
    else:
        vol_series = pd.to_numeric(stats_df['Volatility (ann.)'], errors='coerce')
        best_idx = int(vol_series.idxmin()) if vol_series.notna().any() else 0
    
    tan_ret = float(pd.to_numeric(stats_df.loc[best_idx, 'Achieved Return'], errors='coerce'))
    tan_vol = float(pd.to_numeric(stats_df.loc[best_idx, 'Volatility (ann.)'], errors='coerce'))
    if not np.isfinite(tan_ret) or not np.isfinite(tan_vol):
        tan_ret, tan_vol = float('nan'), float('nan')
    return W, stats_df, tan_ret, tan_vol

W, stats_df, tan_ret, tan_vol = _build_frontier(mu_vec_opt, Sigma_opt)

# Load parcels once (if the sheet is missing, you just get an empty table)
lots_df = _read_lots_from_path(filename, "Lots")

def _cost_adjust_stats(W, stats_df, units, last_px, fx_map, include_flags):
    mv_aud = float((pd.Series(units, dtype=float).reindex(W.index).fillna(0.0) *
                   last_px.reindex(W.index).astype(float) *
                   pd.Series(fx_map).reindex(W.index).fillna(1.0)).sum())
    sale_date = pd.Timestamp(prices.index[-1])

    costs = []
    for col in W.columns:
        w = W[col].reindex(W.index).fillna(0.0)
        trade, _resid = make_trade_plan(units, last_px, fx_map, w, include_flags)
        c = evaluate_transaction_costs(trade, lots_df, sale_date, MARGINAL_TAX_RATE)
        # Achieved (model) return already in stats_df, but compute directly to be safe:
        ach = float(mu_vec_opt.reindex(W.index).fillna(0.0).values @ w.values)
        net_ret = ach - (c["total_cost"] / mv_aud if mv_aud > 0 else 0.0)
        costs.append({"Target (%)": col,
                      "Txn Costs (AUD)": c["total_cost"],
                      "Net Achieved Return": net_ret})
    extra = pd.DataFrame(costs)
    out = stats_df.merge(extra, on="Target (%)", how="left")
    out["Net Sharpe"] = (out["Net Achieved Return"] - rf_annual) / out["Volatility (ann.)"]
    return out

    units = pd.Series(0, index=mu_vec_opt.index)
    last_px_hold = prices.ffill().iloc[-1].reindex(units.index)
    include_flags = {t: True for t in units.index}
    
    stats_df = _cost_adjust_stats(W, stats_df, units, last_px_hold, fx_map_all, include_flags)
### BLOCK 6 Transaction costs
def _market_of(ticker: str) -> str:
    t = str(ticker)
    if t.startswith("^"): return "INDEX"
    if t.endswith(".AX"): return "ASX"
    return "US"

def compute_brokerage(trade_df: pd.DataFrame) -> tuple[float, pd.Series]:
    """Return (total_brokerage_AUD, per_row_series)."""
    if trade_df.empty:
        return 0.0, pd.Series(dtype=float)

    fees = []
    asx_buy_candidates = []  # (row_idx, trade_value) eligible for $0

    for i, r in trade_df.iterrows():
        # Skip rows with no actual trade
        units = float(r.get("Δ Units", 0.0))
        if abs(units) < 1e-12:
            fees.append(0.0)
            continue

        mkt = _market_of(r["Security"])
        px  = float(r.get("Last Px (AUD)", 0.0))
        trade_val = abs(units) * px

        if mkt == "US":
            fee = 0.0
        elif mkt == "ASX":
            fee = max(BROKERAGE["ASX"]["min_fee"], BROKERAGE["ASX"]["rate"] * trade_val)
            # Track eligible “first buy ≤ $1k”
            if units > 0 and trade_val <= BROKERAGE["ASX"]["first_buy_free_threshold"] + 1e-9:
                asx_buy_candidates.append((i, trade_val))
        else:
            fee = 0.0

        fees.append(fee)

    fees = pd.Series(fees, index=trade_df.index, name="Brokerage (AUD)")

    # Apply “first ASX buy ≤ $1k is $0” to ONE eligible row
    if asx_buy_candidates:
        # (Leave as smallest-eligible; change to reverse=True to pick the largest-eligible instead.)
        idx0 = sorted(asx_buy_candidates, key=lambda x: x[1])[0][0]
        fees.loc[idx0] = 0.0

    return float(fees.sum()), fees


def _read_lots_from_path(xl_path, sheet="Lots") -> pd.DataFrame:
    """
    Lots sheet schema:
      Security | AcqDate | Units | CostBaseAUD
    """
    try:
        df = pd.read_excel(xl_path, sheet_name=sheet)
    except Exception:
        return pd.DataFrame(columns=["Security","AcqDate","Units","CostBaseAUD"])

    if df.empty: 
        return pd.DataFrame(columns=["Security","AcqDate","Units","CostBaseAUD"])

    df = df.rename(columns={c: c.strip() for c in df.columns})
    if "AcqDate" in df.columns:
        df["AcqDate"] = pd.to_datetime(df["AcqDate"], errors="coerce")
    for col in ["Units","CostBaseAUD"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["Security","AcqDate","Units","CostBaseAUD"])
    df["Security"] = df["Security"].astype(str).str.strip()
    return df

def _is_long_term_au(acq_date: pd.Timestamp, sale_date: pd.Timestamp) -> bool:
    """
    Australian CGT 50% discount rule:
    asset must be held for at least 12 months between acquisition and sale.
    Uses relativedelta to handle leap years correctly.
    """
    if pd.isna(acq_date) or pd.isna(sale_date):
        return False
    return sale_date >= (pd.Timestamp(acq_date) + relativedelta(years=1))

def _allocate_sale_to_lots(lots: pd.DataFrame, sell_units: float, sale_price_aud: float,
                           sale_date: pd.Timestamp, method: str = "HIFO"):
    """
    Consume lot units to satisfy a sale. Returns list of dicts with:
      qty, acq_date, proceed, cost_base, gain, long_term
    """
    if lots.empty or sell_units <= 0:
        return []

    lots = lots.copy()
    if "AcqDate" in lots.columns:
        lots["AcqDate"] = pd.to_datetime(lots["AcqDate"], errors="coerce")

    # Sort by matching method
    if method.upper() == "HIFO":
        lots = lots.sort_values(by=["CostBaseAUD", "AcqDate"], ascending=[False, True])
    else:  # FIFO
        lots = lots.sort_values(by=["AcqDate"], ascending=True)

    out = []
    remaining = float(sell_units)
    for _, L in lots.iterrows():
        if remaining <= 0:
            break
        have = float(L["Units"])
        if have <= 0:
            continue

        qty = min(remaining, have)
        cb_unit = float(L["CostBaseAUD"])
        acq = pd.Timestamp(L["AcqDate"])

        proceed = float(sale_price_aud) * qty
        cost_base = cb_unit * qty
        gain = proceed - cost_base
        long_term = _is_long_term_au(acq, sale_date)

        out.append({
            "qty": qty,
            "acq_date": acq,
            "proceed": proceed,
            "cost_base": cost_base,
            "gain": gain,
            "long_term": bool(long_term),
        })
        remaining -= qty

    return out

def compute_cgt_tax(trade_df: pd.DataFrame, lots_df: pd.DataFrame, sale_date: pd.Timestamp,
                    marginal_rate: float, carry_forward_loss: float = 0.0,
                    method: str = "HIFO") -> tuple[float, dict]:
    """
    Returns (tax_AUD, breakdown_dict) with an 'audit' DataFrame so users can see exactly
    how CGT was computed (per-lot).
    """
    if trade_df.empty:
        return 0.0, {"st_gain":0.0, "lt_gain":0.0, "losses":0.0,
                     "discounted_lt_after_losses":0.0, "taxable":0.0, "audit": pd.DataFrame()}

    lots_df = lots_df.copy()
    if "AcqDate" in lots_df.columns:
        lots_df["AcqDate"] = pd.to_datetime(lots_df["AcqDate"], errors="coerce")

    # group lots by security for fast lookup
    lots_by_sec = {s: g.copy() for s, g in lots_df.groupby("Security")} if not lots_df.empty else {}

    audit_rows = []
    st_gain = 0.0; lt_gain = 0.0; losses = 0.0

    for _, r in trade_df.iterrows():
        dU = int(r.get("Δ Units", 0))
        if dU >= 0:  # only sells trigger CGT
            continue
        sec  = str(r["Security"])
        px_aud = float(r["Last Px (AUD)"])
        sell_qty = abs(dU)

        ledger = _allocate_sale_to_lots(
            lots_by_sec.get(sec, pd.DataFrame(columns=["Security","AcqDate","Units","CostBaseAUD"])),
            sell_qty, px_aud, sale_date, method=method
        )
        sold = 0.0
        for row in ledger:
            sold += row["qty"]
            g = row["gain"]
            audit_rows.append({
                "Security": sec,
                "Qty": row["qty"],
                "AcqDate": row["acq_date"],
                "SaleDate": pd.Timestamp(sale_date),
                "Proceeds": row["proceed"],
                "CostBase": row["cost_base"],
                "Gain": row["gain"],
                "LongTermEligible": bool(row["long_term"])
            })
            if g >= 0:
                if row["long_term"]:
                    lt_gain += g
                else:
                    st_gain += g
            else:
                losses += -g  # store as positive

        # excess sells beyond recorded lots → treat as zero-gain (conservative)
        _unused = max(0.0, sell_qty - sold)

    # Apply losses (including carry-forward) first, optimally vs ST then LT
    rem_losses = float(carry_forward_loss) + float(losses)
    st_off = min(rem_losses, st_gain);  st_gain -= st_off;  rem_losses -= st_off
    lt_off = min(rem_losses, lt_gain);  lt_gain -= lt_off;  rem_losses -= lt_off

    # 50% discount on remaining long-term gains (AU individual rule)
    discounted_lt = 0.5 * max(0.0, lt_gain)
    taxable = max(0.0, st_gain + discounted_lt)

    tax = float(marginal_rate) * float(taxable)
    audit_df = pd.DataFrame(audit_rows)

    bkd = {
        "st_gain": float(st_gain),
        "lt_gain": float(lt_gain),
        "losses": float(losses + carry_forward_loss),
        "discounted_lt_after_losses": float(discounted_lt),
        "taxable": float(taxable),
        "audit": audit_df
    }
    return float(tax), bkd


def evaluate_transaction_costs(trade_df: pd.DataFrame, lots_df: pd.DataFrame,
                               sale_date: pd.Timestamp, marginal_rate: float) -> dict:
    brok_total, brok_series = compute_brokerage(trade_df)
    tax_total, tax_bkd = compute_cgt_tax(trade_df, lots_df, sale_date,
                                         marginal_rate=MARGINAL_TAX_RATE,
                                         carry_forward_loss=CAPITAL_LOSS_CARRY_FWD,
                                         method=LOT_MATCH_METHOD)
    return {"brokerage": brok_total, "cgt_tax": tax_total,
            "total_cost": brok_total + tax_total, "breakdown": tax_bkd,
            "per_row_brokerage": brok_series}

def _update_lots_after_trades(lots_df: pd.DataFrame, trade_df: pd.DataFrame,
                              sale_date: pd.Timestamp, fx_map: pd.Series | dict):
    """
    Apply executed trades to the Lots table:
      - Sells: decrement matched lots using the current LOT_MATCH_METHOD (HIFO/FIFO).
      - Buys:  append a new lot with AcqDate = sale_date and CostBaseAUD = Last Px (AUD).
    Returns a NEW lots DataFrame (original not mutated).
    """
    out = lots_df.copy()
    if "AcqDate" in out.columns:
        out["AcqDate"] = pd.to_datetime(out["AcqDate"], errors="coerce")

    for _, tr in trade_df.iterrows():
        sec = str(tr["Security"])
        dU = int(tr.get("Δ Units", 0))
        px_aud = float(tr.get("Last Px (AUD)", 0.0))

        if dU < 0:
            # Sells: consume existing lots in the same order used for CGT allocation
            lot_block = out[out["Security"] == sec].copy()
            if LOT_MATCH_METHOD.upper() == "HIFO":
                lot_block = lot_block.sort_values(by=["CostBaseAUD","AcqDate"], ascending=[False, True])
            else:
                lot_block = lot_block.sort_values(by=["AcqDate"], ascending=True)

            remaining = abs(dU)
            for i in lot_block.index:
                if remaining <= 0:
                    break
                have = float(out.at[i, "Units"])
                take = min(remaining, have)
                out.at[i, "Units"] = have - take
                remaining -= take

            # remove fully consumed lots
            out = out[out["Units"] > 0.0].copy()

        elif dU > 0:
            # Buys: create a new lot at today's AUD price
            out = pd.concat([out, pd.DataFrame([{
                "Security": sec,
                "AcqDate": pd.Timestamp(sale_date),
                "Units": int(dU),
                "CostBaseAUD": px_aud
            }])], ignore_index=True)

        # dU == 0 → no action for this row

    return out
### Block 7 Writing into the excel (i.e. formatting and building the actual sheet)
# ------------------------------------------------------------
# 10) WRITE TO EXCEL 
# ------------------------------------------------------------

# ---- 10A) Read seeds (no COM; avoids UsedRange issues) ----
seed_units, seed_include = _read_holdings_seed_from_path(filename, "Holdings")
tilt_seed = _read_tilts_seed_from_path(filename, "Tilts")

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

# ---- 10B) Combined dialog (holdings + tilts) ----
res = edit_holdings_and_tilts_dialog(
    prices=prices,
    exclude=EXCLUDE_FROM_OPT,
    seed_units=seed_units,
    seed_include=seed_include,
    seed_tilts=tilt_seed
)
if res is None:
    units = seed_units.copy()
    include_flags = seed_include.copy()
    last_px_hold = prices.ffill().iloc[-1].reindex(units.index)
    tilt_df = tilt_seed.copy()
else:
    units, last_px_hold, prices, include_flags, tilt_df = res

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
    if usd_cols:
        prices_aud.update(prices.loc[:, usd_cols].mul(fx, axis=0))

    d = (prices_aud.reset_index()
         .melt(id_vars="Date", var_name="Security", value_name="Close")
         .sort_values(["Security", "Date"]))
    d["Return"] = d.groupby("Security", sort=False)["Close"].pct_change(fill_method=None)
    d = d.dropna()

    df_cov_wide = d.pivot(index="Date", columns="Security", values="Return").sort_index()
    Sigma_daily = df_cov_wide.cov()

    d["LogRet"] = np.log1p(d["Return"])
    mu_log_ann = d.groupby("Security")["LogRet"].mean() * 252.0
    mu_ann_geo = np.expm1(mu_log_ann)

    return prices_aud, d, df_cov_wide, Sigma_daily, mu_ann_geo

# === Rebuild core analytics ===
prices_aud_for_returns, df_melt, df_cov_wide, Sigma_daily, mu_ann_geo = _rebuild_core_from_prices(prices)

# ---- Factors (FF5 + MOM) & betas ----
FF5_LOOKBACK_DAYS = globals().get("FF5_LOOKBACK_DAYS", 252*2)
ff = get_ff5_mom_daily()
ff_win = ff.tail(FF5_LOOKBACK_DAYS)
fac_cols = [c for c in ff_win.columns if c != "RF"]
B, alpha_daily, resid_var = compute_ff5_betas(df_cov_wide, ff_win, min_obs=120)

# ---- Choose μ and Σ source ----
USE_FF5 = True
if USE_FF5 and (B is not None) and not B.empty:
    Fcov_daily = ff_win[fac_cols].cov()
    S_diag = resid_var.reindex(B.index).clip(lower=0.0).fillna(0.0)
    Sigma_ff_daily = B @ Fcov_daily @ B.T + np.diag(S_diag)
    Sigma_ff_daily = pd.DataFrame(Sigma_ff_daily, index=B.index, columns=B.index)

    f_mean_ann = ff_win[fac_cols].mean() * 252.0
    mu_ff_ann  = (alpha_daily * 252.0).reindex(B.index).fillna(0.0) + (B @ f_mean_ann).rename(None) + rf_annual

    securities_opt = [t for t in Sigma_ff_daily.index if t not in EXCLUDE_FROM_OPT]
    Sigma_opt = Sigma_ff_daily.loc[securities_opt, securities_opt]
    mu_vec_opt = mu_ff_ann.reindex(securities_opt)
    exp_ret_label = "Expected Return (annual, FF5+MOM)"
else:
    securities_all = list(Sigma_daily.columns)
    mu_vec_all = mu_ann_geo.reindex(securities_all)
    valid_all = [s for s in securities_all
                 if pd.notna(mu_vec_all.get(s)) and pd.notna(Sigma_daily.loc[s, s])]
    securities_opt = [s for s in valid_all if s not in EXCLUDE_FROM_OPT]
    Sigma_opt = Sigma_daily.loc[securities_opt, securities_opt]
    mu_vec_opt = mu_vec_all.reindex(securities_opt)
    exp_ret_label = "Expected Return (ann., geom)"

# Tables used later
n_opt = len(securities_opt)
cov_plus = pd.DataFrame(0.0, index=securities_opt + ['w'], columns=securities_opt + ['w'])
cov_plus.iloc[:n_opt, :n_opt] = Sigma_opt.values
exp_ret_df = mu_vec_opt.rename(exp_ret_label).to_frame()

# FX map used by Holdings + trade plan
usd_aud    = get_usd_aud_fx()
fx_map_all = fx_to_aud_for_tickers(prices.columns, usd_aud)

# ---- 10D) Reopen Excel and WRITE everything, then close ----
if USE_XLWINGS:
    try:
        with xw.App(visible=False, add_book=False) as app:
            wb = app.books.open(filename, update_links=False, read_only=False)
            if bool(wb.api.ReadOnly):
                raise RuntimeError("Workbook opened read-only; close it in Excel and try again.")
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

            # Portfolio Statistics
            start_s_row = start_w_row + W.shape[0] + 4
            opt.range(f"A{start_s_row}").value = 'Portfolio Statistics'
            opt.range(f"A{start_s_row+1}").options(pd.DataFrame, index=False, header=True).value = stats_df

            # ================= Efficient Frontier chart updater =================
            def _col_letter(idx0: int) -> str:
                n = idx0 + 1  # A=1
                letters = ""
                while n:
                    n, rem = divmod(n - 1, 26)
                    letters = chr(65 + rem) + letters
                return letters
            
            def _get_chart_by_title(opt_sheet, title_text: str):
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
                raise RuntimeError(f"Chart with title '{title_text}' not found on sheet '{opt_sheet.name}'")
            
            def update_efficient_frontier_chart(
                opt_sheet,
                stats_df: pd.DataFrame,
                start_s_row: int,
                *,
                rf_annual: float,
                tan_ret: float,
                tan_vol: float,
                current_point: tuple | None = None,
                title_text: str = "Efficient Frontier & CAL (rf=4.00%)"
            ):
                
                # ---- Validate columns in stats_df ----
                cols = list(stats_df.columns)
                try:
                    j_ret = cols.index("Achieved Return")
                    j_vol = cols.index("Volatility (ann.)")
                except ValueError:
                    raise RuntimeError("stats_df must have columns 'Achieved Return' and 'Volatility (ann.)'.")
            
                nrows = int(stats_df.shape[0])
                if nrows <= 0:
                    raise RuntimeError("stats_df has no rows; nothing to plot.")
            
                # ---- Build Excel range references for X (Vol) and Y (Return) from the table you wrote ----
                header_row = start_s_row + 1      # header row in Excel where you wrote stats_df header
                first_row  = header_row + 1       # first data row
                col_vol = _col_letter(j_vol)      # zero-based -> Excel column letters relative to column A
                col_ret = _col_letter(j_ret)
                x_rng = opt_sheet.range(f"{col_vol}{first_row}:{col_vol}{first_row + nrows - 1}").api
                y_rng = opt_sheet.range(f"{col_ret}{first_row}:{col_ret}{first_row + nrows - 1}").api
            
                # ---- Grab the chart ----
                rf = rf_annual
                ch = _get_chart_by_title(opt_sheet, title_text)
            
                # ---- Clear existing series (keep object & styling) ----
                try:
                    while ch.SeriesCollection().Count > 0:
                        ch.SeriesCollection(1).Delete()
                except Exception:
                    pass
            
                # Excel ChartType constants (avoid win32com constants; use literals)
                XL_XY_SCATTER              = -4169  # points only
                XL_XY_SCATTER_LINES        = 74     # scatter with lines (markers on)
                XL_MARKERSTYLE_NONE        = -4142
                XL_MARKERSTYLE_CIRCLE      = 8
                XL_MARKERSTYLE_PLUS        = 2
                XL_AXIS_CATEGORY           = 1      # for XY charts Excel still treats X as a value axis, but this works for formatting
                XL_AXIS_VALUE              = 2
            
                # ---- Efficient Frontier (smooth line, no markers) ----
                s_front = ch.SeriesCollection().NewSeries()
                s_front.Name = '="Efficient Frontier"'
                s_front.XValues = x_rng
                s_front.Values  = y_rng
                s_front.ChartType = XL_XY_SCATTER_LINES
                try:
                    s_front.MarkerStyle = XL_MARKERSTYLE_NONE
                    s_front.Smooth = True
                    # optional line weight for visibility
                    s_front.Format.Line.Weight = 1.5
                except Exception:
                    pass
            
                # ---- CAL (smooth line from (0, rf) to (tan_vol, tan_ret)) ----
                if np.all(np.isfinite([rf_annual, tan_ret, tan_vol])):
                    s_cal = ch.SeriesCollection().NewSeries()
                    s_cal.Name = '="CAL"'
                    s_cal.XValues = (0.0, float(tan_vol))
                    s_cal.Values  = (float(rf_annual), float(tan_ret))
                    s_cal.ChartType = XL_XY_SCATTER_LINES
                    try:
                        s_cal.MarkerStyle = XL_MARKERSTYLE_NONE
                        s_cal.Smooth = True
                        s_cal.Format.Line.Weight = 1.25
                    except Exception:
                        pass
            
                # ---- MVP (single point at min volatility) ----
                try:
                    vol_series = pd.to_numeric(stats_df["Volatility (ann.)"], errors="coerce")
                    ret_series = pd.to_numeric(stats_df["Achieved Return"], errors="coerce")
                    mask = vol_series.notna() & ret_series.notna()
                    if mask.any():
                        idx_mvp = vol_series[mask].idxmin()
                        mvp_x = float(vol_series.loc[idx_mvp])
                        mvp_y = float(ret_series.loc[idx_mvp])
                        s_mvp = ch.SeriesCollection().NewSeries()
                        s_mvp.Name    = '="MVP"'
                        s_mvp.XValues = (mvp_x,)   # tuples, not lists, play nicer with COM
                        s_mvp.Values  = (mvp_y,)
                        s_mvp.ChartType = XL_XY_SCATTER
                        try:
                            s_mvp.MarkerStyle = XL_MARKERSTYLE_CIRCLE
                            s_mvp.MarkerSize  = 8
                        except Exception:
                            pass
                except Exception as e:
                    print(f"[chart] MVP error: {e}")
            
                # ---- Current portfolio (single point) ----
                if current_point is not None:
                    try:
                        curr_vol, curr_ret = map(float, current_point)
                        if np.isfinite(curr_vol) and np.isfinite(curr_ret):
                            s_cur = ch.SeriesCollection().NewSeries()
                            s_cur.Name    = '="Current"'
                            s_cur.XValues = (curr_vol,)
                            s_cur.Values  = (curr_ret,)
                            s_cur.ChartType = XL_XY_SCATTER
                            try:
                                s_cur.MarkerStyle = XL_MARKERSTYLE_PLUS
                                s_cur.MarkerSize  = 10
                            except Exception:
                                pass
                    except Exception as e:
                        print(f"[chart] Current point error: {e}")
            
                # ---- Axis number formats to percentages (best-effort) ----
                for ax_type in (XL_AXIS_CATEGORY, XL_AXIS_VALUE):
                    try:
                        ch.Axes(ax_type).TickLabels.NumberFormat = "0.0%"
                    except Exception:
                        pass
            
                # Keep legend; if chart had one it stays. Optionally ensure it exists:
                try:
                    ch.HasLegend = True
                except Exception:
                    pass
            # ================= End chart updater =================            
            # -------- Example usage (fits your existing variables) --------
            # Compute current portfolio point if you want it plotted; otherwise pass current_point=None.
            current_point = None
            try:
                curr_w = current_holdings_weights(
                    units=units,
                    last_prices=last_px_hold,
                    investable=list(Sigma_opt.index),
                    fx_to_aud=fx_map_all
                ).reindex(Sigma_opt.index).fillna(0.0)
            
                mu_use = mu_vec_opt.reindex(Sigma_opt.index).fillna(0.0).values
                S_use  = Sigma_opt.values
                wv     = curr_w.values
            
                curr_ret = float(mu_use @ wv)
                curr_vol = float(np.sqrt(wv @ S_use @ wv) * np.sqrt(252.0))
                current_point = (curr_vol, curr_ret)
            except Exception as e:
                print(f"[chart] Current point compute error: {e}")
                current_point = None
            
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
                    title_text="Efficient Frontier & CAL (rf=4.00%)",
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

          
            # ---- Build trade plan & costs BEFORE writing Trade Plan/Costs/Tilts ----
            trade_rec, resid_rec = make_trade_plan(
                units, last_px_hold, fx_map_all, w_star, include_zero_lines=True, include_flags=include_flags
            )
            costs_rec = evaluate_transaction_costs(
                trade_rec, lots_df, pd.Timestamp(prices.index[-1]), MARGINAL_TAX_RATE
            )

            trade_rec = trade_rec.copy()
            trade_rec["Brokerage (AUD)"] = costs_rec["per_row_brokerage"].reindex(trade_rec.index).fillna(0.0).round(2)
            # drop any legacy promo cols
            trade_rec.drop(columns=[c for c in trade_rec.columns if c.lower().startswith("promo")],
                           errors="ignore", inplace=True)

            # ---- Achieved factor tilts table (from B and w_star) ----
            tilts_out = None
            if (B is not None) and (not B.empty):
                factor_order = ["Mkt-RF","SMB","HML","RMW","CMA","MOM"]
                w_use = w_star.reindex(B.index).fillna(0.0)
                if float(w_use.sum()) > 0:
                    w_use = w_use / float(w_use.sum())
                achieved_series = (B.T @ w_use).rename("Achieved β").reindex(factor_order)

                if isinstance(tilt_df, pd.DataFrame) and not tilt_df.empty:
                    tgt = tilt_df.reindex(factor_order)
                    tilts_out = pd.DataFrame({
                        "Use?":      tgt["Use?"].astype(bool).map({True: "Yes", False: "No"}),
                        "Target β":  pd.to_numeric(tgt["Target"], errors="coerce"),
                        "Band":      pd.to_numeric(tgt["Band"],   errors="coerce"),
                        "Achieved β": achieved_series,
                    })
                    tilts_out["Diff"] = tilts_out["Achieved β"] - tilts_out["Target β"]
                    tilts_out["Within Band?"] = (tilts_out["Diff"].abs() <= tilts_out["Band"]).map({True: "Yes", False: "No"})
                else:
                    tilts_out = achieved_series.to_frame()

            # ---------- Layout anchors (avoid overlaps) ----------
            anchor_row = start_s_row + stats_df.shape[0] + 4
            TP_COL, COST_COL, TILT_COL = "A", "J", "M"

            # ---------- LEFT: Trade Plan ----------
            opt.range(f"{TP_COL}{anchor_row}").value = "Trade Plan (rounded units)"
            opt.range(f"{TP_COL}{anchor_row+1}").options(pd.DataFrame, index=False, header=True).value = trade_rec

            # basic formatting
            tp_rows = trade_rec.shape[0] + 1
            tp_first = anchor_row + 1
            tp_data_first = tp_first + 1
            # ---------- Format Trade Plan numbers ----------
            try:
                for col_name, fmt in {
                    "Security": "@",
                    "Current Units": "0",
                    "Target Units": "0",
                    "Last Px (AUD)": "0.0000",
                    "Cash Flow (AUD)": "$0.00",
                    "Brokerage (AUD)": "$0.00",
                }.items():
                    if col_name in trade_rec.columns:
                        col_idx = list(trade_rec.columns).index(col_name)
                        col_letter = chr(ord("A") + col_idx)
                        opt.range(f"{col_letter}{tp_data_first}:{col_letter}{tp_first+tp_rows}").api.NumberFormat = fmt
            except Exception as e:
                print(f"[format] Trade Plan formatting skipped: {e}")
                if "Brokerage (AUD)" in trade_rec.columns:
                    bidx = list(trade_rec.columns).index("Brokerage (AUD)")
                    bcol = chr(ord("A") + bidx)
                    opt.range(f"{bcol}{tp_data_first}:{bcol}{tp_first+tp_rows}").api.NumberFormat = "0.00"
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
                    for col_name in ["Target β","Band","Achieved β","Diff"]:
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
                    tgt = pd.to_numeric(tilts_out.get("Target β", np.nan), errors="coerce")
                    ach = pd.to_numeric(tilts_out.get("Achieved β", np.nan), errors="coerce")
                    rng_df = rng_df.join(tgt.rename("Target β")).join(ach.rename("Achieved β"))
                    rng_df["Within Range?"] = (rng_df["Target β"] >= rng_df["Min β"]) & (rng_df["Target β"] <= rng_df["Max β"])
            
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
                    for col_name in ["Min β","Max β","Target β","Achieved β"]:
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
            ff5s.range('A1').options(pd.DataFrame, index=True, header=True).value = ff

            # ---- Update Lots and overwrite Holdings with target units (for next run) ----
            UPDATED_LOTS = _update_lots_after_trades(lots_df, trade_rec, pd.Timestamp(prices.index[-1]), fx_map_all)
            try:
                sht_lots = wb.sheets['Lots']
            except Exception:
                sht_lots = wb.sheets.add('Lots', after=wb.sheets[-1])
            sht_lots.used_range.clear_contents()
            sht_lots.range("A1").value = [["Security","AcqDate","Units","CostBaseAUD"]]
            sht_lots.range("A2").options(index=False, header=False).value = UPDATED_LOTS

            tgt_units_full = compute_target_units_for_holdings(
                units, last_px_hold, fx_map_all, w_star, include_flags
            )
            _write_holdings_sheet(wb, prices, tgt_units_full, include_flags,
                                  sheet_name="Holdings", fx_to_aud_map=fx_map_all)

            wb.save()
            wb.close()

    except Exception as e:
        print(f"[Excel fallback] xlwings/COM error → exporting CSVs instead: {e}")
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

print("Workbook Successfully Updated")

# --- Optional: auto-open the workbook in Excel (independent of xlwings) ---
OPEN_AFTER_SAVE = bool(CFG.get("open_after_save", True))

def _os_open(path):
    try:
        # Windows
        os.startfile(path)  # type: ignore[attr-defined]
    except AttributeError:
        # macOS / Linux fallback
        import subprocess, sys
        if sys.platform == "darwin":
            subprocess.run(["open", path])
        else:
            subprocess.run(["xdg-open", path])

if OPEN_AFTER_SAVE:
    _os_open(filename)

# --- Create a Desktop shortcut (optional, safe in any context) ---
try:
    if HAS_WIN32COM:
        shortcut_path = str(Path.home() / "Desktop" / "Portfolio Optimiser.lnk")

        # Prefer the exe if it exists; otherwise point at the script we’re running.
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
                candidate = APP_DIR / "Portfolio_Optimiser3110.py"
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
