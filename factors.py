"""Factor-data layer: FF5/MOM downloads, caching, region mapping, RBA rate.

Extracted from Portfolio_Optimiser.py (module split, 2026-07-03).

Contents:
  get_rba_cash_rate_target_current   Latest RBA cash rate target (HTML → CSV → default).
  _cache_path / _cached_read         TTL'd CSV cache under ~/.portfolio_optimiser_cache.
  FF5_REGION_URLS                    Ken French daily FF5 + MOM zips per region.
  region_for_ticker                  Ticker → factor region (overrides > heuristic).
  _load_regions_json / _save_...     User region overrides persisted in regions.json.
  get_ff5_daily / get_mom_daily /
  get_ff5_mom_daily                  Region-aware daily factor loaders.
  compute_factor_recent_stats /
  auto_recommend_factor_tilts        Trailing-N-day factor-momentum scorer.

Constants canonical here; engine imports them back (tlh.py pattern).

Cross-module contract:
  REGIONS_JSON_PATH defaults to ./regions.json. The engine overrides it right
  after import (`factors.REGIONS_JSON_PATH = APP_DIR / "regions.json"`) so the
  frozen exe resolves beside the workbook, not the PyInstaller temp dir.
  USER_REGION_OVERRIDES is a shared mutable dict: the engine .update()s it from
  regions.json + Holdings `Region` column at startup and the dialog assigns
  into it — never rebind it, or the copies diverge.
"""
from __future__ import annotations

import hashlib
import io
import json
import re
import time
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import requests

ANNUAL_TRADING_DAYS = 252  # annualisation constant (kept local, as in metrics.py)


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
# FF5 + Momentum Data Loaders (region-aware)
# ---------------------------------------------------------------------
# Ken French publishes daily factor data for several regions. We use:
#   US           — the canonical FF5 + MOM (also the "global" momentum series).
#   AP_EX_JAPAN  — Asia-Pacific ex Japan. The closest daily series for ASX names.
#                  Note: there is no AP-incl-Japan daily series; we use a 3-region
#                  dispatch (US / AP-ex-Japan / Japan) to cover IJP.AX cleanly.
#   JAPAN        — Japan FF5 + MOM, used for IJP.AX only.
FF5_REGION_URLS = {
    "US": (
        "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_daily_CSV.zip",
        "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_daily_CSV.zip",
    ),
    "AP_EX_JAPAN": (
        "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Asia_Pacific_ex_Japan_5_Factors_Daily_CSV.zip",
        "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Asia_Pacific_ex_Japan_MOM_Factor_Daily_CSV.zip",
    ),
    "JAPAN": (
        "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Japan_5_Factors_Daily_CSV.zip",
        "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Japan_MOM_Factor_Daily_CSV.zip",
    ),
    "EUROPE": (
        "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Europe_5_Factors_Daily_CSV.zip",
        "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Europe_MOM_Factor_Daily_CSV.zip",
    ),
    # NOTE: Emerging Markets daily factors are NOT published by Ken French (only
    # monthly), so VGE.AX and any other EM tracker stays bucketed into AP_EX_JAPAN
    # by the heuristic. The R² will be lower than for pure-AP names — that's a
    # known limitation, not a bug. Documented 2026-06-12.
}

# yfinance exchange suffixes for the European region. Used by the heuristic
# in region_for_ticker to classify natively-listed European stocks.
EUROPEAN_EXCHANGE_SUFFIXES = (
    ".L",   # London
    ".PA",  # Euronext Paris
    ".DE",  # XETRA / Frankfurt
    ".AS",  # Euronext Amsterdam
    ".MI",  # Borsa Italiana
    ".SW",  # SIX Swiss Exchange
    ".MC",  # Bolsa de Madrid
    ".BR",  # Euronext Brussels
    ".HE",  # Helsinki
    ".ST",  # Stockholm
    ".CO",  # Copenhagen
    ".OL",  # Oslo
    ".LS",  # Lisbon
    ".VI",  # Vienna
    ".IR",  # Euronext Dublin
    ".AT",  # Athens
)
# Backward-compat aliases (legacy code still references these).
FF5_DAILY_ZIP = FF5_REGION_URLS["US"][0]
MOM_DAILY_ZIP = FF5_REGION_URLS["US"][1]


# Hardcoded ticker -> factor region overrides. Use these for securities whose
# correct factor region differs from what the .AX suffix would suggest — e.g.
# ASX-listed ETFs that track US/global indices have their *underlying exposure*
# in the US, not Asia-Pacific. Without these overrides, the FF5 regression
# uses the wrong factor set and R² collapses (see 2026-06-12 diagnostics:
# IVV.AX had R² 0.32 against AP-Mkt vs SPY 0.81 against US-Mkt, same underlying).
#
# Runtime override path: a `Region` column in the Holdings sheet takes
# precedence over this dict, letting the user reclassify any ticker without
# editing source. Build order: Holdings column > TICKER_REGION_OVERRIDES > heuristic.
TICKER_REGION_OVERRIDES: dict[str, str] = {
    # ASX-listed ETFs whose underlying AND trading microstructure align with US.
    # Empirically validated by R² improvement when classified as US (vs AP-ex-Japan)
    # in the 2026-06-12 diagnostics — see Regression_Diagnostics sheet.
    "IVV.AX": "US",         # iShares S&P 500           (R² 0.32 -> 0.49)
    "IOO.AX": "US",         # iShares S&P Global 100    (R² 0.32 -> 0.47)
    "VGS.AX": "US",         # Vanguard MSCI World ex Aus (R² 0.37 -> 0.48)
    "QUAL.AX": "US",        # VanEck MSCI World Quality (R² 0.27 -> 0.40)
    "VLUE.AX": "US",        # iShares Edge MSCI Value   (R² 0.33 -> 0.36)
    "VVLU.AX": "US",        # Vanguard Global Value     (R² 0.30 -> 0.57)
    # ASX-listed European tracker -> Europe (Ken French daily Europe factors now loaded).
    "IEU.AX": "EUROPE",     # iShares Europe ETF
    # MTUM.AX (global momentum, ASX-listed) was tried as US but R² dropped
    # 0.49 -> 0.26. Trading-microstructure synchroneity with AP markets
    # dominates the underlying-region consideration at daily frequency. Left
    # out of the dict so it defaults to AP_EX_JAPAN via the .AX heuristic.
    # Australian broad-market benchmark
    "^AORD": "AP_EX_JAPAN",
}

# Runtime user overrides loaded from Holdings sheet (Region column, if present).
# Populated by the engine early in the pipeline; falls back to empty dict so
# region_for_ticker keeps working in legacy / fresh installs. Shared mutable
# dict — mutate in place (.update() / item assignment), never rebind.
USER_REGION_OVERRIDES: dict[str, str] = {}


def region_for_ticker(ticker: str) -> str:
    """Map a security ticker to its Ken French factor region.

    Resolution order:
      1. USER_REGION_OVERRIDES (Holdings sheet `Region` column) — runtime user override.
      2. TICKER_REGION_OVERRIDES (hardcoded above) — known classification corrections.
      3. Heuristic by yfinance exchange suffix:
         - .T                              -> Japan
         - .AX (and special case IJP.AX)   -> AP_EX_JAPAN (Japan if IJP)
         - any European exchange suffix    -> Europe
         - else (US-listed)                -> US
    """
    t = str(ticker).upper().strip()
    if t in USER_REGION_OVERRIDES:
        return USER_REGION_OVERRIDES[t]
    if t in TICKER_REGION_OVERRIDES:
        return TICKER_REGION_OVERRIDES[t]
    if t == "IJP.AX":
        return "JAPAN"
    if t.endswith(".T"):  # Tokyo Stock Exchange
        return "JAPAN"
    if t.endswith(".AX"):
        return "AP_EX_JAPAN"
    for _sfx in EUROPEAN_EXCHANGE_SUFFIXES:
        if t.endswith(_sfx.upper()):
            return "EUROPE"
    return "US"


# User region overrides live in regions.json beside the workbook — kept out
# of Excel entirely so the Holdings sheet stays focused on positions, and out
# of source so user choices survive code-level changes to TICKER_REGION_OVERRIDES.
# Default is cwd-relative; the engine overrides this to APP_DIR / "regions.json"
# right after import (APP_DIR handles the frozen-exe case).
REGIONS_JSON_PATH = Path("regions.json")


def _load_regions_json() -> dict[str, str]:
    """Load ticker -> region map from regions.json. Silently returns {} if missing.

    Validates regions against FF5_REGION_URLS — silently drops unknown values
    (e.g. an outdated entry for a region the code no longer supports).
    """
    if not REGIONS_JSON_PATH.exists():
        return {}
    try:
        with REGIONS_JSON_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[region] regions.json load failed ({e}); starting fresh.")
        return {}
    valid = set(FF5_REGION_URLS.keys())
    out: dict[str, str] = {}
    for k, v in (data or {}).items():
        if not isinstance(k, str) or not isinstance(v, str):
            continue
        ticker = k.upper().strip()
        region = v.upper().strip()
        if ticker and region in valid:
            out[ticker] = region
    return out


def _save_regions_json(mapping: dict[str, str]) -> bool:
    """Atomically write the ticker -> region map back to regions.json."""
    try:
        REGIONS_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = REGIONS_JSON_PATH.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(mapping, f, indent=2, sort_keys=True)
        tmp.replace(REGIONS_JSON_PATH)
        return True
    except Exception as e:
        print(f"[region] regions.json save failed: {e}")
        return False


def _download_mom_csv(url: str) -> pd.DataFrame:
    """Parse a Ken French MOM zip at the given URL into a daily MOM DataFrame."""
    r = requests.get(url, timeout=60)
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


def _download_ff5_csv(url: str) -> pd.DataFrame:
    """Parse a Ken French FF5 zip at the given URL into a daily 5-factor DataFrame."""
    resp = requests.get(url, timeout=60)
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


def get_mom_daily(region: str = "US") -> pd.DataFrame:
    """Get daily momentum factor data for the given region."""
    url = FF5_REGION_URLS[region][1]
    df = _cached_read(url, lambda: _download_mom_csv(url), ttl_days=7)
    df = df.copy()
    if "MOM" not in df.columns:
        df["MOM"] = pd.to_numeric(df.iloc[:, 0], errors="coerce")
        df = df[["MOM"]]
    df.index = pd.to_datetime(df.index)
    return df.sort_index()


def get_ff5_daily(region: str = "US", cache_csv_path: str | None = None) -> pd.DataFrame:
    """Get daily Fama-French 5 factor data for the given region."""
    url = FF5_REGION_URLS[region][0]
    df = _cached_read(url, lambda: _download_ff5_csv(url), ttl_days=7)
    if cache_csv_path:
        try:
            df.to_csv(cache_csv_path, index=True)
        except Exception as e:
            print(f"[ff5] Could not write cache_csv_path: {e}")
    return df


def get_ff5_mom_daily(region: str = "US") -> pd.DataFrame:
    """Get combined FF5 + MOM daily factors for the given region."""
    ff5 = get_ff5_daily(region=region)
    mom = get_mom_daily(region=region)
    out = ff5.join(mom, how="inner").sort_index()
    return out[["Mkt-RF", "SMB", "HML", "RMW", "CMA", "MOM", "RF"]]


# ============================================================================
# AUTO-RECOMMEND FACTOR TILTS — trailing-3M factor-momentum scorer
# ----------------------------------------------------------------------------
# Picks tilt magnitudes by ranking each of (Mkt-RF, SMB, HML, RMW, CMA, MOM)
# on trailing N-day annualised Sharpe. Magnitude scales linearly with Sharpe,
# clipped so no single factor dominates. Use for the dialog's Auto Recommend
# button or as a baseline for `--factor-recs` CLI inspection.
#
# Conservative default magnitudes: a factor with Sharpe ≈ 2.0 → +0.20 tilt.
# A factor with Sharpe ≈ -2.0 → -0.20 tilt. Clipped at ±0.30 so a runaway
# factor can't blow out portfolio concentration. Lookback default 63 trading
# days (~3 calendar months) per the user's choice on 2026-06-19.
# ============================================================================
FACTOR_TILT_LOOKBACK_DAYS = 63
FACTOR_TILT_MAX_MAGNITUDE = 0.30
FACTOR_TILT_SHARPE_TO_MAG = 0.10  # Sharpe × this = tilt magnitude (before clip)
FACTOR_NAMES = ("Mkt-RF", "SMB", "HML", "RMW", "CMA", "MOM")


def compute_factor_recent_stats(ff_data: pd.DataFrame,
                                 lookback_days: int | None = None) -> pd.DataFrame:
    """Compute trailing-N-day annualised return, vol, Sharpe per factor.

    Returns a DataFrame indexed by factor name with columns
    [ann_return, ann_vol, sharpe, recent_n_days]. Excludes RF.
    """
    lookback_days = lookback_days or FACTOR_TILT_LOOKBACK_DAYS
    if ff_data is None or ff_data.empty:
        return pd.DataFrame()
    # Strip RF; we score the active factors only.
    facs = [c for c in FACTOR_NAMES if c in ff_data.columns]
    if not facs:
        return pd.DataFrame()
    tail = ff_data[facs].tail(lookback_days).dropna(how="all")
    if tail.empty:
        return pd.DataFrame()
    rows = []
    for f in facs:
        s = pd.to_numeric(tail[f], errors="coerce").dropna()
        if s.empty:
            continue
        # ff5 factors are already daily excess returns (decimals).
        ann_ret = float(s.mean() * ANNUAL_TRADING_DAYS)
        ann_vol = float(s.std() * np.sqrt(ANNUAL_TRADING_DAYS))
        sharpe = ann_ret / ann_vol if ann_vol > 0 else 0.0
        rows.append({
            "factor": f,
            "ann_return": ann_ret,
            "ann_vol": ann_vol,
            "sharpe": sharpe,
            "n_days": int(len(s)),
        })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).set_index("factor")


def auto_recommend_factor_tilts(ff_data: pd.DataFrame,
                                 lookback_days: int | None = None,
                                 max_magnitude: float | None = None,
                                 sharpe_to_mag: float | None = None) -> dict[str, float]:
    """Auto-recommend factor tilt targets from trailing-N-day factor Sharpes.

    Each factor's target = clip(Sharpe × sharpe_to_mag, -max_mag, +max_mag).
    Returns a dict like {"Mkt-RF": 0.10, "SMB": -0.05, "MOM": +0.18, ...}
    suitable for passing as `tilt_targets` to solve_frontier_point_cvxpy.

    Empty dict if FF data is missing or insufficient.
    """
    max_magnitude = max_magnitude or FACTOR_TILT_MAX_MAGNITUDE
    sharpe_to_mag = sharpe_to_mag or FACTOR_TILT_SHARPE_TO_MAG
    stats = compute_factor_recent_stats(ff_data, lookback_days=lookback_days)
    if stats.empty:
        return {}
    out = {}
    for f, row in stats.iterrows():
        sharpe = float(row["sharpe"])
        target = sharpe * sharpe_to_mag
        target = max(-max_magnitude, min(max_magnitude, target))
        out[f] = round(target, 4)
    return out
