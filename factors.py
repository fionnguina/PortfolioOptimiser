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
import statsmodels.api as sm
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

def get_rba_cash_rate_series(default_current: float = 0.04) -> "pd.Series | None":
    """Historical RBA Cash Rate Target as a decimal series (monthly, ffill-able).

    A Sharpe ratio must net off the risk-free rate that actually prevailed on
    each day. Charging today's 4.35% against 2020-21 — when the cash rate was
    0.10% — understates a decade of excess return and was worth ~0.2 Sharpe on
    the reported figures (see the 2026-08-13 review).

    Source: RBA table F1.1, series FIRMMCRT (Cash Rate Target, monthly avg).
    Returns None on failure so callers can fall back to the scalar rate —
    a missing series must degrade to the old behaviour, never to a silent 0.
    """
    url_csv = "https://www.rba.gov.au/statistics/tables/csv/f1.1-data.csv"
    cache = _CACHE_DIR / "rba_cash_rate_target.csv"

    def _parse(text: str) -> "pd.Series | None":
        try:
            df = pd.read_csv(io.StringIO(text), skiprows=10)
        except Exception:
            return None
        date_col = df.columns[0]
        if "FIRMMCRT" not in df.columns:
            return None
        idx = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce")
        vals = pd.to_numeric(df["FIRMMCRT"], errors="coerce")
        ser = pd.Series(vals.values, index=idx).dropna().sort_index()
        ser = ser[~ser.index.duplicated(keep="last")]
        return (ser / 100.0) if len(ser) >= 24 else None

    # Fresh fetch, then disk cache, then give up.
    try:
        text = requests.get(url_csv, timeout=30).text
        ser = _parse(text)
        if ser is not None:
            try:
                cache.write_text(text, encoding="utf-8")
            except Exception:
                pass
            return ser
    except Exception:
        pass
    try:
        if cache.exists():
            ser = _parse(cache.read_text(encoding="utf-8"))
            if ser is not None:
                print("[rf] RBA cash-rate series served from disk cache")
                return ser
    except Exception:
        pass
    print("[rf][WARN] RBA cash-rate series unavailable — Sharpe/Sortino will "
          "fall back to a flat current rate")
    return None


# ---------------------------------------------------------------------
# Caching for FF5 + MOM Data
# ---------------------------------------------------------------------
_CACHE_DIR = Path.home() / ".portfolio_optimiser_cache"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _cache_path(url: str) -> Path:
    key = hashlib.md5(url.encode("utf-8")).hexdigest()
    return _CACHE_DIR / f"{key}.csv"

def _cached_read(url: str, build_df_fn, ttl_days: int = 7) -> pd.DataFrame:
    """Load from cache if recent, else build and cache.

    Empty frames are never cached and a cached empty file is treated as a
    miss — a transient empty download once got cached as a 10-byte header
    and poisoned every run for the TTL window (2026-07-06, US MOM)."""
    p = _cache_path(url)
    try:
        if p.exists() and (time.time() - p.stat().st_mtime) <= ttl_days * 86400:
            df = pd.read_csv(p, index_col=0, parse_dates=[0])
            if not df.empty:
                df.index = pd.to_datetime(df.index)
                return df.sort_index()
            print(f"[cache] cached file for {url[-40:]} is EMPTY — treating as miss")
    except Exception as e:
        print(f"[cache] Read miss: {e}")

    df = build_df_fn()
    if df is None or df.empty:
        raise ValueError(f"factor download returned no data rows: {url}")
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
    # Keep exactly (date, mom): the 202605 regeneration of the US file added
    # a trailing comma per row, which shifted fields against our 2-col header
    # and silently emptied the frame (2026-07-06).
    data = [header] + [",".join(ln.strip().split(",")[:2])
                       for ln in raw[first:] if num_rx.match(ln)]
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


# --- Fama-French beta regressions (moved from the monolith 2026-07-10) -------
# Dimson-style FF5 betas via OLS; multi-region wrapper stitches per-region betas.

def compute_ff5_betas(
    df_returns_wide: pd.DataFrame,
    ff5_returns: pd.DataFrame,
    min_obs: int = 120,
    n_lags: int = 1,
    return_stats: bool = False,
):
    """
    Estimate FF5+MOM betas per security via OLS with a Dimson (1979) correction for
    non-synchronous trading.

    Each factor enters contemporaneously PLUS n_lags lagged terms, and the reported
    beta is the SUM of the contemporaneous and lagged coefficients. This recovers the
    true exposure of assets that trade in a different timezone from the US factors:
    ASX-listed ETFs close ~16h before the US market, so their same-day beta to US
    factors is spuriously near zero while the lagged term carries the real loading.

    When `return_stats=True`, also returns a per-security DataFrame of diagnostic stats
    (n_obs, R², adj R², per-factor contemporaneous t-stats, alpha t-stat, residual σ).

    Returns:
        (B, alpha_daily, resid_var)                 when return_stats=False
        (B, alpha_daily, resid_var, stats_df)       when return_stats=True
    """
    joined = df_returns_wide.join(ff5_returns, how="inner").dropna(how="any").sort_index()
    if joined.empty:
        empty_stats = pd.DataFrame() if return_stats else None
        return (None, None, None, empty_stats) if return_stats else (None, None, None)

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

    # Diagnostic columns: filled per security when return_stats=True.
    stats_rows = [] if return_stats else None

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

        if return_stats:
            # tvalues is indexed by parameter name. Contemporaneous t-stat per factor
            # is the right "is this factor significant?" sniff test even though the
            # reported beta is the Dimson sum (contemporaneous + lags).
            row = {
                "Security": sec,
                "N obs": int(model.nobs),
                "R^2": float(model.rsquared),
                "R^2 adj": float(model.rsquared_adj),
                "alpha_daily": float(model.params.get("const", np.nan)),
                "alpha_t": float(model.tvalues.get("const", np.nan)),
                "resid_std_daily": float(np.sqrt(resid_var.loc[sec])) if pd.notna(resid_var.loc[sec]) else np.nan,
            }
            for f in factors:
                row[f"{f}_t"] = float(model.tvalues.get(f, np.nan))
            stats_rows.append(row)

    if return_stats:
        stats_df = pd.DataFrame(stats_rows).set_index("Security") if stats_rows else pd.DataFrame()
        return B, alpha_daily, resid_var, stats_df
    return B, alpha_daily, resid_var


def compute_ff5_betas_multi_region(
    df_returns_wide: pd.DataFrame,
    regional_factors: dict,
    region_map,
    min_obs: int = 120,
    n_lags: int = 1,
    reference_region: str = "US",
    standardise_factors: bool = True,
    return_stats: bool = False,
):
    """Compute FF5+MOM betas where each security is regressed against its home-region factor set.

    Each security's beta vector lives in the canonical 6-factor space (Mkt-RF, SMB, HML, RMW,
    CMA, MOM) but the underlying factors are the security's regional series — i.e. an ASX ETF's
    "Mkt-RF" beta is its loading against the Asia-Pacific ex Japan market factor, not the US one.

    When `standardise_factors` is True (default), each non-reference region's factor returns are
    rescaled so their per-factor volatility matches the reference region (US by default). This
    means a security's "Mkt-RF" beta is expressed in "units of US-Mkt-RF volatility" regardless of
    home region, making cross-region aggregation Σ w_i × β_i^f mathematically clean. The reference
    region's betas are unchanged — preserves backward continuity with the US-only model.

    Args:
        df_returns_wide: wide DataFrame of asset daily returns (one column per security).
        regional_factors: {region_key: factor_df_with_RF}. Region keys must match `region_map` output.
        region_map: callable taking a security column name and returning a region key.
        reference_region: which region's factor vols define the common scale (default "US").
        standardise_factors: when True, rescale non-reference regional factors to match the
            reference region's per-factor volatility. See task #6 design notes.

    Returns: (B, alpha_daily, resid_var) in the same shape as compute_ff5_betas.
    """
    securities = list(df_returns_wide.columns)
    by_region: dict[str, list[str]] = {}
    for sec in securities:
        by_region.setdefault(region_map(sec), []).append(sec)

    # Compute the per-factor scaling map ahead of the regression loop so we can log it.
    factor_cols = ["Mkt-RF", "SMB", "HML", "RMW", "CMA", "MOM"]
    scaling: dict[str, dict[str, float]] = {}
    if standardise_factors and reference_region in regional_factors:
        ref_df = regional_factors[reference_region]
        ref_vol = {f: float(ref_df[f].std()) for f in factor_cols if f in ref_df.columns}
        for region, df_r in regional_factors.items():
            if region == reference_region or df_r is None or df_r.empty:
                continue
            scaling[region] = {}
            for f in factor_cols:
                if f in df_r.columns and f in ref_vol:
                    own = float(df_r[f].std())
                    scaling[region][f] = (ref_vol[f] / own) if own > 0 else 1.0
                else:
                    scaling[region][f] = 1.0
        if scaling:
            print(
                "[ff5] factor standardisation (vs " + reference_region + " vol): "
                + "; ".join(
                    f"{r}: " + ", ".join(f"{f}={s:.2f}x" for f, s in factors.items())
                    for r, factors in scaling.items()
                )
            )

    B_parts, alpha_parts, resid_parts, stats_parts = [], [], [], []
    for region, secs in by_region.items():
        ff = regional_factors.get(region)
        if ff is None or ff.empty or not secs:
            continue
        if region in scaling:
            ff = ff.copy()
            for f, mult in scaling[region].items():
                if f in ff.columns:
                    ff[f] = ff[f] * mult
        sub = df_returns_wide[secs]
        result = compute_ff5_betas(
            sub, ff, min_obs=min_obs, n_lags=n_lags, return_stats=return_stats,
        )
        if return_stats:
            B_r, alpha_r, resid_r, stats_r = result
        else:
            B_r, alpha_r, resid_r = result
            stats_r = None
        if B_r is not None and not B_r.empty:
            B_parts.append(B_r)
        if alpha_r is not None:
            alpha_parts.append(alpha_r)
        if resid_r is not None:
            resid_parts.append(resid_r)
        if return_stats and stats_r is not None and not stats_r.empty:
            # Tag each row with the region it was regressed against — and whether
            # this region's factors were rescaled to the reference vol.
            stats_r = stats_r.copy()
            stats_r.insert(0, "Region", region)
            stats_r.insert(1, "Standardised", region in scaling)
            stats_parts.append(stats_r)

    B = pd.concat(B_parts).reindex(securities) if B_parts else None
    alpha = pd.concat(alpha_parts).reindex(securities) if alpha_parts else None
    resid = pd.concat(resid_parts).reindex(securities) if resid_parts else None
    if return_stats:
        stats_df = pd.concat(stats_parts).reindex(securities) if stats_parts else pd.DataFrame()
        return B, alpha, resid, stats_df
    return B, alpha, resid
