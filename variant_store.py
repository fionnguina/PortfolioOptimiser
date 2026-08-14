"""Persist per-variant OOS return series so PBO/CSCV becomes computable.

The 2026-08-14 validation could compute a Deflated Sharpe (which needs only
the spread of trial Sharpes, recoverable from logs/*.log) but NOT the
Probability of Backtest Overfitting, which needs each variant's full return
SERIES. Those were never kept. This module keeps them from here on.

Design notes that matter:

  * ONE hook. oos_engine calls VARIANT_SINK at the end of every walk-forward,
    so research sweeps and production runs are captured alike with no
    call-site churn and no chance of a variant quietly not being recorded.

  * TWO keys, not one. `config_key` hashes the strategy knobs ONLY;
    `data_key` hashes the price panel's shape and date range. PBO compares
    configs evaluated on the SAME data, so it selects one data_key and varies
    config_key. Collapsing these into a single fingerprint — the mistake the
    OOS cache makes deliberately for its own purposes — would make the store
    useless for this.

  * Failure is silent and non-fatal. A telemetry store must never take down
    a trading pipeline. Every entry point swallows its own exceptions.

Layout:
    .cache/variants/index.jsonl        one JSON line per (config_key, data_key)
    .cache/variants/<key>.pkl          the return series
"""
from __future__ import annotations

import hashlib
import json
import os
import pickle
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

STORE_SUBDIR = Path(".cache") / "variants"
INDEX_NAME = "index.jsonl"
# Guard against unbounded growth from daily production runs. ~21KB a series,
# so this is tens of thousands of variants before it matters.
MAX_STORE_MB = 500.0


def _store_dir(app_dir=None) -> Path:
    base = Path(app_dir) if app_dir else Path.cwd()
    d = base / STORE_SUBDIR
    d.mkdir(parents=True, exist_ok=True)
    return d


def _hash(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:16]


def config_key(config: dict) -> str:
    """Hash of the strategy knobs alone — data range deliberately excluded."""
    return _hash(config)


def data_key(returns: pd.Series, nav_aud=None) -> str:
    """Hash of the EVALUATION SETUP — window plus NAV — so PBO holds it fixed.

    NAV belongs here, not in config_key. The nightly evidence run sweeps
    100k/250k/500k/1M with an identical strategy: same config, same window,
    but genuinely different returns because IBKR's $5 minimum bites hardest at
    small scale. Keying on the window alone made all four collide, so the
    store kept whichever ran first and mislabelled one NAV's series as the
    config's. Putting NAV here separates them AND keeps PBO honest — it fixes
    one data_key, so it compares configs at a single scale rather than
    mistaking a brokerage-drag difference for a strategy difference.
    """
    if returns is None or len(returns) == 0:
        return "empty"
    idx = returns.index
    d = {"n": int(len(returns)), "start": str(idx.min()), "end": str(idx.max())}
    if nav_aud is not None:
        try:
            d["nav"] = round(float(nav_aud), 2)
        except Exception:
            d["nav"] = str(nav_aud)
    return _hash(d)


def summarise(returns: pd.Series, ppy: float | None = None) -> dict:
    """Index-row summary. ppy is derived from the series' own calendar unless
    given — the OOS panel is a UNION of AU+US sessions (~258/yr), so assuming
    252 mislabels every annualised figure in the index."""
    r = pd.to_numeric(returns, errors="coerce").dropna()
    if len(r) < 3:
        return {}
    if ppy is None:
        try:
            from validation import _infer_ppy
            ppy = _infer_ppy(r)
        except Exception:
            ppy = 252.0
    sd = float(r.std(ddof=1))
    cum = float((1.0 + r).cumprod().iloc[-1])
    yrs = len(r) / ppy
    dd = ((1.0 + r).cumprod() / (1.0 + r).cumprod().cummax() - 1.0).min()
    return {
        "n_obs": int(len(r)),
        "ann_return": float(cum ** (1.0 / yrs) - 1.0) if yrs > 0 else None,
        "ann_vol": float(sd * np.sqrt(ppy)),
        "sharpe_ann": float(r.mean() / sd * np.sqrt(ppy)) if sd > 0 else None,
        "max_drawdown": float(dd),
        "periods_per_year": float(ppy),
        "skew": float(((r - r.mean()) / sd ** 1).pow(3).mean() / sd ** 2) if sd > 0 else None,
    }


def persist_variant(returns: pd.Series, config: dict, meta: dict | None = None,
                    app_dir=None) -> str | None:
    """Record one variant. Returns its key, or None if skipped/failed.

    Deduplicates on (config_key, data_key): re-running the same config over
    the same window is the same trial, not a new one. A production run on a
    later date has a new data_key and is therefore recorded separately, which
    is correct — it IS a different evaluation.
    """
    try:
        if returns is None or len(returns) < 30:
            return None
        d = _store_dir(app_dir)
        ck, dk = config_key(config), data_key(returns, (meta or {}).get("nav_aud"))
        key = f"{ck}_{dk}"
        path = d / f"{key}.pkl"
        index = d / INDEX_NAME
        if path.exists():
            return key

        try:
            size_mb = sum(f.stat().st_size for f in d.glob("*.pkl")) / 1e6
            if size_mb > MAX_STORE_MB:
                print(f"[variants][WARN] store at {size_mb:.0f}MB > "
                      f"{MAX_STORE_MB:.0f}MB cap — not recording; prune "
                      f"{d} or raise MAX_STORE_MB")
                return None
        except Exception:
            pass

        ser = pd.to_numeric(returns, errors="coerce").dropna()
        with open(path, "wb") as fp:
            pickle.dump(ser, fp, protocol=pickle.HIGHEST_PROTOCOL)

        row = {
            "key": key, "config_key": ck, "data_key": dk,
            "recorded_at": datetime.now().isoformat(timespec="seconds"),
            "start": str(ser.index.min().date()),
            "end": str(ser.index.max().date()),
            **summarise(ser),
            "config": config,
            **(meta or {}),
        }
        with open(index, "a", encoding="utf-8") as fp:
            fp.write(json.dumps(row, default=str) + "\n")
        return key
    except Exception as e:
        print(f"[variants] not recorded ({type(e).__name__}: {e})")
        return None


def load_index(app_dir=None) -> pd.DataFrame:
    try:
        idx = _store_dir(app_dir) / INDEX_NAME
        if not idx.exists():
            return pd.DataFrame()
        rows = [json.loads(l) for l in idx.read_text(encoding="utf-8").splitlines() if l.strip()]
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def load_series(key: str, app_dir=None) -> pd.Series | None:
    try:
        with open(_store_dir(app_dir) / f"{key}.pkl", "rb") as fp:
            return pickle.load(fp)
    except Exception:
        return None


def load_trial_matrix(app_dir=None, data_key_filter: str | None = None) -> pd.DataFrame:
    """Variants as columns on a shared index — the input PBO/CSCV needs.

    Picks the data_key with the most distinct configs when none is given,
    because comparing configs across different evaluation windows is exactly
    the error this store exists to prevent.
    """
    idx = load_index(app_dir)
    if idx.empty:
        return pd.DataFrame()
    if data_key_filter is None:
        counts = idx.groupby("data_key")["config_key"].nunique()
        if counts.empty:
            return pd.DataFrame()
        data_key_filter = counts.idxmax()
    sel = idx[idx["data_key"] == data_key_filter].drop_duplicates("config_key")
    cols = {}
    for _, row in sel.iterrows():
        s = load_series(row["key"], app_dir)
        if s is not None and len(s):
            cols[row["config_key"]] = s
    return pd.DataFrame(cols).sort_index() if cols else pd.DataFrame()
