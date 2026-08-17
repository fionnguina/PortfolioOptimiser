"""Actual (broker-truth) NAV series (module split #18, 2026-07-09).

The fund's real performance record: reconstruct daily NAV from seed lots +
executed fills, then chain-link onto broker NetLiquidation where available.
  compute_actual_nav_series          Reconstruct NAV from seed lots + fills (mixed ccy).
  _load_broker_nav_series            Broker NetLiq (AUD) by day from ibkr_nav_log.jsonl.
  compute_actual_nav_series_spliced  Reconstruction spliced to broker RETURNS at the seam.

Engine re-exports these + syncs APP_DIR (used only for the default broker-nav path).
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

# Synced by the engine after import (default broker-nav-log location).
APP_DIR = Path(".")


def compute_actual_nav_series(prices, fills_path, seed_path,
                              fx_usdaud=None, cash_aud: float = 0.0):
    """Reconstruct daily NAV series from seed lots + executed fills.

    Returns a pd.Series of portfolio market value indexed by date,
    starting from the earliest seed/fill date. Dates before that are
    excluded — the live account did not exist there.

    Used by slide 3 to plot the real "Actual NAV" path of the user's
    paper/live account. Previously slide 3 plotted a hypothetical
    (no-tilts target × historical prices), which the ensemble line
    already covered — and hid the fact that the held lots were
    defensive while the chart pretended they were long-only equity.

    Currency: matches `prices` (typically mixed USD/AUD per the engine's
    convention). Return-based comparisons normalise out the unit, so
    this is safe for chart display even with mixed-currency holdings.
    """
    events: list[tuple[pd.Timestamp, str, float]] = []

    sp = (Path(seed_path) if seed_path and not hasattr(seed_path, "exists")
          else seed_path)
    if sp is not None and sp.exists():
        try:
            for item in json.loads(sp.read_text(encoding="utf-8")):
                try:
                    u = int(round(float(item.get("Units") or 0)))
                    if u <= 0:
                        continue
                    events.append((
                        pd.Timestamp(item.get("AcqDate")).normalize(),
                        str(item.get("Security", "")).strip(),
                        float(u),
                    ))
                except Exception:
                    continue
        except Exception:
            pass

    fp = (Path(fills_path) if fills_path and not hasattr(fills_path, "exists")
          else fills_path)
    if fp is not None and fp.exists():
        try:
            with open(fp, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        r = json.loads(line.replace("NaN", "null"))
                    except Exception:
                        continue
                    qf = float(r.get("qty_filled") or 0)
                    if qf <= 0:
                        continue
                    side = str(r.get("side", "")).upper()
                    delta = qf if side == "BUY" else -qf
                    ticker = str(r.get("ticker", "")).strip()
                    if not ticker:
                        continue
                    ts = r.get("exec_timestamp") or r.get("rec_log_run_at")
                    try:
                        d = pd.Timestamp(ts).normalize()
                    except Exception:
                        continue
                    events.append((d, ticker, delta))
        except Exception:
            pass

    if not events:
        return pd.Series(dtype=float)

    events.sort(key=lambda e: e[0])
    first_date = events[0][0]

    dates = prices.index[prices.index >= first_date]
    if len(dates) == 0:
        return pd.Series(dtype=float)

    by_ticker: dict[str, list[tuple[pd.Timestamp, float]]] = {}
    for d, tk, du in events:
        by_ticker.setdefault(tk, []).append((d, du))

    # FX: `prices` is the engine's MIXED USD/AUD panel, so summing it raw adds
    # USD position values to AUD ones. At AUDUSD~0.65 that understated the US
    # sleeve badly enough to put the whole series ~15% below broker NetLiq.
    fxs = None
    if fx_usdaud is not None:
        try:
            fxs = pd.to_numeric(pd.Series(fx_usdaud), errors="coerce")
            fxs.index = pd.to_datetime(fxs.index).tz_localize(None)
            fxs = fxs.reindex(dates).ffill().bfill()
        except Exception:
            fxs = None

    nav = pd.Series(0.0, index=dates)
    for tk, evs in by_ticker.items():
        if tk not in prices.columns:
            continue
        evs.sort(key=lambda e: e[0])
        units_series = pd.Series(0.0, index=dates)
        cum = 0.0
        ei = 0
        for d in dates:
            while ei < len(evs) and evs[ei][0] <= d:
                cum += evs[ei][1]
                ei += 1
            units_series.loc[d] = cum
        px = pd.to_numeric(prices[tk].reindex(dates),
                            errors="coerce").ffill().fillna(0.0)
        if fxs is not None and not str(tk).endswith(".AX") and not str(tk).startswith("^"):
            px = px * fxs                      # USD -> AUD
        nav = nav.add(units_series * px, fill_value=0.0)

    # NetLiquidation = positions + cash. Omitting cash left the reconstruction
    # short by the full cash balance (~4.6% of NAV here) and biased every
    # return, since a cash buffer damps them.
    if cash_aud:
        nav = nav + float(cash_aud)
    return nav


def _load_broker_nav_series(path=None) -> pd.Series:
    """Broker-truth NAV (NetLiquidation, AUD) by date from ibkr_nav_log.jsonl
    (written by ibkr_paper_exec.py --snapshot-nav / the daily wrapper).
    Last snapshot per calendar day wins. Empty series if no log."""
    p = Path(path) if path is not None else (APP_DIR / "ibkr_nav_log.jsonl")
    if not p.exists():
        return pd.Series(dtype=float)
    vals: dict = {}
    try:
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    d = pd.Timestamp(r["ts"]).normalize().tz_localize(None)
                    nl = float(r.get("net_liquidation_aud") or 0)
                    if nl > 0:
                        vals[d] = nl  # later rows overwrite = last-per-day
                except Exception:
                    continue
    except Exception:
        return pd.Series(dtype=float)
    return pd.Series(vals).sort_index()


def load_broker_positions(path=None) -> dict:
    """Broker-truth positions from the MOST RECENT snapshot in ibkr_nav_log.jsonl.

    Returns {ticker: {"units": float, "avg_cost_local": float|None,
                      "currency": str|None}}, plus a "_ts" key holding the
    snapshot timestamp. Empty dict if there is no log or no usable row.

    Unlike _load_broker_nav_series (which builds a per-day SERIES), this wants
    a single point-in-time picture, so the LAST parseable row with a positions
    array wins outright.

    `avg_cost_local` is per-share in the instrument's trading currency and is
    absent on rows written before 2026-07-17 (the field was added when we
    discovered the fills log had missed real executions) -> None for those.
    """
    p = Path(path) if path is not None else (APP_DIR / "ibkr_nav_log.jsonl")
    if not p.exists():
        return {}
    latest = None
    try:
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if isinstance(r.get("positions"), list):
                    latest = r  # later rows overwrite = most recent wins
    except Exception:
        return {}
    if latest is None:
        return {}

    out: dict = {}
    for item in latest.get("positions") or []:
        try:
            tkr = str(item.get("ticker", "")).strip().upper()
            if not tkr:
                continue
            acl = item.get("avg_cost_local")
            out[tkr] = {
                "units": float(item.get("units") or 0.0),
                "avg_cost_local": (float(acl) if acl is not None else None),
                "currency": item.get("currency"),
            }
        except Exception:
            continue
    if out:
        out["_ts"] = latest.get("ts")
    return out


def last_position_change_date(path=None):
    """Date the broker book last actually MOVED — the local date of the most
    recent ibkr_nav_log snapshot whose position UNITS differ from the snapshot
    before it.

    Position units change ONLY on a fill, so a unit change between consecutive
    NAV snapshots is broker-truth execution timing that does NOT depend on IBKR
    confirming fills (qty_filled is permanently 0 — TWS serves no execution
    history; the fills log never back-fills). This is what anchors the live
    rebalance-cadence gate; the old fills-based anchor (qty_filled > 0) never
    fired, so the 6W gate never engaged and live could rebalance on drift alone.

    Market moves don't change units, so calm snapshots compare equal (no false
    anchor). Returns a tz-naive, midnight-normalised pd.Timestamp of the last
    change, or None if there are fewer than 2 snapshots or the book has never
    been observed to move.
    """
    p = Path(path) if path is not None else (APP_DIR / "ibkr_nav_log.jsonl")
    if not p.exists():
        return None
    snaps = []
    try:
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    snaps.append(json.loads(line))
                except Exception:
                    continue
    except Exception:
        return None
    # ISO timestamps with a consistent offset sort chronologically as strings.
    snaps.sort(key=lambda s: str(s.get("ts") or ""))

    def _units(snap) -> dict:
        out: dict = {}
        for pos in (snap.get("positions") or []):
            try:
                u = int(round(float(pos.get("units") or 0)))
            except (TypeError, ValueError):
                continue
            if u != 0:   # a fully-closed position simply drops out of the map
                out[str(pos.get("ticker", "")).strip().upper()] = u
        return out

    rows = []
    for s in snaps:
        try:
            # .date() takes the wall-clock LOCAL date (tz-aware safe); wrap back
            # to a tz-naive midnight Timestamp so the caller's date math (against
            # a tz-naive "today") never raises.
            d = pd.Timestamp(pd.Timestamp(s.get("ts")).date())
        except Exception:
            continue
        rows.append((d, _units(s)))
    if len(rows) < 2:
        return None
    last = None
    for i in range(1, len(rows)):
        if rows[i][1] != rows[i - 1][1]:
            last = rows[i][0]
    return last


RECON_MAX_MEDIAN_DAILY_ERR = 0.0025   # 25bps median; above this, don't extrapolate
# What the last call actually returned, so callers can label it honestly
# rather than always claiming "fills recon + broker".
LAST_NAV_SOURCE = "unknown"


def _first_broker_cash(path=None) -> float:
    """Earliest recorded cash balance, for the pre-broker head."""
    try:
        p = Path(path) if path is not None else (APP_DIR / "ibkr_nav_log.jsonl")
        if not p.exists():
            return 0.0
        best = None
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:
                continue
            ts, c = r.get("ts"), r.get("cash_aud")
            if ts is None or c is None:
                continue
            t = pd.Timestamp(str(ts)).tz_localize(None) if pd.Timestamp(str(ts)).tzinfo else pd.Timestamp(str(ts))
            if best is None or t < best[0]:
                best = (t, float(c))
        return float(best[1]) if best else 0.0
    except Exception:
        return 0.0


def compute_actual_nav_series_spliced(prices, fills_path, seed_path,
                                      broker_nav_path=None,
                                      fx_usdaud=None) -> pd.Series:
    """Actual-NAV path: fills-log reconstruction, upgraded to BROKER truth
    where ibkr_nav_log.jsonl has data (user directive 2026-07-08 — the
    fund's performance record is the broker's number).

    Currency subtlety: the reconstruction lives in the mixed USD/AUD price
    convention while NetLiquidation is pure AUD, so LEVELS cannot be mixed.
    We chain-link instead: reconstruction path up to the first broker
    snapshot, broker RETURNS applied cumulatively from there. On overlap
    days a return divergence > 50bps prints a [drift][WARN] — that gap is
    reconstruction error (missed fees/marks), worth knowing about."""
    recon = compute_actual_nav_series(prices, fills_path, seed_path,
                                      fx_usdaud=fx_usdaud,
                                      cash_aud=_first_broker_cash(broker_nav_path))
    broker = _load_broker_nav_series(broker_nav_path)
    if len(broker) < 2 or recon.empty:
        globals()["LAST_NAV_SOURCE"] = "fills recon only (no broker log)"
        return recon
    seam = broker.index[0]
    recon_at_seam = recon.reindex(recon.index.union(broker.index)).ffill().asof(seam)
    if not np.isfinite(recon_at_seam) or recon_at_seam <= 0:
        return recon
    spliced_tail = recon_at_seam * (broker / float(broker.iloc[0]))
    out = pd.concat([recon[recon.index < seam], spliced_tail]).sort_index()
    # Validate on the OVERLAP, and refuse to extrapolate if it fails.
    #
    # The reconstruction derives a NAV PATH from lots_seed.json, which is a
    # broker POSITION SNAPSHOT with nominal AcqDates — not a transaction
    # history. IBKR serves no fill history and every order in the fills log
    # shows qty_filled=0 / qty_remaining=qty_requested / is_done=false, i.e.
    # genuinely unfilled. You cannot recover a path from a snapshot, so where
    # the seed's nominal dates disagree with what was actually held the
    # reconstruction is simply wrong — measured 11-15% below broker before
    # 2026-08-04, converging only once the seed caught up.
    #
    # So: if it cannot match broker on the days where BOTH exist, it has not
    # earned the right to speak for the days where only it exists. Fall back
    # to broker-only rather than emit a plausible-looking fiction.
    try:
        rr = recon.pct_change().reindex(broker.index).dropna()
        br = broker.pct_change().dropna()
        both = rr.index.intersection(br.index)
        err = (rr.reindex(both) - br.reindex(both)).abs().dropna()
        if len(err) >= 3:
            med = float(err.median())
            n_bad = int((err > 0.005).sum())
            if med > RECON_MAX_MEDIAN_DAILY_ERR:
                print(f"[nav][WARN] fills-log reconstruction FAILED validation "
                      f"(median daily error {med*100:.2f}% over {len(err)} overlap "
                      f"day(s), {n_bad} above 50bps). lots_seed.json is a position "
                      f"SNAPSHOT, not a transaction history, so the pre-broker path "
                      f"cannot be derived — using BROKER-ONLY NAV rather than "
                      f"splicing an unvalidated head.")
                globals()["LAST_NAV_SOURCE"] = "broker NetLiq only (recon failed validation)"
                return broker
            if n_bad:
                print(f"[nav] reconstruction passed validation (median daily error "
                      f"{med*100:.2f}%) with {n_bad} day(s) above 50bps")
    except Exception:
        pass
    globals()["LAST_NAV_SOURCE"] = "fills recon (validated) + broker NetLiq"
    return out
