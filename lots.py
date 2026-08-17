"""Lot-book construction + expansion (module split #18, 2026-07-09).

Pure functions — no engine globals. Reconstruct the FIFO/HIFO lot book from
its authoritative sources and expand trades into per-lot CGT rows:
  _read_lots_from_path        Read a Lots sheet (Excel) into a normalised frame.
  _build_lots_from_fills_log  Rebuild lots from ibkr_fills_log.jsonl (broker truth).
  _build_lots_from_holdings   Seed lots from current Holdings units + last prices.
  expand_with_lots            Expand a trade frame into per-lot sale rows (FIFO/HIFO).

Engine re-exports these from its namespace for backward compat. The deprecated
_update_lots_after_trades stays in the engine (it reads the LOT_MATCH_METHOD global).
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

# expand_with_lots matches sells to lots using cgt's trade-frame helpers.
from cgt import _trade_delta_col, _security_from_row

# --- lot-book vs broker reconciliation thresholds -------------------------
# Units are whole shares, so anything above half a share is a real break, not
# rounding. 50bps on average cost is deliberately loose: the lot book converts
# each fill at its fx_map rate, so small FX/rounding differences are expected
# and we only want to hear about genuine cost-basis corruption.
LOT_RECON_UNIT_TOL = 0.5
LOT_RECON_COST_BPS_TOL = 50.0


def _fill_timestamp(r: dict):
    """Best-effort tz-naive pd.Timestamp for a fills-log row, or None.

    Mirrors the acquisition-date precedence used when building lots
    (exec_timestamp, else rec_log_run_at). Naive because the fills log writes
    naive ISO ("2026-06-22T11:53:24") and the seed watershed is naive too;
    mixing tz-aware and naive raises on comparison.
    """
    ts = r.get("exec_timestamp") or r.get("rec_log_run_at")
    if not ts:
        return None
    try:
        t = pd.Timestamp(ts)
    except Exception:
        return None
    if t is pd.NaT:
        return None
    try:
        if t.tzinfo is not None:
            t = t.tz_localize(None)
    except Exception:
        return None
    return t


def _normalise_fx_map(fx_map) -> dict:
    """Normalise a ticker -> AUD-rate map (Series, dict, or None) to a plain
    dict, dropping missing/non-positive rates."""
    if isinstance(fx_map, pd.Series):
        return {str(k).strip(): float(v) for k, v in fx_map.items()
                if pd.notna(v) and float(v) > 0}
    if isinstance(fx_map, dict):
        return {str(k).strip(): float(v) for k, v in fx_map.items()
                if v is not None and float(v) > 0}
    return {}


def _implied_local_avg(g, fx_hist):
    """Lot-book average cost expressed in the asset's LOCAL currency.

    Each lot's CostBaseAUD was fixed at ACQUISITION — that is what AU CGT
    requires, translation at the rate prevailing on the acquisition date — so
    dividing by the SAME date's rate recovers the local cost the broker
    reports. Returns None when any lot's date or rate is unusable, so the
    caller can fall back rather than compare a half-converted number.
    """
    if fx_hist is None or len(fx_hist) == 0:
        return None
    try:
        fxs = pd.Series(fx_hist).copy()
        fxs.index = pd.to_datetime(fxs.index).tz_localize(None).normalize()
        fxs = fxs[~fxs.index.duplicated(keep="last")].sort_index()
    except Exception:
        return None
    tot_u = 0.0
    tot_v = 0.0
    for _, row in g.iterrows():
        try:
            u = float(row["Units"])
            aud = float(row["CostBaseAUD"])
            # NORMALISE. Lot timestamps carry sub-second precision
            # ('2026-08-03T09:33:30.149710') and Series.asof against a
            # date-resolution index raises "Cannot losslessly convert units",
            # which silently sank this whole comparison. A CGT translation
            # uses the acquisition DATE anyway.
            d = pd.Timestamp(row["AcqDate"]).tz_localize(None).normalize()
        except Exception:
            return None
        if u <= 0 or not np.isfinite(aud):
            continue
        try:
            f = float(fxs.asof(d))
        except Exception:
            return None
        if not np.isfinite(f) or f <= 0:
            return None
        tot_u += u
        tot_v += u * (aud / f)
    return (tot_v / tot_u) if tot_u > 0 else None


def reconcile_lots_vs_broker(lots_df, broker_positions, fx_map=None, *,
                             fx_hist=None,
                             unit_tol: float = LOT_RECON_UNIT_TOL,
                             cost_bps_tol: float = LOT_RECON_COST_BPS_TOL) -> list:
    """Compare the rebuilt lot book against broker-truth positions.

    Returns a list of human-readable warning strings; empty means reconciled.
    Pure — prints nothing, raises nothing.

    WHY THIS EXISTS: the lot book is rebuilt from ibkr_fills_log.jsonl on every
    run, and _build_lots_from_fills_log only counts rows with qty_filled > 0.
    When ibkr_paper_exec.py exits while an order is still PreSubmitted, the row
    freezes at qty_filled=0 and a REAL fill is silently dropped — the lot book
    then keeps a position that was sold and misses one that was bought, so the
    CGT cost bases are wrong and the 6W cadence anchor never sees the fill.
    That is precisely what happened to the 2026-07-08 SOXL/VEA pair. TWS serves
    no historical executions (reqExecutions returns 0 even with a 30d filter,
    verified 2026-07-17), so the broker's avg_cost_local is the ONLY after-the-
    fact cost anchor available — hence checking against it every run.

    Units are compared first and directly: that is currency-free and catches a
    missed fill outright.

    Average cost is compared in LOCAL currency when `fx_hist` is supplied.
    The earlier design converted the broker's local cost at the CURRENT rate
    and claimed "an FX error cancels on both sides" — it does not. The lot
    book's CostBaseAUD was fixed at ACQUISITION (which is what AU CGT
    requires), so only one side moves with the spot rate and the check drifts
    apart as FX does. On 2026-08-17 that reported PDBC +115bps, VEA +115bps
    and SMH +180bps as "CGT cost base may be wrong" when the cost bases were
    fine — the AUD had simply moved ~1.1% since acquisition. Re-expressing
    each lot at its own acquisition-date rate brings all three inside ±70bps.

    Without `fx_hist` the old AUD comparison is used for foreign tickers, but
    the warning says the number is FX-drift-contaminated rather than implying
    a cost-base error.
    """
    warnings: list = []
    if not broker_positions:
        return warnings

    fx_lookup = _normalise_fx_map(fx_map)
    # "_ts" and any future metadata keys are not tickers.
    broker = {str(k).strip().upper(): v
              for k, v in broker_positions.items()
              if not str(k).startswith("_") and isinstance(v, dict)}

    lot_units: dict = {}
    lot_avg: dict = {}
    lot_local: dict = {}
    if lots_df is not None and len(lots_df) > 0:
        df = lots_df.copy()
        df["Security"] = df["Security"].astype(str).str.strip().str.upper()
        df["Units"] = pd.to_numeric(df["Units"], errors="coerce").fillna(0.0)
        df["CostBaseAUD"] = pd.to_numeric(df["CostBaseAUD"], errors="coerce")
        for sec, g in df.groupby("Security"):
            lot_units[sec] = float(g["Units"].sum())
            # CostBaseAUD is PER UNIT (see _build_lots_from_fills_log), so the
            # book-level average is units-weighted across that ticker's lots.
            valid = g[g["CostBaseAUD"].notna() & (g["Units"] > 0)]
            tot = float(valid["Units"].sum())
            if tot > 0:
                lot_avg[sec] = float(
                    (valid["Units"] * valid["CostBaseAUD"]).sum() / tot)
                if "AcqDate" in valid.columns:
                    _il = _implied_local_avg(valid, fx_hist)
                    if _il is not None and _il > 0:
                        lot_local[sec] = _il

    for tkr in sorted(set(lot_units) | set(broker)):
        b = broker.get(tkr)
        b_units = float(b.get("units") or 0.0) if b else 0.0
        l_units = float(lot_units.get(tkr, 0.0))

        if abs(l_units - b_units) > unit_tol:
            warnings.append(
                f"{tkr}: lot book has {l_units:g} units, broker has {b_units:g} "
                f"— a fill the log missed (qty_filled=0) is the usual cause")
            # Cost basis is meaningless while the units disagree.
            continue

        if b is None:
            continue
        b_avg_local = b.get("avg_cost_local")
        l_avg_aud = lot_avg.get(tkr)
        # avg_cost_local is absent on rows written before 2026-07-17.
        if b_avg_local is None or l_avg_aud is None or b_units <= 0:
            continue

        is_local_aud = tkr.endswith(".AX")
        l_local = lot_local.get(tkr)
        if l_local is not None and not is_local_aud:
            # Currency-free: both sides in the asset's own currency.
            bps = (l_local - float(b_avg_local)) / float(b_avg_local) * 1e4
            if abs(bps) > cost_bps_tol:
                warnings.append(
                    f"{tkr}: lot-book avg cost {l_local:,.4f} vs broker "
                    f"{float(b_avg_local):,.4f} (local ccy, {bps:+.0f}bps) "
                    f"— CGT cost base may be wrong")
            continue

        fx = fx_lookup.get(tkr, 1.0 if is_local_aud else None)
        if not fx or fx <= 0:
            continue
        b_avg_aud = float(b_avg_local) * float(fx)
        if b_avg_aud <= 0:
            continue
        bps = (l_avg_aud - b_avg_aud) / b_avg_aud * 1e4
        if abs(bps) > cost_bps_tol:
            # Say WHICH comparison produced the number, and don't advise
            # passing fx_hist when it was passed and turned out unusable.
            _caveat = ("" if is_local_aud else
                       (" [spot-FX comparison — includes FX drift since "
                        "acquisition; fx_hist unusable for this ticker]"
                        if fx_hist is not None else
                        " [spot-FX comparison — includes FX drift since "
                        "acquisition; pass fx_hist for a currency-free check]"))
            warnings.append(
                f"{tkr}: lot-book avg cost A${l_avg_aud:,.4f} vs broker "
                f"A${b_avg_aud:,.4f} ({bps:+.0f}bps) — CGT cost base may be "
                f"wrong{_caveat}")

    return warnings


# ---------------------------------------------------------------------------
# Seed reconciliation — turning the drift WARNING into a CORRECTION
# ---------------------------------------------------------------------------
# reconcile_lots_vs_broker (above) only reports. It has reported the same drift
# every run since 2026-07-28 because nothing acts on it, and the drift only
# grows: by 2026-08-03 the book held VLUE.AX 3374 units against a broker 2974.
#
# WHY THE FILLS LOG CANNOT BE REPAIRED. The obvious fix — go back and fill in
# qty_filled from the broker — is impossible on this account, verified twice:
#   * reqExecutions returns 0 even with a 30-day ExecutionFilter (2026-07-17);
#   * ib.trades() is scoped to the CURRENT API session, and the daily wrapper
#     restarts IB Gateway, so yesterday's orders are simply gone.
# --check-fills can therefore only ever catch fills that land while the session
# that placed them is still alive. Anything filling after the run exits (every
# US leg, since the US session is 23:30-06:00 AEST) is unrecoverable from the
# API. The fills log is structurally incomplete and no amount of re-querying
# changes that.
#
# WHAT SURVIVES is the broker's position + averageCost, which IS exact (units *
# (mark_local - avg_cost_local) reproduces unrealizedPNL to the cent). So the
# lot book is reconciled to POSITIONS rather than reconstructed from fills.
#
# COST BASE IS DELIBERATELY NOT REWRITTEN on units that already exist. For an
# AU investor the CGT cost base of a foreign asset is the AUD amount at the
# ACQUISITION date, so re-deriving it from the broker's local avg cost at
# TODAY's FX would corrupt a correct historical figure — it would look tidier
# and be wrong. Only genuinely new units get a cost, and they are converted at
# the snapshot's own implied FX (they were acquired within a day or so of it).

def reconcile_seed_to_broker(seed_lots, broker_positions, *, as_of, fx_map=None,
                             unit_tol: float = LOT_RECON_UNIT_TOL) -> tuple:
    """Reconcile a lot seed to broker-truth positions. PURE — no I/O.

    Returns (new_seed_lots, actions) where actions is a list of dicts
    {ticker, action, units, detail} describing every change, for logging and
    for the operator to review. Action is one of:
      ok            — units agree, nothing changed
      added         — broker holds MORE: a buy the fills log missed
      opened        — ticker absent from the seed entirely
      reduced       — broker holds FEWER: a sell the fills log missed (FIFO)
      closed        — position fully exited
      unpriceable   — broker units exist but no usable cost anchor; left alone

    New units are priced so the book's units-weighted average cost lands on the
    broker's, which keeps the TOTAL cost base exact rather than merely close.
    If that implied per-unit cost comes out nonsensical (<=0, or more than 5x
    the broker average — which means the pre-existing book cost was already
    wrong), it is rejected in favour of the plain broker average and flagged.
    """
    actions: list = []
    broker = {str(k).strip().upper(): v
              for k, v in (broker_positions or {}).items()
              if not str(k).startswith("_") and isinstance(v, dict)}

    by_ticker: dict = {}
    for lot in (seed_lots or []):
        by_ticker.setdefault(str(lot.get("Security", "")).strip().upper(),
                             []).append(dict(lot))

    as_of_iso = pd.Timestamp(as_of).to_pydatetime().isoformat()
    out: list = []

    for tkr in sorted(set(by_ticker) | set(broker)):
        lots = sorted(by_ticker.get(tkr, []),
                      key=lambda L: pd.Timestamp(L.get("AcqDate")))
        book_units = sum(float(L.get("Units") or 0.0) for L in lots)
        b = broker.get(tkr)
        b_units = float(b.get("units") or 0.0) if b else 0.0

        if b is None or b_units <= 0:
            if lots:
                actions.append({"ticker": tkr, "action": "closed",
                                "units": -book_units,
                                "detail": (f"broker holds none; dropping "
                                           f"{book_units:g} book unit(s). "
                                           f"Realised CGT on the exit is NOT "
                                           f"recoverable — the sale price was "
                                           f"never logged")})
            continue

        delta = b_units - book_units
        if abs(delta) <= unit_tol:
            out.extend(lots)
            actions.append({"ticker": tkr, "action": "ok", "units": 0.0,
                            "detail": f"{b_units:g} units agree"})
            continue

        if delta < 0:
            # Broker holds fewer — units were sold. Reduce FIFO (oldest first),
            # matching the engine's LOT_MATCH_METHOD.
            to_drop = -delta
            kept = []
            for L in lots:
                u = float(L.get("Units") or 0.0)
                if to_drop <= 0:
                    kept.append(L)
                elif u <= to_drop:
                    to_drop -= u
                else:
                    L = dict(L)
                    L["Units"] = u - to_drop
                    to_drop = 0.0
                    kept.append(L)
            out.extend(kept)
            actions.append({"ticker": tkr, "action": "reduced", "units": delta,
                            "detail": (f"broker {b_units:g} < book "
                                       f"{book_units:g}; released {-delta:g} "
                                       f"unit(s) FIFO. Realised CGT on that "
                                       f"sale is NOT recoverable")})
            continue

        # Broker holds more — units were bought. Price the new lot.
        b_avg_local = b.get("avg_cost_local")
        fx = (fx_map or {"AUD": 1.0}).get(
            str(b.get("currency") or "").upper() or "AUD")
        if b_avg_local is None or not fx or fx <= 0:
            out.extend(lots)
            actions.append({"ticker": tkr, "action": "unpriceable",
                            "units": delta,
                            "detail": (f"broker holds {delta:g} more unit(s) "
                                       f"but no usable cost/FX anchor — left "
                                       f"alone, reconcile manually")})
            continue

        b_avg_aud = float(b_avg_local) * fx
        book_cost = sum(float(L.get("Units") or 0.0)
                        * float(L.get("CostBaseAUD") or 0.0) for L in lots)
        implied = (b_units * b_avg_aud - book_cost) / delta
        note = ""
        if not (0 < implied <= 5 * b_avg_aud):
            note = (f" (implied unit cost A${implied:,.4f} rejected as "
                    f"nonsensical — the pre-existing book cost was already "
                    f"wrong; used broker average instead)")
            implied = b_avg_aud

        out.extend(lots)
        out.append({
            "Security": tkr,
            # Dated at the snapshot, not backdated: it is the earliest date we
            # can EVIDENCE, and a later AcqDate delays 12-month LT-discount
            # eligibility — i.e. it errs toward over-provisioning tax, never
            # under. Backdating to win the discount would be a guess in the
            # taxpayer's favour with no support.
            "AcqDate": as_of_iso,
            "Units": delta,
            "CostBaseAUD": round(implied, 7),
            "SeedAsOf": as_of_iso,
        })
        actions.append({
            "ticker": tkr, "action": ("opened" if not lots else "added"),
            "units": delta,
            "detail": (f"broker {b_units:g} > book {book_units:g}; added "
                       f"{delta:g} unit(s) @ A${implied:,.4f}{note}"),
        })

    # One watershed for the whole file: every fills-log row at or before this
    # stamp is already reflected in the seed and must not be replayed.
    for lot in out:
        lot["SeedAsOf"] = as_of_iso
    return out, actions


def derive_fx_from_snapshot(snapshot: dict) -> dict:
    """{CURRENCY: local->AUD rate} implied by a broker NAV snapshot row.

    `mkt_value_base` IS A MISNOMER — it is the position's value in its own
    TRADING currency, not the account base currency. ib_insync's
    PortfolioItem.marketValue is local, and the log field was named "base"
    anyway. Verified exactly against 2026-08-03: mkt_value_base equals
    units * mark_local to the cent for the USD rows (PDBC, SMH, VEA) as well
    as the AUD ones. So a per-row FX cannot be read off a single position —
    dividing one by the other just yields 1.0 and silently books US cost bases
    ~42% light. (That is the AUD-vs-local mixup this codebase keeps hitting.)

    The account-level identity is what carries the rate: `gross_positions_aud`
    IS genuine AUD, so once the AUD positions are subtracted the residual is
    the foreign block's AUD value. Requires exactly ONE foreign currency to be
    unambiguous — the universe is AUD + USD, and if a third ever appears this
    returns {} rather than guessing.
    """
    out = {"AUD": 1.0}
    snapshot = snapshot or {}
    positions = snapshot.get("positions") or []
    try:
        gross_aud = float(snapshot.get("gross_positions_aud"))
    except (TypeError, ValueError):
        return out

    aud_local = 0.0
    foreign: dict = {}
    for p in positions:
        try:
            local_val = float(p.get("units") or 0.0) * float(p.get("mark_local") or 0.0)
        except (TypeError, ValueError):
            continue
        ccy = str(p.get("currency") or "").upper()
        if ccy == "AUD":
            aud_local += local_val
        elif ccy:
            foreign[ccy] = foreign.get(ccy, 0.0) + local_val

    if len(foreign) != 1:
        return out
    ccy, block = next(iter(foreign.items()))
    if block <= 0:
        return out
    fx = (gross_aud - aud_local) / block
    # A plausible-rate guard: a wrong FX corrupts a CGT cost base silently,
    # so refuse rather than propagate a nonsense rate.
    if 0.1 < fx < 10.0:
        out[ccy] = fx
    return out


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


def _build_lots_from_fills_log(
    fills_path,
    fx_map: "pd.Series | dict | None" = None,
    lot_match_method: str = "HIFO",
    seed_path = None,
) -> pd.DataFrame:
    """Reconstruct the lot book from `ibkr_fills_log.jsonl`.

    The fills log is the authoritative record of what transacted. Only
    rows with `qty_filled > 0` contribute. BUYs add a new lot at
    `avg_fill_price_local` converted to AUD; SELLs decrement matched
    lots using HIFO/FIFO so the resulting book reflects the live
    position basis at any point in time.

    Optional `seed_path` — a JSON list of lot dicts (Security, AcqDate,
    Units, CostBaseAUD) representing positions that existed BEFORE the
    fills log started. Required when migrating an existing portfolio
    onto the engine; the triage script writes this file. Fills are
    replayed on top of the seed, so a sell after the seed correctly
    decrements the seed lot.

    SEED WATERSHED (2026-07-17). Each seed lot may carry `SeedAsOf`, the
    instant the snapshot was TAKEN. When present, fills at or before it are
    SKIPPED: the seed already reflects them, so replaying one would
    double-count. This matters because the seed is broker-derived while the
    fills log is order-derived — back-filling historical fills into the log
    (see ibkr_paper_exec.py --check-fills --write) would otherwise corrupt a
    freshly re-seeded book. `SeedAsOf` is deliberately NOT `AcqDate`: the
    2026-07-17 re-seed carries AcqDate 2026-07-08 (first broker evidence) yet
    was taken on 07-17. Absent (legacy seeds) -> no filtering, prior behaviour
    exactly. It is read but not returned — the Lots schema is fixed and
    nav.py/cgt.py read the same file.

    Diagnostics on the returned frame's `.attrs` (this module stays
    print-free): `seed_as_of` and `pre_seed_fills_skipped`.

    Cost basis AUD priority per BUY row:
      1. `avg_fill_price_local * fx_map[ticker]` if both present
      2. `rec_px_aud` (the engine's planned AUD price) as fallback
      3. row skipped if neither produces a positive cost basis

    Returns an empty Lots-format DataFrame if no seed AND no filled
    rows are available.
    """
    base_cols = ["Security", "AcqDate", "Units", "CostBaseAUD"]
    p = Path(fills_path) if not hasattr(fills_path, "exists") else fills_path

    seed_lots: list[dict] = []
    # SEED WATERSHED: the instant the seed snapshot was TAKEN. Every fill at or
    # before it is already baked into the seed's units/cost, so re-applying such
    # a fill would DOUBLE-COUNT. Note this is NOT AcqDate: the 2026-07-17 re-seed
    # carries AcqDate 2026-07-08 (first broker evidence) but was taken on 07-17,
    # so filtering on AcqDate would wrongly re-apply anything in between.
    # Absent (legacy seeds) -> stays None -> no filtering, i.e. exact prior
    # behaviour. Kept OUT of the returned frame: the Lots schema is fixed and
    # nav.py/cgt.py read this same file.
    seed_as_of = None
    if seed_path is not None:
        sp = Path(seed_path) if not hasattr(seed_path, "exists") else seed_path
        if sp.exists():
            try:
                seed_data = json.loads(sp.read_text(encoding="utf-8"))
                for item in seed_data:
                    try:
                        u = int(round(float(item.get("Units") or 0)))
                        cb = float(item.get("CostBaseAUD") or 0)
                        if u <= 0 or cb <= 0:
                            continue
                        _sao = item.get("SeedAsOf")
                        if _sao:
                            # tz-naive throughout: exec_timestamp is naive
                            # ("2026-06-22T11:53:24"), so a tz-aware watershed
                            # would raise on comparison.
                            _t = pd.Timestamp(_sao)
                            if _t.tzinfo is not None:
                                _t = _t.tz_localize(None)
                            seed_as_of = _t if seed_as_of is None else max(seed_as_of, _t)
                        seed_lots.append({
                            "Security": str(item.get("Security", "")).strip(),
                            "AcqDate": pd.Timestamp(item.get("AcqDate")),
                            "Units": u,
                            "CostBaseAUD": cb,
                        })
                    except Exception:
                        continue
            except Exception:
                pass

    if not p.exists() and not seed_lots:
        return pd.DataFrame(columns=base_cols)

    fx_lookup = _normalise_fx_map(fx_map)

    rows = []
    n_pre_seed = 0
    if p.exists():
        try:
            with open(p, "r", encoding="utf-8") as f:
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
                    if seed_as_of is not None:
                        _ts = _fill_timestamp(r)
                        # Undated fills can't be placed either side of the
                        # watershed. Drop them: with a seed present, applying an
                        # unplaceable fill risks double-counting, and silently
                        # inflating the book is worse than ignoring one row.
                        if _ts is None or _ts <= seed_as_of:
                            n_pre_seed += 1
                            continue
                    rows.append(r)
        except Exception:
            rows = []

    if not rows and not seed_lots:
        return pd.DataFrame(columns=base_cols)

    rows.sort(key=lambda r: r.get("exec_timestamp", ""))
    lots: list[dict] = list(seed_lots)

    for r in rows:
        sec = str(r.get("ticker", "")).strip()
        if not sec:
            continue
        qty = float(r.get("qty_filled") or 0)
        side = str(r.get("side", "")).upper()
        ts = r.get("exec_timestamp") or r.get("rec_log_run_at")
        try:
            acq = pd.Timestamp(ts)
        except Exception:
            acq = pd.Timestamp.utcnow()

        if side == "BUY":
            local_px = r.get("avg_fill_price_local")
            try:
                local_px = float(local_px) if local_px is not None else None
            except Exception:
                local_px = None
            fx = fx_lookup.get(sec, 1.0 if sec.endswith(".AX") else None)
            if local_px is not None and fx is not None and fx > 0:
                cost_aud = local_px * fx
            else:
                cost_aud = float(r.get("rec_px_aud") or 0.0)
            if cost_aud <= 0:
                continue
            lots.append({
                "Security": sec,
                "AcqDate": acq,
                "Units": int(round(qty)),
                "CostBaseAUD": cost_aud,
            })
        elif side == "SELL":
            block_idx = [i for i, lt in enumerate(lots) if lt["Security"] == sec]
            if not block_idx:
                continue
            if str(lot_match_method).upper() == "HIFO":
                block_idx.sort(key=lambda i: (-lots[i]["CostBaseAUD"], lots[i]["AcqDate"]))
            else:
                block_idx.sort(key=lambda i: lots[i]["AcqDate"])
            remaining = qty
            for i in block_idx:
                if remaining <= 0:
                    break
                have = float(lots[i]["Units"])
                take = min(remaining, have)
                lots[i]["Units"] = have - take
                remaining -= take
            lots = [lt for lt in lots if lt["Units"] > 0]

    out = pd.DataFrame(lots, columns=base_cols) if lots \
        else pd.DataFrame(columns=base_cols)
    # Diagnostics ride on .attrs rather than the frame: the Lots schema is fixed
    # (nav.py/cgt.py/the Excel sheet all read it) and this module stays
    # print-free. The engine surfaces these on the [lots] line.
    out.attrs["seed_as_of"] = seed_as_of
    out.attrs["pre_seed_fills_skipped"] = n_pre_seed
    return out


def _build_lots_from_holdings(
    units: "pd.Series | dict",
    last_px_aud: "pd.Series | dict",
    today: "pd.Timestamp | None" = None,
) -> pd.DataFrame:
    """CGT-naive baseline: single lot per held ticker at today's AUD price.

    Plumbed-but-not-wired alternative for users who don't have a fills
    history (e.g. moving off IBKR to a broker without per-fill export).
    Loses any real cost-basis history — every existing position is
    treated as freshly acquired at the current price.
    """
    base_cols = ["Security", "AcqDate", "Units", "CostBaseAUD"]
    if today is None:
        today = pd.Timestamp.utcnow().normalize()

    u = pd.Series(units, dtype=float) if not isinstance(units, pd.Series) else units.astype(float)
    px = pd.Series(last_px_aud, dtype=float) if not isinstance(last_px_aud, pd.Series) else last_px_aud.astype(float)

    rows = []
    for tk, qty in u.items():
        try:
            qty_i = int(round(float(qty)))
        except Exception:
            continue
        if qty_i <= 0:
            continue
        p = float(px.get(tk, 0.0) or 0.0)
        if p <= 0:
            continue
        rows.append({
            "Security": str(tk),
            "AcqDate": pd.Timestamp(today),
            "Units": qty_i,
            "CostBaseAUD": p,
        })
    if not rows:
        return pd.DataFrame(columns=base_cols)
    return pd.DataFrame(rows, columns=base_cols)


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
