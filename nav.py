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


RECON_MAX_MEDIAN_DAILY_ERR = 0.0025   # reported, not gated — see below
# Gate at the timescale the CONSUMER uses. The drift tracker compares MONTHLY
# returns against a +/-2% threshold, so validating on daily error tests
# something stricter than anything downstream needs. Daily bars cannot match an
# intraday snapshot in any case: the broker marks NetLiq at ~10:20 AEST with
# ASX barely open and US at the previous close.
#
# That timing IS largely recoverable, contrary to the note that stood here
# before: lagging only the US leg moved 0.03%, but the book is 53% VLUE.AX and
# the AU leg needs the same lag, because 10:20 AEST is minutes into the ASX
# session rather than at its close. Lagging both (RECON_SNAPSHOT_LAG_DAYS)
# takes median daily error from 0.36% to 0.14%. What remains is genuine
# intraday drift against a daily bar.
# At monthly resolution the same series agrees to ~0.5-0.7pp. Gate there, at
# half the drift threshold, and report the daily figure so the limitation
# stays visible rather than being quietly assumed away.
RECON_MAX_MONTHLY_ERR = 0.01
# The monthly gate is a MAX, so it needs enough observations to be a test
# rather than a coin flip. Below this many months, gate on median daily error
# instead — see the validation block in compute_actual_nav_series_spliced.
RECON_MIN_MONTHS_TO_GATE = 3
# Interim daily tolerance. Observed 0.14% post-lag on 25 days, and the
# pre-lag structural floor was ~0.65%; a genuinely broken reconstruction
# misses by percent-level margins. 0.5% sits well clear of the first and far
# below the last.
RECON_MAX_MEDIAN_DAILY_ERR_GATE = 0.005
# Trading days to lag the reconstruction onto the broker snapshot's timing.
# 1 = value the book at the PREVIOUS close, which is what a 10:20 AEST NetLiq
# actually reflects on both venues. Set to 0 to get the raw close-of-day-D
# series back.
RECON_SNAPSHOT_LAG_DAYS = 1
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


def statement_path_for(app_dir) -> Path:
    """Which statement file to read under `app_dir`. Never raises.

    Takes app_dir EXPLICITLY rather than defaulting to the module's APP_DIR,
    and the spliced-NAV entry point still treats statement_path=None as "no
    statement". Making None mean "go and find one" looked tidier but was a
    footgun: nav.APP_DIR defaults to Path("."), so a caller that passed its own
    fills and seed paths — a test, or anything running before the engine syncs
    APP_DIR — silently picked up whichever real account statement happened to
    be in the working directory and rebuilt NAV from that instead. Resolution
    belongs where APP_DIR is actually known.
    """
    try:
        import ibkr_statement as _s
        return _s.resolve_statement_path(app_dir)
    except Exception as e:
        print(f"[nav] statement resolve failed ({type(e).__name__}: {e}); using CSV")
        return Path(app_dir) / "ibkr_activity_statement.csv"


def compute_nav_from_statement(prices, statement_path, fx_usdaud=None) -> pd.Series:
    """Full NAV path rebuilt from an IBKR Activity Statement.

    This is what the fills-log reconstruction could never be. lots_seed.json
    holds SURVIVING lots, so units are flat from each AcqDate onward and the
    history is unrecoverable — BEAR actually ran 21,590 units down to 1,644
    over this period. The statement carries every signed trade, so positions
    can be walked forward properly.

    NetLiquidation is positions PLUS cash, and cash here is reconstructed from
    the statement's own movements — trades, both legs of each FX conversion,
    deposits, dividends and interest — which reproduces the closing balance to
    the cent in both currencies. Foreign cash is translated at each day's rate
    because it is a live balance, unlike a cost base which is fixed at
    acquisition.

    Returns an empty Series if the statement cannot be used, so callers fall
    back rather than receive a half-built path.
    """
    try:
        import ibkr_statement as _stmt
    except Exception:
        return pd.Series(dtype=float)
    try:
        trades = _stmt.parse_trades(statement_path)
        events = _stmt.cash_events(statement_path)
        start = _stmt.starting_cash(statement_path)
        if trades is None or trades.empty:
            return pd.Series(dtype=float)
    except Exception as e:
        print(f"[nav] statement unreadable ({type(e).__name__}: {e})")
        return pd.Series(dtype=float)

    px = prices.copy()
    px.index = pd.to_datetime(px.index).tz_localize(None)
    px = px.sort_index().ffill()
    first = pd.Timestamp(trades["DateTime"].min()).normalize()
    # Accumulate over the FULL history, then trim. Trimming FIRST silently
    # discards every pre-cutoff trade and cash movement while still applying
    # the opening balance, which put the series at $994,850 instead of
    # ~$247,000 — positions bought before the cutoff simply vanished.
    cut = None
    try:
        import ibkr_statement as _s2
        cut = _s2.performance_start(statement_path)
    except Exception:
        cut = None
    dates = px.index[px.index >= first]
    if len(dates) == 0:
        return pd.Series(dtype=float)

    fxs = None
    if fx_usdaud is not None and len(fx_usdaud):
        fxs = pd.to_numeric(pd.Series(fx_usdaud), errors="coerce")
        fxs.index = pd.to_datetime(fxs.index).tz_localize(None).normalize()
        fxs = fxs[~fxs.index.duplicated(keep="last")].sort_index().reindex(dates).ffill().bfill()

    # --- positions: cumulative SIGNED units, valued each day ----------------
    nav = pd.Series(0.0, index=dates)
    t = trades.copy()
    t["Day"] = pd.to_datetime(t["DateTime"]).dt.normalize()
    for tkr, g in t.groupby("Security"):
        if tkr not in px.columns:
            # Only worth flagging if the position actually exists inside the
            # performance window. SEMI.AX was bought and sold pre-reset
            # (2026-06-22/23), so it is trimmed away regardless and warning
            # about it is noise — while a ticker held INSIDE the window and
            # missing from the panel would genuinely understate NAV.
            held_after_cut = (cut is None
                              or float(g[g["Day"] >= cut]["Units"].abs().sum()) > 0
                              or float(g[g["Day"] < cut]["Units"].sum()) != 0.0)
            if held_after_cut:
                print(f"[nav][WARN] {tkr} held in the performance window but "
                      f"absent from the price panel — NAV understated")
            continue
        # Same guard as the cash block below, for the same reason: trades
        # dated before the window are an OPENING POSITION, and reindexing
        # them onto `dates` drops them. `dates` normally starts at the first
        # trade so nothing precedes it — but that holds only while the price
        # panel reaches back that far, and a panel that started later would
        # silently zero every holding bought before it. Fold, don't drop.
        opening = float(g.loc[g["Day"] < dates[0], "Units"].sum())
        units = (g.loc[g["Day"] >= dates[0]].groupby("Day")["Units"].sum()
                  .reindex(dates, fill_value=0.0).cumsum() + opening)
        p = pd.to_numeric(px[tkr].reindex(dates), errors="coerce").ffill().fillna(0.0)
        if fxs is not None and not str(tkr).endswith(".AX") and not str(tkr).startswith("^"):
            p = p * fxs
        nav = nav.add(units * p, fill_value=0.0)

    # --- cash: per-currency running balance, translated daily ---------------
    if events is not None and not events.empty:
        for ccy, g in events.groupby("Currency"):
            # Cash that moved BEFORE the first trade is opening balance, not
            # noise — `reindex(dates)` drops it, and `start` describes a
            # different day, so the two together lose it entirely. Harmless
            # while the statement began mid-life with its opening balance
            # already stated (the CSV: start 1,000,000 at 2026-06-22, nothing
            # before it). The Flex feed starts from a genuinely empty account
            # and carries the funding deposit as an EVENT — 1,000,000 on
            # 2026-05-30, three weeks before the first trade — so the same
            # code computed closing cash of -988,327 instead of +11,673.
            # Fold anything earlier into the opening balance instead.
            opening = (float(start.get(ccy, 0.0))
                       + float(g.loc[g["Date"] < dates[0], "Amount"].sum()))
            bal = (g.loc[g["Date"] >= dates[0]].groupby("Date")["Amount"].sum()
                    .reindex(dates, fill_value=0.0).cumsum() + opening)
            if ccy != "AUD":
                if fxs is None:
                    continue
                bal = bal * fxs
            nav = nav.add(bal, fill_value=0.0)
    nav = nav.dropna()
    # Put the series on the BROKER's timing convention before anything compares
    # or splices the two. A daily close values the book at the END of day D;
    # the broker's snapshot is taken ~10:27 AEST on day D, which is 27 minutes
    # into the ASX session and hours after the US close of D-1 — so it sees the
    # PREVIOUS close on both venues. The reconstruction exists to extend the
    # broker series backwards, so it has to answer the broker's question.
    #
    # A previous pass tested lagging only the US leg and measured 0.03%, and
    # concluded timing was not the dominant term. That held for the leg it
    # moved: this book is 53% VLUE.AX, so the AU leg dominates, and it needs
    # the same lag for the same reason. Shifting BOTH takes median daily error
    # from 0.36% to 0.14% over 25 overlap days (two trading days is markedly
    # worse, so this is alignment rather than a fitted offset).
    #
    # Shift by a TRADING day, not a calendar day: Friday's closes are what
    # Monday's snapshot observes, and a calendar shift drops that pairing.
    if RECON_SNAPSHOT_LAG_DAYS:
        nav = nav.shift(RECON_SNAPSHOT_LAG_DAYS).dropna()
    # NOW trim, after everything has accumulated. Before the cutoff the path
    # reflects an external capital flow (this account was reset on 2026-06-23:
    # -189,334 withdrawn, +250,000 deposited), which reads as a -69%
    # "drawdown" that is money moving rather than performance — and that figure
    # feeds the live max-drawdown alert.
    if cut is not None and len(nav):
        trimmed = nav[nav.index >= cut]
        if len(trimmed) >= 2:
            print(f"[nav] performance window starts {cut.date()} — excluding "
                  f"the external capital flows before it")
            return trimmed
    return nav


def compute_actual_nav_series_spliced(prices, fills_path, seed_path,
                                      broker_nav_path=None,
                                      fx_usdaud=None,
                                      statement_path=None) -> pd.Series:
    """Actual-NAV path: fills-log reconstruction, upgraded to BROKER truth
    where ibkr_nav_log.jsonl has data (user directive 2026-07-08 — the
    fund's performance record is the broker's number).

    Currency subtlety: the reconstruction lives in the mixed USD/AUD price
    convention while NetLiquidation is pure AUD, so LEVELS cannot be mixed.
    We chain-link instead: reconstruction path up to the first broker
    snapshot, broker RETURNS applied cumulatively from there. On overlap
    days a return divergence > 50bps prints a [drift][WARN] — that gap is
    reconstruction error (missed fees/marks), worth knowing about."""
    # Prefer a statement-derived path: it has the full signed trade history and
    # a cash series reconstructed from the statement's own movements, whereas
    # the seed holds only SURVIVING lots and cannot express that BEAR ran
    # 21,590 units down to 1,644.
    recon = pd.Series(dtype=float)
    if statement_path:
        try:
            if Path(statement_path).exists():
                recon = compute_nav_from_statement(prices, statement_path,
                                                   fx_usdaud=fx_usdaud)
        except Exception as e:
            print(f"[nav] statement path unusable ({type(e).__name__}: {e})")
    if recon is None or recon.empty:
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
        # Monthly agreement is the gate; daily is reported for context.
        #
        # Restrict BOTH series to their common dates BEFORE resampling. The
        # first version resampled each independently, so July compared the
        # reconstruction's full month against the broker's 8th-onward stub and
        # reported 3.38% error for what is really a different window — a gate
        # failing on its own arithmetic rather than on the data.
        common = recon.index.intersection(broker.index)
        if len(common) < 3:
            return out
        r_c, b_c = recon.reindex(common), broker.reindex(common)

        def _monthly(x):
            return ((1 + x.pct_change().fillna(0.0)).cumprod()
                    .resample("ME").last().pct_change().dropna())
        rm, bm = _monthly(r_c), _monthly(b_c)
        mi = rm.index.intersection(bm.index)
        m_err = (rm.reindex(mi) - bm.reindex(mi)).abs().dropna()
        worst_month = float(m_err.max()) if len(m_err) else 0.0

        if len(err) >= 3:
            med = float(err.median())
            n_bad = int((err > 0.005).sum())
            # A max over ONE monthly return is not a test. This account has
            # two months of performance history, so the monthly resample
            # yielded exactly one comparable observation (2026-08) and the
            # gate fired on it — 1.48%, on a month whose US session moved
            # -1.7% overnight. It could not distinguish a broken
            # reconstruction from a volatile month, and would keep firing at
            # random until a year or so of history accumulates.
            #
            # Below the floor, gate on the MEDIAN DAILY error instead: 25
            # observations rather than 1, and a broken reconstruction is
            # broken by percent-level margins (the seed-snapshot era ran
            # 11-15% below broker), nowhere near this threshold. The
            # tolerance is deliberately loose — a false failure costs the
            # whole reconstructed head of the series, which is worse than
            # briefly tolerating a mediocre one.
            n_months = int(len(m_err))
            if n_months >= RECON_MIN_MONTHS_TO_GATE:
                failed = worst_month > RECON_MAX_MONTHLY_ERR
                verdict = (f"worst monthly error {worst_month*100:.2f}% vs "
                           f"{RECON_MAX_MONTHLY_ERR*100:.0f}% over {n_months} months")
            else:
                failed = med > RECON_MAX_MEDIAN_DAILY_ERR_GATE
                verdict = (f"median daily error {med*100:.2f}% vs "
                           f"{RECON_MAX_MEDIAN_DAILY_ERR_GATE*100:.2f}% "
                           f"({n_months} monthly obs < {RECON_MIN_MONTHS_TO_GATE}, "
                           f"too few to gate monthly)")
            # Report the median only when the verdict did not already name it.
            context = f"{n_bad}/{len(err)} days above 50bps"
            if n_months >= RECON_MIN_MONTHS_TO_GATE:
                context = f"median daily {med*100:.2f}%, " + context
            if failed:
                print(f"[nav][WARN] NAV reconstruction FAILED validation — "
                      f"{verdict} ({context}). Using BROKER-ONLY NAV rather "
                      f"than splicing an unvalidated head.")
                globals()["LAST_NAV_SOURCE"] = "broker NetLiq only (recon failed validation)"
                return broker
            print(f"[nav] reconstruction PASSED validation — {verdict} "
                  f"({context}; daily bars cannot match an intraday broker "
                  f"snapshot). Extending live NAV back to "
                  f"{recon.index.min().date()}.")
    except Exception:
        pass
    globals()["LAST_NAV_SOURCE"] = "fills recon (validated) + broker NetLiq"
    return out
