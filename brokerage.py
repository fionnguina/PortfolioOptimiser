"""Broker fee profiles + transaction-cost computation.

Extracted from Portfolio_Optimiser.py for testability + module-split prep.
Pure functions taking trade_df + module-level constants for fee schedules.

Contains:
  * BROKER_PROFILES — fee schedules for IBKR, CMC, Saxo (Classic/Platinum),
    Tiger. Each profile has OOS backtest cost params (au/us flat fees +
    spread bps) AND live trade-plan params (min_fee, rate, first-buy promo).
  * ACTIVE_BROKER_PROFILE + BROKER_CONFIG — selected profile (canonical here;
    engine + IBKR scripts import it back).
  * BROKERAGE — derived dict the live `compute_brokerage` reads. Updates
    automatically when ACTIVE_BROKER_PROFILE changes (because both are
    module-level and re-evaluated at import).
  * MIN_TRADE_VALUE — sub-AUD-11 trades suppressed (round-trip noise).
  * _market_of, suppress_small_trades_by_value, compute_brokerage.

Cross-module dep: imports `_trade_delta_col`, `_security_from_row` from
cgt.py (those are the canonical homes for trade-df column lookup).

Used by:
  Portfolio_Optimiser.make_trade_plan, evaluate_transaction_costs,
  Portfolio_Optimiser._log_config_snapshot,
  ibkr_*.py scripts (read BROKER_CONFIG for the live fee schedule).
"""
from __future__ import annotations

import pandas as pd

from cgt import _trade_delta_col, _security_from_row


# === Broker fee profiles =====================================================
BROKER_PROFILES = {
    "cmc_markets": {
        "name":            "CMC Markets",
        # OOS backtest cost model
        "au_flat_fee_aud": 11.0,    # CMC: $11 min on AU equity / ETF
        "us_flat_fee_aud":  0.0,    # CMC: $0 fixed on US (FX is separate)
        "au_spread_bps":   5.0,
        "us_spread_bps":   5.0,
        "fx_spread_bps":  10.0,     # CMC AUD↔USD conversion
        # Live trade-plan brokerage
        "live_asx_min_fee":               11.0,
        "live_asx_rate":                  0.0010,   # 0.10%
        "live_asx_first_buy_free_thresh": 1000.0,   # CMC <$1k first-buy promo
        "live_us_min_fee":                 0.0,
        "live_us_rate":                    0.0,
    },
    "ibkr_pro_au": {
        "name":            "Interactive Brokers (Pro AU)",
        # OOS backtest cost model
        # IBKR Tiered: AU 0.08% min AUD 5.00; US USD 0.0035/share min USD 1.00
        "au_flat_fee_aud":  5.0,    # min binds for trades <~AUD 6.25k
        "us_flat_fee_aud":  1.5,    # USD $1 min ≈ AUD $1.50 (conservative)
        "au_spread_bps":    3.0,    # Smart order routing tighter than retail
        "us_spread_bps":    3.0,
        "fx_spread_bps":    0.5,    # IDEALPRO ~0.2 bps + USD $2 min commission
        # Live trade-plan brokerage
        "live_asx_min_fee":               5.0,
        "live_asx_rate":                  0.0008,   # 0.08% IBKR Tiered
        "live_asx_first_buy_free_thresh": 0.0,      # No free-trade promo
        "live_us_min_fee":                1.5,
        "live_us_rate":                   0.0002,   # ~2 bps avg (USD 0.0035/share, ETF universe)
    },
    "saxo_au_classic": {
        # Saxo Bank Australia — Classic tier (entry-level, no minimum
        # account balance). Source: Saxo's published price list as of
        # 2026-06. Numbers should be verified against the user's live
        # quote since Saxo's AU fees can shift with promotional periods
        # and currency. The "$X AUD min" floors bind much harder than
        # IBKR's $5 at small trade sizes — for a $1k ASX trade, Classic
        # commission is the AUD 8 min not the 0.10% rate.
        #
        # Why this is interesting: Saxo's free Simulation environment
        # gives a full $1M+ paper account WITHOUT requiring real
        # funding (IBKR caps paper at 5x cash, forcing the user to
        # fund $200k to validate at the wholesale entry size). API is
        # REST/WebSocket OpenAPI, distinct from IBKR's ib_insync.
        "name":            "Saxo Bank AU (Classic)",
        # OOS backtest cost model
        "au_flat_fee_aud":  8.0,    # min binds for trades <~AUD 8k
        "us_flat_fee_aud":  7.5,    # USD $5 min ≈ AUD $7.50
        "au_spread_bps":    5.0,    # Wider than IBKR Smart routing
        "us_spread_bps":    5.0,
        "fx_spread_bps":    5.0,    # ~50 pip on majors at Classic tier
        # Live trade-plan brokerage
        "live_asx_min_fee":               8.0,      # AUD 8 min
        "live_asx_rate":                  0.0010,   # 0.10% Classic
        "live_asx_first_buy_free_thresh": 0.0,
        "live_us_min_fee":                7.5,      # USD 5 → ~AUD 7.50
        "live_us_rate":                   0.0008,   # ~0.02 USD/share, est 8 bps avg on ETFs
    },
    "saxo_au_platinum": {
        # Saxo Platinum — requires AUD 250k balance (or 6mo trade
        # volume). Tighter than Classic but still wider than IBKR.
        # Listed for users who already meet the Platinum threshold.
        "name":            "Saxo Bank AU (Platinum)",
        "au_flat_fee_aud":  6.0,
        "us_flat_fee_aud":  6.0,
        "au_spread_bps":    4.0,
        "us_spread_bps":    4.0,
        "fx_spread_bps":    3.5,
        "live_asx_min_fee":               6.0,
        "live_asx_rate":                  0.0008,
        "live_asx_first_buy_free_thresh": 0.0,
        "live_us_min_fee":                6.0,
        "live_us_rate":                   0.0006,
    },
    "tiger_au": {
        # Tiger Brokers Australia Pty Ltd. ASIC-regulated AU entity,
        # OpenAPI via the official `tigeropen` Python SDK.
        #
        # FEE FIGURES BELOW ARE BEST-GUESS AGAINST 2026 PUBLISHED RATES
        # AND MUST BE VERIFIED AGAINST THE USER'S LIVE QUOTE BEFORE ANY
        # LIVE EXECUTION. See TIGER_AU_VERIFICATION.md for the question
        # list to pose to Tiger AU support before relying on these.
        #
        # If verified, Tiger AU's fees are materially cheaper than both
        # IBKR Pro AU and Saxo Classic — which would make it the most
        # attractive broker for a fee-conscious wholesale fund. The
        # catch is asset-coverage and paper-account terms (both
        # AU-entity specific and historically tighter than global Tiger).
        "name":            "Tiger Brokers AU (PROVISIONAL)",
        # OOS backtest cost model
        "au_flat_fee_aud":  2.99,   # very low minimum
        "us_flat_fee_aud":  1.5,    # USD 0.99 ≈ AUD 1.50
        "au_spread_bps":    4.0,    # retail SOR — tighter than Saxo retail, wider than IBKR Smart
        "us_spread_bps":    4.0,
        "fx_spread_bps":    5.0,    # ~50 pip retail FX
        # Live trade-plan brokerage
        "live_asx_min_fee":               2.99,
        "live_asx_rate":                  0.00029,  # 0.029% — Tiger's headline AU rate
        "live_asx_first_buy_free_thresh": 0.0,
        "live_us_min_fee":                1.5,
        "live_us_rate":                   0.0005,   # ~5 bps avg on ETFs (USD 0.0049/share)
    },
}

# Switch broker here. BROKER_CONFIG + downstream BROKERAGE follow automatically.
ACTIVE_BROKER_PROFILE = "ibkr_pro_au"
BROKER_CONFIG = BROKER_PROFILES[ACTIVE_BROKER_PROFILE].copy()


# === Live trade-plan brokerage structure (derived from BROKER_CONFIG) ========
BROKERAGE = {
    "ASX": {
        "first_buy_free_threshold": float(BROKER_CONFIG["live_asx_first_buy_free_thresh"]),
        "min_fee":                  float(BROKER_CONFIG["live_asx_min_fee"]),
        "rate":                     float(BROKER_CONFIG["live_asx_rate"]),
    },
    "US": {
        "min_fee": float(BROKER_CONFIG["live_us_min_fee"]),
        "rate":    float(BROKER_CONFIG["live_us_rate"]),
    },
}

MIN_TRADE_VALUE = 11.0


def _market_of(ticker: str) -> str:
    t = str(ticker)
    if t.startswith("^"):
        return "INDEX"
    if t.endswith(".AX"):
        return "ASX"
    return "US"


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
