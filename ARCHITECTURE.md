# Architecture

How the engine actually works under the hood. Read [README.md](README.md) first for the 5-second overview.

---

## 1. Data flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                                INPUTS (per run)                              │
├──────────────────────────────────────────────────────────────────────────────┤
│  Stock Analysis.xlsm                                                         │
│    ├── Holdings sheet ─ user-edited current units per ticker                 │
│    ├── Tilts sheet ─ factor target/band/use flags                            │
│    └── Lots sheet ─ acquisition history for CGT FIFO/HIFO                    │
│                                                                              │
│  yfinance (5y daily prices, ~46 tickers)                                     │
│    └── per-region FF5+MOM factor history (Ken French data library)           │
│                                                                              │
│  IBKR delayed snapshot (if TWS open in paper) ─ overrides last-prices only   │
│                                                                              │
│  USDAUD=X FX series                                                          │
│                                                                              │
│  regions.json ─ user region overrides (e.g., NDQ.AX → US)                    │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          STAGE 1: DATA PREP                                  │
├──────────────────────────────────────────────────────────────────────────────┤
│  ─ Drop return outliers (|r| > 30%)                                          │
│  ─ FX-adjust USD tickers to AUD                                              │
│  ─ Build df_cov_wide (returns matrix), Sigma_daily, mu_ann_geo               │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         STAGE 2: FF5 + UNIVERSE                              │
│                  (`_run_ff5_and_frontier_setup`)                             │
├──────────────────────────────────────────────────────────────────────────────┤
│  ─ Standardise factor vols across regions (vs US baseline)                   │
│  ─ Per-asset rolling FF5+MOM regression → B (loadings matrix)                │
│  ─ Drop assets whose regression fails → securities_opt                       │
│  ─ Build Sigma_opt, mu_vec_opt for the OPT universe                          │
│  ─ Solve the efficient frontier (cvxpy long-only max-Sharpe)                 │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                  STAGE 3: HOLDINGS DIALOG (CustomTkinter)                    │
├──────────────────────────────────────────────────────────────────────────────┤
│  User can edit current units, tilts, include flags                           │
│  Click "Auto recommend tilts" to use risk-optimal targets                    │
│  Cancel → use sheet-seed defaults                                            │
│  Save → updated values flow into the trade plan                              │
│  Dialog REBUILDS FF5/frontier on the final universe                          │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│             STAGE 4: ENSEMBLE WALK-FORWARD (OOS backtest)                    │
│                 (`run_oos_ensemble_walk_forward`)                            │
├──────────────────────────────────────────────────────────────────────────────┤
│  ─ Walk forward from 2yr-of-data warm-up to today, 6W rebalance              │
│  ─ At each rebalance, solve 5 candidates:                                    │
│       Modest    (SPY return + 0%)                                            │
│       Aggressive (SPY return + 5%)                                           │
│       Bold      (SPY return + 10%)                                           │
│       Maximum   (SPY return + 15%)                                           │
│       Stretch   (SPY return + 25%)                                           │
│  ─ Score each candidate's BACKWARD returns by:                               │
│       IR vs SPY (rolling 12M with EWMA halflife=60d)                         │
│       Softmax → ensemble weights                                             │
│       Temperature λ=3.0                                                      │
│  ─ Blend with FORWARD signal (20/50d SMA cross + 52W DD on SPY)              │
│       backward_alpha = 0.5                                                   │
│  ─ Final mix = blended portfolio                                             │
│  ─ Apply IBKR brokerage + AU CGT (FY-netted, 12mo discount, FIFO/HIFO)       │
│  ─ Track conditional rebalancing (early-trigger on SPY DD-deepen)            │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                  STAGE 5: LIVE ENSEMBLE RECOMMENDATION                       │
├──────────────────────────────────────────────────────────────────────────────┤
│  Use TODAY's mu/Sigma (live 2y window)                                       │
│  Score per-candidate using OOS history (10y of evidence)                     │
│  Apply same backward/forward blend                                           │
│  W_ENSEMBLE_SER = final live target weights                                  │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                       STAGE 6: TRADE PLAN                                    │
│                       (`make_trade_plan`)                                    │
├──────────────────────────────────────────────────────────────────────────────┤
│  Inputs: current_units, last_px (yfinance OR IBKR), fx_map, w_target         │
│  Output: trade_df with delta_units per ticker + cash flow + brokerage        │
│  CGT computed via `evaluate_transaction_costs` (lots → realised gains)       │
│  Auto-select among ensemble / with_tilts / no_tilts by validation Sharpe     │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                  STAGE 7: PERSISTENCE + REPORTING                            │
├──────────────────────────────────────────────────────────────────────────────┤
│  trade_recommendation_log.jsonl  ← JSONL append: regime mix, weights, trades │
│  live_nav_history.jsonl          ← JSONL append: {date, nav_aud}             │
│  cash_ledger.jsonl               ← JSONL append: cumulative cost tracking    │
│  Stock Analysis.xlsm             ← xlwings write: OPT, OOS_Validation,       │
│                                                   CGT_Audit, Drift_Fills,    │
│                                                   Drift_NAV, Actual_Fills,   │
│                                                   Cash_Ledger                │
│  Reports/Portfolio_Report.pptx   ← Roadshow deck (5 slides)                  │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Key data structures

These are the names you'll see ~50% of the time when grepping. Knowing them shortcuts everything.

### Live universe + analytics
- `prices` (DataFrame, T × 46) — daily close prices, AUD-converted for USD tickers
- `df_cov_wide` (DataFrame, T × N) — daily returns, outlier-filtered
- `Sigma_daily` (DataFrame, N × N) — daily covariance for full universe
- `mu_ann_geo` (Series, N) — annualised geometric mean return per ticker

### OPT universe (FF5-validated subset)
- `securities_opt` (list) — tickers with successful FF5 regression
- `Sigma_opt` (DataFrame) — sliced to OPT
- `mu_vec_opt` (Series) — sliced to OPT
- `B` (DataFrame, N × 6) — FF5+MOM loadings per asset
- `f_mean_ann` (Series, 6) — annualised mean factor returns
- `Fcov_daily` (DataFrame, 6 × 6) — daily factor covariance

### Solutions
- `W` (DataFrame) — frontier weights at each target return point
- `stats_df` (DataFrame) — frontier point statistics (vol, return, Sharpe)
- `w_star` (Series) — no-tilts max-Sharpe weights (the tangency point)
- `w_star_with_tilts` (Series) — same but with user-imposed factor tilt constraints
- `W_ENSEMBLE_SER` (Series) — live regime-adaptive ensemble weights ⭐ the live recommendation
- `tan_ret`, `tan_vol` (float) — tangency point coordinates

### Ensemble specifics
- `ensemble_mix_live` (Series) — current softmax weights across the 5 slots
- `oos_returns_daily` (Series) — OOS walk-forward strategy returns
- `oos_weights_history` (DataFrame) — per-rebalance weights
- `oos_per_candidate_returns` (DataFrame) — per-slot daily returns for scoring
- `oos_softmax_history` (DataFrame) — historical softmax weights

### Trade plan
- `current_holdings_units` (Series) — units per ticker the user holds (from sheet/dialog)
- `last_px_hold` (Series) — last price per ticker in native currency
- `fx_map_all` (dict / Series) — per-ticker FX multiplier (1.0 for AUD, USD/AUD for USD)
- `trade_rec` (DataFrame) — the final trade plan (Curr / Target / Delta / Last Px / Cash Flow / Brokerage)
- `costs_rec` (dict) — `{brokerage, cgt_tax, total_cost, breakdown, per_row_brokerage}`

### State + diagnostics
- `results` (dict) — drives the PPT summary band: portfolio value, brokerage, CGT, cash, drift vs prior
- `lots_df` (DataFrame) — acquisition history for CGT (loaded from Lots sheet)

---

## 3. The regime ensemble in detail

The cornerstone idea. Most engines pick one optimal portfolio; this one picks 5 and blends them dynamically.

**Why 5 slots:** Different return targets create different risk profiles. Modest leans defensive (more bonds, gold); Stretch leans aggressive (more leveraged tech). At any given moment the "correct" target depends on the regime — which we don't know in advance.

**Why softmax-blend (not pick one):** Sharp pick-one is fragile to estimator noise. A soft blend produces a smoother trajectory and lower turnover.

**Backward scoring (IR vs SPY):**
- For each slot, compute trailing 12M information ratio vs SPY (EWMA halflife=60d)
- Softmax-normalise across slots with temperature λ=3.0
- High λ = more peaked (lean into winners), low λ = flatter

**Why IR vs Sortino:** Sortino has a pathological failure with extreme-negative-target slots — their low downside vol can game the score. IR vs benchmark fixes this. (See [project_ensemble_state_2026_06_15.md](memory file) for the empirical evidence.)

**Forward signal (20/50 SMA cross + 52W DD):**
- 20d SMA > 50d SMA on SPY → favour high-target slots (uptrend)
- SPY drawdown > -10% → favour low-target slots (defensive)
- Blended 50/50 with backward score via `backward_alpha=0.5`

**Net result:** the live ensemble is mostly Maximum + Stretch in benign markets, drops toward Modest + Aggressive during regime shifts.

---

## 4. CGT model in detail

The biggest tax-honesty win. Most "after-tax" Sharpe ratios overstate by 100+ bps because they don't model:

- **FY netting**: AU CGT bunches gains and losses within a financial year (Jul 1 – Jun 30). The OOS engine accumulates intra-year and computes tax once at FY-end with cross-offsets.
- **Carry-forward losses**: Unused losses roll to next FY, can offset future gains.
- **12-month LT discount**: 50% on gains where the lot is held ≥365 days.
- **Lot-level matching**: FIFO/HIFO per-parcel, not portfolio-average.

Implementation: `compute_cgt_tax` returns `(tax_aud, breakdown_dict)`. Breakdown includes:
- `st_gain` / `lt_gain` (post-offset)
- `losses` (pre-offset total available)
- `loss_carry_forward` (post-offset remainder, for the Deferred Tax callout on the PPT)
- `discounted_lt_after_losses` (the actually-taxed LT portion)
- `taxable` (the final base)
- `audit` (per-parcel DataFrame for the CGT_Audit Excel sheet)

The OOS walk-forward uses a `LotBook` class that simulates lot creation + consumption across the backtest period. The live trade plan uses a snapshot from the Lots sheet.

---

## 5. The drift tracker (Tier-1 #3)

Three layers, each with its own JSONL + Excel sheet.

### v1 — Recommendation log

`trade_recommendation_log.jsonl` — one JSONL entry per run with:
- `run_at`, `selected_mode`, `broker`, `cgt_mtr`
- `portfolio_value_aud`, `universe_size`
- `regime_mix` (5-slot softmax weights)
- `target_weights` (per ticker, > 1e-6)
- `current_units` (per ticker, ≠ 0)
- `expected_brokerage_aud`, `expected_cgt_aud`
- `recommended_trades` (list of `{ticker, side, delta_units, px_aud, delta_value_aud, brokerage_aud}`)

This is the *intent log* — what the engine thought it should do.

### v2 — Fill comparison

`Actual_Fills` sheet in workbook (user-edited): `Fill Date | Ticker | Side | Units | Px AUD | Fees AUD | Notes`.

`compute_fill_drift()` joins fills against the rec log (most recent rec per ticker on/before fill date) and computes:
- Slippage bps (positive = worse than expected)
- Fee delta (actual − expected)
- Time-to-fill (calendar days)
- Adherence (matched a recommendation? yes/no)

Output: `Drift_Fills` sheet.

### v3 — Live NAV drift

`live_nav_history.jsonl` — appended each run with `{date, nav_aud}`.

`compute_monthly_nav_drift()` computes per-month live vs OOS-expected returns starting from `LIVE_TRADING_START_DATE` (config knob, currently `None`). Output: `Drift_NAV` sheet with `Drift` and `Cumulative Drift` columns.

Plus live MaxDD from peak. Warnings fire if:
- Monthly drift > 2%
- Cumulative drift > 5%
- Live DD < -10%
- Slippage > 25 bps on any fill
- Fees > 2× expected

---

## 6. Persistent cash ledger

Separate from the drift tracker — answers "where did the money go?"

`cash_ledger.jsonl` — appended each run with portfolio value, net invested, cash, brokerage, CGT, mode, broker. No same-day dedup; every run is a row.

`Cash_Ledger` sheet shows:
- Top band: target vs latest, drift vs target, drift vs first record, cum brokerage, cum CGT, total cost
- Per-run table with **Unexplained Δ** column = `Δ_portfolio + brokerage_this_run + cgt_this_run`

Interpretation: Unexplained Δ should equal the period's market move on the held positions. If brokerage and CGT are real outflows (post-IBKR-execution), this is the actual signal of "what the market gave you." Pre-execution (hypothetical brokerage), it's market-move + the brokerage you didn't actually pay.

---

## 7. IBKR integration architecture

```
┌─────────────────────────┐
│  Portfolio_Optimiser.py │
│  ┌───────────────────┐  │
│  │ Live trade plan   │  │
│  │  ↓                │  │      ┌──────────────────────────┐
│  │ last_px_hold      │──┼─────►│ fetch_ibkr_live_prices_  │
│  │  (yfinance)       │  │      │  native(tickers)         │
│  │  ↓                │  │      │   ↓                      │
│  │ apply_ibkr_price_ │◄─┼──────┤ IBKR delayed snapshots   │
│  │  override()       │  │      │  (TWS paper, port 7497)  │
│  └───────────────────┘  │      └──────────────────────────┘
└─────────────────────────┘
                                       │
                                       │ ib_insync API
                                       │ (read-only path)
                                       ▼
                            ┌──────────────────────────┐
                            │   TWS / IB Gateway       │
                            │   Paper account DUQ...   │
                            └──────────────────────────┘

Standalone scripts (not loaded by engine):
─────────────────────────────────────────
  ibkr_paper_test.py     ─ Phase 0: auth + summary print
  ibkr_price_check.py    ─ PoC: delayed vs yfinance comparison
  ibkr_seed_paper.py     ─ Phase 1.5: pretend-trade engine units → paper
                            (--execute submits MARKET BUYs; default is dry-run)
  ibkr_dry_run.py        ─ Phase 2: format rebalance as IBKR Order objects.
                            Prints only. NEVER calls placeOrder.
```

**Three safety layers** on the order-submitting scripts (seed + future Phase 3):
1. **Port hardcoded to 7497** (paper). Live ports never appear in code.
2. **`_refuse_if_live()`** check — connected account must start with `DU`. Anything else aborts.
3. **CLI flag gating** — `--execute` required; dry-run is the default. Then an interactive `YES` prompt before sending.

---

## 8. Known architectural compromises

These were intentional trade-offs, not bugs. Future refactor candidates.

| Choice | Why we did it | What it costs |
|---|---|---|
| **Monolithic 9k-line file** | Started as a notebook; the cohesion outweighs the split cost while everything is in flux | Harder to test in isolation. Modules would help once shape is stable. |
| **44 `globals()[...]` mutations** | `_run_ff5_and_frontier_setup` writes ~20 globals to keep call sites simple | Hard to reason about state. Race-prone if we ever parallelise. |
| **xlwings + COM (vs openpyxl-only)** | Live Excel write keeps charts intact across runs | Requires Excel installed + closed. No headless option. |
| **PyInstaller --onefile** | Single .exe is easier for the user | 165 MB, slow startup (~3s unpacking) |
| **yfinance for history + IBKR for live** | Best of both worlds (free 12y history + clean live prices) | Two data sources to monitor; divergence checks needed |
| **Hardcoded `_DEV_BASE`** | User's setup is fixed | Not portable to a second machine without editing |

See [AUDIT.md](AUDIT.md) for the full list of refactor candidates triaged by priority.

---

## 9. Run sequencing (where things happen in Portfolio_Optimiser.py)

| Range | What |
|---|---|
| `1-90` | Imports + build stamp + CLI flag detection |
| `90-450` | Config block: brokers, CGT, drift, weight caps, rebal freq |
| `450-490` | Config snapshot logger |
| `490-3700` | Helpers (CGT model, lot book, trade plan builder, IBKR pricing fetch, drift tracker primitives) |
| `1240-1265` | yfinance prices download |
| `1300-2450` | More helpers + dialog builders |
| `3088-3330` | `_run_ff5_and_frontier_setup` (the canonical FF5 setup) |
| `4640-4710` | Pre-dialog FF5 build (for tilt seeding) |
| `4680-4710` | Holdings dialog invocation + post-dialog FF5 rebuild |
| `5540-6520` | OOS walk-forward + ensemble engine + metrics |
| `6520-6700` | GFC stress test (gated on `--stress-test`) |
| `7600-7700` | Live ensemble + recommendation log |
| `7700-7770` | Drift tracker (fills + NAV + DD warnings) |
| `7770-7830` | Cash ledger |
| `7400-9050` | Trade plan + Excel writes |
| `8830-10100` | PPT export |

Use this map when grep'ing for a specific behaviour.

---

## 10. Glossary

| Term | Meaning |
|---|---|
| **OPT** | The Fama-French-validated subset of the universe used for portfolio optimisation. |
| **Sigma_opt / mu_vec_opt** | Covariance + expected-return inputs for the frontier solver, sliced to OPT. |
| **Tangency** | Long-only max-Sharpe portfolio. Pure mean-variance optimum without tilt constraints. |
| **Frontier** | Set of optimal portfolios across the return spectrum. The tangency is one point on it. |
| **Tilt** | A target loading on a Fama-French factor (e.g., HML=+0.05 = +5% value tilt). |
| **Ensemble** | The regime-adaptive softmax blend of 5 candidate portfolios. Live default. |
| **Walk-forward** | Time-respecting OOS backtest. At time t we only see data ≤ t. |
| **OOS** | Out-of-sample. Walk-forward returns + metrics from `run_oos_ensemble_walk_forward`. |
| **IR vs SPY** | Information ratio (excess return / tracking error) vs the SPY benchmark. |
| **FY** | Australian financial year (Jul 1 – Jun 30). |
| **LT** | Long-term, ≥365 days held. Eligible for 50% CGT discount. |
| **MTR** | Marginal tax rate. User is 30%. |
| **NAV** | Net asset value. The portfolio's mark-to-market value in AUD. |
| **MaxDD** | Maximum drawdown from the prior peak. |
| **Slippage** | Difference between expected and actual fill price, in bps. |
