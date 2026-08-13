# LOCKBOX HISTORY — retired validation windows

Snapshots taken per the refresh procedure in [LOCKBOX.md](LOCKBOX.md) before
each window change. Records what the retired validation window said, so the
evidence trail survives the refresh.

---

## Window 2: dev 2015→2026-06-30 / val 2026-08-01→2028-08-01 (retired 2026-08-13)

**Lifetime:** created 2026-07-03 (Refresh #1), retired 2026-08-13 (Refresh #2).
**Data lockbox at retirement:** 2026-06-30.
**Peeks used: 0 of 7.** No shipping decision cited validation-window evidence.

### Why it was retired

Not a documented trigger — none of the three had fired (peek budget 0/7,
six weeks elapsed, no architecture change). Retired by **user directive
2026-08-13** following a review of the deck's headline performance slide,
which found the published backtest ran to *today* and was therefore
outside the lockbox entirely, creeping forward each day into this
window's own sealed dates. The boundary was moved to 2026-07-30 and the
lockbox given a second, narrower scope covering the REPORTED backtest.

### What this window said

Nothing. It was a forward, live-evidence-only window and it was never
peeked. The evidence it accumulated (2026-08-01 → 2026-08-13, nine
trading days of paper NAV) is too short to carry any inference and is
folded into the new dev window.

### What this window taught us (survives the refresh)

- **A lockbox that governs the research CLI does not govern what you
  publish.** The deck is built by `--auto-pipeline`, which is deliberately
  unlockboxed so the live solve sees current regimes. For six weeks that
  meant every number on the investor-facing slide was computed outside the
  boundary. Scope has to be named per-consumer, not per-mode.
- **A backtest window that ends "today" silently eats the validation
  window.** The 2026-08-12 deck already covered 12 days of sealed dates.
- Reporting defects found in the same review — price-only `^AORD`, rf=0
  Sharpe, phantom-flat benchmark days, 252-day annualisation on a ~258-day
  calendar — were all *presentation* bugs that never touched the solver.
  The one that touches results — pre-inception back-fill defeating the
  coverage gate — was fixed the same day through a pre-registered gate
  (`PREREG_backfill_lookahead_fix.md`). **It was worth ~1.7%/yr and 0.11
  Sharpe**, 6-8x the noise floor. Full-period restated to +11.44%/yr,
  Sharpe 0.85, MaxDD -22.27%, alpha vs SPY -3.49%/yr.
- **A "must improve" gate is the wrong instrument for a bug fix.** It would
  have rejected this one for making a wrong number smaller. The gate has to
  be "is the degradation understood and bounded".
- **The look-ahead flattered the VALIDATION window ~3x harder than dev**
  (Sharpe -0.17 vs -0.06), because Feb-2020-onward is when the most tickers
  were freshly listed. Validation alpha vs SPY flips +0.37% -> -0.90%, which
  retires Window 1's "beats SPY when the regime turns" vindication. The
  engine buys lower vol and shallower drawdowns for ~3.5%/yr of absolute
  return; it does not beat SPY in either window.

### Production config at retirement

Unchanged from Window 1: 5-slot blend, no crash hedge, 6W rebal + 3% skip
+ 5%DD/10d early trigger, TLH 21 pairs, 9 thematics cap-0, PMGOLD.AX cap-0,
SECTOR_GROUP_CAPS empty, MU_SHRINKAGE_LAMBDA=0, LT_DEFER_WINDOW_DAYS=0.
Verified at the new boundary (`--walk-forward-cv`, lockbox 2026-07-30):
+13.17%/yr, Sharpe 0.96, MaxDD −20.49%, α vs SPY −1.77%/yr — **but this is the
PRE-FIX figure, still carrying the back-fill look-ahead.** Restated post-fix:
**+11.44%/yr, Sharpe 0.85, MaxDD −22.27%, α vs SPY −3.49%/yr.**

---

## Window 1: dev 2015→Feb 2020 / val Feb 2020→2026 (retired 2026-07-03)

**Lifetime:** created 2026-06-18, retired 2026-07-03 (peek budget 7/7 exhausted).
**Data lockbox at retirement:** 2026-06-30 (strict, engine-enforced since 2026-06-27).

### Baseline result (peek 1, 2026-06-18)

The 5-slot ensemble GENERALISED: dev Sharpe 0.90 → val Sharpe 1.03.
Loses to SPY in the bull dev window (α −4.07%/yr), beats SPY when the regime
turns (val α +1.07%/yr). Volatility-managed-beta thesis vindicated.

### Peek ledger (all 7)

| # | Date | Change family | Verdict |
|---|------|---------------|---------|
| 1 | 2026-06-18 | 5-slot ensemble baseline | GENERALISES (0.90 → 1.03) |
| 2 | 2026-06-18 | TLH layer | net uplift ~0, machinery kept |
| 3 | 2026-06-18 | Cost-aware solver (turnover penalty) | DEAD — dev +0.35, val −0.14 (overfit) |
| 4 | 2026-06-18 | SKIP_REBAL_DELTA tuning | DEAD — null on validation |
| 5 | 2026-06-19 | Stretch+hedge | DEAD — fold-mean MaxDD lied (−17% fold-mean vs −34% full-period); shipped + reverted `a660598` |
| 6 | 2026-07-02 | μ-shrinkage (James-Stein, λ=0.50) | DEAD — dev 0.78 vs 0.91, val 0.92 vs 1.08, val MaxDD −24.22% vs −19.17%; full-window CV win was a path artifact |
| 7 | 2026-07-02 | LT-deferral (unconditional, 126d) | DEAD — dev 0.94 vs 0.91 but val 1.03 vs 1.09, val MaxDD −20.31% vs −18.06%; shield slows de-risking at violent turns |

### What this window taught us (survives the refresh)

- Full-period peak-to-trough MaxDD is the only honest drawdown gate;
  fold-mean structurally understates multi-year drawdowns.
- Fold-mean deltas within ~2×SE are noise; run-to-run noise floor
  ~10-30bps return / ~0.00-0.02 Sharpe from yfinance re-download jitter.
- "Guaranteed" tax savings (LT-deferral) still lose to regime-dependent
  drift costs — the engine earns its edge at violent turns; anything that
  slows de-risking there is suspect regardless of its static-arithmetic appeal.
- Full-window CV wins that fail dev/val are path artifacts (μ-shrinkage).
- Engine ceiling on this window: Sharpe ~0.94-0.97, MaxDD ~−20 to −26%,
  trails SPY ~2%/yr absolute (gap ≈ CGT drag; pre-tax it wins).
  **[SUPERSEDED 2026-08-13 — every figure in this window, including the val
  α +1.07%/yr vindication above, carried the back-fill look-ahead. Post-fix
  the ceiling is Sharpe ~0.85 and SPY is not beaten in either window.]**

### Production config at retirement

5-slot blend, no crash hedge, 6W rebal + 3% skip + 5%DD/10d early trigger,
TLH 21 pairs (~+1%/yr), 9 thematics cap-0 (error-max revert), PMGOLD.AX cap-0
TLH-only, SECTOR_GROUP_CAPS empty, MU_SHRINKAGE_LAMBDA=0, LT_DEFER_WINDOW_DAYS=0.
Verified 10Y production frame: Sharpe ~0.93-0.95, +13.7-14%/yr, MaxDD −26.06%.
