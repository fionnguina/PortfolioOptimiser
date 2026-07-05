# LOCKBOX HISTORY — retired validation windows

Snapshots taken per the refresh procedure in [LOCKBOX.md](LOCKBOX.md) before
each window change. Records what the retired validation window said, so the
evidence trail survives the refresh.

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

### Production config at retirement

5-slot blend, no crash hedge, 6W rebal + 3% skip + 5%DD/10d early trigger,
TLH 21 pairs (~+1%/yr), 9 thematics cap-0 (error-max revert), PMGOLD.AX cap-0
TLH-only, SECTOR_GROUP_CAPS empty, MU_SHRINKAGE_LAMBDA=0, LT_DEFER_WINDOW_DAYS=0.
Verified 10Y production frame: Sharpe ~0.93-0.95, +13.7-14%/yr, MaxDD −26.06%.
