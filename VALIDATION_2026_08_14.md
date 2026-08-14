# Overfitting validation — deflated Sharpe + universe vintage

Run 2026-08-14, lockbox 2026-07-30, back-fill look-ahead fixed (`5bc9481`).
Both tests target weaknesses the 2026-08-13 audit established and that no
amount of additional backtesting fixes on its own.

---

## 1. Deflated Sharpe Ratio — does the edge survive the search?

The config was selected from a long series of backtest evaluations against a
single 10-year path. A Sharpe reported without adjusting for that search is
the maximum of a sample, not an estimate of the mean.

**Trial input.** `metrics_history.jsonl` is NOT usable here — its 154 records
are production re-runs of the same config (sd 0.077, mostly yfinance jitter),
which would set the null bar far too low and manufacture a pass. The correct
input is the spread across genuinely distinct variants: **47 FULL-PERIOD
Sharpes recovered from `logs/*.log`** (thematics, μ-shrinkage, LT-deferral,
trend sleeve, inverse hedge, low-vol diversifier, QIS, λ/halflife sweeps,
cap experiments, …), mean 0.880, **sd 0.040**.

| | rf = 0 | rf = RBA contemporaneous |
|---|---|---|
| Observed Sharpe (ann) | 0.993 | **0.847** |
| Skew | −0.777 | −0.778 |
| Kurtosis | 7.73 | 7.74 |
| PSR(SR > 0) | 0.9989 | **0.9955** |
| Null max-SR from 47 trials (ann) | 0.090 | 0.090 |
| **Deflated Sharpe** | 0.9972 | **0.9902** |

**Reading: the edge survives its own search.** DSR 0.99 says that, given 47
trials with the observed spread, a strategy this good is very unlikely to be
search luck.

**The caveat that matters.** The 47 variants are correlated perturbations of
one 5-slot ensemble, not independent draws from a strategy space. That
understates true search variance, so 0.040 is a floor and DSR 0.99 is
optimistic. Sensitivity:

| assumed cross-variant sd | null max-SR (ann) | DSR |
|---|---|---|
| **0.04 (observed)** | 0.090 | **0.9902** |
| 0.10 | 0.225 | 0.9723 |
| 0.20 | 0.450 | 0.8891 ← fails 95% |
| 0.30 | 0.676 | 0.7008 |
| 0.50 | 1.126 | 0.1941 |

The true search variance would have to be **~5× the observed** before the
edge became questionable. Given the search stayed inside one architecture,
0.04–0.10 is the defensible range and the edge holds across it.

**Note the higher moments.** Skew −0.78 and kurtosis 7.7 are materially
non-normal — occasional large losses. PSR already discounts for this (it is
why PSR sits below what a naive t-test would give), but it is a real
characteristic to disclose, not a statistical footnote.

---

## 2. Minimum Track Record Length — how long until we actually know?

| to establish, at 95% confidence | live data required |
|---|---|
| Sharpe > 0 | **4.05 years** |
| Sharpe > the search-adjusted null | **5.07 years** |

**Paper trading to date: ~38 trading days = 0.15 years.**

This is the single most useful number in the project. It says plainly: the
live record cannot confirm or refute the backtest for roughly **four to five
years**, and any interim judgement — in either direction — is noise. It
should govern how the fund is described to family investors today.

---

## 3. Universe vintage — could this have been run in 2016?

The 47-ticker list was written in 2026 knowing which ETFs worked; 16 did not
exist at the backtest start. The back-fill fix corrects a ticker's *timing*;
it cannot correct the fact that the *candidate list* is hindsight.

`PORTOPT_UNIVERSE_VINTAGE=2016-08-16` restricts the panel to the 31
instruments already trading then. Full-period CV, identical config:

| | full universe | 2016 vintage | Δ |
|---|---|---|---|
| Ann return | +11.44% | **+8.46%** | **−2.98 pp/yr** |
| Sharpe | 0.85 | **0.64** | **−0.21** |
| MaxDD | −22.27% | −20.65% | +1.62 pp (shallower) |
| α vs SPY | −3.49% | **−6.48%** | −2.99 pp |

### The gap is entirely post-2021

| period | mean Δ return | mean Δ Sharpe |
|---|---|---|
| 2016-2020 | **+0.92 pp** | +0.136 |
| 2021-2025 | **−4.24 pp** | −0.266 |

Per fold: 2016 and 2017 are identical to two decimal places. The gap opens in
2021 and widens monotonically — **2024 −6.34 pp, 2025 −11.29 pp**. In 2025 the
full universe returned +24.44% against the vintage universe's +13.15%.

### What this does and does not prove

**It is not proof of cheating.** A real investor could have bought VLUE.AX
when it listed in 2021, and the coverage gate correctly makes the engine wait
24 months for real history. Holding newly-listed instruments as they become
available is what an actual investor does, and the widening opportunity set is
a legitimate source of return.

**What is hindsight is the selection.** A 2021 investor would have had to pick
VLUE.AX out of hundreds of newly-listed ETFs without knowing it would work.
The 2026 author did know. So **−2.98 %/yr is an UPPER bound on the
hindsight-selection cost**, because it strips out the legitimate
opportunity-set expansion along with the bias. The honest value of the
strategy lies somewhere in **+8.46% to +11.44%/yr**, and closer to the top of
that range only to the extent you believe the ETF picks were skill.

**The risk story survives intact.** MaxDD is essentially unchanged (−20.65%
vs −22.27%) and is marginally *better* on the vintage universe. Drawdown
control does not depend on the late-listing instruments. What shrinks is the
return edge.

### The uncomfortable corollary

**The most recent numbers are the most hindsight-contaminated.** The slide's
3Y row (+24.44%, Sharpe 1.24) sits entirely inside the window where the gap is
widest. The 10Y figure is the more honest headline precisely because it
averages over years when the universe was not yet hindsight-selected.

---

## What would move this further

- **PBO / CSCV** needs each variant's full return series, not just its summary
  Sharpe. The 47 logs carry only the latter. Capturing per-variant return
  series from here on would make it computable in future.
- The only evidence that settles anything is forward live performance, and
  MinTRL above prices that at 4-5 years.
