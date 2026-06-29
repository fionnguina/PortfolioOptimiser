"""Regression tests for softmax_ensemble_weights — the heart of the regime
ensemble. Locks in the IR-vs-benchmark scoring + softmax temperature behaviour
so a module-split refactor can't silently shift live regime mix.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

# softmax_ensemble_weights moved to ensemble.py (Phase 4 split, 2026-06-29).
import sys
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
from ensemble import softmax_ensemble_weights as _softmax_fn


@pytest.fixture(scope="module")
def softmax():
    return _softmax_fn


def _synth_returns(n_days: int, candidate_specs: dict, seed: int = 42) -> pd.DataFrame:
    """Build a per-candidate daily-returns DataFrame.

    candidate_specs: {name: (mean_daily, std_daily)} — drawn from N(mean, std).
    """
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2020-01-01", periods=n_days)
    cols = {}
    for name, (mu, sigma) in candidate_specs.items():
        cols[name] = rng.normal(mu, sigma, size=n_days)
    return pd.DataFrame(cols, index=idx)


def _synth_benchmark(n_days: int, mean_daily: float = 0.0003, std_daily: float = 0.01,
                     seed: int = 7) -> pd.Series:
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2020-01-01", periods=n_days)
    return pd.Series(rng.normal(mean_daily, std_daily, size=n_days), index=idx)


def test_empty_returns_equal_weight(softmax):
    """No data -> uniform weights (warm-up fallback)."""
    out = softmax(pd.DataFrame(columns=["A", "B", "C"]))
    assert list(out.index) == ["A", "B", "C"]
    assert np.allclose(out.to_numpy(), [1 / 3, 1 / 3, 1 / 3])


def test_short_history_equal_weight(softmax):
    """<60 obs -> warm-up uniform weights regardless of returns."""
    df = _synth_returns(40, {"A": (0.001, 0.01), "B": (-0.001, 0.01)})
    out = softmax(df)
    assert np.allclose(out.to_numpy(), [0.5, 0.5])


def test_zero_candidates(softmax):
    """No columns -> empty Series."""
    out = softmax(pd.DataFrame())
    assert out.empty


def test_winner_takes_most_with_high_lambda(softmax):
    """One clearly-winning candidate + high lambda -> >70% weight."""
    df = _synth_returns(252, {
        "winner": (0.002, 0.01),
        "loser":  (-0.001, 0.01),
        "flat":   (0.0,   0.01),
    })
    bench = _synth_benchmark(252)
    out = softmax(df, lambda_temp=5.0, benchmark_returns=bench)
    assert out.sum() == pytest.approx(1.0, abs=1e-9)
    assert out["winner"] > 0.7, f"expected winner > 70%, got {out['winner']:.3f}"


def test_lambda_zero_flattens_to_equal(softmax):
    """lambda=0 -> exp(0)=1 for every score -> uniform regardless of scores."""
    df = _synth_returns(252, {"A": (0.002, 0.01), "B": (-0.002, 0.01)})
    bench = _synth_benchmark(252)
    out = softmax(df, lambda_temp=0.0, benchmark_returns=bench)
    assert np.allclose(out.to_numpy(), [0.5, 0.5])


def test_higher_lambda_more_peaked(softmax):
    """Same input + higher lambda -> winner's weight strictly grows."""
    df = _synth_returns(252, {"A": (0.002, 0.01), "B": (-0.001, 0.01)})
    bench = _synth_benchmark(252)
    w_low = softmax(df, lambda_temp=1.0, benchmark_returns=bench)
    w_high = softmax(df, lambda_temp=5.0, benchmark_returns=bench)
    winner = w_low.idxmax()
    assert w_high[winner] > w_low[winner]


def test_weights_sum_to_one(softmax):
    """Softmax invariant: weights are a probability distribution."""
    df = _synth_returns(252, {"A": (0.001, 0.01), "B": (0.001, 0.02),
                              "C": (0.0, 0.015), "D": (-0.0005, 0.01)})
    bench = _synth_benchmark(252)
    out = softmax(df, lambda_temp=3.0, benchmark_returns=bench)
    assert out.sum() == pytest.approx(1.0, abs=1e-9)
    assert (out >= 0).all()


def test_all_nan_candidate_gets_min_score(softmax):
    """A candidate with all-NaN scores gets filled with min(others), not random
    or NaN-propagated. Confirms the s.fillna(s.min) safety net."""
    df = _synth_returns(252, {"good": (0.002, 0.01), "bad": (-0.002, 0.01)})
    df["broken"] = np.nan
    bench = _synth_benchmark(252)
    out = softmax(df, lambda_temp=3.0, benchmark_returns=bench)
    assert out.sum() == pytest.approx(1.0, abs=1e-9)
    # broken should rank with bad (both at min score), strictly below good
    assert out["good"] > out["broken"]
    assert out["good"] > out["bad"]


def test_benchmark_path_penalises_lagger_more_than_abs_sharpe(softmax):
    """The Sortino-pathology fix in the docstring: a low-vol candidate that
    quietly lags benchmark gets a competitive ABSOLUTE Sharpe (tiny vol /
    small positive mean -> looks fine) but a clearly negative IR (active
    return < 0). So the lagger's weight should be STRICTLY LOWER under the
    benchmark path than under the abs-Sharpe path."""
    bench = _synth_benchmark(252, mean_daily=0.0015, std_daily=0.012)
    df = _synth_returns(252, {
        "beats_bench":  (0.0020, 0.012),  # beats bench in active terms
        "low_vol_lag":  (0.0005, 0.003),  # positive abs return, lags bench
    })
    w_ir = softmax(df, lambda_temp=3.0, benchmark_returns=bench)
    w_abs = softmax(df, lambda_temp=3.0, benchmark_returns=None)
    # The lagger should be penalised more harshly by the IR scoring than by
    # the absolute-Sharpe scoring. If both branches collapsed onto identical
    # logic this assertion would fail.
    assert w_ir["low_vol_lag"] < w_abs["low_vol_lag"], (
        f"IR path should penalise lagger more: ir={w_ir['low_vol_lag']:.4f}, "
        f"abs={w_abs['low_vol_lag']:.4f}"
    )


def test_negative_ir_candidate_gets_low_weight(softmax):
    """A candidate that systematically lags benchmark gets low weight even if
    its absolute return is positive — the IR scoring fix for the Sortino
    pathology documented in the function docstring."""
    bench = _synth_benchmark(252, mean_daily=0.001, std_daily=0.005)
    # `lagger` has positive absolute return but lags benchmark in active terms
    df = _synth_returns(252, {
        "winner": (0.0015, 0.005),  # beats bench
        "lagger": (0.0005, 0.005),  # positive but trails bench
    })
    out = softmax(df, lambda_temp=3.0, benchmark_returns=bench)
    assert out["winner"] > out["lagger"]


def test_lookback_window_truncates_history(softmax):
    """lookback_days=120 should only consider the trailing 120 obs even if
    the input DataFrame has more history. Long-ago crash shouldn't drown
    out recent recovery."""
    df = _synth_returns(252, {"A": (0.001, 0.01), "B": (0.0, 0.01)})
    # Inject a crash into the first 100 days of A — should be ignored with
    # lookback=120 since those days fall outside the window.
    df.iloc[:100, df.columns.get_loc("A")] = -0.05
    bench = _synth_benchmark(252)
    out_short = softmax(df, lookback_days=120, lambda_temp=3.0,
                        benchmark_returns=bench)
    out_full = softmax(df, lookback_days=252, lambda_temp=3.0,
                       benchmark_returns=bench)
    # A's recent (last 120d) returns aren't crashed, so short-window favours
    # A more than full-window does
    assert out_short["A"] > out_full["A"]


def test_returns_match_benchmark_index_order(softmax):
    """The benchmark Series may be supplied out-of-order or partially overlapping;
    the function should align and not blow up."""
    df = _synth_returns(252, {"A": (0.001, 0.01), "B": (0.0, 0.01)})
    bench = _synth_benchmark(300)  # longer than df
    # shuffle bench index — function should sort it internally
    bench_shuffled = bench.sample(frac=1.0, random_state=11)
    out = softmax(df, lambda_temp=3.0, benchmark_returns=bench_shuffled)
    assert out.sum() == pytest.approx(1.0, abs=1e-9)
    assert (out >= 0).all()
