"""Mean-variance optimisation core: long-only Markowitz solvers + Σ shrinkage.

Extracted from Portfolio_Optimiser.py (module split #18, 2026-07-09).

Contents:
  max_sharpe_long_only               Long-only tangency via the kappa transform
                                     (min-variance fallback if no positive excess).
  solve_frontier_point_cvxpy         Long-only Markowitz at a target return, with
                                     optional factor-tilt constraints + turnover penalty.
  solve_frontier_point_cvxpy_with_tilts  Hard-tilt convenience wrapper.
  solve_candidate_portfolios         Solves all ensemble slots for one rebalance.
  _ledoit_wolf_cc                    Ledoit-Wolf (2004) constant-correlation Σ shrinkage.

Cross-module contract (factors.py / tlh.py pattern — the ENGINE owns the config;
solvers reads it through module attributes the engine syncs once after both are
defined):
  PER_ASSET_WEIGHT_CAPS / SECTOR_GROUP_CAPS  — read via globals().get() inside the
    solvers; the engine assigns `solvers.PER_ASSET_WEIGHT_CAPS = ...` etc. after
    its cap-override block so the caps bind here. Empty fallbacks = no caps.
  ENSEMBLE_SLOTS / ENSEMBLE_SLOT_NAMES       — canonical in the engine; the engine
    assigns them here after defining them. solve_candidate_portfolios defaults
    slots=None → resolves to the synced module ENSEMBLE_SLOTS.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import cvxpy as cp

# --- Config injected by the engine after import (see module docstring) --------
PER_ASSET_WEIGHT_CAPS: dict = {}
SECTOR_GROUP_CAPS: dict = {}
ENSEMBLE_SLOTS: tuple = ()
ENSEMBLE_SLOT_NAMES: tuple = ()


def max_sharpe_long_only(mu, Sigma, rf: float = 0.0) -> pd.Series:
    """Long-only maximum-Sharpe weights via the standard kappa transform
    (min y'Sigma y s.t. (mu-rf)'y = 1, y >= 0; then normalise). Falls back to the
    minimum-variance long-only portfolio if the Sharpe problem is infeasible
    (e.g. no positive excess returns). Returns weights indexed like the inputs.
    """
    mu = pd.to_numeric(pd.Series(mu), errors="coerce")
    Sigma = pd.DataFrame(Sigma)
    idx = [c for c in Sigma.index if c in Sigma.columns and c in mu.index]
    mu = mu.reindex(idx)
    Sig = Sigma.loc[idx, idx]
    good = mu.notna() & ~Sig.isna().any(axis=1)
    idx = [i for i in idx if bool(good.get(i, False))]
    if not idx:
        return pd.Series(dtype=float)

    mu_v = mu.reindex(idx).to_numpy(dtype=float)
    S_v = Sig.loc[idx, idx].to_numpy(dtype=float)
    S_v = S_v + 1e-10 * np.eye(len(idx))
    n = len(idx)
    excess = mu_v - float(rf)

    # Per-asset weight caps: y[i] <= cap[i] * sum(y) → w[i] <= cap[i] post-norm.
    _caps = globals().get("PER_ASSET_WEIGHT_CAPS", {}) or {}
    # Sector/theme group caps (same kappa-transform trick per group).
    _gcaps = globals().get("SECTOR_GROUP_CAPS", {}) or {}
    _group_idx = [
        ([_i for _i, _t in enumerate(idx) if _t in set(_g.get("tickers", []))],
         float(_g.get("cap", 1.0)))
        for _g in _gcaps.values()
    ]
    _group_idx = [(gi, gc) for gi, gc in _group_idx if gi]

    w = None
    if np.any(excess > 0):
        y = cp.Variable(n, nonneg=True)
        cons_tg = [excess @ y == 1]
        for _i, _ticker in enumerate(idx):
            if _ticker in _caps:
                cons_tg.append(y[_i] <= float(_caps[_ticker]) * cp.sum(y))
        for _gi, _gc in _group_idx:
            cons_tg.append(
                cp.sum(cp.hstack([y[_i] for _i in _gi])) <= _gc * cp.sum(y)
            )
        try:
            prob = cp.Problem(cp.Minimize(cp.quad_form(y, S_v)), cons_tg)
            prob.solve(solver=cp.OSQP, verbose=False)
            if y.value is None:
                prob.solve(solver=cp.ECOS, verbose=False)
        except Exception:
            pass
        if y.value is not None and float(np.nansum(y.value)) > 1e-12:
            w = np.clip(np.asarray(y.value, dtype=float), 0.0, None)
            w = w / w.sum()

    if w is None:  # fallback: minimum-variance long-only (same caps applied)
        wv = cp.Variable(n, nonneg=True)
        cons_mv = [cp.sum(wv) == 1]
        for _i, _ticker in enumerate(idx):
            if _ticker in _caps:
                cons_mv.append(wv[_i] <= float(_caps[_ticker]))
        for _gi, _gc in _group_idx:
            cons_mv.append(cp.sum(cp.hstack([wv[_i] for _i in _gi])) <= _gc)
        try:
            cp.Problem(cp.Minimize(cp.quad_form(wv, S_v)), cons_mv).solve(solver=cp.OSQP, verbose=False)
            if wv.value is None:
                cp.Problem(cp.Minimize(cp.quad_form(wv, S_v)), cons_mv).solve(solver=cp.ECOS, verbose=False)
        except Exception:
            pass
        if wv.value is None:
            return pd.Series(np.full(n, 1.0 / n), index=idx)
        w = np.clip(np.asarray(wv.value, dtype=float), 0.0, None)
        w = (w / w.sum()) if w.sum() > 0 else np.full(n, 1.0 / n)

    return pd.Series(w, index=idx)


def solve_frontier_point_cvxpy(
    mu: pd.Series,
    Sigma: pd.DataFrame,
    target_return: float,
    *,
    use_inequality: bool = True,
    B: pd.DataFrame | None = None,
    tilt_targets: pd.Series | dict | None = None,
    tilt_bands: pd.Series | dict | None = None,
    use_mask: dict | None = None,
    tilt_mode: str = "soft",
    tilt_penalty: float = 1e4,
    w_prev: pd.Series | None = None,
    turnover_penalty: float = 0.0,
) -> tuple[np.ndarray, bool, str]:
    """
    Long-only Markowitz with optional factor tilt constraints.

    Turnover penalty (cost-aware solver): if w_prev is supplied and
    turnover_penalty > 0, adds turnover_penalty * ||w - w_prev||_1 to the
    objective. Reduces unnecessary rebalancing churn that would realise CGT.
    Units: penalty is in the same units as w'Σw (daily variance), so a
    value around 1e-4 to 1e-3 is typically the right ballpark.
    """
    mu = pd.Series(mu).reindex(Sigma.index)
    mu = pd.to_numeric(mu, errors="coerce")

    keep = mu.index[mu.notna()]
    Sigma_use = Sigma.loc[keep, keep].copy()
    mu_use = mu.loc[keep].astype(float)

    # Drop assets with any NaN covariance row/col
    good = ~(Sigma_use.isna().any(axis=1) | Sigma_use.isna().any(axis=0))
    Sigma_use = Sigma_use.loc[good, good]
    mu_use = mu_use.reindex(Sigma_use.index)

    if len(mu_use) == 0:
        return np.array([]), False, "No valid assets"

    S = Sigma_use.to_numpy(dtype=float)
    S = S + 1e-10 * np.eye(len(S))

    n = len(mu_use)
    w = cp.Variable(n)

    constraints = [cp.sum(w) == 1, w >= 0]

    # Per-asset weight caps for assets where the mean-variance solver would
    # over-allocate based on inflated trailing μ (leveraged ETFs, volatility
    # products). Read from the PER_ASSET_WEIGHT_CAPS module-level dict; any
    # ticker NOT in the dict is uncapped. Setting cap=0 forces w=0 (exclude).
    _caps = globals().get("PER_ASSET_WEIGHT_CAPS", {}) or {}
    if _caps:
        for _i, _ticker in enumerate(mu_use.index):
            if _ticker in _caps:
                constraints.append(w[_i] <= float(_caps[_ticker]))

    # Sector/theme group caps: summed weight of each correlated cluster
    # ≤ cap. Complements the per-asset caps above — see SECTOR_GROUP_CAPS.
    _gcaps = globals().get("SECTOR_GROUP_CAPS", {}) or {}
    for _gspec in _gcaps.values():
        _gset = set(_gspec.get("tickers", []))
        _gidx = [_i for _i, _t in enumerate(mu_use.index) if _t in _gset]
        if _gidx:
            constraints.append(
                cp.sum(cp.hstack([w[_i] for _i in _gidx]))
                <= float(_gspec.get("cap", 1.0))
            )

    if use_inequality:
        constraints.append(mu_use.to_numpy(dtype=float) @ w >= float(target_return))
    else:
        constraints.append(mu_use.to_numpy(dtype=float) @ w == float(target_return))

    slack_terms = []
    if B is not None and tilt_targets is not None and tilt_bands is not None:
        B_use = B.reindex(mu_use.index)

        if isinstance(tilt_targets, dict):
            tilt_targets = pd.Series(tilt_targets)
        if isinstance(tilt_bands, dict):
            tilt_bands = pd.Series(tilt_bands)
        if use_mask is None:
            use_mask = {}

        tilt_targets = pd.to_numeric(tilt_targets, errors="coerce")
        tilt_bands = pd.to_numeric(tilt_bands, errors="coerce")

        for f in tilt_targets.index:
            if not bool(use_mask.get(f, False)):
                continue
            if f not in B_use.columns:
                continue

            t = float(tilt_targets.get(f, 0.0))
            b = float(tilt_bands.get(f, 0.0))
            v = pd.to_numeric(B_use[f], errors="coerce").fillna(0.0).to_numpy(dtype=float)

            if tilt_mode.lower() == "hard":
                constraints.append(v @ w <= t + b)
                constraints.append(v @ w >= t - b)
            else:
                s_pos = cp.Variable(nonneg=True)
                s_neg = cp.Variable(nonneg=True)
                constraints.append(v @ w <= (t + b) + s_pos)
                constraints.append(v @ w >= (t - b) - s_neg)
                slack_terms.extend([s_pos, s_neg])

    objective = cp.quad_form(w, S)
    if slack_terms and tilt_mode.lower() == "soft":
        objective = objective + float(tilt_penalty) * cp.sum(cp.hstack(slack_terms))

    # Turnover (cost-aware) penalty: ||w - w_prev||_1 in weight space.
    if w_prev is not None and float(turnover_penalty) > 0:
        try:
            w_prev_arr = (pd.Series(w_prev).reindex(mu_use.index)
                           .fillna(0.0).to_numpy(dtype=float))
            objective = objective + float(turnover_penalty) * cp.norm(w - w_prev_arr, 1)
        except Exception:
            # If w_prev can't be aligned, fall back silently to no penalty.
            pass

    prob = cp.Problem(cp.Minimize(objective), constraints)

    try:
        prob.solve(solver=cp.OSQP, verbose=False)
        if w.value is None:
            prob.solve(solver=cp.ECOS, verbose=False)
    except Exception as e:
        return np.full(len(Sigma.index), np.nan), False, f"Solver error: {e}"

    if w.value is None:
        return np.full(len(Sigma.index), np.nan), False, "Infeasible"

    w_sub = np.asarray(w.value).reshape(-1)
    w_full = pd.Series(0.0, index=Sigma.index)
    w_full.loc[mu_use.index] = w_sub

    note = "CVXPY success"
    if slack_terms and tilt_mode.lower() == "soft":
        note = "CVXPY success (soft tilts)"

    return w_full.to_numpy(dtype=float), True, note


def solve_frontier_point_cvxpy_with_tilts(
    mu: pd.Series,
    Sigma: pd.DataFrame,
    target_return: float,
    B: pd.DataFrame,
    tilt_targets: pd.Series,
    tilt_bands: pd.Series,
    use_mask: dict,
    *,
    use_inequality: bool = True,
):
    return solve_frontier_point_cvxpy(
        mu,
        Sigma,
        target_return,
        use_inequality=use_inequality,
        B=B,
        tilt_targets=tilt_targets,
        tilt_bands=tilt_bands,
        use_mask=use_mask,
        tilt_mode="hard",
    )


def solve_candidate_portfolios(
    mu: pd.Series,
    Sigma: pd.DataFrame,
    spy_mu: float | None,
    slots: tuple[tuple[str, float | None], ...] | None = None,
    w_prev: pd.Series | None = None,
    turnover_penalty: float = 0.0,
    tilt_targets: dict | None = None,
    tilt_bands: dict | None = None,
    B: pd.DataFrame | None = None,
    use_mask: dict | None = None,
    tilt_mode: str = "soft",
) -> dict[str, pd.Series]:
    """Solve all 5 candidate portfolios for a single rebalance.

    Returns {slot_name: weights}. If a return-floor slot is infeasible (target
    too high for the universe), that slot falls back to the next-most-aggressive
    feasible slot, then ultimately to tangency. This means in unfavourable
    universes the ensemble degenerates gracefully toward defensive — exactly
    when defensive is appropriate.

    Turnover penalty: when turnover_penalty > 0 and w_prev is given, the
    return-floor slots (non-tangency) include a ||w - w_prev||_1 penalty in
    their objective. Tangency (Modest) uses the kappa transform and isn't
    affected — it's already low-turnover by construction.
    """
    if slots is None:
        slots = ENSEMBLE_SLOTS
    out: dict[str, pd.Series] = {}
    tangency = max_sharpe_long_only(mu, Sigma, rf=0.0)
    if tangency is None or tangency.empty:
        # Cannot solve anything — return empty for all slots.
        return {name: pd.Series(dtype=float) for name, _ in slots}
    tangency_mu = float((mu.reindex(tangency.index).fillna(0.0) * tangency).sum())

    for name, premium in slots:
        if premium is None:
            out[name] = tangency.copy()
            continue
        if spy_mu is None or not np.isfinite(spy_mu):
            # No benchmark anchor → fall back to tangency for that slot.
            out[name] = tangency.copy()
            continue
        target_ret = float(spy_mu) + float(premium)
        # Tangency floor applies ONLY to positive-premium slots. (Kept as a
        # guard against premium <= 0 ever being added back in — Modest at +0%
        # bypasses the floor and is allowed to undershoot tangency if needed.)
        if float(premium) > 0:
            target_ret = max(target_ret, tangency_mu)
        try:
            w_arr, ok, _note = solve_frontier_point_cvxpy(
                mu, Sigma, target_ret, use_inequality=True,
                w_prev=w_prev, turnover_penalty=turnover_penalty,
                B=B, tilt_targets=tilt_targets, tilt_bands=tilt_bands,
                use_mask=use_mask, tilt_mode=tilt_mode,
            )
            if ok and w_arr is not None and len(w_arr) > 0 and np.isfinite(w_arr).all():
                w = pd.Series(w_arr, index=Sigma.index)
                w = w[w > 1e-6]
                if not w.empty and w.sum() > 0:
                    out[name] = w / w.sum()
                    continue
        except Exception:
            pass
        # Infeasible — defer to the most recently solved candidate (or tangency).
        out[name] = out[ENSEMBLE_SLOT_NAMES[max(0, list(ENSEMBLE_SLOT_NAMES).index(name) - 1)]].copy() if out else tangency.copy()
    return out


def _ledoit_wolf_cc(returns_df: "pd.DataFrame") -> tuple:
    """Ledoit-Wolf (2004) linear shrinkage of the sample covariance toward the
    constant-correlation target. Parameter-free — optimal intensity estimated
    from data. Better-conditioned Σ → more robust MV weights (Σ-side, avoids
    the μ-side error-max trap). Returns (cov_df, shrinkage_intensity).
    Verified standalone 2026-07-09 (PSD, symmetric, diag-preserving)."""
    X = returns_df.dropna(how="any").values
    T, N = X.shape
    cols = returns_df.columns
    if T < 2 or N < 2:
        return returns_df.cov(), 0.0
    Xc = X - X.mean(axis=0)
    S = (Xc.T @ Xc) / T
    var = np.diag(S).copy()
    std = np.sqrt(var)
    outer_std = np.outer(std, std)
    with np.errstate(divide="ignore", invalid="ignore"):
        corr = S / outer_std
    r_bar = (np.nansum(corr) - N) / (N * (N - 1))
    F = r_bar * outer_std
    np.fill_diagonal(F, var)
    Xc2 = Xc ** 2
    pi_mat = (Xc2.T @ Xc2) / T - S ** 2
    pi_hat = pi_mat.sum()
    term1 = ((Xc ** 3).T @ Xc) / T
    theta = term1 - var[:, None] * S
    np.fill_diagonal(theta, 0.0)
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = np.outer(std, 1.0 / std)
    rho_hat = np.diag(pi_mat).sum() + r_bar * np.nansum(ratio * theta)
    gamma = float(np.sum((F - S) ** 2))
    if gamma <= 0:
        delta = 0.0
    else:
        delta = max(0.0, min(1.0, ((pi_hat - rho_hat) / gamma) / T))
    # Rescale MLE (/T) sample cov back to unbiased (/(T-1)) convention on the
    # unshrunk part so levels match pandas .cov().
    Sigma = delta * F + (1.0 - delta) * S
    Sigma *= T / (T - 1)
    return pd.DataFrame(Sigma, index=cols, columns=cols), float(delta)


def _qis_shrinkage(returns_df: "pd.DataFrame") -> tuple:
    """Ledoit-Wolf (2020/2022) NONLINEAR covariance shrinkage via Quadratic-Inverse
    Shrinkage (QIS). Ported from the authors' reference QIS.m (Olivier Ledoit &
    Michael Wolf, BSD-2-Clause). Unlike lw_cc's single global shrinkage target,
    QIS shrinks each sample eigenvalue individually toward a data-driven nonlinear
    profile — provably optimal in high dimensions.

    Returns (cov_df, intensity) where intensity = mean |Δλ|/λ (eigenvalue-adjustment
    magnitude, a diagnostic). Parameter-free. Falls back to lw_cc on any degeneracy
    (too few obs, c>=1 pathologies) so the solve never breaks.

    Verified standalone 2026-07-13: symmetric, PSD, better-conditioned than sample,
    and recovers the sample cov in the T>>N (c->0) limit."""
    cols = returns_df.columns
    X = returns_df.dropna(how="any").values.astype(float)
    T, N = X.shape
    # QIS needs T > N+1 (p<=n) for the primary branch; fall back otherwise.
    if T < 12 or N < 2:
        return _ledoit_wolf_cc(returns_df)
    Y = X - X.mean(axis=0, keepdims=True)          # demean (k=1)
    n = T - 1
    p = N
    c = p / n
    S = (Y.T @ Y) / n
    S = (S + S.T) / 2.0
    try:
        lam, u = np.linalg.eigh(S)                 # ascending eigenvalues
    except np.linalg.LinAlgError:
        return _ledoit_wolf_cc(returns_df)
    lam = lam.real
    kept = max(0, p - n)                           # first (p-n) are ~null when p>n
    lam_pos = np.clip(lam[kept:], 1e-16, None)
    invlam = 1.0 / lam_pos
    L = len(invlam)
    # Bandwidth (per reference QIS.m): h = min(c^2,1/c^2)^0.35 / p^0.35.
    h = (min(c ** 2, 1.0 / c ** 2) ** 0.35) / (p ** 0.35)
    # Lj[i,j] = invlam[j]; Lj_i[i,j] = invlam[j] - invlam[i].
    Lj = np.tile(invlam, (L, 1))
    Lj_i = Lj - Lj.T
    denom = Lj_i ** 2 + (h ** 2) * (Lj ** 2)
    theta = np.mean(Lj * Lj_i / denom, axis=1)     # Stein shrinker (real part)
    Htheta = np.mean(Lj * (h * Lj) / denom, axis=1)  # conjugate (imag part)
    Atheta2 = theta ** 2 + Htheta ** 2
    if p <= n:
        delta = 1.0 / ((1.0 - c) ** 2 * invlam
                       + 2.0 * c * (1.0 - c) * invlam * theta
                       + c ** 2 * invlam * Atheta2)
    else:
        delta0 = 1.0 / ((c - 1.0) * np.mean(invlam))
        delta = np.concatenate([np.full(kept, delta0), 1.0 / (invlam * Atheta2)])
    # Preserve the trace: rescale shrunk eigenvalues to sum to the sample's.
    denom_sum = float(np.sum(delta))
    if not np.isfinite(denom_sum) or denom_sum <= 0:
        return _ledoit_wolf_cc(returns_df)
    delta_qis = delta * (float(np.sum(lam)) / denom_sum)
    Sigma = (u * delta_qis) @ u.T
    Sigma = (Sigma + Sigma.T) / 2.0
    intensity = float(np.mean(np.abs(delta_qis - lam) / np.clip(lam, 1e-16, None)))
    return pd.DataFrame(Sigma, index=cols, columns=cols), intensity


def estimate_covariance(returns_df: "pd.DataFrame", method: str = "lw_cc") -> tuple:
    """Dispatch Σ estimation by method. Returns (cov_df, intensity) where
    intensity is a scalar diagnostic (LW shrinkage δ, or a QIS proxy, else 0.0).

    Methods (see [[reference-cov-estimator-experiment]]):
      sample  — plain sample covariance (no shrinkage).
      lw_cc   — Ledoit-Wolf (2004) constant-correlation linear shrinkage (INCUMBENT).
      qis     — Ledoit-Wolf (2020) nonlinear shrinkage (Quadratic-Inverse Shrinkage).
    Unknown methods fall back to the incumbent lw_cc (never break the solve)."""
    m = str(method or "lw_cc").lower()
    if m in ("sample", "none", "off"):
        return returns_df.cov(), 0.0
    if m == "qis":
        return _qis_shrinkage(returns_df)
    return _ledoit_wolf_cc(returns_df)
