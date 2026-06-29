"""Worker process for parallel OOS execution (Phase 3b, 2026-06-29).

Spawned by the main engine's scale-sensitivity loop via
ProcessPoolExecutor. Each worker:

  1. Imports Portfolio_Optimiser.py in OOS_KERNEL_MODE=1, which runs
     all imports + config + helper / class definitions, then exits
     cleanly via sys.exit(0) right after the OOS function is defined.
     The exit raises SystemExit during exec_module, which we catch —
     the module object is still fully populated.
  2. Pulls run_oos_ensemble_walk_forward_cached from the module.
  3. Loads pickled (prices_aud, kwargs) from the input file written
     by the parent.
  4. Calls the function, writes the result back as pickle.

Designed for ProcessPoolExecutor with `submit` semantics — the
parent dispatches one job per cache-miss NAV, workers process them
concurrently. Cache hits short-circuit at the parent level so we
don't spawn workers for free results.

Per-worker startup cost: ~25-30s (data download + FF5 fit + function
defs). Per-NAV OOS cost: ~60s. So a 3-NAV parallel sweep at 3 cores:
  Sequential:  3 × (30+60) = 270s
  Parallel:    ~30s setup + 60s OOS (3 workers concurrent) = ~90s
  Saved:       ~180s (3× speedup)

The setup cost would amortize if workers stayed alive across multiple
OOS calls, but ProcessPoolExecutor recycles per job. For the typical
4-NAV scale-sensitivity workload that's fine.
"""
from __future__ import annotations

import os
import pickle
import sys
from pathlib import Path


def run_oos_in_worker(in_pickle_path: str, out_pickle_path: str) -> None:
    """Worker entry. Reads (prices_aud, kwargs) from in_pickle_path,
    runs OOS, writes result to out_pickle_path. NO exceptions caught
    here — let them propagate so ProcessPoolExecutor surfaces them."""
    # Set OOS_KERNEL_MODE so the engine exits cleanly after defining
    # the OOS function. Also bypass the Holdings freshness check (which
    # fires earlier in the script and would block worker startup), and
    # force non-interactive mode (no dialog).
    os.environ["OOS_KERNEL_MODE"] = "1"
    os.environ["HOLDINGS_FRESHNESS_BYPASS"] = "1"
    # _AUTO_PIPELINE_MODE detection in the engine looks at sys.argv;
    # set it so the dialog path is skipped.
    if "--auto-pipeline" not in sys.argv:
        sys.argv.append("--auto-pipeline")

    # Locate Portfolio_Optimiser.py — same directory as this worker
    engine_path = Path(__file__).resolve().parent / "Portfolio_Optimiser.py"
    if not engine_path.exists():
        raise FileNotFoundError(f"Engine not found at {engine_path}")

    # Import in kernel mode
    import importlib.util
    spec = importlib.util.spec_from_file_location("_oos_kernel_module",
                                                     str(engine_path))
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except SystemExit:
        pass  # kernel mode early-exit — expected

    # Grab the cached wrapper (handles disk cache transparently)
    fn = getattr(module, "run_oos_ensemble_walk_forward_cached", None)
    if fn is None:
        raise RuntimeError(
            "Worker could not find run_oos_ensemble_walk_forward_cached "
            "in engine module — kernel mode exit may have been before "
            "the function was defined."
        )

    # Load the job input
    with open(in_pickle_path, "rb") as f:
        prices_aud, kwargs = pickle.load(f)

    # Run the OOS — cached wrapper handles disk cache hit/miss
    result = fn(prices_aud, **kwargs)

    # Write the result
    with open(out_pickle_path, "wb") as f:
        pickle.dump(result, f, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    # Allow direct CLI invocation for debugging: python oos_worker.py in.pkl out.pkl
    if len(sys.argv) != 3:
        print("Usage: oos_worker.py <input_pickle> <output_pickle>",
              file=sys.stderr)
        sys.exit(2)
    run_oos_in_worker(sys.argv[1], sys.argv[2])
