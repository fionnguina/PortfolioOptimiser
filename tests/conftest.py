"""Pytest config + shared helpers.

The engine is a single ~9k-line module with heavy side-effects at import time
(yfinance downloads, Excel dialogs, PPT exports). For unit tests we extract
individual function definitions via AST and exec them into a clean namespace —
no module-level code runs.

Usage in a test file:
    from conftest import extract_funcs
    ns = extract_funcs("compute_cgt_tax", "compute_fill_drift")
    result = ns["compute_cgt_tax"](...)
"""
from __future__ import annotations

import ast
import json
from pathlib import Path
import pandas as pd
import numpy as np


ENGINE_PATH = Path(__file__).resolve().parent.parent / "Portfolio_Optimiser.py"


def extract_funcs(*names: str, extra_consts: tuple[str, ...] = ()) -> dict:
    """Pull named functions + optional module-level constants from the engine
    into a fresh namespace WITHOUT executing the engine's module-level code.

    Returns a dict with the functions + constants installed. Common runtime
    dependencies (pd, np, json, Path, math, os, sys, time) are pre-loaded.
    """
    src = ENGINE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(src)

    import math, os, sys, time
    from dateutil.relativedelta import relativedelta
    import datetime as _dt
    ns: dict = {
        "pd": pd, "np": np, "json": json, "Path": Path,
        "math": math, "os": os, "sys": sys, "time": time,
        "relativedelta": relativedelta, "datetime": _dt,
    }

    # Pull constants first (functions may reference them at exec time).
    want_consts = set(extra_consts)
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id in want_consts and node.value is not None:
                try:
                    ns[node.target.id] = ast.literal_eval(node.value)
                except Exception:
                    pass
        elif isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name) and tgt.id in want_consts:
                    try:
                        ns[tgt.id] = ast.literal_eval(node.value)
                    except Exception:
                        pass

    # Pull function definitions.
    found = set()
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            exec(ast.get_source_segment(src, node), ns)
            found.add(node.name)
    missing = set(names) - found
    if missing:
        raise RuntimeError(f"Functions not found in engine: {sorted(missing)}")
    return ns
