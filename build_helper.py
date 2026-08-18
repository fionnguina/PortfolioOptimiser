# build_helper.py
import subprocess
import sys
import shutil
from pathlib import Path

PROJECT_NAME = "Portfolio Optimiser"

# Modules we know often appear at runtime even if not imported explicitly
EXTRA_RUNTIME_DEPS = [
    # First-party modules imported INSIDE functions rather than at module top.
    # extract_top_level_imports() cannot see those, so they reach the binary
    # only via PyInstaller's own bytecode scan — declared here so the
    # dependency is stated rather than incidental. nav.compute_nav_from_statement
    # imports this lazily, and without it the statement-based NAV path returns
    # an empty Series in the frozen build and silently falls back.
    "ibkr_statement",
    # requests stack
    "requests", "urllib3", "idna", "certifi", "charset_normalizer",
    # date parsing / tz
    "dateutil", "dateutil.tz", "pytz", "tzdata",
    # scientific stack helpers
    "numpy", "pandas", "scipy", "statsmodels", "patsy",
    # excel / io
    "openpyxl", "et_xmlfile", "xlwings",
    # yfinance deps
    "yfinance", "frozendict", "peewee", "platformdirs", "websockets",
    "beautifulsoup4", "bs4", "soupsieve", "protobuf",
    "curl_cffi", "cffi", "pycparser", "multitasking",
    # win32 COM
    "win32com", "win32com.client", "pythoncom", "pywintypes",
    # tkinter UI
    "tkinter", "tkinter.ttk", "tkinter.messagebox", "tkinter.constants",
    # modern UI theming
    "customtkinter", "darkdetect",
]

# Data-heavy libs that need resources collected (DLLs, data files)
COLLECT_ALL_LIBS = [
    "numpy", "pandas", "scipy", "statsmodels",
    "openpyxl", "xlwings", "yfinance",
    "customtkinter",  # ships theme JSON + assets that must be bundled
    # cvxpy + backend solvers ship compiled DLLs and data files that PyInstaller's
    # default crawl misses (it bundles only the .py files). Without --collect-all
    # the .exe crashes at `import cvxpy` with WinError 3 on the cvxpy data dir.
    "cvxpy", "osqp", "scs", "clarabel",
]

# Optional: modules to exclude to silence irrelevant warnings/size bloat
EXCLUDES = [
    "xlwings.rest",                        # pulls Werkzeug etc. if you don't use the REST server
    "scipy._lib.array_api_compat.torch",   # we don't use torch backend
]

def extract_top_level_imports(py_file: Path) -> list[str]:
    """
    Extract imported module names from a Python source file via AST
    (includes function-level lazy imports; ignores docstrings, where the
    old regex once picked up a phantom module from prose like "from idx[0]").
    Returns unique, sorted list of module names (top-level only).
    """
    import ast
    text = py_file.read_text(encoding="utf-8", errors="ignore")
    mods = set()
    for node in ast.walk(ast.parse(text)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                mods.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            mods.add(node.module.split('.')[0])
    return sorted(m for m in mods if not m.startswith('_'))

def _free_dist_exe(root: Path) -> None:
    """Fail-fast guard for the recurring dist-lock gotcha: hung --noconsole
    launches keep the old exe locked, the artefact cleanup below swallows the
    rmtree failure silently, and PyInstaller then dies ~4 minutes later at its
    final os.remove. Detect the lock up front and kill the orphaned instances
    (a locked target exe means the requested build cannot succeed anyway)."""
    import os
    import time
    exe = root / "dist" / f"{PROJECT_NAME}.exe"
    if not exe.exists():
        return
    try:
        with open(exe, "ab"):
            return  # writable → not locked
    except PermissionError:
        pass
    print(f"[build] dist exe is LOCKED (hung instances?) — running taskkill...")
    subprocess.run(["taskkill", "/F", "/IM", f"{PROJECT_NAME}.exe"],
                   capture_output=True)
    time.sleep(2)
    try:
        with open(exe, "ab"):
            pass
        print("[build] lock released; continuing.")
    except PermissionError:
        raise SystemExit(
            f"[build] ABORT: {exe} is still locked after taskkill. "
            f"Close whatever holds it (Task Manager → {PROJECT_NAME}) and re-run."
        )


# Tracked files the ENGINE rewrites on every run. They are checked in on
# purpose — lots_seed is the CGT lot book and portfolio_state drives OOS
# starting NAV, so losing them loses history — but they are not code, and
# counting them as drift is what made "-dirty" meaningless. Keep this list
# tight: anything added here stops being able to make a build dirty.
RUNTIME_STATE = {
    "lots_seed.json",
    "portfolio_state.json",
    "pending_watch_resolved.json",
    "Stock Analysis.xlsm",
}


def _write_version_file(root: Path) -> tuple[str, str]:
    """Write _version.py with git SHA + build timestamp. Returns (sha, ts)."""
    import datetime
    sha = "unknown"
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(root), stderr=subprocess.DEVNULL
        ).decode("utf-8").strip()
        # Mark dirty if the tree differs from HEAD. Use `diff HEAD`, NOT bare
        # `diff`: the latter compares tree-vs-INDEX only, so a `git add`-ed-but-
        # uncommitted change reports clean and the exe gets stamped as matching
        # a clean sha while actually containing staged, never-committed edits —
        # silently defeating the [build] drift check.
        #
        # But RUNTIME_STATE is excluded, because those tracked files are
        # rewritten by every engine run. With them counted, the tree was never
        # clean and EVERY build stamped "-dirty" — so the marker carried no
        # information and "is my exe stale?" became a manual diff every time
        # (2026-08-10). Dirty now means CODE drift, which is the only kind that
        # changes what the binary does.
        changed = subprocess.check_output(
            ["git", "diff", "--name-only", "HEAD"],
            cwd=str(root), stderr=subprocess.DEVNULL
        ).decode("utf-8", errors="replace").splitlines()
        code_changed = sorted(
            f for f in (c.strip() for c in changed)
            if f and f not in RUNTIME_STATE
        )
        if code_changed:
            sha += "-dirty"
            print(f"[build][WARN] stamping {sha} — {len(code_changed)} "
                  f"uncommitted code file(s); this exe maps to no commit:")
            for f in code_changed[:10]:
                print(f"[build][WARN]     {f}")
            if len(code_changed) > 10:
                print(f"[build][WARN]     ... and {len(code_changed) - 10} more")
    except Exception:
        pass
    ts = datetime.datetime.now().isoformat(timespec="seconds")
    (root / "_version.py").write_text(
        f'# Auto-generated by build_helper.py — do not edit.\n'
        f'GIT_SHA = "{sha}"\n'
        f'BUILD_TIME = "{ts}"\n',
        encoding="utf-8",
    )
    return sha, ts


def build():
    root = Path(__file__).resolve().parent
    main_py = root / "Main.py"
    app_py  = root / "Portfolio_Optimiser.py"  # your main script body
    icon    = root / "icon.ico"

    if not main_py.exists():
        raise FileNotFoundError("Main.py not found beside build_helper.py")
    if not app_py.exists():
        raise FileNotFoundError("Portfolio_Optimiser.py not found beside build_helper.py")

    # Stamp the build with git SHA + timestamp BEFORE PyInstaller runs so the
    # generated _version.py is bundled into the .exe and logged on startup.
    sha, ts = _write_version_file(root)
    print(f"[build] version stamp: GIT_SHA={sha}, BUILD_TIME={ts}")

    _free_dist_exe(root)

    # Clean old artefacts
    for p in [root / "build", root / "dist", root / f"{PROJECT_NAME}.spec"]:
        try:
            if p.is_dir():
                shutil.rmtree(p)
            elif p.exists():
                p.unlink()
        except Exception:
            pass

    detected = extract_top_level_imports(app_py)
    print("Detected top-level imports:", detected)

    # Invoke PyInstaller via the active Python so we don't depend on the venv's Scripts/
    # dir being on PATH. sys.executable resolves to the venv python when build_helper.py
    # is launched as `./.venv/Scripts/python.exe build_helper.py`.
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--noconfirm",
        "--noconsole",
        "--onefile",
        "--name", PROJECT_NAME,
    ]
    if icon.exists():
        cmd += ["--icon", str(icon)]

    # Include your source file as data (not required for running, but handy)
    cmd += ["--add-data", f"{app_py.name};."]

    # Force-include _version.py so the build stamp survives the .exe.
    # extract_top_level_imports() filters out underscore-prefixed modules, so
    # without this PyInstaller never sees the `from _version import` line and
    # GIT_SHA falls back to 'dev' at runtime.
    cmd += ["--hidden-import", "_version"]
    cmd += ["--add-data", "_version.py;."]

    # Collect resources for heavy libs
    for lib in COLLECT_ALL_LIBS:
        cmd += ["--collect-all", lib]

    # Exclude noisy/unneeded modules (optional)
    for mod in EXCLUDES:
        cmd += ["--exclude-module", mod]

    # For every detected import: include hidden-import + collect-submodules
    all_mods = sorted(set(detected) | set(EXTRA_RUNTIME_DEPS))
    for mod in all_mods:
        cmd += ["--hidden-import", mod]
        cmd += ["--collect-submodules", mod]

    # Entry point
    cmd.append(str(main_py))

    print("\nRunning build:\n", " ".join(cmd))
    subprocess.run(cmd, check=True)

    # Post-build: copy runtime data files next to the .exe. These are loaded
    # via APP_DIR at runtime which (when frozen) resolves to dist/, so they
    # need to sit alongside the executable rather than be unpacked from the
    # PyInstaller --add-data temp dir.
    dist_dir = root / "dist"
    runtime_data_files = [
        "tlh_pairs.json",
        "regions.json",
    ]
    for name in runtime_data_files:
        src = root / name
        if src.exists():
            dst = dist_dir / name
            try:
                shutil.copy2(src, dst)
                print(f"[build] copied runtime data: {name} → {dst}")
            except Exception as e:
                print(f"[build] WARNING: failed to copy {name}: {e}")
        else:
            print(f"[build] skip runtime data {name}: source not found")

    print("\n[OK] Build complete. Check the 'dist' folder for your new .exe.")

if __name__ == "__main__":
    build()
