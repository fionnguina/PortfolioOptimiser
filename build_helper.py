# build_helper.py
import re
import subprocess
import shutil
from pathlib import Path

PROJECT_NAME = "Portfolio Optimiser"

# Modules we know often appear at runtime even if not imported explicitly
EXTRA_RUNTIME_DEPS = [
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
]

# Data-heavy libs that need resources collected (DLLs, data files)
COLLECT_ALL_LIBS = [
    "numpy", "pandas", "scipy", "statsmodels",
    "openpyxl", "xlwings", "yfinance",
]

# Optional: modules to exclude to silence irrelevant warnings/size bloat
EXCLUDES = [
    "xlwings.rest",                        # pulls Werkzeug etc. if you don't use the REST server
    "scipy._lib.array_api_compat.torch",   # we don't use torch backend
]

def extract_top_level_imports(py_file: Path) -> list[str]:
    """
    Extract top-level imported module names from a Python source file.
    Returns unique, sorted list of module names (top-level only).
    """
    text = py_file.read_text(encoding="utf-8", errors="ignore")
    pat = re.compile(r'^\s*(?:from|import)\s+([A-Za-z0-9_\.]+)', re.MULTILINE)
    mods = set()
    for m in pat.findall(text):
        top = m.split('.')[0]
        if not top.startswith('_'):
            mods.add(top)
    return sorted(mods)

def build():
    root = Path(__file__).resolve().parent
    main_py = root / "Main.py"
    app_py  = root / "Portfolio_Optimiser.py"  # your main script body
    icon    = root / "icon.ico"

    if not main_py.exists():
        raise FileNotFoundError("Main.py not found beside build_helper.py")
    if not app_py.exists():
        raise FileNotFoundError("Portfolio_Optimiser.py not found beside build_helper.py")

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

    cmd = [
        "pyinstaller",
        "--noconfirm",
        "--noconsole",
        "--onefile",
        "--name", PROJECT_NAME,
    ]
    if icon.exists():
        cmd += ["--icon", str(icon)]

    # Include your source file as data (not required for running, but handy)
    cmd += ["--add-data", f"{app_py.name};."]

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
    print("\n✅ Build complete. Check the 'dist' folder for your new .exe.")

if __name__ == "__main__":
    build()
