# Main.py
import multiprocessing as mp
import importlib.util
import sys
import os
import io
import datetime as _dt
import faulthandler  # catches hard crashes (segfault, COM crash) with traceback


def _setup_logging():
    """Redirect stdout/stderr to a timestamped log file beside the executable.

    The build is --noconsole, so prints and tracebacks would otherwise vanish.
    We tee output to `run_YYYY-MM-DD_HH-MM-SS.log` AND keep writing to the
    original streams when present, so running from source still shows live
    output. Also keeps a `run.log` symlink-like copy (overwritten each run)
    for quick "latest" inspection.

    Old run.log/run.log.N files from the prior rotation scheme are migrated
    on first encounter. Logs older than KEEP_LATEST timestamped files are
    auto-pruned so the dist folder stays tidy.
    """
    # Locate the directory the user actually launched from.
    base_dir = os.path.dirname(sys.executable) if getattr(sys, "frozen", False) \
        else os.path.dirname(os.path.abspath(__file__))

    # Generate timestamped filename for THIS run.
    ts = _dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"run_{ts}.log"
    log_path = os.path.join(base_dir, log_filename)

    # Cleanup: remove old rotating run.log.N files from the legacy scheme.
    # Also delete the previous run.log copy (we'll overwrite it below).
    try:
        for name in os.listdir(base_dir):
            if name.startswith("run.log.") or name == "run.log":
                try:
                    os.remove(os.path.join(base_dir, name))
                except Exception:
                    pass
    except Exception:
        pass

    # Auto-prune timestamped logs: keep the most recent KEEP_LATEST.
    KEEP_LATEST = 10
    try:
        ts_logs = sorted(
            (n for n in os.listdir(base_dir)
             if n.startswith("run_") and n.endswith(".log")),
            reverse=True,
        )
        for old in ts_logs[KEEP_LATEST:]:
            try:
                os.remove(os.path.join(base_dir, old))
            except Exception:
                pass
    except Exception:
        pass

    try:
        log_fh = open(log_path, "w", encoding="utf-8", buffering=1)
    except Exception:
        return None  # logging is best-effort; never block startup on it

    header = (
        f"=== Portfolio Optimiser run @ {_dt.datetime.now().isoformat(timespec='seconds')} ===\n"
        f"executable: {sys.executable}\n"
        f"frozen:     {bool(getattr(sys, 'frozen', False))}\n"
        f"base_dir:   {base_dir}\n"
        "----------------------------------------\n"
    )
    log_fh.write(header)

    class _Tee(io.TextIOBase):
        def __init__(self, *streams):
            self._streams = [s for s in streams if s is not None]
        def write(self, s):
            for st in self._streams:
                try:
                    st.write(s)
                    st.flush()
                except Exception:
                    pass
            return len(s)
        def flush(self):
            for st in self._streams:
                try: st.flush()
                except Exception: pass

    sys.stdout = _Tee(sys.__stdout__, log_fh)
    sys.stderr = _Tee(sys.__stderr__, log_fh)
    # Also write a "run.log" copy (overwritten each run) for quick "latest"
    # inspection without having to find the newest timestamped file.
    try:
        latest_copy = open(os.path.join(base_dir, "run.log"),
                            "w", encoding="utf-8", buffering=1)
        latest_copy.write(header)
        sys.stdout = _Tee(sys.__stdout__, log_fh, latest_copy)
        sys.stderr = _Tee(sys.__stderr__, log_fh, latest_copy)
    except Exception:
        pass  # latest-copy is a convenience; never block on it
    return log_path


def main():
    log_path = _setup_logging()
    if log_path:
        print(f"[log] writing run log to: {log_path}")
    # Catch hard crashes (SIGSEGV, Windows AV from Excel COM, etc.) — without
    # this, the exe vanishes silently when xlwings/COM detonates. faulthandler
    # writes a Python-level traceback when the process dies. In --noconsole
    # PyInstaller builds, sys.stderr is the tee wrapper which has no fileno,
    # so we point faulthandler at the underlying log file directly.
    try:
        if log_path:
            _fh_log = open(log_path, "a", encoding="utf-8")
            faulthandler.enable(file=_fh_log)
        else:
            faulthandler.enable()  # falls back to real stderr if any
    except Exception as _fh_err:
        # Silent — faulthandler is best-effort safety; never block startup.
        pass

    # Bundle-aware import for Portfolio_Optimiser.py
    script_name = "Portfolio_Optimiser.py"
    script_path = os.path.join(os.path.dirname(sys.executable), script_name)

    if not os.path.exists(script_path):
        # When running from source (not .exe)
        script_path = os.path.join(os.path.dirname(__file__), script_name)

    try:
        spec = importlib.util.spec_from_file_location("__main__", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    except Exception:
        # Make sure the traceback lands in the log even if the GUI swallows it.
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    mp.freeze_support()
    main()
