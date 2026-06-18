# Main.py
import multiprocessing as mp
import importlib.util
import sys
import os
import io
import datetime as _dt


def _setup_logging():
    """Redirect stdout/stderr to a log file beside the executable.

    The build is --noconsole, so prints and tracebacks would otherwise vanish.
    We tee output to a log file (run.log) AND keep writing to the original
    streams when present, so running from source still shows live output.
    """
    # Locate the directory the user actually launched from.
    base_dir = os.path.dirname(sys.executable) if getattr(sys, "frozen", False) \
        else os.path.dirname(os.path.abspath(__file__))
    log_path = os.path.join(base_dir, "run.log")

    # Rotate prior run.log files: run.log -> run.log.1, .1 -> .2, ..., keep last 7.
    # Lets us go back and diff against a working run if today's break.
    KEEP = 7
    try:
        for i in range(KEEP - 1, 0, -1):
            src = os.path.join(base_dir, f"run.log.{i}" if i > 0 else "run.log")
            dst = os.path.join(base_dir, f"run.log.{i+1}")
            if os.path.exists(src):
                if os.path.exists(dst):
                    try: os.remove(dst)
                    except Exception: pass
                try: os.rename(src, dst)
                except Exception: pass
        if os.path.exists(log_path):
            dst = os.path.join(base_dir, "run.log.1")
            if os.path.exists(dst):
                try: os.remove(dst)
                except Exception: pass
            try: os.rename(log_path, dst)
            except Exception: pass
    except Exception:
        pass  # rotation is best-effort; never block startup

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
    return log_path


def main():
    log_path = _setup_logging()
    if log_path:
        print(f"[log] writing run log to: {log_path}")

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
