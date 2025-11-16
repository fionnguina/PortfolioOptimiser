# Main.py
import multiprocessing as mp
import importlib.util
import sys
import os

def main():
    # Bundle-aware import for Portfolio_Optimiser.py
    script_name = "Portfolio_Optimiser.py"
    script_path = os.path.join(os.path.dirname(sys.executable), script_name)

    if not os.path.exists(script_path):
        # When running from source (not .exe)
        script_path = os.path.join(os.path.dirname(__file__), script_name)

    spec = importlib.util.spec_from_file_location("__main__", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

if __name__ == "__main__":
    mp.freeze_support()
    main()
