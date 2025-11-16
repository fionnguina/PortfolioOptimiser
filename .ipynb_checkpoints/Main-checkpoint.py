import traceback

def main():
    try:
        import portfolio_optimiser   # this is your big code file
        portfolio_optimiser.run_all()  # this runs everything inside it
    except Exception:
        print("Something went wrong:")
        traceback.print_exc()
        input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()
