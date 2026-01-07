#!/usr/bin/env python3
import sys
import logging
from scripts.common import config
from asset_allocation.ui import cli

def setup_logging():
    logging.basicConfig(
        level=logging.INFO if config.ENABLE_LOGGING else logging.WARNING,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        filename=config.BASE_DIR / 'app.log',
        filemode='a'
    )
    # Console handler for errors only (since we use stdout for UI)
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    logging.getLogger('').addHandler(console)

def main():
    setup_logging()
    try:
        # NOTE: asset_allocation.ui is missing/broken. 
        # Commenting out to allow main.py to be valid for debugging.
        # cli.main_loop()
        print("CLI main loop skipped as asset_allocation package is missing.")
    except KeyboardInterrupt:
        print("\nExiting...")
        sys.exit(0)
    except Exception as e:
        print(f"Critical Error: {e}")
        logging.exception("Critical error in main loop")
        sys.exit(1)

if __name__ == "__main__":
    main()
