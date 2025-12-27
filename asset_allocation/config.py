import os
import sys
from pathlib import Path
from colorama import init, Fore, Style

# Initialize colorama
init(autoreset=True)

# --- Constants & Configuration ---

# Base Directory (Project Root)
# Assuming this config.py is in asset_allocation/config.py, 
# and the project root is one level up (AssetAllocation/)
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "Data"

# Ensure Data directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)

# UI Colors
COLOR_INDEX = Fore.YELLOW
COLOR_DATE = Fore.CYAN
COLOR_NUMBER = Fore.GREEN
COLOR_STRING = Fore.MAGENTA
COLOR_RESET = Style.RESET_ALL

# App Settings
DEFAULT_SMA_PERIOD = 50
DEFAULT_BOLLINGER_PERIOD = 20
DEFAULT_BOLLINGER_STD = 2

# Feature Flags / Toggles
ENABLE_LOGGING = True
