
import os
from pathlib import Path
from typing import Optional
from colorama import init, Fore, Style
from dotenv import load_dotenv

# Initialize colorama
init(autoreset=True)

# Load environment variables from .env file
# Load environment variables from .env file
load_dotenv(override=True)

# --- Constants & Configuration ---

def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Environment variable '{name}' is strictly required but not set.")
    return value

def _require_env_path(name: str) -> Path:
    return Path(_require_env(name))

def _optional_env_path(name: str) -> Optional[Path]:
    value = os.environ.get(name)
    if not value:
        return None
    return Path(value)

# Base Directory (Project Root)
# scripts/common/config.py -> scripts/common -> scripts -> ProjectRoot
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"

# Ensure Data directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Azure Configuration
# STRICT ENFORCEMENT: All containers and account details must be present.
AZURE_STORAGE_ACCOUNT_NAME = _require_env('AZURE_STORAGE_ACCOUNT_NAME')
# Connection string is optional IF account name and managed identity work, 
# but usually we want at least one. However, the user asked for NO defaults.
# We will require Account Name as the primary identity.
AZURE_STORAGE_CONNECTION_STRING = _require_env('AZURE_STORAGE_CONNECTION_STRING') 

# Yahoo Credentials
YAHOO_USERNAME = _require_env("YAHOO_USERNAME")
YAHOO_PASSWORD = _require_env("YAHOO_PASSWORD")

# Azure Storage Container Names
AZURE_CONTAINER_MARKET = os.environ.get("AZURE_CONTAINER_MARKET")
AZURE_CONTAINER_FINANCE = os.environ.get("AZURE_CONTAINER_FINANCE")
AZURE_CONTAINER_EARNINGS = os.environ.get("AZURE_CONTAINER_EARNINGS")
AZURE_CONTAINER_TARGETS = os.environ.get("AZURE_CONTAINER_TARGETS")
AZURE_CONTAINER_COMMON = os.environ.get("AZURE_CONTAINER_COMMON")
AZURE_CONTAINER_RANKING = os.environ.get("AZURE_CONTAINER_RANKING", "ranking")


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

# Data Settings
YAHOO_MAX_PERIOD = 99999999999
DATA_FRESHNESS_SECONDS = 4 * 60 * 60

# Debug Configuration
DEBUG_SYMBOLS = ['AAPL', 'MSFT', 'F', 'BAC']

# Playwright Configuration
# STRICT ENFORCEMENT: HEADLESS_MODE must be explicit (True/False)
_headless_str = _require_env("HEADLESS_MODE").lower()
if _headless_str not in ['true', 'false']:
    raise ValueError("HEADLESS_MODE must be 'true' or 'false'")
HEADLESS_MODE = _headless_str == "true"

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Playwright Paths
DOWNLOADS_PATH = _optional_env_path("DOWNLOADS_PATH")
USER_DATA_DIR = _optional_env_path("PLAYWRIGHT_USER_DATA_DIR")

# Internal Data Config
TICKERS_TO_ADD = [
    {
    'Symbol': 'SPY',
    'Description': 'S&P 500 Index ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Index'
    },
    {
    'Symbol': 'DIA',
    'Description': 'Dow Jones Index ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Index'
    },
    {
    'Symbol': 'QQQ',
    'Description': 'Nasdaq Index ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Index'
    },
    {
    'Symbol': '^VIX',
    'Description': 'Volatility Index ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Index'
    },
    {
    'Symbol': 'UST',
    'Description': 'US Treasury ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Index'
    },
    {
    'Symbol': 'IWC',
    'Description': 'Micro Cap ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Market Cap'
    },
    {
    'Symbol': 'VB',
    'Description': 'Small Cap ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Market Cap'
    },
]
