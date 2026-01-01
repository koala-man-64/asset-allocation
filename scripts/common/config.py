
import os
from pathlib import Path
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

# Base Directory (Project Root)
# scripts/common/config.py -> scripts/common -> scripts -> ProjectRoot
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"

# Ensure Data directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Azure Configuration
# Prioritize Account Name (Identity), allow Connection String as fallback
AZURE_STORAGE_ACCOUNT_NAME = os.environ.get('AZURE_STORAGE_ACCOUNT_NAME')
AZURE_STORAGE_CONNECTION_STRING = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
# Note: AZURE_CONTAINER_NAME legacy line removed as we now define it below strictly.

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
# Set to a list of symbols (e.g., ['AAPL', 'MSFT']) to restrict the scraper to only these.
# Set to [] or None to run on the full universe.
DEBUG_SYMBOLS = ['AAPL', 'MSFT', 'F', 'BAC']

# Playwright Configuration
HEADLESS_MODE = True
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Playwright Paths
# STRICT MODE: These must be set in the environment (.env or system)
DOWNLOADS_PATH = _require_env_path("DOWNLOADS_PATH")
USER_DATA_DIR = _require_env_path("PLAYWRIGHT_USER_DATA_DIR")

# Yahoo Credentials
YAHOO_USERNAME = os.environ.get("YAHOO_USERNAME")
YAHOO_PASSWORD = os.environ.get("YAHOO_PASSWORD")

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

# Azure Storage Container Names
# Standardized names: AZURE_CONTAINER_[MARKET|FINANCE|EARNINGS|TARGETS|COMMON]
# STRICT MODE REVISION: Allow None at import time so jobs can run with partial config.
# Specific scripts will fail at runtime if their required container is missing.
AZURE_CONTAINER_MARKET = os.environ.get("AZURE_CONTAINER_MARKET")
AZURE_CONTAINER_FINANCE = os.environ.get("AZURE_CONTAINER_FINANCE")
AZURE_CONTAINER_EARNINGS = os.environ.get("AZURE_CONTAINER_EARNINGS")
AZURE_CONTAINER_TARGETS = os.environ.get("AZURE_CONTAINER_TARGETS")
AZURE_CONTAINER_COMMON = os.environ.get("AZURE_CONTAINER_COMMON")

# Legacy/Backup for backward compatibility (Deprecated, will refactor out)
AZURE_CONTAINER_NAME = AZURE_CONTAINER_MARKET
AZURE_CONFIG_CONTAINER_NAME = AZURE_CONTAINER_COMMON
AZURE_CONTAINER_PRICE_TARGETS = AZURE_CONTAINER_TARGETS
