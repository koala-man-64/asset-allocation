
import os
from pathlib import Path
from typing import Optional
from colorama import init, Fore, Style
from dotenv import load_dotenv

# Initialize colorama
init(autoreset=True)

# Load environment variables from .env file for local development.
# CI/tests can disable this via DISABLE_DOTENV=true.
_disable_dotenv_raw = (os.environ.get("DISABLE_DOTENV") or "").strip()
if _disable_dotenv_raw.lower() not in {"1", "true", "yes"}:
    load_dotenv(override=False)

# --- Constants & Configuration ---

def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Environment variable '{name}' is strictly required but not set.")
    return value


def require_env_bool(name: str) -> bool:
    raw = require_env(name).strip().lower()
    if raw in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value for {name}: {raw!r}")

def _require_env_path(name: str) -> Path:
    return Path(require_env(name))

def _optional_env_path(name: str) -> Optional[Path]:
    value = os.environ.get(name)
    if not value:
        return None
    return Path(value)

# Base Directory (Project Root)
# scripts/common/config_shared.py -> scripts/common -> scripts -> ProjectRoot
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"

# Ensure Data directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Azure Configuration
# STRICT ENFORCEMENT: Storage account and common container are required for everyone.
AZURE_STORAGE_ACCOUNT_NAME = require_env('AZURE_STORAGE_ACCOUNT_NAME')
AZURE_STORAGE_CONNECTION_STRING = require_env('AZURE_STORAGE_CONNECTION_STRING') 
AZURE_CONTAINER_COMMON = require_env("AZURE_CONTAINER_COMMON")

# Optional lake/medallion containers (may be unused depending on deployment contract).
# Kept optional to preserve backward compatibility for environments that still use per-domain containers.
AZURE_CONTAINER_BRONZE = require_env("AZURE_CONTAINER_BRONZE")
AZURE_CONTAINER_SILVER = require_env("AZURE_CONTAINER_SILVER")
AZURE_CONTAINER_GOLD = require_env("AZURE_CONTAINER_GOLD")

# Yahoo Credentials
YAHOO_USERNAME = os.environ.get("YAHOO_USERNAME")
YAHOO_PASSWORD = os.environ.get("YAHOO_PASSWORD")

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
HEADLESS_MODE = require_env_bool("HEADLESS_MODE")

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Playwright Paths
DOWNLOADS_PATH = _optional_env_path("DOWNLOADS_PATH")
USER_DATA_DIR = _optional_env_path("PLAYWRIGHT_USER_DATA_DIR")

# Shared Data Configuration
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
