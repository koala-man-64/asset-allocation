
import os
from pathlib import Path
from colorama import init, Fore, Style
from dotenv import load_dotenv

# Initialize colorama
init(autoreset=True)

# Load environment variables from .env file
load_dotenv()

# --- Constants & Configuration ---

# Base Directory (Project Root)
# scripts/common/config.py -> scripts/common -> scripts -> ProjectRoot
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "Data"

# Ensure Data directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Azure Configuration
# Prioritize Account Name (Identity), allow Connection String as fallback
AZURE_STORAGE_ACCOUNT_NAME = os.environ.get('AZURE_STORAGE_ACCOUNT_NAME')
AZURE_STORAGE_CONNECTION_STRING = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
AZURE_CONTAINER_NAME = os.environ.get('AZURE_CONTAINER_NAME', 'market-data')

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
