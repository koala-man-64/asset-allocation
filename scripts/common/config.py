
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
DEBUG_SYMBOLS = ['AAPL', 'MSFT', 'F']

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
    {
    'Symbol': 'VO',
    'Description': 'Mid Cap ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Market Cap'
    },
    {
    'Symbol': 'VV',
    'Description': 'Large Cap ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Market Cap'
    },
    {
    'Symbol': 'XLK',
    'Description': 'Technology Sector ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Sector'
    },
    {
    'Symbol': 'XLU',
    'Description': 'Utilities Sector ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Sector'
    },
    {
    'Symbol': 'XLC',
    'Description': 'Communication Services Sector ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Sector'
    },
    {
    'Symbol': 'XLY',
    'Description': 'Consumer Discretionary Sector ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Sector'
    },
    {
    'Symbol': 'XLP',
    'Description': 'Consumer Staples Sector ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Sector'
    },
    {
    'Symbol': 'XLE',
    'Description': 'Energy Sector ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Sector'
    },
    {
    'Symbol': 'XLF',
    'Description': 'Financials Sector ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Sector'
    },
    {
    'Symbol': 'XLV',
    'Description': 'Healthcare Sector ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Sector'
    },
    {
    'Symbol': 'XLI',
    'Description': 'Industrials Sector ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Sector'
    },
    {
    'Symbol': 'XLB',
    'Description': 'Materials Sector ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Sector'
    },
    {
    'Symbol': 'XLRE',
    'Description': 'Real Estate Sector ETF',
    'Sector': 'Market Analysis',
    'Industry': 'Sector'
    },
    {
    'Symbol': 'ERX',
    'Description': 'Direxion Daily Energy Bull 2X Shares',
    'Sector': 'Energy',
    'Industry': 'ETF'
    },
    {
    'Symbol': 'UTSL',
    'Description': 'Direxion Daily Utilities Bull 3X Shares',
    'Sector': 'Utilites',
    'Industry': 'ETF'
    }
]
