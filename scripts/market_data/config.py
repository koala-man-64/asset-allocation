
from scripts.common.config_shared import *

# Local Specific Requirements
AZURE_CONTAINER_MARKET = require_env("AZURE_CONTAINER_MARKET")

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
