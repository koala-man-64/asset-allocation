import os
from playwright.sync_api import sync_playwright
from typing import Tuple, Optional
from playwright.sync_api import Playwright, Browser, BrowserContext, Page, Download, TimeoutError as PlaywrightTimeout
import sys
import glob
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import asyncio
import re
import random
import time
from pathlib import Path
import math
import uuid
import csv
import copy
from datetime import datetime, timedelta
import nasdaqdatalink
from multiprocessing import Pool, cpu_count
import concurrent.futures
import pytz
import fnmatch

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator

from bs4 import BeautifulSoup
import requests
import pickle
from dateutil.relativedelta import relativedelta
import ta
import itertools
import warnings
# sys.path.append(os.path.abspath('G:\My Drive\Python\SchwabCrawler'))
from scripts.common import playwright_lib as pl
# Suppress warnings
warnings.filterwarnings('ignore')



class Portfolio:
    def __init__(self):
        self.OpenPositions = {} # symbol index
        self.PositionsHistory = {} # date-symbol index
        self.CashBalanceHistory = {} # date index
        self.EquityBalanceHistory = {} # date index
        self.PortfolioBalanceHistory = {} # date index
        self.BenchmarkBalanceHistory = {}
        
    def UpdatePortfolioPrices(self, prices, date, col):
        equity_value = 0
        for pos_index, pos in enumerate(self.OpenPositions):
            position = self.OpenPositions[pos]
            if len(prices[prices['Symbol'] == position.Symbol][col]) > 0:
                position.Price = prices[prices['Symbol'] == position.Symbol][col].values[0]
                position.MarketValue = round(position.Price * position.Quantity, 2)
                position.ProfitLoss = round(position.MarketValue - position.CostBasis, 2)
            position.LastUpdated = date  
            self.OpenPositions[pos] = position
            equity_value += position.MarketValue
            
        self.UpdateEquityBalance(date, equity_value)
        if date not in self.CashBalanceHistory.keys():
            self.UpdateCashBalance(date, self.CurrentCashBalance())  
    
    
    
    def ClosePosition(self, symbol, price, date):
        # lookup open position based on symbol
        pos = self.OpenPositions[symbol]
        
        # if position isn't found, return None
        if pos is None:
            return None
        # else close out position
        else:        
            # update position object with new data
            pos.Price = price
            pos.MarketValue = round(pos.Price * pos.Quantity, 2)
            pos.ProfitLoss = round(pos.MarketValue - pos.CostBasis, 2)
            pos.LastUpdated = date
            pos.DateClosed = date
            
            # remove from open positions
            del self.OpenPositions[symbol]
            
            # update positions history
            self.PositionsHistory[pos.Symbol + '-' + pos.DateOpened.strftime("%Y-%m-%d")] = pos # add to positions history
            
            # update cashbalance history
            if date in self.CashBalanceHistory:
                self.UpdateCashBalance(date, self.CurrentCashBalance() + pos.MarketValue) # add from cash balance  
            
            # update equitybalance history
            if date in self.EquityBalanceHistory:
                self.UpdateEquityBalance(date, self.CurrentEquityBalance() - pos.MarketValue) # subtract from cash balance
                    
            # update portfoliobalancehistory
            self.UpdatePortfolioBalance(date, self.CurrentCashBalance() + self.CurrentEquityBalance())
            
            write_line(f'{pos.Quantity:,} {pos.Symbol} sold on {pos.DateClosed.strftime("%Y-%m-%d")} @ ${pos.Price:,.2f} for ${pos.MarketValue:,.2f}')
            
        return pos
    
    def UpdateCashBalance(self, date, amount):
        self.CashBalanceHistory[pd.to_datetime(date)] = round(amount, 2)
        
    def UpdateEquityBalance(self, date, amount):
        self.EquityBalanceHistory[pd.to_datetime(date)] = round(amount, 2)
        
    def UpdatePortfolioBalance(self, date, amount):
        self.PortfolioBalanceHistory[pd.to_datetime(date)] = round(amount, 2)
    
    def UpdateBenchmarkBalance(self, date, amount):
        self.BenchmarkBalanceHistory[pd.to_datetime(date)] = round(amount, 2)
    
    def CurrentCashBalance(self):
        # check cash balance
        latest_date = max(self.CashBalanceHistory.keys())
        latest_cash_balance = self.CashBalanceHistory[latest_date]
        return latest_cash_balance
    
    def CurrentEquityBalance(self):
        # check cash balance
        latest_date = max(self.EquityBalanceHistory.keys())
        latest_equity_balance = self.EquityBalanceHistory[latest_date]
        return latest_equity_balance
    
    def CurrentBenchmarkBalance(self):
        # check cash balance
        latest_date = max(self.BenchmarkBalanceHistory.keys())
        latest_equity_balance = self.BenchmarkBalanceHistory[latest_date]
        return latest_equity_balance
    
    def CurrentPortfolioBalance(self):
        return self.CurrentCashBalance() + self.CurrentEquityBalance()
    
    def OpenPosition(self, symbol, price, quantity, date):
        # create position object
        pos = Position(symbol, price, quantity, date)

        # if enough cash, minus cash and add to OpenPositions and PositionsHistory
        if pos.CostBasis <= self.CurrentCashBalance():  
                      
            # update positions history
            self.PositionsHistory[pos.Symbol + '-' + pos.DateOpened.strftime("%Y-%m-%d")] = pos # add to positions history
     
            # update open positions
            self.OpenPositions[symbol] = pos # add to open positions
            
            # update cashbalance history
            if date not in self.CashBalanceHistory:
                self.UpdateCashBalance(date, self.CurrentCashBalance())
            self.UpdateCashBalance(date, self.CashBalanceHistory[date] - pos.CostBasis) # subtract from cash balance
            
            # update equitybalance history
            if date not in self.EquityBalanceHistory:
                self.UpdateEquityBalance(date, self.CurrentEquityBalance())
            self.UpdateEquityBalance(date, self.EquityBalanceHistory[date] + pos.CostBasis) # add to equity balance
            
            # update portfoliobalancehistory
            self.UpdatePortfolioBalance(date, self.CashBalanceHistory[date] + self.EquityBalanceHistory[date])
            
        # else send some kind of error response
        else:
            return None
        
        return pos
    
    def AbsoluteReturn():        
        return 0.0
    
    def TotalReturn():
        return 0.0
    
    def AnnualizedReturn():
        return 0.0
    
    def UnrealizedProfitLoss():
        return 0.0
    
    def MaxDrawdown():
        return 0.0
    
    def Variance():
        return 0.0
    
    def WinLossRatio():
        return 0.0
    
    def AverageLossPercent():
        return 0.0
    
    def TotalTrades():
        return 0
    
    def AverageWinPercent():
        return 0.0

class Position:
    def __init__(self, symbol, price, quantity, date):
        self.Symbol = symbol
        self.Quantity = quantity
        self.Price = price
        self.AvgPrice = price
        self.MarketValue = float(round(self.Quantity * self.Price,2))
        self.CostBasis = float(round(self.Quantity * self.AvgPrice,2))
        self.ProfitLoss = float(round(self.MarketValue - self.CostBasis,2))
        self.DateOpened = date
        self.DateClosed = None
        self.LastUpdated = date
    
    def BuyShares(self, buy_quantity, date):
        # build trade object
        t = Trade(date, 'Buy', self.Symbol, self.Price, buy_quantity)
        
        # update quantity and marketvalue
        old_quantity = self.Quantity
        old_avg_price = self.AvgPrice
        self.Quantity += t.Quantity
        self.MarketValue = self.Quantity * self.Price
        
        # calculate and update new cost basis
        self.CostBasis = round(((old_quantity * old_avg_price) + (buy_quantity * self.Price)), 2)
        
        # calculate and update new avg price
        self.AvgPrice = round(self.CostBasis / self.Quantity, 2)
        
        # update lastupdated
        self.LastUpdated = date
        
        return t
    
    def SellShares(self, buy_quantity, date):
        # build trade object
        t = Trade(date, 'Sell', self.Symbol, self.Price, buy_quantity)
        
        # update quantity and marketvalue
        old_quantity = self.Quantity
        old_avg_price = self.AvgPrice
        self.Quantity -= t.Quantity
        self.MarketValue = self.Quantity * self.Price
        
        # calculate and update new cost basis
        self.CostBasis = round(((old_quantity * old_avg_price) - (t.Quantity * self.Price)), 2)
        
        # calculate and update new avg price
        self.AvgPrice = round(self.CostBasis / self.Quantity, 2)
        
        # update lastupdated
        self.LastUpdated = date
        
        return t
    
    def __repr__(self):
        return f'Symbol: {self.Symbol}, Quantity: {self.Quantity}, Price: {self.Price}, MarketValue: {self.MarketValue}, CostBasis: {self.CostBasis}, ProfitLoss: {self.ProfitLoss}'

class Strategy:
    def __init__(self):
        self.LookbackBars = 30
        self.RiskFreeTicker = 'SPY'
        self.TopNPerGroup = 5
        self.ReturnThreshold = .02
        self.TopNSectors = 20
        self.TopNFinal = 5
        self.YearRangeThreshold = .5
        self.VolumeThreshold = 100000
        self.PriceThreshold = .5
        self.ReallocateThreshold = 5
        self.StopLossThreshold = .03
        self.TakeProfitThreshold = .12
        self.PositionsToMaintain = 4
    
    def __repr__(self):
        return f'LookbackBars: {self.LookbackBars}, RiskFreeTicker: {self.RiskFreeTicker}, TopNPerGroup: {self.TopNPerGroup}, TopNSectors: {self.TopNSectors}, YearRangeThreshold: {self.YearRangeThreshold}, VolumeThreshold: {self.VolumeThreshold}, PriceThreshold: {self.PriceThreshold}'

class BacktestResult:
    def __init__(self):
        self.StartDate = datetime.date.today() - timedelta(days=30)
        self.EndDate = datetime.date.today()
        self.MetricsSnapshots = pd.DataFrame()
        self.Portfolio = Portfolio()
        self.Strategy = Strategy()
        self.SortinoRatio = 0.0
        self.AnnualizedRatio = 0.0
        self.MaxDrawdown = 0.0
        self.Variance = 0.0
        self.TradeHistory = []
        self.ID = uuid.uuid4()
        
    # def __repr__(self):
    #     return f'LookbackBars: {self.LookbackBars}, RiskFreeTicker: {self.RiskFreeTicker}, TopNPerGroup: {self.TopNPerGroup}, TopNSectors: {self.TopNSectors}, YearRangeThreshold: {self.YearRangeThreshold}, VolumeThreshold: {self.VolumeThreshold}, PriceThreshold: {self.PriceThreshold}'

class MetricsSnapshot:
    def __init__(self):
        self.Date = datetime.date.today()
        self.OverallReturn = 0.0
        self.AnnualizedReturn = 0.0
        self.SortinoRatio = 0.0
        self.SharpeRatio = 0.0
        self.InformationRatio = 0.0
        self.UnrealizedProfitLoss = 0.0
        self.MaxDrawdown = 0.0
        self.Variance = 0.0
        self.WinLossRatio = 0.0
        self.AverageLossPercent = 0.0
        self.AverageWinPercent = 0.0
        self.CashBalance = 0.0
        self.AccountBalance = 0.0
        self.PortfolioValue = 0.0
        
    def __repr__(self):
        return f'Date: {self.Date}, AnnualizedReturn: {self.AnnualizedReturn}, SortinoRatio: {self.SortinoRatio}, AccountBalance: {self.AccountBalances}'

class Trade:
    def __init__(self, date, action, symbol, price, quantity):
        self.Date = date
        self.Action = action
        self.Symbol = symbol
        self.Price = price
        self.Quantity = quantity
        self.ProfitLoss = 0.0
        
    def __repr__(self):
        return f'Date: {self.Date}, Action: {self.Action}, Symbol: {self.Symbol}, Price: {self.Price}, Quantity: {self.Quantity}, ProfitLoss: {self.ProfitLoss}'

def load_ticker_list(file_path: Path) -> list:
    """
    Robustly loads a list of tickers from a CSV file.
    Handles empty files, missing files, and optional 'Ticker' header.
    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    if not file_path.exists():
        return []
    
    try:
        # Check for empty file
        if file_path.stat().st_size == 0:
            return []

        # Peek to see if there's a header
        df_peek = pd.read_csv(file_path, nrows=1, header=None)
        if df_peek.empty:
            return []
            
        first_val = str(df_peek.iloc[0, 0])
        
        # If header looks like "Ticker" or "Symbol", treat as having header
        if first_val.strip().lower() in ['ticker', 'symbol']:
            df = pd.read_csv(file_path)
            col_name = 'Ticker' if 'Ticker' in df.columns else 'Symbol'
            if col_name in df.columns:
                return df[col_name].dropna().unique().tolist()
        
        # Otherwise assume headerless single column
        df = pd.read_csv(file_path, header=None)
        return df.iloc[:, 0].dropna().unique().tolist()

    except Exception as e:
        write_line(f"Warning: Failed to load ticker list from {file_path}: {e}")
        return []

def add_line_to_file(file_path, text_line):
    """
    Adds a line of text to a file. Creates the file and necessary directories if they don't exist.
    
    Args:
        file_path (str): Path to the file.
        text_line (str): The line of text to add.
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Append the text line to the file
    with open(file_path, 'a') as file:
        file.write(text_line + '\n')


def calculate_sma(dataframe, period=30, column='Close'):
    """Calculate Simple Moving Average."""
    return dataframe[column].rolling(window=period).mean()

def calculate_macd(dataframe, short_period=12, long_period=26, signal_period=9):
    """Calculate Moving Average Convergence Divergence."""
    short_ema = dataframe['Close'].ewm(span=short_period, adjust=False).mean()
    long_ema = dataframe['Close'].ewm(span=long_period, adjust=False).mean()
    dataframe['MACD'] = short_ema - long_ema
    dataframe['Signal_Line'] = dataframe['MACD'].ewm(span=signal_period, adjust=False).mean()
    return dataframe['Signal_Line']

def calculate_bollinger_bands(dataframe, period=20, num_of_std=2):
    """Calculate Bollinger Bands."""
    rolling_mean = dataframe['Close'].rolling(window=period).mean()
    rolling_std = dataframe['Close'].rolling(window=period).std()
    dataframe['Bollinger_High'] = rolling_mean + (rolling_std * num_of_std)
    dataframe['Bollinger_Low'] = rolling_mean - (rolling_std * num_of_std)
    dataframe['Bollinger_Close'] = (dataframe['Close'] - dataframe['Bollinger_Low']) / (dataframe['Bollinger_High'] - dataframe['Bollinger_Low'])
    dataframe['Bollinger_Range'] = (dataframe['Bollinger_High'] - dataframe['Bollinger_Low']) / dataframe['Close']
    return dataframe

def calculate_obv(dataframe):
    """Calculate On-Balance Volume."""
    df = dataframe.copy()
    df['Daily_Return'] = df['Close'].diff()
    df['Volume_Direction'] = np.where(df['Daily_Return'] > 0, df['Volume'], -df['Volume'])
    df['OBV'] = df['Volume_Direction'].rolling(window=26, min_periods=1).sum()
    df['Avg_Volume'] = df['Volume'].rolling(window=26, min_periods=1).mean()
    df['OBV_Adjusted'] = df['OBV'] / df['Avg_Volume']
    return df['OBV_Adjusted']

def calculate_volume_oscillator(dataframe, short_period=12, long_period=26):
    """Calculate Volume Oscillator."""
    short_ma = dataframe['Volume'].rolling(window=short_period).mean()
    long_ma = dataframe['Volume'].rolling(window=long_period).mean()
    dataframe['Volume_Oscillator'] = short_ma - long_ma
    return dataframe['Volume_Oscillator']

def calculate_adl(dataframe):
    """Calculate Accumulation/Distribution Line with volume normalized by a 26-day rolling average."""
    df = dataframe.copy()
    # Calculate Money Flow Multiplier (MFM)
    range = df['High'] - df['Low']
    range.replace(0, 1, inplace=True)  # Replace zeros in the range to avoid division by zero
    mfm = ((df['Close'] - df['Low']) - (df['High'] - df['Close'])) / range

    # Normalize volume by 26-day rolling average volume
    avg_volume = df['Volume'].rolling(window=26, min_periods=1).mean()
    normalized_volume = df['Volume'] / avg_volume

    # Calculate Money Flow Volume (MFV)
    mfv = mfm * normalized_volume

    # Cumulative sum to get ADL
    df['ADL'] = mfv.cumsum()
    return df['ADL']

def calculate_rsi(dataframe, period=14):
    """Calculate Relative Strength Index."""
    delta = dataframe['Close'].pct_change()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    dataframe['RSI'] = 100 - (100 / (1 + rs))
    return dataframe['RSI']

def calculate_stochastic_oscillator(dataframe, period=20):
    """Calculate Stochastic Oscillator."""
    low_min = dataframe['Low'].rolling(window=period).min()
    high_max = dataframe['High'].rolling(window=period).max()
    dataframe['Stochastic_High'] = dataframe['High'].rolling(window=period).max()
    dataframe['Stochastic_Low'] = dataframe['Low'].rolling(window=period).min()
    dataframe['Stochastic_Oscillator'] = ((dataframe['Close'] - low_min) / (high_max - low_min)) #* 100
    dataframe['Stochastic_Oscillator_Range'] = ((high_max - low_min) / dataframe['Close']) #* 100
    return dataframe

def calculate_cci(dataframe, period=20):
    """Calculate Commodity Channel Index using logarithmic prices."""
    # Apply logarithmic transformation
    dataframe['Log_High'] = np.log(dataframe['High'])
    dataframe['Log_Low'] = np.log(dataframe['Low'])
    dataframe['Log_Close'] = np.log(dataframe['Close'])
    
    # Calculate Typical Price on log scale
    TP = (dataframe['Log_High'] + dataframe['Log_Low'] + dataframe['Log_Close']) / 3
    
    # Calculate CCI
    CCI = ((TP - TP.rolling(window=period).mean()) / (0.015 * TP.rolling(window=period).std())) / TP.rolling(window=period).mean()
    
    return CCI

def calculate_atr(dataframe, period=14):
    """Calculate Average True Range."""
    high_low = dataframe['High'] - dataframe['Low']
    high_close = np.abs(dataframe['High'] - dataframe['Close'].shift())
    low_close = np.abs(dataframe['Low'] - dataframe['Close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    atr = true_range.rolling(window=period).mean()
    return atr

def normalize_moving_average(dataframe, period=20, ma_type='SMA'):
    """Normalize Moving Average as a percentage of the close price."""
    if ma_type == 'SMA':
        ma = dataframe['Close'].rolling(window=period).mean()
    elif ma_type == 'EMA':
        ma = dataframe['Close'].ewm(span=period, adjust=False).mean()
    
    normalized_ma = (ma / dataframe['Close']) * 100 - 100
    column_name = f'Normalized_{ma_type}_{period}'
    dataframe[column_name] = normalized_ma
    return dataframe[column_name]

def standardize_macd(dataframe, short_period=12, long_period=26, signal_period=9):
    """Standardize MACD by calculating its Z-score."""
    macd = dataframe['MACD']
    macd_mean = macd.rolling(window=26).mean()  # Using the long period for rolling mean
    macd_std = macd.rolling(window=26).std()  # Using the long period for rolling std
    standardized_macd = (macd - macd_mean) / macd_std
    dataframe['Standardized_MACD'] = standardized_macd
    return dataframe['Standardized_MACD']

def normalize_bollinger_bands(dataframe, period=20, num_of_std=2):
    """Normalize Bollinger Band width as a percentage of the moving average."""
    ma = dataframe['Close'].rolling(window=period).mean()
    rolling_std = dataframe['Close'].rolling(window=period).std()
    bollinger_high = ma + (rolling_std * num_of_std)
    bollinger_low = ma - (rolling_std * num_of_std)
    bollinger_width = (bollinger_high - bollinger_low) / ma * 100  # Normalize as percentage of MA
    
    dataframe['Normalized_Bollinger_Width'] = bollinger_width
    return dataframe

def normalize_obv(dataframe, window=20):
    """Normalize On-Balance Volume by its moving average."""
    obv = calculate_obv(dataframe)  # Assuming this function is already defined
    obv_ma = obv.rolling(window=window).mean()
    normalized_obv = obv / obv_ma
    return normalized_obv

def standardize_volume_oscillator(dataframe, short_window=12, long_window=26):
    """Standardize Volume Oscillator by calculating its Z-score."""
    vo = calculate_volume_oscillator(dataframe, short_window, long_window)  # Ensure function is defined
    vo_mean = vo.rolling(window=long_window).mean()
    vo_std = vo.rolling(window=long_window).std()
    standardized_vo = (vo - vo_mean) / vo_std
    dataframe['Standardized_VO'] = standardized_vo
    return dataframe

def normalize_adl(dataframe):
    """Normalize Accumulation/Distribution Line (ADL) changes by total volume."""
    adl = calculate_adl(dataframe)  # Ensure calculate_adl function is defined
    adl_delta = adl.diff()  # Get daily changes (delta) in ADL
    normalized_adl = adl_delta / dataframe['Volume']
    dataframe['Normalized_ADL'] = normalized_adl
    return dataframe

def normalize_atr(dataframe, period=14):
    """Normalize Average True Range (ATR) as a percentage of the close price."""
    atr = calculate_atr(dataframe, period=period)  # Ensure calculate_atr function is defined
    normalized_atr = (atr / dataframe['Close']) * 100
    dataframe['Normalized_ATR'] = normalized_atr
    return dataframe

def standardize_bollinger_band_width(dataframe, period=20, num_of_std=2):
    """Standardize Bollinger Band width by calculating its Z-score."""
    rolling_mean = dataframe['Close'].rolling(window=period).mean()
    rolling_std = dataframe['Close'].rolling(window=period).std()
    bollinger_high = rolling_mean + (rolling_std * num_of_std)
    bollinger_low = rolling_mean - (rolling_std * num_of_std)
    bollinger_width = bollinger_high - bollinger_low
    
    # Standardize the Bollinger Band width
    width_mean = bollinger_width.rolling(window=period).mean()
    width_std = bollinger_width.rolling(window=period).std()
    standardized_width = (bollinger_width - width_mean) / width_std
    
    dataframe['Standardized_BB_Width'] = standardized_width
    return dataframe



def perform_ta_wrapper(args):
    return perform_technical_analysis(*args)

def get_historical_data(symbol, drop_prior=False, get_latest=False, page=None):
    try:
        csv_path = pl.get_yahoo_price_data(page, symbol)
        if csv_path:
            df = pd.read_csv(csv_path)
            return df, symbol
    except Exception as e:
        write_line(f"Error fetching data for {symbol}: {e}")
    return None, symbol

async def get_historical_data_async(symbol, drop_prior=False, get_latest=False, page=None):
    try:
        url = f"https://finance.yahoo.com/quote/{symbol}/history?p={symbol}"
        # Use simple period1/period2 params if needed, or default to max history which yahoo web usually provides
        # Actually pl.download_yahoo_price_data_async just downloads what's there.
        csv_path = await pl.download_yahoo_price_data_async(page, url)
        if csv_path:
             df = pd.read_csv(csv_path)
             return df, symbol
    except Exception as e:
         print(f"Error fetching data for {symbol}: {e}")
    return None, symbol

def get_historical_data_wrapper(args):
    return get_historical_data(*args)

import pandas as pd

def is_greater_than_past_n_days(df, n=5, percentage_increase=0.0):
    """
    Check if the latest close is greater than the average of the past n closes by a certain percentage.

    :param df: Dataframe containing stock data
    :param n: Number of past closes to consider
    :param percentage_increase: Percentage increase threshold (0.05 for 5%)
    :return: Boolean indicating if the condition is met
    """
    if len(df) < n + 1:  # +1 to include the latest day
        raise ValueError(f"Dataframe has fewer than {n + 1} rows.")

    # Calculate the average of the past n closes
    avg_past_n_closes = df.iloc[-(n + 1):-1]['Close'].mean()

    # Calculate the threshold
    threshold = avg_past_n_closes * (1 + percentage_increase)

    # Check if the latest close is greater than the threshold
    return df.iloc[-1]['Close'] > threshold


def is_higher_than_all_past_n_days(df, n=5, percentage_increase=0.0):
    """
    Check if the latest close is a certain percentage greater than each of the past n closes.

    :param df: Dataframe containing stock data
    :param n: Number of past closes to consider
    :param percentage_increase: Percentage increase threshold (0.05 for 5%)
    :return: Boolean indicating if the condition is met
    """
    if len(df) < n + 1:  # +1 to include the latest day
        raise ValueError(f"Dataframe has fewer than {n + 1} rows.")

    # Get the latest close
    latest_close = df.iloc[-1]['High']

    # Calculate the thresholds for each of the past n days
    thresholds = df.iloc[-(n + 1):-1]['Close'] * (1 + percentage_increase)

    # Check if the latest close is greater than all of the thresholds
    return (latest_close > thresholds).all()

def is_lower_than_all_past_n_days(df, n=5, percentage_increase=0.0):
    """
    Check if the latest close is a certain percentage greater than each of the past n closes.

    :param df: Dataframe containing stock data
    :param n: Number of past closes to consider
    :param percentage_increase: Percentage increase threshold (0.05 for 5%)
    :return: Boolean indicating if the condition is met
    """
    if len(df) < n + 1:  # +1 to include the latest day
        raise ValueError(f"Dataframe has fewer than {n + 1} rows.")

    # Get the latest close
    latest_close = df.iloc[-1]['Low']

    # Calculate the thresholds for each of the past n days
    thresholds = df.iloc[-(n + 1):-1]['Close'] * (1 - percentage_increase)

    # Check if the latest close is greater than all of the thresholds
    return (latest_close < thresholds).all()

def process_chunk(chunk):
    # Replace with your logic
    chunk['c'] = chunk['a'] + chunk['b']
    return chunk

def get_bail_trend(bar_count):
    # Load DataFrame
    df = pd.read_pickle(pl.COMMON_DIR / 'Stocks/^VIX.pickle')

    # Ensure the date column is in datetime format
    df['Date'] = pd.to_datetime(df['Date'])

    # Calculate daily returns
    df['daily_return'] = df['Close'] / df['Close'].shift(1) - 1
    df['daily_return'].fillna(0, inplace=True)

    # Set modifier to be applied to std val
    mod = 3

    # Initialize empty columns for thresholds
    df['Pos Threshold'] = 0.0
    df['Neg Threshold'] = 0.0
    df['Both Threshold'] = 0.0

    # Loop over the DataFrame in 60-day windows
    for i in range(bar_count, len(df)):
        # Define the window (previous 60 days)
        window_df = df[i-bar_count:i-1] # minus one so we calculate thresholds excluding current date

        # Compute for positive and negative daily returns separately
        df_pos = window_df[window_df['daily_return'] > 0]
        df_neg = window_df[window_df['daily_return'] < 0]

        # Compute and set the thresholds
        if not df_pos.empty:
            pos_std = df_pos['daily_return'].std()
            pos_median = abs(df_pos['daily_return'].median())
            # df.at[i, 'Pos Threshold'] = window_df.iloc[-1]['Close'] + float((mod*pos_std + pos_median))
            df.at[i, 'Pos Threshold'] = float((mod*pos_std + pos_median))

        if not df_neg.empty:
            neg_std = df_neg['daily_return'].std()
            neg_median = abs(df_neg['daily_return'].median())
            # df.at[i, 'Neg Threshold'] = window_df.iloc[-1]['Close'] + float((mod*neg_std + neg_median))
            df.at[i, 'Neg Threshold'] = float((mod*neg_std + neg_median))

        # Compute and set the threshold for both
        both_std = window_df['daily_return'].std()
        both_median = abs(window_df['daily_return'].median())
        # df.at[i, 'Both Threshold'] = window_df.iloc[-1]['Close'] + float((mod*both_std + both_median))
        df.at[i, 'Both Threshold'] = float((mod*both_std + both_median))

    df['interpretation'] = 0

    df.rename(columns={'Date': 'date'}, inplace=True)
    return df[['date', 'interpretation']]

async def refresh_stock_data_async(df_symbols, lookback_bars, drop_prior, get_latest, browser, page, context):
    skip_reload = False    
    if not skip_reload:
        write_line('Retrieving historical data...')

        # 1) Clean symbols
        df_symbols = df_symbols.dropna(subset=['Symbol'])
        symbols = [
            row['Symbol'] 
            for _, row in df_symbols.iterrows() 
            if '.' not in row['Symbol']
        ]

        # 2) Try cache
        historical_path     = pl.COMMON_DIR / 'get_historical_data_output.csv'
        freshness_threshold = 4 * 60 * 60
        df_concat = pd.DataFrame()

        if historical_path.exists() and (time.time() - historical_path.stat().st_mtime) < freshness_threshold:
            ts  = datetime.fromtimestamp(historical_path.stat().st_mtime)
            print(f"✅  Using cached historical data ({ts:%Y-%m-%d %H:%M})")
            # df_concat = pd.read_csv(historical_path)
        else:
            print("♻️  Cache missing or stale → downloading fresh historical data…")
            semaphore = asyncio.Semaphore(5)
            async def fetch(symbol):
                async with semaphore:
                    page = await context.new_page()
                    try:
                        # your existing async fetch; returns a pandas.DataFrame
                        return await get_historical_data_async(symbol, drop_prior, get_latest, page)
                    except Exception as e:
                        # Log and swallow the error for this symbol
                        print(f"[Error] symbol={symbol}: {e}")
                        return None
                    finally:
                        await page.close()
            
            # kick off all fetches
            tasks  = [fetch(sym) for sym in symbols if "." not in sym]
            frames = await asyncio.gather(*tasks, return_exceptions=False)

            # filter out failures (None)
            valid_frames = [df[0] for df in frames if df is not None]

            # concatenate & write cache
            df_concat = pd.concat(valid_frames, ignore_index=True)
            historical_path.parent.mkdir(parents=True, exist_ok=True)
            df_concat.to_csv(historical_path, index=False)
            print(f"💾  Wrote fresh data to {historical_path}")
        return
   

    
def monitor_stock_data(df_symbols, lookback_bars, drop_prior, get_latest):
    data_tuples = [(row['Symbol'], drop_prior, get_latest, True) for _, row in df_symbols.iterrows()]
    n_cores = cpu_count()    
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        # Results will contain the result of process_row applied to each row
        results = list(executor.map(get_historical_data_wrapper,  data_tuples))
    results = [tup[0] for tup in results]
    concatenated_df = pd.concat(results)
    return concatenated_df
        

def apply_strategy_filter(df_combined: pd.DataFrame, strat: Strategy):
    # Calculate the average volume for each symbol
    average_volume = df_combined.groupby('Symbol')['Volume'].median()

    # Create a boolean mask to identify symbols with average volume < 100000
    mask = average_volume < strat.VolumeThreshold

    # Filter the DataFrame based on the mask
    df_combined = df_combined[~df_combined['Symbol'].isin(average_volume[mask].index)]

    # Calculate the average price for each symbol
    average_price = df_combined.groupby('Symbol')['Close'].mean()

    # Create a boolean mask to identify symbols with average price
    mask = average_price < strat.PriceThreshold

    # Filter the DataFrame based on the mask
    df_combined = df_combined[~df_combined['Symbol'].isin(average_price[mask].index)]

    # Sort the DataFrame by 'Symbol' and 'Date'
    df_combined = df_combined.sort_values(['Symbol', 'Date'])

    # Group by 'Symbol'
    grouped = df_combined.groupby('Symbol')
    
    dates = sorted(df_combined['Date'].unique())

    # Calculate the highest and lowest close within a rolling window of 252 trading days
    df_combined['HighClosePastYear'] = grouped['Close'].rolling(window=252, min_periods=1).max().shift(1).reset_index(level=0, drop=True)
    df_combined['LowClosePastYear'] = grouped['Close'].rolling(window=252, min_periods=1).min().shift(1).reset_index(level=0, drop=True)

    # latest_indexes = df_combined.groupby('Symbol')['Date'].idxmax()
    df_combined['LookbackReturn'] = (df_combined['Close'] / df_combined['Close'].shift(strat.LookbackBars)) - 1
    
    # Filter the DataFrame based on the latest indexes
    df_combined_last = df_combined.loc[df_combined.groupby('Symbol')['Date'].transform('max') == df_combined['Date']]
    lower_threshold = df_combined_last['LowClosePastYear'] + strat.YearRangeThreshold * (df_combined_last['HighClosePastYear'] - df_combined_last['LowClosePastYear'])

    # Filter symbols with average LookbackReturn > 0.01
    # Group by 'Symbol', get the latest N rows, and filter based on median
    result = df_combined_last.groupby('Symbol').apply(lambda group: group.tail(strat.LookbackBars)).groupby('Symbol').filter(lambda x: x['LookbackReturn'].median() > strat.ReturnThreshold)

    # Get unique symbols
    filtered_symbols = result['Symbol'].unique()
    df_combined = df_combined[df_combined['Symbol'].isin(filtered_symbols)]
    
    # df_combined = df_combined.groupby('Symbol').filter(lambda x: x['LookbackReturn'].median() > 1+strat.ReturnThreshold)


    # Filter the DataFrame based on the condition
    df_combined = df_combined[df_combined['Symbol'].isin(df_combined_last[df_combined_last['Close'] <= lower_threshold]['Symbol'])]

    # Replace inf and -inf with NaN
    df_combined = df_combined.replace([np.inf, -np.inf], np.nan)

    # Drop rows with NaN values in the 'Sortino' column
    if 'Sortino' in df_combined.columns.values:
        df_combined = df_combined.dropna(subset=['Sortino'])

    duplicates = df_combined.duplicated(subset=['Date', 'Symbol'], keep=False)
    
    # # store refreshed df_combined
    # store_csv(df_combined, 'Data/df_combined')
    
    # # write df_combined to csv for analysis elsewhere
    df_combined.to_csv(pl.COMMON_DIR / 'df_combined.csv')

    return df_combined

def calculate_daily_returns(close_prices):
    """
    Calculate daily returns from close prices.
    """
    daily_returns = close_prices.pct_change()
    return daily_returns

def calculate_rolling_beta(stock_returns, benchmark_returns, window):
    """Calculate the rolling beta of the stock relative to the benchmark."""
    covariance = stock_returns.rolling(window=window).cov(benchmark_returns)
    variance = benchmark_returns.rolling(window=window).var()
    beta = covariance / variance
    return beta

def calculate_rolling_jensens_alpha(df, df_benchmark, risk_free_rate, window):
    """
    Calculate the rolling Jensen's Alpha from a DataFrame containing Date, StockClose, and BenchmarkClose columns.
    
    :param df: DataFrame with 'Date', 'StockClose', and 'BenchmarkClose'.
    :param risk_free_rate: Annual risk-free rate, expressed as a decimal.
    :param window: Rolling window size for calculation.
    :return: Pandas Series of the rolling Jensen's Alpha.
    """
    # Convert risk-free rate to a daily rate assuming 252 trading days
    daily_risk_free_rate = (1 + risk_free_rate) ** (1/252) - 1
    
    # Calculate daily returns for stock and benchmark
    df['DailyReturn'] = calculate_daily_returns(df['Close'])
    df_benchmark['DailyReturn'] = calculate_daily_returns(df_benchmark['Close'])
    
    # Calculate rolling beta
    rolling_beta = calculate_rolling_beta(df['DailyReturn'], df_benchmark['DailyReturn'], 365*2)
    
    # Calculate expected market return over the window
    expected_market_return = df_benchmark['DailyReturn'].rolling(window=window).mean()
    
    # Calculate expected return using CAPM
    expected_return = daily_risk_free_rate + rolling_beta * (expected_market_return - daily_risk_free_rate)
    
    # Calculate actual average return over the window
    actual_return = df['DailyReturn'].rolling(window=window).mean()
    
    # Calculate Jensen's Alpha
    jensens_alpha = actual_return - expected_return
    
    return pd.DataFrame(jensens_alpha).reset_index().drop_duplicates(subset=['index'])['DailyReturn']


def calculate_null_percentage(df):
    total_len = len(df)
    # For each column, build a single boolean condition.
    # Then take the sum and divide by the total length.
    mask = (
        df.isna() 
        | (df == np.inf) 
        | (df == -np.inf) 
        # | (df == 0)
    )
    
    # Summation by column of True/False
    # True = 1, so sum of True = total count of matching rows
    null_counts = mask.sum()
    null_percentage = null_counts / total_len * 100
    return null_percentage


def plot_portfolio_balance(backtest_result: BacktestResult, df_combined: pd.DataFrame, backtest_date: datetime):
        # Convert the string dates to datetime objects
        dates = backtest_result.Portfolio.PortfolioBalanceHistory.keys()
        amounts = list(backtest_result.Portfolio.PortfolioBalanceHistory.values())
        df_rf = df_combined[(df_combined['Symbol'] == backtest_result.Strategy.RiskFreeTicker) & (df_combined['Date'] >= backtest_result.StartDate) & (df_combined['Date'] <= backtest_date)]
        # First, we sort the dataframe by date
        df_rf.sort_values('Date', inplace=True)
        
        # Then, we calculate the initial number of shares bought
        initial_investment = 100000
        num_shares = initial_investment / df_rf.iloc[0]['Close']

        # Now, we calculate the value of these shares over time
        df_rf['Benchmark Balance'] = num_shares * df_rf['Close']
        df_rf = df_rf[['Date', 'Benchmark Balance']]
        
        df_balances = pd.DataFrame({'Date': dates, 'Portfolio Balance': amounts})#(dates, amounts), columns=['Date', 'PortfolioBalance'])
        df_balances['Date'] = pd.to_datetime(df_balances['Date'])
        new_index = pd.date_range(start=df_balances['Date'].min(), end=df_balances['Date'].max())
        df_balances.set_index('Date', inplace=True)
        #df_balances = df_balances.reindex(new_index)
       # df_balances = df_balances.fillna(method='ffill')
        #df_balances.reset_index(inplace=True)
        # now merge the two dataframes
      #  df_balances = pd.merge(df_balances, df_rf, on='Date', how='outer')
        
        # Set 'Date' as the index (necessary for ffill to work correctly)
        #df_balances.set_index('Date', inplace=True)

        # Use ffill to forward-fill the NA values
        df_balances.ffill(inplace=True)
        df_balances.bfill(inplace=True)
        df_balances.reset_index(inplace=True)
        plt.close('all')
        fig, ax = plt.subplots()
        ax.plot(df_balances['Date'], df_balances['Portfolio Balance'], marker='o', label='Porfolio')
        #ax.plot(df_balances['Date'], df_balances['Benchmark Balance'], marker='x', label='Benchmark')
        ax.legend()
        ax.grid()

        ax.set_title(f'Portfolio Performance vs {backtest_result.Strategy.RiskFreeTicker} - {backtest_result.StartDate.strftime("%Y-%m-%d")} to {backtest_result.EndDate.strftime("%Y-%m-%d")}', fontsize=14)
        formatter = ticker.FuncFormatter(lambda x, pos: '${:,.2f}'.format(x))
        ax.yaxis.set_major_formatter(formatter)

        # Set y-axis limit
        ax.set_ylim([0, 1.1*round(np.max([df_balances['Portfolio Balance'].max()]))])#, df_balances['Benchmark Balance'].max()])*1.1, 2)])
        ax.set_xticklabels(df_balances['Date'], rotation=45)
        ax.set_xticks(df_balances['Date'])
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))  # Set major ticks to appear every 3 months
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))  # Format dates as 'mm/dd'\
       # plt.tight_layout()  # Adjust layout for better appearance
       # plt.get_current_fig_manager().window.state('iconic')
        plt.show(block=False)
        plt.pause(3)
    


def run_test(df_symbols: pd.DataFrame, df_combined: pd.DataFrame, backtest_result: BacktestResult) -> BacktestResult:
    write_line(f'Running backtest from {backtest_result.StartDate.strftime("%m-%d-%Y")} to {backtest_result.EndDate.strftime("%m-%d-%Y")}')
    
    # init balances
    backtest_result.Portfolio.UpdateCashBalance(backtest_result.StartDate-timedelta(days=1), 100000)
    backtest_result.Portfolio.UpdateEquityBalance(backtest_result.StartDate-timedelta(days=1), 0)
    backtest_result.Portfolio.UpdatePortfolioBalance(backtest_result.StartDate-timedelta(days=1), 100000)


    # iterate through list of dates
    backtest_dates = pd.date_range(start=backtest_result.StartDate, end=backtest_result.EndDate).tolist()
    backtest_dates.sort()
    backtest_dates = [date for date in backtest_dates if date in df_combined['Date'].values]
    for dates_index, backtest_date in enumerate(backtest_dates):
        write_line(f'Backtest date -> {backtest_date.strftime("%m-%d-%Y")} | PortfolioValue: ${backtest_result.Portfolio.CurrentPortfolioBalance():,.2f}')        
        
        # init starting balances to previous day balances
        backtest_result.Portfolio.UpdateCashBalance(backtest_result.StartDate, backtest_result.Portfolio.CashBalanceHistory[backtest_result.StartDate-timedelta(days=1)])
        backtest_result.Portfolio.UpdateEquityBalance(backtest_result.StartDate, backtest_result.Portfolio.EquityBalanceHistory[backtest_result.StartDate-timedelta(days=1)])
        backtest_result.Portfolio.UpdatePortfolioBalance(backtest_result.StartDate, backtest_result.Portfolio.PortfolioBalanceHistory[backtest_result.StartDate-timedelta(days=1)])
        
        # DEBUGGING #
        # first day of backtest
        # if dates_index == 0:
        #     # debugging, open three random positions to test takeprofit/stoploss/reallocate
        #     limit = backtest_result.Portfolio.CurrentCashBalance() / backtest_result.Strategy.PositionsToMaintain
        #     for i in range(backtest_result.Strategy.PositionsToMaintain):
        #         rand_position = generate_random_position(limit, backtest_date, list(set(df_combined['Symbol'])))
        #         backtest_result.Portfolio.OpenPosition(rand_position.Symbol, rand_position.AvgPrice, rand_position.Quantity, rand_position.DateOpened)
        
        # Define the start and end dates of the range to analyze based on current backtest_date
        end_date = pd.to_datetime(backtest_date - timedelta(days=1))

        start_date = end_date - timedelta(days=backtest_result.Strategy.LookbackBars)#pd.to_datetime('2023-01-31')

        # Filter the DataFrame based on the date range
        df_filtered_date = df_combined[(df_combined['Date'] >= start_date) & (df_combined['Date'] <= end_date)]
        
        # update portfolio prices with 
        price_col = 'Close'
        backtest_result.Portfolio.UpdatePortfolioPrices(df_filtered_date[df_filtered_date['Date'] ==  end_date][['Symbol', price_col]], backtest_date, price_col)
        plot_portfolio_balance(backtest_result, df_combined, backtest_date)
        if end_date not in df_combined['Date'].values:
            write_line(f'Skipping {end_date.strftime("%m-%d-%Y")}')
            continue
        
        

        
        
        

        # # Sample data
        # x = np.linspace(0, 2 * np.pi, 10)
        # y = np.sin(x)
        # data = {'X Values': x, 'Y Values': y}

        # # Create a figure and a set of subplots (this gives us ax to work with)
        # fig, ax = plt.subplots(1, 1)

        # # Plot the data
        # ax.plot(x, y)

        # # Add a table. loc='right' places the table to the right of the plot
        # cell_text = [[round(val, 2) for val in col_data] for col_data in data.values()]
        # ax.table(cellText=cell_text, loc='right')

        # # Adjust the layout to make room for the table
        # plt.subplots_adjust(left=0.2, top=0.8)

        # plt.show(block=False)
        # plt.pause(3)

        
        
        
        
        

        
        # iterate through current open positions in portfolio
        open_symbols = copy.deepcopy(list(set(backtest_result.Portfolio.OpenPositions.keys())))
        for symbol in open_symbols:
            position = backtest_result.Portfolio.OpenPositions[symbol]

            # grab previous day close price for symbol
            df_last_symbol = df_filtered_date[df_filtered_date['Symbol'] == symbol]
            last_price = df_last_symbol.loc[df_last_symbol['Date'].idxmax()]['Close']
            
            # check stop loss
            close_position = False
            close_trigger = ''
            if position.ProfitLoss <= -(position.CostBasis * backtest_result.Strategy.StopLossThreshold) and backtest_result.Strategy.StopLossThreshold < 0:
                close_position = True
                close_trigger = 'StopLossThreshold'
            
            # # check take profit
            if (position.ProfitLoss >= position.CostBasis * backtest_result.Strategy.TakeProfitThreshold) and backtest_result.Strategy.TakeProfitThreshold > 0:
                close_position = True
                close_trigger = 'TakeProfitThreshold'
                
            # check if reallocate threshold has passed
            if backtest_result.Strategy.ReallocateThreshold > 0:
                threshold_date = backtest_date - timedelta(days=backtest_result.Strategy.ReallocateThreshold)        
                if position.DateOpened < threshold_date:
                    close_position = True
                    close_trigger = 'ReallocateThreshold'
            
            # stop loss, take profit or reallocate threshold -> close position
            if close_position:
                write_line(f'{close_trigger} position triggered for {symbol} | ProfitLoss: ${position.ProfitLoss:,.2f}, DateOpened: {position.DateOpened.strftime("%m-%d-%Y")}, DateClosed: {backtest_date}')
                backtest_result.Portfolio.ClosePosition(position.Symbol, position.Price, backtest_date)
        
        # Determine if new positions are needed
        current_cash_position = backtest_result.Portfolio.CurrentCashBalance()
        new_position_count = backtest_result.Strategy.PositionsToMaintain - len(backtest_result.Portfolio.OpenPositions)
        if new_position_count == 0:
            continue
        
        # calculate primary ranks and apply primary filter
        df_filtered_date = apply_strategy_filter(df_filtered_date, backtest_result.Strategy)

        # Calculate the cutoff date and apply to df_combined
        cutoff_date = backtest_date - pd.DateOffset(days=backtest_result.Strategy.LookbackBars)
        df_filtered_date = df_filtered_date[df_filtered_date['Date'] > cutoff_date]
        df_filtered_date = df_filtered_date.reset_index(drop=True)
        
        # TODO: calculate calculate secondary ranks (on previously filtered data) and apply secondary filter
        
        # calculate ideal allocation
        df_eval = evaluate_current(df_symbols, df_filtered_date, backtest_result.Strategy, print=False).head(backtest_result.Strategy.TopNFinal)
        df_eval = df_eval[df_eval['Blended'] != 0].sort_values('Blended', ascending=False)
        
        # Filter df down to final results based on number of positions needed
        df_eval = df_eval.head(new_position_count)
        target_value_per_position = current_cash_position / new_position_count

        # Filter the DataFrame
        df_eval_current = df_filtered_date[(df_filtered_date['Symbol'].isin(df_eval['Symbol'].unique().tolist())) & (df_filtered_date['Date'] == df_filtered_date['Date'].max())]
        
        for index, stock in df_eval_current.iterrows():
            # grab latest price
            price = float(df_eval_current[df_eval_current['Symbol'] == stock['Symbol']]['Close'])
            
            # set target quantity based on price
            target_quantity = int(target_value_per_position / price)
            
            # check if there is an open position for the stock
            open_pos = None
            if stock['Symbol'] in backtest_result.Portfolio.OpenPositions.keys():
                open_pos = backtest_result.Portfolio.OpenPositions[stock['Symbol']]
            
            # if there is an open position then check OpenQuantity
            if open_pos is not None:
                
                # Determine buy or sell and quantity, add trade to history
                t = None
                if open_pos.Quantity > target_quantity:
                    t = open_pos.SellShares(open_pos.Quantity - target_quantity, backtest_date)
                elif open_pos.Quantity < target_quantity:
                    t = open_pos.BuyShares(target_quantity - open_pos.Quantity, backtest_date)
                if t is not None:
                    write_line(f'{t.Action} {t.Quantity} shares of {t.Symbol} @ ${t.Price:,.2f} for ${t.Price*t.Quantity:,.2f}')
                    backtest_result.TradeHistory.concat(t)
                
            else:
                write_line(f'Open position triggered for {stock["Symbol"]} | Quantity: {target_quantity}, Price: ${price:,.2f}, DateOpened: {backtest_date.strftime("%m-%d-%Y")}')
                backtest_result.Portfolio.OpenPosition(stock['Symbol'], price, target_quantity, backtest_date)
                
        # calculate snapshot metrics
        
        # build snapshot metric object and add row to backtest_result.MetricsSnapshot
            # # Create an instance of the MetricsSnapshot class
            # snapshot = MetricsSnapshot()

            # # Convert the instance to a DataFrame
            # data = pd.DataFrame([snapshot.__dict__])


def evaluate_current(df_symbols: pd.DataFrame, df_combined: pd.DataFrame, strat: Strategy, print:bool) -> pd.DataFrame:
    
    df_combined = apply_strategy_filter(df_combined, strat)
    
    # Calculate the cutoff date and apply to df_combined
    cutoff_date = df_combined['Date'].max() - pd.DateOffset(days=strat.LookbackBars)
    df_combined = df_combined[df_combined['Date'] > cutoff_date]
    df_combined = df_combined.reset_index(drop=True)
    current_date = df_combined['Date'].max()
    
    # get returns per sector
    df_sector_returns = df_combined.groupby('Sector')['Sortino'].median().reset_index()

    # loop through groups
    df_top_symbols = pd.DataFrame()
    grouper = list(set(df_sector_returns['Sector']))
    for index, group in enumerate(grouper):
        # write_line(f'Processing {group} ({index}/{len(grouper)})')
        
        # grab symbols for group
        group_symbols = df_symbols[df_symbols['Sector'] == group]['Symbol'].to_list()
        
        # grab data for group from df_combined and set the index
        df_group_filtered = df_combined[df_combined['Symbol'].isin(group_symbols)]
        df_group_filtered = df_group_filtered.set_index(['Symbol', 'Date'])
        
        # rank symbols according to Sortino
        df_group_filtered = rank_indicators(df_group_filtered, ['Sortino'], clear=True)
            
        # aggregate momentum indicators into one
        # if 'Rank_Sortino' in df_group_filtered.columns:
        #     df_group_filtered.drop('Rank_Sortino', axis=1, inplace=True)
        # df_group_filtered = aggregate_indicators(df_group_filtered, ['Sortino'], 'Rank_Sortino' )    
        
        # grab top n symbols by Sortino
        top_n_symbols = get_topn_symbols(df_group_filtered, strat.TopNPerGroup, 'Sortino')
        # top_n_symbols = list(set(df_group_filtered['Symbol']))
        # filter group dataframe down to only top n symbols
        #filtered_df = df_group_filtered[df_group_filtered.index.get_level_values('Symbol').isin(top_n_symbols)]

        # print top Sortino for section
        #write_section(f'Sortino Ranking for {group} as of {datetime.today().strftime("%m/%d/%y")}', top_n_symbols)
      #  write_line(f'Sortino ranked for {group}')

        # aggregate rankings
        weights = {'Sortino': 1.0 }
        #df_group_filtered = aggregate_indicators_weighted(df_group_filtered, ['Rank_Volume', 'Rank_Volatility', 'Rank_Trend', 'Rank_Momentum', 'Rank_Sortino'], weights, 'Rank_Overall' )    
        df_group_filtered = aggregate_indicators_weighted(df_group_filtered, ['Sortino'], weights, 'Rank_Overall' )    

        # calculate median and stddev for Sortino rankings
        df_group_filtered = df_group_filtered.reset_index()        
        grouped_df = df_group_filtered.groupby('Symbol')['Rank_Overall'].agg(['median', 'std'])
        grouped_df = grouped_df.rename(columns={'median': "Rank_Median", "std": "Rank_Std"})
        grouped_df['Rank_Std'] = grouped_df['Rank_Std'].replace(0, 1)
        
        # set adjusted rank based on a penalization factor for stddev
        penalization_factor = 2  # Adjust this factor to control the degree of penalization
        grouped_df['Rank_Adjusted'] = grouped_df['Rank_Median'] / (penalization_factor * grouped_df['Rank_Std'])

        # grab top n symbols based on adjusted rank and it to dataframe of all top symbols
        top_symbols = grouped_df.nlargest(strat.TopNPerGroup, 'Rank_Adjusted').index.to_list()
        df_top_symbols = pd.concat([df_top_symbols, grouped_df[grouped_df.index.isin(top_symbols)]])
    #####################################################################################################    
        

    #####################################################################################################
    # grab and perform technical analysis on top symbols
    #####################################################################################################
    # grab all top symbols
    top_symbols = df_top_symbols.index.get_level_values(0).unique().sort_values().to_list()

    # reload and filter df_combined for top symbols
    #df_combined = load_csv('Data/df_combined').reset_index(drop=True)
    df_combined = df_combined[df_combined['Symbol'].isin(top_symbols)]
    # Find the latest date in the DataFrame
    latest_date = df_combined['Date'].max()

    # Define a cutoff date 90 days prior to the latest date
    cutoff_date = latest_date - pd.Timedelta(days=90)

    # Filter the DataFrame to include only rows with dates greater than or equal to the cutoff date
    df_combined_tmp = df_combined# df_combined[df_combined['Date'] >= cutoff_date]
    
    # perform technical analysis on the top symbols
    df_top_combined = pd.DataFrame()
    for index, symbol in enumerate(top_symbols):
        write_inline(f'Performing technical analysis {index} / {len(top_symbols)} - {symbol}')
        df_symbol = df_combined_tmp[df_combined_tmp['Symbol'] == symbol]
        df_symbol = add_ta(df_symbol)
        if df_symbol is not None:
            df_top_combined = pd.concat([df_top_combined, df_symbol])
    cutoff_date = df_top_combined['Date'].max() - pd.DateOffset(days=strat.LookbackBars)
    df_top_combined = df_top_combined[df_top_combined['Date'] > cutoff_date]
    df_top_combined.set_index(['Symbol', 'Date'], inplace=True)
    #del df_combined
    #####################################################################################################


    #####################################################################################################
    # grab all the indicator columns added from ta and rank them
    #####################################################################################################
    indicator_columns = [col for col in df_top_combined.columns if col.islower()]
    if len(indicator_columns) > 0:
        # rank volume indicators
        volume_indicators = [string for string in indicator_columns if "volume_" in string and string != 'volume_vwap']
        if True:
            df_top_combined = rank_indicators(df_top_combined, volume_indicators, clear=True)

        # aggregate volume indicators into one
        if 'Rank_Volume' in df_top_combined.columns:
            df_top_combined.drop('Rank_Volume', axis=1, inplace=True)
        df_top_combined = aggregate_indicators(df_top_combined, volume_indicators, 'Rank_Volume' )
        
        # grab top
        top_n_symbols = get_topn_symbols(df_top_combined, strat.TopNPerGroup, 'Rank_Volume', strat.LookbackBars)

        # Filter df_top_combined to include only the symbols from the top 3 at the last date
        filtered_df = df_top_combined[df_top_combined.index.get_level_values('Symbol').isin(top_n_symbols)]

        # write volume reocmmendation
        #write_section(f'Volume Ranking Top {strat.TopNPerGroup} as of {current_date.strftime("%m/%d/%y")}', top_n_symbols)


        # rank volatility indicators
        volatility_indicators = [string for string in indicator_columns if "volatility_" in string]
        if True:
            df_top_combined = rank_indicators(df_top_combined, volatility_indicators, clear=True)
            
        # aggregate volatility indicators into one      
        if 'Rank_Volatility' in df_top_combined.columns:
            df_top_combined.drop('Rank_Volatility', axis=1, inplace=True)
        df_top_combined = aggregate_indicators(df_top_combined, volatility_indicators, 'Rank_Volatility' )   
        
        # grab top
        top_n_symbols = get_topn_symbols(df_top_combined, strat.TopNPerGroup, 'Rank_Volatility', strat.LookbackBars)

        # Filter df_top_combined to include only the symbols from the top 3 at the last date
        filtered_df = df_top_combined[df_top_combined.index.get_level_values('Symbol').isin(top_n_symbols)]

        # write volume reocmmendation
        #write_section(f'Volatility Ranking Top {strat.TopNPerGroup} as of {current_date.strftime("%m/%d/%y")}', top_n_symbols)

        # rank trend indicators
        trend_indicators = [string for string in indicator_columns if "trend_" in string]
        if True:
            df_top_combined = rank_indicators(df_top_combined, trend_indicators, clear=True)
        
        # aggregate trend indicators into one
        if 'Rank_Trend' in df_top_combined.columns:
            df_top_combined.drop('Rank_Trend', axis=1, inplace=True)
        df_top_combined = aggregate_indicators(df_top_combined, trend_indicators, 'Rank_Trend' )    
        
        # grab top
        top_n_symbols = get_topn_symbols(df_top_combined, strat.TopNPerGroup, 'Rank_Trend', strat.LookbackBars)

        # Filter df_top_combined to include only the symbols from the top 3 at the last date
        filtered_df = df_top_combined[df_top_combined.index.get_level_values('Symbol').isin(top_n_symbols)]

        # write trend reocmmendation
        #write_section(f'Trend Ranking Top {strat.TopNPerGroup} as of {current_date.strftime("%m/%d/%y")}', top_n_symbols)

        # rank momentum indicators
        momentum_indicators = [string for string in indicator_columns if "momentum_" in string]
        if True:
            df_top_combined = rank_indicators(df_top_combined, momentum_indicators, clear=True)
            
        # aggregate momentum indicators into one
        if 'Rank_Momentum' in df_top_combined.columns:
            df_top_combined.drop('Rank_Momentum', axis=1, inplace=True)
        df_top_combined = aggregate_indicators(df_top_combined, momentum_indicators, 'Rank_Momentum' )    
        
        # grab top
        top_n_symbols = get_topn_symbols(df_top_combined, strat.TopNPerGroup, 'Rank_Momentum', strat.LookbackBars)

        # Filter df_top_combined to include only the symbols from the top 3 at the last date
        filtered_df = df_top_combined[df_top_combined.index.get_level_values('Symbol').isin(top_n_symbols)]

        # write trend reocmmendation
        #write_section(f'Momentum Ranking Top {strat.TopNPerGroup} as of {current_date.strftime("%m/%d/%y")}', top_n_symbols)
    #####################################################################################################


    #####################################################################################################
    # aggregate all rank values
    #####################################################################################################
    weights = {
        'Rank_Volume': .0, 
        'Rank_Volatility': 0.2, 
        'Rank_Trend': 0.4,
        'Rank_Momentum': 0.3
    }
    df_top_combined = aggregate_indicators_weighted(df_top_combined, ['Rank_Volume', 'Rank_Volatility', 'Rank_Trend', 'Rank_Momentum'], weights, 'Rank_Overall' )    
    df_top_combined.reset_index(inplace=True)
    while True:
        try:
            df_top_combined.to_csv(pl.COMMON_DIR / 'df_top_combined.csv')
            break
        except Exception as e:
            write_line("ERROR - Failed saving df_top_combined")
            go_to_sleep(30, 60)
    df_top_combined.set_index(['Symbol', 'Date'], inplace=True)
    #####################################################################################################


    #####################################################################################################
    # calculate median and std of ranks, add new adjusted rank based on penalization of std
    #####################################################################################################
    filtered_df = df_top_combined.reset_index()    
    del df_top_combined
    grouped_df = filtered_df.groupby('Symbol')['Rank_Overall'].agg(['median', 'std'])
    # del filtered_df
    grouped_df = grouped_df.rename(columns={'median': "Rank_Median", "std": "Rank_Std"})
    penalization_factor = 2  # Adjust this factor to control the degree of penalization
    grouped_df['Rank_Adjusted'] = grouped_df['Rank_Median'] / (penalization_factor * grouped_df['Rank_Std'])
    top_symbols = grouped_df.nlargest(5, 'Rank_Adjusted').index.to_list()
    top_symbols.sort()
    #####################################################################################################


    #####################################################################################################
    # init weights
    #####################################################################################################
    weights = []
    for i in range(len(top_symbols)):
        weights.concat(i*(1/len(top_symbols)))
    weights.concat(1.0)
    weights_combinations = list(itertools.product(weights, repeat=len(top_symbols)))
    weights_combinations = [tup for tup in weights_combinations if sum(tup) == 1]
    weighted_results = {}
    #####################################################################################################


    #####################################################################################################
    # calculate ideal weights to maximize return and minimize variance
    #####################################################################################################
    col_to_grab = 'Close'
    filtered_df.set_index(['Symbol', 'Date'], inplace=True)
    reshaped_df = filtered_df[col_to_grab].unstack(level='Symbol')
    for weight in weights_combinations:
        reshaped_df = add_rv(reshaped_df, top_symbols, weight)
        weighted_results[weight] = {'Return': reshaped_df["Weighted Return"][-1], 'Variance': reshaped_df["Weighted Variance"][-1]}
    optimal_weights_minvar = min(weighted_results, key=lambda k: weighted_results[k]['Variance'])
    optimal_weights_maxret = max(weighted_results, key=lambda k: weighted_results[k]['Return'])
    optimial_weights_combined = [(a + b) / 2 for a, b in zip(optimal_weights_maxret, optimal_weights_minvar)]
    #write_line(f'Top Symbols as of {current_date.strftime("%m/%d/%y")}: {top_symbols}')
    # Create DataFrame
    df = pd.DataFrame({
        'Symbol': top_symbols,
        'MinVar': optimal_weights_minvar,
        'MaxRet': optimal_weights_maxret,
        'Blended': optimial_weights_combined
    })
    write_section(f'Optimal Weights as of {current_date.strftime("%m/%d/%y")}', df)
    #####################################################################################################

    return df

def get_active_tickers():
    selected_columns = [
        "ticker",  # Symbol
        "comp_name",  # Name
        "comp_name_2", 
        "sic_4_desc",  # Description
        "zacks_x_sector_desc",  # Sector
        "zacks_x_ind_desc",  # Industry
        "zacks_m_ind_desc",  # Industry_2
        "optionable_flag",  # Optionable
        "country_name",  # Country
        "active_ticker_flag",
        "ticker_type"
    ]

    rename_mapping = {
        "ticker": "Symbol",
        "comp_name": "Name",
        "sic_4_desc": "Description",
        "zacks_x_sector_desc": "Sector",
        "zacks_x_ind_desc": "Industry",
        "zacks_m_ind_desc": "Industry_2",
        "optionable_flag": "Optionable",
        "country_name": "Country"
    }

    # Fetch data with only selected columns and active tickers
    nasdaqdatalink.ApiConfig.verify_ssl = False
    
    # Try environment variable first (GitHub Secrets), then fallback to local file
    api_key = os.environ.get('NASDAQ_API_KEY')
    if api_key:
        nasdaqdatalink.ApiConfig.api_key = api_key
    else:
        key_path = pl.COMMON_DIR / 'nasdaq_key.txt'
        if key_path.exists():
            nasdaqdatalink.read_key(filename=str(key_path))
        else:
            print(f"Warning: NASDAQ API key not found in env 'NASDAQ_API_KEY' or at {key_path}")
    df = nasdaqdatalink.get_table(
        "ZACKS/MT",
        paginate=True,
        qopts={"columns": selected_columns}
    )
    
    df = df[df['active_ticker_flag'] == "Y"]
    df = df[df['ticker_type'] == "S"]
    df = df[df['ticker_type'] == "S"]

    # Merge 'comp_name' with 'comp_name_2' if missing
    df["comp_name"] = np.where(
        (df["comp_name"].isnull()) | (df["comp_name"].str.strip() == ""),
        df["comp_name_2"],
        df["comp_name"]
    )

    # Drop comp_name_2 since we already merged it
    df.drop(columns=["comp_name_2", "active_ticker_flag", "ticker_type"], inplace=True)

    # Rename columns
    df.rename(columns=rename_mapping, inplace=True)

    return df

def get_symbols():
    
    # grab list of sp500 tickers
    df_symbols = pd.DataFrame()
    
    
    file_path = pl.COMMON_DIR / 'df_symbols.csv'
    # file_path = 'Data/df_russell3000.pickle'
    
    if os.path.exists(file_path):
        # Get the timestamp of the file's last modification
        timestamp = os.path.getmtime(file_path)
        
        # Convert the timestamp to a readable date format
        last_updated = time.ctime(timestamp)
        
        # Calculate the current timestamp minus 7 days
        seven_days_ago = time.time() - (7 * 24 * 60 * 60)
        
        # Compare the timestamps
        if timestamp > 0:#seven_days_ago:
            #df_symbols = get_sp500(file_path)
            # df_symbols = get_russell_3000(file_path)
            
            df_symbols = get_active_tickers()
        else:
            # open the file for reading binary data
            df_symbols = load_csv(file_path)
    else:
        #df_symbols = get_sp500(file_path)
        # df_symbols = get_russell_3000(file_path)
        df_symbols = get_active_tickers() 
        store_csv(df_symbols, file_path)
        
    tickers_to_add = [ {
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
                        }
                        ,
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
    # 2. Load the two other files that contain symbols you want to remove
    # 2. Load the two other files that contain symbols you want to remove
    blacklist_path = pl.COMMON_DIR / 'blacklist.csv'
    blacklist_financial_path = pl.COMMON_DIR / 'blacklist_financial.csv'
    
    symbols_to_remove = set()
    symbols_to_remove.update(load_ticker_list(blacklist_path))
    symbols_to_remove.update(load_ticker_list(blacklist_financial_path))
    
    # 3. Filter your main df to exclude those symbols
    if symbols_to_remove:
        write_line(f"Excluding {len(symbols_to_remove)} blacklisted symbols.")
        df_symbols = df_symbols[~df_symbols['Symbol'].isin(symbols_to_remove)]

    # 5. (Optional) Reset the index if you like a clean 0…N index
    df_symbols = df_symbols.reset_index(drop=True)

    # Get the list of unique symbols
    symbols = list(df_symbols['Symbol'].unique())
    for ticker_to_add in tickers_to_add:
        if not ticker_to_add['Symbol'] in df_symbols['Symbol'].to_list():
            df_symbols = pd.concat([df_symbols, pd.DataFrame.from_dict([ticker_to_add])], ignore_index=True)
    df_symbols.drop_duplicates()
    store_csv(df_symbols, file_path)
    pd.DataFrame(tickers_to_add).to_csv(pl.COMMON_DIR / 'market_analysis_tickers.csv', index=False)
    df_symbols.to_csv(pl.COMMON_DIR / 'stock_tickers.csv', index=False)
    return df_symbols

def load_df_combined(symbols):
    num_threads = 4
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Results will contain the result of process_row applied to each row
        results = list(executor.map(load_csv, [pl.COMMON_DIR / 'Yahoo/Price Data' / f'{symbol}.csv' for symbol in symbols]))
    df_combined = pd.DataFrame()
    if len([df for df in results if df is not None]) > 0:
        write_line(f'Combining results of {len(symbols)}')
        df_combined = pd.concat(results, ignore_index=True)
    return df_combined


def remove_dupes(df, columns=['Symbol', 'Date']):
    if df is None:
        return df
    return df.drop_duplicates(subset=columns)

def generate_random_position(limit, date, pool):
    # Read the CSV file into a DataFrame
    #df_symbols = load_csv('Data/df_russell3000.pickle')[['Symbol']]

    # Select a random row from the DataFrame
    random_symbol = random.choice(pool)

    # Create an instance of the Position class
    quantity = 0    
    
    #price = round(random.uniform(900.0, 1000.0), 2) # stop loss test
    price = round(random.uniform(1.0, 5.0)) # take profit test
    
    # take profit test
    if limit is None:
        quantity = random.randint(1, 10000)
    else:
        quantity = int(limit / price)
           
    position = Position(random_symbol, price, quantity, date)
    return position

def generate_random_date(days_back=60, date=datetime.today().date()):
    return date - timedelta(days=random.randint(0, days_back))

def write_section(title, s):
    print ("\n--------------------------------------------------")
    print (title)
    print ("--------------------------------------------------")
    if isinstance(s, np.ndarray):
        for i in range(len(s)):
            print("{}: {}".format(i+1, s[i]))
    else:
        print(s)
    print ("--------------------------------------------------\n")

def write_line(msg):
    '''
    Print a line to the console w/ a timestamp
    Parameters:
        str:
    '''
    # sys.stdout.write('\r' + ' ' * 120 + '\r')
    # sys.stdout.flush()
    ct = datetime.now()
    ct = ct.strftime('%Y-%m-%d %H:%M:%S')
    print('{}: {}'.format(ct, msg))

def write_inline(text, endline=False):
    if not endline:
        sys.stdout.write('\r' + ' ' * 120 + '\r')
        sys.stdout.flush()
        ct = datetime.now()
        ct = ct.strftime('%Y-%m-%d %H:%M:%S')
        print('{}: {}'.format(ct, text), end='')
    else:
        print('\n\n', end='')
  
def rank_indicators(df, indicators, clear=False) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return df
    df = df[~df.index.duplicated(keep='first')]
    # have to set the direction to sort/rank each indicator so pull it from a manually created csv
    # clicked on whatever link ta library had in its documentation
    indicator_directions = pd.read_csv(pl.COMMON_DIR / 'indicators.csv')
    for i_index, indicator in enumerate(indicators):
        write_inline(f'Processing {indicator} ({i_index}/{len(indicators)})')
        
        # grab ohlcv data + iterated indicator
        df_subset = df[['Open', 'High', 'Low', 'Close', 'Volume', indicator]]
        # if clear:
        #     df_subset[indicator] = np.nan
        df_subset_filtered = df_subset[~df_subset[indicator].isna()]
        
        # grab a list of the dates to be iterated through
        dates_list = df_subset_filtered.index.get_level_values('Date').unique().tolist()
        
        # grab a list of symbols to be iterated through
        symbols_list = df_subset_filtered.index.get_level_values('Symbol').unique().tolist()
        
        # grab the directior to sort/rank for the iterated indicator
        direction = indicator_directions[indicator_directions['indicator_name'] == indicator]['direction'].iloc[0]  
        
        # iterate through dates, rank symbols and update df_subset
        for d_index, date in enumerate(dates_list):
            
            write_inline(f'Processing {date} ({d_index} / {len(dates_list)}) for {indicator} ({i_index}/{len(indicators)}))')
            
            # grabbing ohlcv, etc data for given date
            df_subset_date = df_subset_filtered[df_subset_filtered.index.get_level_values('Date') == date]
        
            # rank all symbols for the given subset date, sort in direction provided from csv
            df_subset_date[indicator] = df_subset_date[indicator].rank(ascending=(True if direction == 'desc' else False))
            
            # adjust indicator value to be a % in order to try and normalize it
            df_subset_date[indicator] = (df_subset_date[indicator] / len(df_subset_date)).round(2) * 100
            # Find non-unique indexes
            non_unique_indexes = df.index.duplicated(keep=False)

            # Get the actual index values of non-unique indexes
            non_unique_values = df_subset.index[df_subset.index.duplicated(keep=False)]
            # update the original subset
            df_subset.loc[df_subset.index.get_level_values('Date') == date, indicator] = df_subset_date[indicator]
        write_inline('')    
        # update sector indicator
        df[indicator] = df_subset[indicator]
    
    return df

def aggregate_indicators(df, indicators, agg_col_name) -> pd.DataFrame:
    # Calculate the median of volume indicators, grouped by 'Symbol' and 'Date' separately for each column
    grouped_median = df.groupby(['Symbol', 'Date'])[indicators].median()

    # Calculate the overall median across all columns
    overall_median = grouped_median.median(axis=1)

    # Assign the overall median values to the new column agg_col_name
    df[agg_col_name] = overall_median
    
    return df
    
def aggregate_indicators_weighted(df, indicators, weights, agg_col_name) -> pd.DataFrame:
    # Apply weights to each indicator column
    for column in weights.keys():
        df[column] *= weights[column]

    # Calculate the weighted sum of indicators, grouped by 'Symbol' and 'Date' separately for each column
    grouped_sum = df.groupby(['Symbol', 'Date'])[indicators].sum()

    # Calculate the overall sum across all columns
    overall_sum = grouped_sum.sum(axis=1)

    # Assign the overall sum values to the new column agg_col_name
    df[agg_col_name] = overall_sum
    
    return df
    
def get_sortino(window):
    df_stock = window
    
    prices = df_stock['Close']#Library.get_close_price(self, [stock])
    daily_return_rp = df_stock['Diff%']#['Library.convert_to_returns(self, prices[stock], False)
    cumulative_return_rp = np.cumprod(daily_return_rp).to_list()[-1]
    # Calculate cumuluative return for the period
    total_return_rp = (cumulative_return_rp) - 1
    
    # Grab the risk free price data
    #prices_rf = df_stock['Close']#Library.get_close_price(self, [self.risk_free_stock])
    daily_return_rf = df_stock['Diff%_RiskFree']#Library.convert_to_returns(self, prices_rf[self.risk_free_stock])
    cumulative_return_rf = np.cumprod(daily_return_rf).to_list()[-1]
    total_return_rf = (cumulative_return_rf) - 1
    # downside_ret = []
    # for x in daily_return_rp:#['Diff%']:#.tail(lookback_bars):     
    #     try:
    #         if x < 1:
    #             downside_ret.concat(abs(x-1))
    #     except:
    #         s = type(x)
        
    daily_return_rp = daily_return_rp - 1
    filtered_data = np.array([x for x in daily_return_rp if x < 0])

    standard_deviation = np.std(daily_return_rp) #abs(np.std(filtered_data))
    #down_stdev = np.mean(downside_ret)#*len(downside_ret)
    result = (total_return_rp - total_return_rf)/standard_deviation
    
    return result
 
def go_to_sleep(range_low = 5, range_high = 20):
    # sleep for certain amount of time
    sleep_time = random.randint(range_low, range_high)
    write_line(f'Sleeping for {sleep_time} seconds...')
    time.sleep(random.randint(range_low, range_high))
 
def perform_technical_analysis(lookback_bars, ticker, df_risk_free): 
    
    write_line(f'Performing technical analysis on  {ticker}')
    
    # Retrieve historical data for ticker
    df_historical, ticker_pickle_path = get_historical_data(ticker, False, False, None)
    if isinstance(df_historical, type(None)) or len(df_historical) == 0:
        write_line(f'Nothing found for ticker {ticker}')
        return None
    
    df_historical = df_historical.sort_values(['Symbol', 'Date'])
    df_historical = df_historical.drop_duplicates(subset='Date')

    # Check to make sure we have enough data to do the analysis
    if len(df_historical) > 50:
        
        # Calculate the rolling average of 'Volume' with a 30-day window
        rolling_avg = df_historical['Volume'].rolling(window=lookback_bars).mean()
        df_historical['Normalized_Volume'] = df_historical['Volume'] / rolling_avg
        
        try:
            
            # Add daily return as a percentage change in the Close price
            df_historical['Diff%'] = df_historical['Close'] / df_historical['Close'].shift(1) - 1
            
            # Add daily return for risk free asset, in this case SPY returns
            if df_risk_free is not None and ticker != 'SPY':
                df_risk_free['Diff%'] = df_risk_free['Close'] / df_risk_free['Close'].shift(1) - 1
                df_historical = pd.merge(df_historical, df_risk_free[['Date', 'Diff%']], on='Date', suffixes=('', '_RiskFree'))#.drop(columns='Date_RiskFree')
            else:
                df_historical['Diff%_RiskFree'] = df_historical['Diff%']
                
            # Drop any duplicates
            df_historical = df_historical.loc[:, ~df_historical.columns.duplicated()]
            
            if df_risk_free is not None:
                df_historical['Jensen_20'] = calculate_rolling_jensens_alpha(df_historical, df_risk_free, .035, 20)
                df_historical['Jensen_50'] = calculate_rolling_jensens_alpha(df_historical, df_risk_free, .035, 50)
            else:
                df_historical['Jensen_20'] = 0
                df_historical['Jensen_50'] = 0
            
            # Calculate indicators for the current symbol
            df_historical['MACD_Signal_Line'] = calculate_macd(df_historical)
            df_historical['Standardized_MACD'] = standardize_macd(df_historical)
            df_historical = calculate_bollinger_bands(df_historical, period=36)
            df_historical['OBV'] = calculate_obv(df_historical)
            df_historical['ADL'] = calculate_adl(df_historical)
            df_historical['RSI'] = calculate_rsi(df_historical)
            df_historical = calculate_stochastic_oscillator(df_historical, period=36)
            df_historical['CCI'] = calculate_cci(df_historical)
            df_historical['Boll_Stoch_Diff'] = (df_historical['Bollinger_High'] - df_historical['Bollinger_Low']) / (df_historical['Stochastic_High'] - df_historical['Stochastic_Low'])
            df_historical['Indicator_Diff'] = df_historical['Stochastic_Oscillator'] - df_historical['Boll_Stoch_Diff']

            # add N-day differences for Close and Volume
            for period in (5, 15, 45):
                df_historical[f'Close_diff_{period}']   = df_historical['Close'].diff(period)
                df_historical[f'Volume_diff_{period}']  = df_historical['Volume'].diff(period)

            # Add returns related columns
            no_of_days = 30
            df_historical[f'30_Days_Forward_Return'] = (df_historical['Close'].transform(lambda x: x.shift(-no_of_days) - x) / df_historical['Close']) * 100
            df_historical['Daily_Return'] = df_historical['Close'].pct_change()
            df_historical['Return_Volatility'] = df_historical['Daily_Return'].rolling(window=no_of_days).std()

            # 1) Identify the exact crossover events
            df_historical['Bullish_Crossover'] = (
                (df_historical['MACD'] > df_historical['MACD_Signal_Line']) &
                (df_historical['MACD'].shift(1) <= df_historical['MACD_Signal_Line'].shift(1))
            )
            df_historical['Bearish_Crossover'] = (
                (df_historical['MACD'] < df_historical['MACD_Signal_Line']) &
                (df_historical['MACD'].shift(1) >= df_historical['MACD_Signal_Line'].shift(1))
            )

            # 2) Forward-fill the last event date
            df_historical['Last_Bullish_Date'] = df_historical.loc[df_historical['Bullish_Crossover'], 'Date']
            df_historical['Last_Bullish_Date'] = df_historical['Last_Bullish_Date'].ffill()
            df_historical['Last_Bearish_Date'] = df_historical.loc[df_historical['Bearish_Crossover'], 'Date']
            df_historical['Last_Bearish_Date'] = df_historical['Last_Bearish_Date'].ffill()

            # 3) Compute days since each event
            df_historical['Days Since MACD Crossover - Bullish'] = (df_historical['Date'] - df_historical['Last_Bullish_Date']).dt.days
            df_historical['Days Since MACD Crossover - Bearish'] = (df_historical['Date'] - df_historical['Last_Bearish_Date']).dt.days

            # Clean up columns
            df_historical.drop(columns=[col for col in df_historical.columns if col in ['Bullish_Crossover', 'Bearish_Crossover', 'Last_Bullish_Date', 'Last_Bearish_Date']])

            df_historical['Daily_Log_Return'] = (df_historical['Close'].pct_change().apply(np.log1p))
            df_historical['Return_30d_Trail'] = (df_historical['Close'].pct_change(periods=30))
            df_historical['Vol_30d'] = (df_historical['Daily_Log_Return'].rolling(window=30, min_periods=20).std().reset_index(level=0, drop=True))
            

            df_historical = ichimoku_score(df_historical)
            
            df_historical['Diff%_Minus_RiskFree'] = df_historical['Daily_Return'] - df_historical['Diff%_RiskFree']
            df_historical['20_Day_MA_Diff%_Minus_RiskFree'] = df_historical['Diff%_Minus_RiskFree'].transform(lambda x: x.rolling(window=20, min_periods=1).mean())
            
            
            # Calculate VWAP using a 20-day rolling window
            df_historical['Typical_Price'] = (df_historical['High'] + df_historical['Low'] + df_historical['Close']) / 3
            df_historical['TP_Volume'] = df_historical['Typical_Price'] * df_historical['Volume']
            df_historical['VWAP'] = (df_historical['TP_Volume'].rolling(window=20).sum()) / (df_historical['Volume'].rolling(window=20).sum())
            df_historical['VWAP_Diff'] = (df_historical['Close'] - df_historical['VWAP']) / df_historical['Close']

            df_historical['20_Day_MA_Diff%'] = df_historical['Daily_Return'].transform(lambda x: x.rolling(window=20, min_periods=1).mean())
            df_historical['20_Day_STD_Diff%'] = df_historical['Daily_Return'].transform(lambda x: x.rolling(window=20, min_periods=1).std())
            df_historical['20_Day_MA_STD_Ratio_Diff%'] = (df_historical['20_Day_MA_Diff%'] / df_historical['20_Day_STD_Diff%']).fillna(0)

            # ============== 52-Week Volume ==============
            df_historical['52_Week_High_Volume'] = df_historical['Volume'].rolling(window=252, min_periods=1).max()
            df_historical['52_Week_Low_Volume'] = df_historical['Volume'].rolling(window=252, min_periods=1).min()
            df_historical['52_Week_Volume_Range_Percent'] = (
                (df_historical['Volume'] - df_historical['52_Week_Low_Volume'])
                / (df_historical['52_Week_High_Volume'] - df_historical['52_Week_Low_Volume'])
            ) * 100
            df_historical['52_Week_Volume_Range_Percent'] = (
                df_historical['52_Week_Volume_Range_Percent']
                .fillna(0)
                .clip(lower=0, upper=100)
            )
            df_historical['52_Week_Volume_Range_Percent'] = (
                df_historical['52_Week_Volume_Range_Percent']
                .rolling(window=20, min_periods=1)
                .mean()
            )
            
            # ============== 52-Week Price ==============
            df_historical['52_Week_Low_Price'] = df_historical['Low'].rolling(window=252, min_periods=1).min()
            df_historical['52_Week_High_Price'] = df_historical['High'].rolling(window=252, min_periods=1).max()
            df_historical['52_Week_Price_Range_Percent'] = (
                (df_historical['Close'] - df_historical['52_Week_Low_Price'])
                / (df_historical['52_Week_High_Price'] - df_historical['52_Week_Low_Price'])
            ) * 100
            df_historical['52_Week_Price_Range_Percent'] = (
                df_historical['52_Week_Price_Range_Percent']
                .fillna(0)
                .clip(lower=0, upper=100)
            )
            df_historical['52_Week_Price_Range_Percent'] = (
                df_historical['52_Week_Price_Range_Percent']
                .rolling(window=20, min_periods=1)
                .mean()
            )
            # =================================================
            
            # Apply transformations
            df_historical.replace('', pd.NA, inplace=True)
            df_historical = df_historical[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Diff%_RiskFree',
            'Jensen_20', 'Jensen_50', 'MACD',
            'Signal_Line', 'MACD_Signal_Line', 'Standardized_MACD', 
            'Bollinger_Close', 'Bollinger_Range', 'Stochastic_Oscillator_Range',
            'CCI', 'Boll_Stoch_Diff', 'Indicator_Diff',
            '30_Days_Forward_Return', 'Daily_Return', 'Return_Volatility', 'Daily_Log_Return', 'Return_30d_Trail', 'Vol_30d',
            'Ichimoku Score', 'VWAP', 'VWAP_Diff']]      

            store_csv(df_historical, ticker_pickle_path)    
        except Exception as e:
            write_line(f'ERROR - Failed stock analysis for {ticker}')
    return df_historical


def perform_stock_analysis(lookback_bars, ticker, sector, industry, df_combined=None):

    if df_combined is None:
        df_combined = load_df_combined(30)
    # Now append df_new to dff
    #df_combined = df_combined.concat(perform_stock_analysis(lookback_bars, 'SPY', 'Benchmark', 'Benchmark'))
    
    df_historical, ticker_pickle_path = get_historical_data(ticker)
    if ticker == 'SPY':
        df_risk_free = df_historical
    else:
        df_risk_free = df_combined[df_combined['Symbol'] == 'SPY']
    if isinstance(df_historical, type(None)) or len(df_historical) == 0:
        write_line(f'Nothing found for ticker {ticker}')
        
    #go_to_sleep()

    if 'Sortino' in df_historical.columns.values:
        df_historical = df_historical[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Sortino']]
    else:
        df_historical = df_historical[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df_historical['Sortino'] = 0
    df_historical['Sector'] = sector
    df_historical['Industry'] = industry
    df_historical = df_historical.drop_duplicates(subset='Date')

    # get indicators
    if len(df_historical) > 50:
        #df_historical = df_historical.iloc[:, :8]
        # Calculate the rolling average of 'Volume' with a 30-day window
        rolling_avg = df_historical['Volume'].rolling(window=lookback_bars).mean()

        # Calculate the normalization factor as the ratio between 'Volume' and the rolling average
        df_historical['Normalized_Volume'] = df_historical['Volume'] / rolling_avg
        
        # variance
        df_historical['Variance_lookback'] = df_historical['Close'].rolling(window=lookback_bars).var()
        df_historical[f'Diff%_Variance_lookback'] = (df_historical['Variance_lookback'] - df_historical['Variance_lookback'].shift(lookback_bars)) / df_historical['Variance_lookback'].shift(lookback_bars)
        
        df_historical['Diff%'] = df_historical['Close'] / df_historical['Open']
        
        if df_risk_free is not None and ticker != 'SPY':
            df_risk_free['Diff%'] = df_risk_free['Close'] / df_risk_free['Open']
            df_historical = pd.merge(df_historical, df_risk_free[['Date', 'Diff%']], on='Date', suffixes=('', '_RiskFree'))#.drop(columns='Date_RiskFree')
        else:
            df_historical['Diff%_RiskFree'] = df_historical['Diff%']
        df_historical = df_historical.loc[:, ~df_historical.columns.duplicated()]
        #df_historical['Sortino'] = 0.0*len(df_historical) if df_risk_free is None else df_historical.rolling(window=lookback_bars).apply(get_sortino, raw=False)
        df_historical.reset_index(inplace=True)
        # Loop through the DataFrame
        if df_risk_free is not None:
            #for i in range(lookback_bars, len(df_historical)):
            lbs = [50, 200]
            for lb in lbs:
                col_name = 'Sortino_'+ str(lb)
                for index, index_value in enumerate(df_historical[pd.isna(df_historical[col_name])].index.values):
                    if index_value < lookback_bars:
                        continue
                    # Get the subset of rows starting from the current row and going back 30 rows
                    subset = df_historical.iloc[index_value-lookback_bars:index_value]
                    
                    # Call the function on the subset
                    if not check_number(df_historical.at[index_value, col_name]):
                        df_historical.at[index_value, col_name] = get_sortino(subset)
        else:
            df_historical['Sortino'] = 0
        df_historical = df_historical[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Sector', 'Industry', 'Normalized_Volume', 'Variance_lookback', 'Diff%_Variance_lookback', 'Diff%', 'Diff%_RiskFree', 'Sortino']]
        df_historical = df_historical.reset_index()
        store_csv(df_historical, ticker_pickle_path)    
        
    #return df_historical

def check_number(var):
    if isinstance(var, str) and var == '':
        return False
        #return 'Empty String'
    elif np.isnan(var):
        return False
        #return 'NaN'
    elif var > 0:
        return True
        #return 'Positive'
    elif var < 0:
        return True
        #return 'Negative'
    elif var == 0:
        return False
        #return 'Zero'
    else:
        return False
        #return 'Not a Number'

def add_rv(df, symbols_list, weights) -> pd.DataFrame:
    """_summary_
    Adds the weighted returns and variance for a provided list of symbols and weights
    Args:
        df (_type_): _description_
        symbols_list (_type_): _description_
        weights (_type_): _description_

    Returns:
        pd.DataFrame: _description_
    """

    df_backup = copy.deepcopy(df)
    df['Weighted Return'] = 1.0
    
    # Calculate the weighted cumulative returns and variance for each column
    for column, weight in zip(symbols_list, weights):
        # Calculate the daily returns
        daily_returns = df[column].pct_change()
        
        # Calculate the cumulative returns
        cumulative_returns = (1 + daily_returns).cumprod()# - 1
        
        # Calculate the weighted cumulative returns
        df['Weighted Return']  = df['Weighted Return'] * (1+((cumulative_returns-1) * weight))
        
    df['Weighted Variance'] = df['Weighted Return'].var()   
    return df

def get_topn_symbols(df, n, rank_column, lookback=100000) -> list:
    # filter df_reset and filter date on lookback bars
    df_reset = df.reset_index()
    cutoff_date = df_reset['Date'].max() - pd.DateOffset(days=lookback)
    df_reset = df_reset[df_reset['Date'] > cutoff_date]
    return df_reset.groupby('Symbol')[rank_column].median().nlargest(n).index.to_list()

def add_macd_to_dataframe(df, column_name):
    """
    Adds MACD and MACD Signal line to the DataFrame.

    Parameters:
    - df: pandas.DataFrame with the data.
    - column_name: the name of the column containing the price data.

    Returns:
    - DataFrame with added 'MACD' and 'MACD_Signal' columns.
    """
    # Calculate the short-term and long-term EMAs of the closing price
    short_ema = df[column_name].ewm(span=12, adjust=False).mean()
    long_ema = df[column_name].ewm(span=26, adjust=False).mean()

    # Calculate the MACD line
    macd = short_ema - long_ema

    # Calculate the signal line
    macd_s = macd.ewm(span=9, adjust=False).mean()

    return macd - macd_s

# def plot_dual_axis_line_chart(df, date_column, primary_y_columns, secondary_y_columns, plot_title, primary_y_label, secondary_y_label, comparison_columns, single_column):
def plot_dual_axis_line_chart(dfs, primary_y_columns, secondary_y_columns, primary_y_label, secondary_y_label, comparison_columns, single_column):
    """
    Plots a line chart with two separate y-axes, each representing a set of specified value columns against a date column.
    Shades area light grey where all columns in comparison_columns are greater than single_column.

    Parameters:
    - df: pandas.DataFrame containing the data.
    - date_column: the name of the column with date data.
    - primary_y_columns: a list of names of the columns to plot on the primary y-axis.
    - secondary_y_columns: a list of names of the columns to plot on the secondary y-axis.
    - comparison_columns: a list of column names to compare against single_column.
    - single_column: the name of the single column to compare against.
    """
    # Group by 'Industry' and then 'Symbol'
    industry_grouped = dfs.groupby('Industry')

    # Iterate through each industry group
    for industry_name, industry_df in industry_grouped:
        print(f"Industry: {industry_name}")
        # Within each industry, group by 'Symbol'
        symbol_grouped = industry_df.groupby('Symbol')

        n = symbol_grouped.ngroups
        
        # Calculate the grid size
        rows = math.ceil(n ** 0.5)
        cols = math.ceil(n / rows)
        
        # Create a figure with subplots
        fig, axes = plt.subplots(rows, cols, figsize=(cols*5, rows*4))
        axes = axes.flatten()  # Flatten in case of a single row/column to avoid indexing issues
        
        # Plot each DataFrame
        counter = 0
        for symbol, df in symbol_grouped:
            print(f"Symbol: {symbol}")
            #df = df.tail(90)
            if n == 1:
                ax1 = axes
            else:
                ax1 = axes[counter]
                counter += 1
            df['Date'] = pd.to_datetime(df['Date'])
            
            # Sort the DataFrame by the date column
            df = df.sort_values(by='Date')

            # Create mask where all comparison columns are greater than the single column
            df['compare'] = 0
            if single_column in df.columns.values: 
                df['compare'] = df[single_column]
            mask = df[comparison_columns].gt(df['compare'], axis=0).all(axis=1)

            # Shade areas where condition is True
            ax1.fill_between(df['Date'], 0, 1, where=mask, transform=ax1.get_xaxis_transform(), color='lightgrey', alpha=0.5, label='Shaded Area')

            # Plot each column specified in primary_y_columns on the primary y-axis
            for col in primary_y_columns:
                ax1.plot(df['Date'], df[col], marker='o', linestyle='-', label=col)
            
            # Set labels and legend for primary y-axis
            # ax1.set_xlabel('Date')
            #ax1.set_ylabel(primary_y_label, color='tab:blue')
            ax1.tick_params(axis='y', labelcolor='tab:blue')
            # ax1.legend(loc='upper left')
            ax1.set_ylim(-.05, .05)     
            
            # Create a second y-axis sharing the same x-axis
            ax2 = ax1.twinx()
            
            # Plot each column specified in secondary_y_columns on the secondary y-axis
            for col in secondary_y_columns:
                ax2.plot(df['Date'], df[col], marker='x', linestyle='--', label=col, color='black')
            
            # Set labels and legend for secondary y-axis
            #ax2.set_ylabel(secondary_y_label, color='tab:red')
            ax2.tick_params(axis='y', labelcolor='tab:red')
            # ax2.legend(loc='upper right')
            
            ax1.set_title(df['Symbol'].iloc[-1])
            ax1.grid(True)
            
            # Rotate x-axis tick labels and set alignment
            ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45, horizontalalignment='right')
            
            # Use MaxNLocator to limit the number of x-axis labels
            ax1.xaxis.set_major_locator(MaxNLocator(nbins=10))  # Adjust 'nbins' as needed
        
        # Hide any unused subplots
        for j in range(counter + 1, len(axes)):
            fig.delaxes(axes[j])
        
        plt.tight_layout()
        plt.show(block=False)
        plt.pause(1)

def add_ta(df) -> pd.DataFrame:   
    #df = None 
    try:
        df = df[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df = df[df['Volume'] != 0]
        ta.add_all_ta_features(df, 'Open', 'High', 'Low', 'Close', 'Volume')
        columns_to_remove = [
            'momentum_ppo',
            'momentum_ppo_signal',
            'momentum_pvo',
            'momentum_pvo_signal',
            'momentum_stoch_rsi_d',
            'momentum_stoch_rsi_k',
            'momentum_stoch_signal',
            'momentum_wr',
            'others_cr',
            'others_dlr',
            'others_dr',
            'trend_adx_neg',
            'trend_adx_pos',
            'trend_aroon_down',
            'trend_aroon_up',
            'trend_ema_fast',
            'trend_ema_slow',
            'trend_kst_diff',
            'trend_kst_sig',
            'trend_macd',
            'trend_macd_signal',
            'trend_mass_index',
            'trend_psar_down',
            'trend_psar_down_indicator',
            'trend_psar_up',
            'trend_psar_up_indicator',
            'trend_sma_fast',
            'trend_sma_slow',
            'trend_visual_ichimoku_a',
            'trend_visual_ichimoku_b',
            'trend_ichimoku_a',
            'trend_ichimoku_b',
            'trend_ichimoku_base',
            'trend_ichimoku_conv',
            'trend_vortex_ind_neg',
            'trend_vortex_ind_pos',
            'volatility_bbh',
            'volatility_bbhi',
            'volatility_bbl',
            'volatility_bbli',
            'volatility_bbm',
            'volatility_bbw',
            'volatility_dch',
            'volatility_dcl',
            'volatility_dcm',
            'volatility_dcw',
            'volatility_kcc',
            'volatility_kch',
            'volatility_kchi',
            'volatility_kcl',
            'volatility_kcli',
            'volatility_kcw',
            'volume_vwap'     
        ]
        
        # drop unnecessary columns
        df = df.drop(columns=columns_to_remove)
        df = df.set_index(['Symbol', 'Date'])
        # perform column transformations
        # trend_trix	desc	adjustment. Add 9 day trix ema, calc diff as %, update trix, drop 9 day ema
        
        # get list of indicators
        indicators = pd.read_csv('Data/indicators.csv')
        
        # grab a list of the dates to be iterated through
        dates_list = df.index.get_level_values('Date').unique().tolist()

        # grab a list of symbols to be iterated through
        symbols_list = df.index.get_level_values('Symbol').unique().tolist()
        for indicator in indicators:
            
            # adjust nvi to be % relative to 100 EMA, per symbol
            if indicator == 'volume_nvi':
                
                # iterate through sector symbols
                for symbol in symbols_list:
                    
                    # grab subset of data for iterated symbol
                    df_subset_symbol = df[df.index.get_level_values('Symbol') == symbol]
                    
                    # add 100 day EMA
                    df_subset_symbol['EMA'] = ta.trend.ema_indicator(df_subset_symbol['Close'], window=100)
                    
                    # adjust nvi by a percentage relative to the 100 day ema, where equal to it is considered
                    df_subset_symbol[indicator] = df_subset_symbol[indicator] / df_subset_symbol['EMA']
                    
                    # update df subset with ajusted indicator for iterated symbol
                    df.loc[df.index.get_level_values('Symbol') == symbol, indicator] = df_subset_symbol[indicator]

            # columns to convert to a percent change
            percent_change_indicators = ['momentum_ao', 'momentum_tsi', 'momentum_uo', 'trend_cci', 'trend_stc', 'volatility_ui']        
            if indicator in percent_change_indicators:        
                # iterate through sector symbols
                for symbol in symbols_list:
                    
                    # grab subset of data for iterated symbol
                    df_subset_symbol = df[df.index.get_level_values('Symbol') == symbol]
                    
                    # add 100 day EMA
                    df_subset_symbol[indicator] = df_subset_symbol[indicator].pct_change() * 100
                    
                    # update df subset with ajusted indicator for iterated symbol
                    df.loc[df.index.get_level_values('Symbol') == symbol, indicator] = df_subset_symbol[indicator]
            
            if indicator == 'momentum_kama':
                # iterate through sector symbols
                for symbol in symbols_list:
                    # grab subset of data for iterated symbol
                    df_subset_symbol = df[df.index.get_level_values('Symbol') == symbol]
                    
                    # adjust nvi by a percentage relative to the 100 day ema, where equal to it is considered
                    df_subset_symbol[indicator] = df_subset_symbol[indicator] / df_subset_symbol['Close']
                    
                    # update df subset with ajusted indicator for iterated symbol
                    df.loc[df.index.get_level_values('Symbol') == symbol, indicator] = df_subset_symbol[indicator]

            #Add 9 day trix ema, calc diff as %, update trix, drop 9 day ema
        
            if indicator == 'trend_trix':
                # iterate through sector symbols
                for symbol in symbols_list:
                    # grab subset of data for iterated symbol
                    df_subset_symbol = df[df.index.get_level_values('Symbol') == symbol]
                    
                    # add 100 day EMA
                    df_subset_symbol['trix_EMA'] = ta.trend.ema_indicator(df_subset_symbol['trend_trix'], window=9)
                    
                    # adjust nvi by a percentage relative to the 100 day ema, where equal to it is considered
                    df_subset_symbol[indicator] = df_subset_symbol[indicator] / df_subset_symbol['trix_EMA']
                    
                    # update df subset with ajusted indicator for iterated symbol
                    df.loc[df.index.get_level_values('Symbol') == symbol, indicator] = df_subset_symbol[indicator]
        df = df.reset_index()
    except Exception as e:
        write_line(f'ERROR: Failed adding technical analysis to {df["Symbol"].iloc[-1]}')
        return None
    return df
  
  
def get_russell_3000(file_path) -> pd.DataFrame:
    url = "http://www.kibot.com/Historical_Data/Russell_3000_Historical_Intraday_Data.aspx"

    # Send a GET request
    response = requests.get(url)

    # Parse the HTML content of the page with BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the table with the class 'ms-classic4-main'
    table = soup.find('table', {'id': 'dataTable'})

    # Get the HTML string of the table
    table_html = str(table)

    # Convert the HTML table to a DataFrame
    df = pd.read_html(table_html, header=0)[0]

    # Specify the column names
    df.columns = ['#', 'Symbol', 'StartDate', 'Size(MB)', 'Description', 'Industry', 'Sector']
    df = df[['Symbol', 'Description', 'Sector', 'Industry']]
    
    with open(file_path, 'wb') as f:
        # serialize the Python object and write it to the file
        pickle.dump(df, f)
    
    return df
    
def get_sp500(file_path) -> pd.DataFrame:
    # Make a GET request to the Wikipedia page
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    response = requests.get(url)

    # Create a BeautifulSoup object from the response content
    soup = BeautifulSoup(response.content, "html.parser")

    # Find the table with id "constituents"
    table = soup.find("table", id="constituents")

    # Extract data from the table
    data = []
    for row in table.find_all("tr")[1:]:  # Skip the header row
        cells = row.find_all("td")
        row_data = [cell.get_text(strip=True) for cell in cells]
        data.concat(row_data)
    # Create a DataFrame from the extracted data
    df_sp500 = pd.DataFrame(data, columns=['Symbol', 'Security', 'Sector', 'Industry', 'Headquarters', 'Date Added', 'CIK', 'Founded'])

    df_sp500 = df_sp500.iloc[:, :4]
    
    with open(file_path, 'wb') as f:
        # serialize the Python object and write it to the file
        pickle.dump(df_sp500, f)
    
    return df_sp500

def get_proxy() -> str:
    # Make a GET request to the proxy website
    url = 'https://www.sslproxies.org/'
    response = requests.get(url)

    # Create a BeautifulSoup object from the response content
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the table that contains the proxy information
    table = soup.find(id='proxylisttable')

    # Get all the rows from the table except the header row
    rows = table.find_all('tr')[1:]

    # Randomly select a proxy from the rows
    random_row = random.choice(rows)
    proxy_data = random_row.find_all('td')

    # Extract the IP address and port from the selected proxy
    ip_address = proxy_data[0].text
    port = proxy_data[1].text

    # Create the proxy URL
    proxy = f'http://{ip_address}:{port}'
    return proxy

def store_csv(obj: pd.DataFrame, file_path):
    """
    Save *obj* to *file_path*, creating parent folders as needed.

    Returns
    -------
    pathlib.Path
        The full path to the saved file.
    """
    target = Path(file_path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)   # create dirs
    obj.to_csv(target, index=False)
    return target

def load_csv(file_path) -> object:
    result = None
    try:
        if os.path.exists(file_path):
            result = pd.read_csv(file_path)
    except Exception as e:
        write_line(f'ERROR: {e}')
    return result

def is_weekend(date):
    return date.weekday() >= 5

def calculate_percent_change(df, start_date, end_date):
    start_open = df.loc[df['Date'] == start_date, 'Open'].values[0]
    end_close = df.loc[df['Date'] == end_date, 'Close'].values[0]

    percent_change = (end_close - start_open) / start_open

    return percent_change.round(4).astype(float)

def download_and_move_csv(url, download_path, target_path, cookies, headers):
    # Step 1: Download the file
    driver.execute_cdp_cmd("Network.enable", {})
    
    def log_request(request):
        print(f"Request: {request['request']['url']}")
    
    # Listen to network requests
    driver.request_interceptor = log_request
    
    driver.get(url)
    if response.status_code == 200:
        # Create the download directory if it doesn't exist
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        
        # Save the file to the initial download path
        with open(download_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully to {download_path}")
    else:
        print(f"Failed to download file from {url}")
        return None

    # Step 2: Move the file to the target directory
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    shutil.move(download_path, target_path)
    print(f"File moved to {target_path}")

    # Step 3: Read the CSV file into a DataFrame
    df = pd.read_csv(target_path)
    return df

def find_latest_file(folder_path, search_string, extensions=['csv','crdownload'], new_extension=None):
    # Ensure extensions is a list, even if a single extension is provided
    if isinstance(extensions, str):
        extensions = [extensions]
    
    counter = 0
    times_to_check = 3
    files = []

    while counter < times_to_check:
        files = []
        # Collect files matching any of the extensions
        for ext in extensions:
            search_pattern = os.path.join(folder_path, f"*.{ext}")
            files.extend(glob.glob(search_pattern))
        
        # Filter files to match only those containing the search string as a word
        filtered_files = [
            file for file in files 
            if re.search(rf"\b{re.escape(search_string)}\b", os.path.basename(file))
        ]
        
        if not filtered_files:
            counter += 1
            time.sleep(random.randint(1, 3))
        else:
            files = filtered_files
            break
        
    if not files:
        print(f"No files found matching pattern: '{search_string}' as a word and extensions '{extensions}'")
        return None

    # If a new_extension is provided, rename the matching files
    if new_extension:
        updated_files = []
        for file in files:
            base, current_ext = os.path.splitext(file)
            # Remove the dot from current_ext for a proper comparison, if needed
            if current_ext.lower() != f".{new_extension.lower()}":
                new_file = f"{base}.{new_extension}"
                try:
                    os.rename(file, new_file)
                    updated_files.concat(new_file)
                    print(f"Renamed '{file}' to '{new_file}'")
                except Exception as e:
                    print(f"Failed to rename '{file}': {e}")
                    updated_files.concat(file)  # Retain original file if rename fails
            else:
                updated_files.concat(file)
        files = updated_files
    
    # Determine the latest file by modification time
    latest_file = max(files, key=os.path.getmtime)
    return latest_file


def ichimoku_score(df, 
                tenkan_len=9, 
                kijun_len=26, 
                senkou_span_b_len=52, 
                displacement=26):
    """
    Computes Ichimoku lines and assigns a bullish/bearish score.
    df must contain 'High', 'Low', and 'Close' columns.

    Returns a copy of df with new columns:
    'tenkan', 'kijun', 'spanA', 'spanB', 'chikou', and 'score'.
    """
    # Work on a copy so we don't mutate the original DataFrame
    df = df.copy()

    # Tenkan-sen (Conversion Line)
    df['tenkan'] = (
        df['High'].rolling(window=tenkan_len).max() +
        df['Low'].rolling(window=tenkan_len).min()
    ) / 2.0

    # Kijun-sen (Base Line)
    df['kijun'] = (
        df['High'].rolling(window=kijun_len).max() +
        df['Low'].rolling(window=kijun_len).min()
    ) / 2.0

    # Senkou Span A (Leading Span A), shifted forward
    df['spanA'] = ((df['tenkan'] + df['kijun']) / 2.0).shift(displacement)

    # Senkou Span B (Leading Span B), shifted forward
    rolling_high_b = df['High'].rolling(window=senkou_span_b_len).max()
    rolling_low_b = df['Low'].rolling(window=senkou_span_b_len).min()
    df['spanB'] = ((rolling_high_b + rolling_low_b) / 2.0).shift(displacement)

    # Chikou Span (Lagging Span), shifted backward
    df['chikou'] = df['Close'].shift(-displacement)

    # Initialize our “Score” column
    df['Ichimoku Score'] = 0

    # (a) Tenkan vs. Kijun
    df.loc[df['tenkan'] > df['kijun'], 'Ichimoku Score'] += 1
    df.loc[df['tenkan'] < df['kijun'], 'Ichimoku Score'] -= 1

    # (b) Price vs. Cloud (Span A / Span B)
    df.loc[df['Close'] > df[['spanA', 'spanB']].max(axis=1), 'Ichimoku Score'] += 2
    df.loc[df['Close'] < df[['spanA', 'spanB']].min(axis=1), 'Ichimoku Score'] -= 2

    # (c) Cloud color: Span A vs. Span B
    df.loc[df['spanA'] > df['spanB'], 'Ichimoku Score'] += 1
    df.loc[df['spanA'] < df['spanB'], 'Ichimoku Score'] -= 1

    # (d) Close vs. Chikou
    df.loc[df['Close'] > df['chikou'], 'Ichimoku Score'] += 1
    df.loc[df['Close'] < df['chikou'], 'Ichimoku Score'] -= 1
    
    return df


def delete_files_with_string(folder_path, search_string, extensions=['csv','crdownload']):
    """
    Deletes files in the specified folder that contain the given search string 
    as a whole word and have one of the specified extensions.
    
    Args:
        folder_path (str): Path to the folder containing the files.
        search_string (str): String to search for as a whole word in file names.
        extensions (list or str): List of file extensions to filter by, or a single extension.
    """
    # Ensure extensions is a list, even if a single extension is provided
    if isinstance(extensions, str):
        extensions = [extensions]
    
    matching_files = []
    
    # Iterate over each extension to collect matching files
    for ext in extensions:
        search_pattern = os.path.join(folder_path, f"*.{ext}")
        files = glob.glob(search_pattern)
        # Filter files where the search string matches as a whole word in the filename (excluding extension)
        matching_files.extend([
            file for file in files
            if re.search(rf"\b{re.escape(search_string)}\b", os.path.splitext(os.path.basename(file))[0])
        ])
    
    if not matching_files:
        print(f"No files found matching the search string '{search_string}' as a whole word with extensions {extensions}.")
    else:
        # Iterate through the list of matching files and delete each one
        for file in matching_files:
            try:
                os.remove(file)
                print(f"Deleted file: {file}")
            except OSError as e:
                print(f"Error deleting file {file}: {e}")
   
def has_anomaly(df, target_cols = [], threshold=3):
    """
    Returns a tuple containing a boolean indicating if there are any anomalies
    and a DataFrame of rows where anomalies occur based on the z-score method
    for numeric columns in the dataset.

    Parameters:
    -----------
    df        : pandas.DataFrame
                The entire dataset to check.
    threshold : float
                The z-score threshold to flag anomalies (default 3).

    Returns:
    --------
    tuple
        (bool, pandas.DataFrame)
        - True if any anomalies found in any numeric column, False otherwise.
        - DataFrame containing rows where anomalies occur.
    """
    # Select only numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns

    # Initialize a boolean to track anomalies and an empty DataFrame for results
    anomalies_exist = False
    anomalies_df = pd.DataFrame()

    # Iterate through each numeric column and check for anomalies
    for col in numeric_cols:
        
        if len(target_cols) > 0 and col not in target_cols:
            continue            
        
        # Calculate day-to-day % change
        pct_changes = df[col].pct_change()

        # If there's not enough data to do % change, skip
        if pct_changes.dropna().empty:
            continue

        # Compute mean and std for the entire series (ignoring NaNs)
        mean_val = pct_changes.mean(skipna=True)
        std_val = pct_changes.std(skipna=True)

        # If std is zero or NaN (e.g., no variability), skip to avoid division by zero
        if pd.isna(std_val) or std_val == 0:
            continue

        # Compute z-score
        zscore = (pct_changes - mean_val) / std_val

        # Identify rows where absolute z-score exceeds threshold
        anomaly_indices = zscore.abs() > threshold

        if anomaly_indices.any():
            anomalies_exist = True

            # Add the rows with anomalies to the anomalies_df
            # Use the original DataFrame's indices for filtering
            anomalies_df = pd.concat([anomalies_df, df[anomaly_indices]], ignore_index=False)

    # Return the boolean flag and the anomalies DataFrame
    return anomalies_exist, anomalies_df

async def get_historical_data_async(ticker, drop_prior, get_latest, page) -> tuple[pd.DataFrame, str]:
   
    # Load df_ticker
    ticker = ticker.replace('.', '-')
    ticker_file_path = str(pl.COMMON_DIR / 'Yahoo' / 'Price Data' / f'{ticker}.csv')
    df_ticker = load_csv(ticker_file_path)    
    if isinstance(df_ticker, type(None)):
        df_ticker = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol'])
    
    # Cast to datetime
    if 'Date' in df_ticker.columns:
        df_ticker['Date'] = pd.to_datetime(df_ticker['Date'])
        
    # If latest isn't needed and data was retrieved
    if not get_latest and df_ticker is not None:
        return df_ticker, ticker_file_path

    # Check drop prior parameter
    if drop_prior:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        df_ticker = df_ticker[df_ticker['Date'] < yesterday.strftime("%Y-%m-%d")]

    # Time-of-day checks (Eastern Time)
    est = pytz.timezone('US/Eastern')
    current_time_est = datetime.now(est)
    market_open = True
    pre_market   = current_time_est.hour < 9 or (current_time_est.hour == 9 and current_time_est.minute < 30)
    after_market = current_time_est.hour > 16 or (current_time_est.hour == 16 and current_time_est.minute > 0)

    # Day-of-week check  (weekday(): 0 = Monday … 6 = Sunday)
    is_it_the_weekend = current_time_est.weekday() >= 5          # 5 = Saturday, 6 = Sunday

    # Final flag
    if pre_market or after_market or is_it_the_weekend:
        market_open = False
    
    # Loop until data is retrived
    retry_counter = 0
    while True:
                        
        if market_open:
            # Filter out rows where 'Date' is today's date            
            df_ticker.drop(df_ticker[df_ticker['Date'] == datetime.today().date().strftime("%Y-%m-%d")].index, inplace=True)

        # Set period2 to the previous market open date, and period1 to 10 years prior
        period1 = datetime.today() - relativedelta(years=10)            
        period1 = int(datetime(period1.year, period1.month, period1.day).timestamp())
        period2 = datetime.today()
        
        # Go back days until period2 isn't a weekend
        while is_weekend(period2):
            period2 = period2 - timedelta(days=1)        
        period2_timestamp = pd.Timestamp(period2.date())  # Convert period2.date() to Pandas Timestamp

        # Check if we have period2 date already in dataframe
        matching_rows = df_ticker[df_ticker['Date'] == period2_timestamp]
        if os.path.exists(ticker_file_path) and len(matching_rows) > 0:
            write_line(f'Data for {ticker} loaded from file')
            break
        else:
            try:
                # Download yahoo price data
                black_path = str(pl.COMMON_DIR / 'blacklist.csv')
                # white_path = str(pl.COMMON_DIR / 'whitelist.csv')
                period2 = int(datetime(period2.year, period2.month, period2.day).timestamp())
                url = f'https://query1.finance.yahoo.com/v7/finance/download/{ticker.replace(".", "-")}?period1={period1}&period2=99999999999&interval=1d&events=history'
                download_path = await pl.download_yahoo_price_data_async(page, url) #pl.get_yahoo_price_data(page, ticker, period1, period2)
                
                # Call the function to download, move, and read the CSV
                path = download_path
                # is_valid, df_blacklist, df_whitelist = check_black_white_list(black_path, white_path, ticker)
                # if is_valid is None or is_valid:
                if os.path.exists(download_path):
    
                        df_response = pd.read_csv(path)
                        delete_files_with_string('C:/Users/rdpro/Downloads', ticker, 'csv')
                        if "Adj Close" in df_response.columns:
                            df_response = df_response.drop('Adj Close', axis=1)
                        df_response['Date'] = pd.to_datetime(df_response['Date'])
                        df_response['Symbol'] = ticker

                        df_ticker = pd.concat([df_ticker, df_response], ignore_index=True)
                        
                        # Sort dataframe by Date, Symbol, and then by Volume in descending order
                        df_ticker = df_ticker.sort_values(by=['Date', 'Symbol', 'Volume'], ascending=[True, True, False])

                        # Drop duplicate combinations of Date and Symbol, keeping the first entry (which has the highest volume)
                        df_ticker = df_ticker.drop_duplicates(subset=['Date', 'Symbol'], keep='first')
                        
                        df_ticker['index'] = range(0, len(df_ticker))       
                        df_ticker['Symbol'] = ticker                        
                        df_ticker = df_ticker.astype({
                            'Open': float,
                            'High': float,
                            'Low': float,
                            'Close': float,
                            'Volume': float,
                        })
                        if df_ticker['Date'].dtype != 'datetime64[ns]':
                            df_ticker['Date'] = pd.to_datetime(df_ticker['Date'])
                        # List of columns to limit decimal places
                        columns_to_limit = ['Open', 'High', 'Low', 'Close']

                        # Limit decimal places for specified columns
                        for col in columns_to_limit:
                            df_ticker[col] = df_ticker[col].round(2).astype(float)
                            
                        columns_to_drop = ['index', 'Beta (5Y Monthly)', 'PE Ratio (TTM)', '1y Target Est', 'EPS (TTM)', 'Earnings Date', 'Forward Dividend & Yield', 'Market Cap']
                        df_ticker = df_ticker.drop(columns=[col for col in columns_to_drop if col in df_ticker.columns])
        
                        store_csv(df_ticker, ticker_file_path)
                        # last_date = df_ticker['Date'].max()
                        # quote_table = si.get_quote_table(ticker, dict_result=False)
                        # selected_keys = ['1y Target Est', 'Market Cap', 'PE Ratio (TTM)', 'Earnings Date', 'Forward Dividend & Yield', 'Beta (5Y Monthly)', 'EPS (TTM)']
                        # #selected_data = {key: quote_table[key] for key in selected_keys if key in quote_table}
                        # for idx, row in quote_table.iterrows():
                        #     key = row["attribute"]
                        #     value = row["value"]
                        #     if key in selected_keys:                    
                        #         df_ticker.loc[df_ticker['Date'] == last_date, key] = value
                            
                        # store_csv(df_ticker, ticker_file_path)
                        break
 
                else:
                    add_line_to_file(black_path, ticker)
                    return
                
            except Exception as e:
                e_str = str(e).lower()
                retry_counter += 1
                if retry_counter >= 5 or 'ailed to download after 3 attempts' in str(e):
                    add_line_to_file(black_path, ticker)
                    break
                elif '404' in e_str or 'list index out of range' in e_str:
                    write_line(f'Skipping {ticker} because no data was found')
                    break
                elif '401' in e_str:
                    write_line(f'ERROR: {ticker} - Unauthorized.')
                    go_to_sleep(30, 60)
                elif '429' in e_str:
                    write_line(f'Sleeping due to excessive requests for {ticker}')
                    go_to_sleep(30, 60)
                elif 'remote' in e_str or 'failed' in e_str or 'http' in e_str:
                    write_line(f'ERROR: {ticker} - {e}')
                    go_to_sleep(15, 30)
                elif 'system cannot find the file specified:' in e_str:
                    write_line(f'ERROR: File not found. {ticker} - {e}')
                    go_to_sleep(15, 30)
                else:
                    write_line(f'Uknown error: {ticker} - {e}')
                    go_to_sleep(30, 60)
    
    return df_ticker.reset_index(drop=True), ticker_file_path    

def get_historical_data(ticker, drop_prior, get_latest, page) -> tuple[pd.DataFrame, str]:
   
    # Load df_ticker
    ticker = ticker.replace('.', '-')
    ticker_file_path = str(pl.COMMON_DIR / 'Yahoo' / 'Price Data' / f'{ticker}.csv')
    df_ticker = load_csv(ticker_file_path)    
    if isinstance(df_ticker, type(None)):
        df_ticker = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol'])
    df_ticker = df_ticker[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol']]
    
    # Cast to datetime
    if 'Date' in df_ticker.columns:
        df_ticker['Date'] = pd.to_datetime(df_ticker['Date'])
        
    # If latest isn't needed and data was retrieved
    if not get_latest and df_ticker is not None:
        return df_ticker, ticker_file_path

    # Check drop prior parameter
    if drop_prior:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        df_ticker = df_ticker[df_ticker['Date'] < yesterday.strftime("%Y-%m-%d")]

    # Time-of-day checks (Eastern Time)
    est = pytz.timezone('US/Eastern')
    current_time_est = datetime.now(est)
    market_open = True
    pre_market   = current_time_est.hour < 9 or (current_time_est.hour == 9 and current_time_est.minute < 30)
    after_market = current_time_est.hour > 16 or (current_time_est.hour == 16 and current_time_est.minute > 0)

    # Day-of-week check  (weekday(): 0 = Monday … 6 = Sunday)
    is_it_the_weekend = current_time_est.weekday() >= 5          # 5 = Saturday, 6 = Sunday

    # Final flag
    if pre_market or after_market or is_it_the_weekend:
        market_open = False
    
    # Loop until data is retrived
    retry_counter = 0
    while True:
                        
        if market_open:
            # Filter out rows where 'Date' is today's date            
            df_ticker.drop(df_ticker[df_ticker['Date'] == datetime.today().date().strftime("%Y-%m-%d")].index, inplace=True)

        # Set period2 to the previous market open date, and period1 to 10 years prior
        period1 = datetime.today() - relativedelta(years=10)            
        period1 = int(datetime(period1.year, period1.month, period1.day).timestamp())
        period2 = datetime.today()
        
        # Go back days until period2 isn't a weekend
        while is_weekend(period2):
            period2 = period2 - timedelta(days=1)        
        period2_timestamp = pd.Timestamp(period2.date())  # Convert period2.date() to Pandas Timestamp

        # Check if we have period2 date already in dataframe
        matching_rows = df_ticker[df_ticker['Date'] == period2_timestamp]
        if os.path.exists(ticker_file_path) and len(matching_rows) > 0:
            write_line(f'Data for {ticker} loaded from file')
            break
        else:
            try:
                # Download yahoo price data
                black_path = str(pl.COMMON_DIR / 'blacklist.csv')
                # white_path = str(pl.COMMON_DIR / 'whitelist.csv')
                period2 = int(datetime(period2.year, period2.month, period2.day).timestamp())
                url = f'https://query1.finance.yahoo.com/v7/finance/download/{ticker.replace(".", "-")}?period1={period1}&period2=99999999999&interval=1d&events=history'
                download_path = asyncio.gather(pl.download_yahoo_price_data_async(page, url)) #pl.get_yahoo_price_data(page, ticker, period1, period2)
                
                # Call the function to download, move, and read the CSV
                path = download_path
                # is_valid, df_blacklist, df_whitelist = check_black_white_list(black_path, white_path, ticker)
                # if is_valid is None or is_valid:
                if os.path.exists(download_path):
    
                        df_response = pd.read_csv(path)
                        delete_files_with_string('C:/Users/rdpro/Downloads', ticker, 'csv')
                        if "Adj Close" in df_response.columns:
                            df_response = df_response.drop('Adj Close', axis=1)
                        df_response['Date'] = pd.to_datetime(df_response['Date'])
                        df_response['Symbol'] = ticker

                        df_ticker = pd.concat([df_ticker, df_response], ignore_index=True)
                        
                        # Sort dataframe by Date, Symbol, and then by Volume in descending order
                        df_ticker = df_ticker.sort_values(by=['Date', 'Symbol', 'Volume'], ascending=[True, True, False])

                        # Drop duplicate combinations of Date and Symbol, keeping the first entry (which has the highest volume)
                        df_ticker = df_ticker.drop_duplicates(subset=['Date', 'Symbol'], keep='first')
                        
                        df_ticker['index'] = range(0, len(df_ticker))       
                        df_ticker['Symbol'] = ticker                        
                        df_ticker = df_ticker.astype({
                            'Open': float,
                            'High': float,
                            'Low': float,
                            'Close': float,
                            'Volume': float,
                        })
                        if df_ticker['Date'].dtype != 'datetime64[ns]':
                            df_ticker['Date'] = pd.to_datetime(df_ticker['Date'])
                        # List of columns to limit decimal places
                        columns_to_limit = ['Open', 'High', 'Low', 'Close']

                        # Limit decimal places for specified columns
                        for col in columns_to_limit:
                            df_ticker[col] = df_ticker[col].round(2).astype(float)
                            
                        columns_to_drop = ['index', 'Beta (5Y Monthly)', 'PE Ratio (TTM)', '1y Target Est', 'EPS (TTM)', 'Earnings Date', 'Forward Dividend & Yield', 'Market Cap']
                        df_ticker = df_ticker.drop(columns=[col for col in columns_to_drop if col in df_ticker.columns])
        
                        store_csv(df_ticker, ticker_file_path)
                        # last_date = df_ticker['Date'].max()
                        # quote_table = si.get_quote_table(ticker, dict_result=False)
                        # selected_keys = ['1y Target Est', 'Market Cap', 'PE Ratio (TTM)', 'Earnings Date', 'Forward Dividend & Yield', 'Beta (5Y Monthly)', 'EPS (TTM)']
                        # #selected_data = {key: quote_table[key] for key in selected_keys if key in quote_table}
                        # for idx, row in quote_table.iterrows():
                        #     key = row["attribute"]
                        #     value = row["value"]
                        #     if key in selected_keys:                    
                        #         df_ticker.loc[df_ticker['Date'] == last_date, key] = value
                            
                        # store_csv(df_ticker, ticker_file_path)
                        break
 
                else:
                    add_line_to_file(black_path, ticker)
                    return
                
            except Exception as e:
                e_str = str(e).lower()
                retry_counter += 1
                if retry_counter >= 5 or 'ailed to download after 3 attempts' in str(e):
                    add_line_to_file(black_path, ticker)
                    break
                elif '404' in e_str or 'list index out of range' in e_str:
                    write_line(f'Skipping {ticker} because no data was found')
                    break
                elif '401' in e_str:
                    write_line(f'ERROR: {ticker} - Unauthorized.')
                    go_to_sleep(30, 60)
                elif '429' in e_str:
                    write_line(f'Sleeping due to excessive requests for {ticker}')
                    go_to_sleep(30, 60)
                elif 'remote' in e_str or 'failed' in e_str or 'http' in e_str:
                    write_line(f'ERROR: {ticker} - {e}')
                    go_to_sleep(15, 30)
                elif 'system cannot find the file specified:' in e_str:
                    write_line(f'ERROR: File not found. {ticker} - {e}')
                    go_to_sleep(15, 30)
                else:
                    write_line(f'Uknown error: {ticker} - {e}')
                    go_to_sleep(30, 60)
    
    return df_ticker.reset_index(drop=True), ticker_file_path    