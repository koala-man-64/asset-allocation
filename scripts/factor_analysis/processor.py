"""
Feature Processor for PCA Analysis.
Handles DataFrame transformations, feature engineering, and parallel execution.
"""
import os
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from scripts.factor_analysis import indicators
from scripts.common.core import write_line

def process_symbol_df(symbol_df):
    symbol = symbol_df['Symbol'].iloc[0] if not symbol_df.empty else "Unknown"
    
    if len(symbol_df) == 0:
        return pd.DataFrame()

    try:
        # Indicator calculations using the separated module
        symbol_df['MACD_Signal_Line'] = indicators.calculate_macd(symbol_df)
        symbol_df['Standardized_MACD'] = indicators.standardize_macd(symbol_df)
        symbol_df = indicators.calculate_bollinger_bands(symbol_df, period=36)
        symbol_df['OBV'] = indicators.calculate_obv(symbol_df)
        symbol_df['ADL'] = indicators.calculate_adl(symbol_df)
        symbol_df['RSI'] = indicators.calculate_rsi(symbol_df)
        symbol_df = indicators.calculate_stochastic_oscillator(symbol_df, period=36)
        symbol_df['CCI'] = indicators.calculate_cci(symbol_df)
        
        # Safe division
        denom = (symbol_df['Stochastic_High'] - symbol_df['Stochastic_Low'])
        diff_boll = symbol_df['Bollinger_High'] - symbol_df['Bollinger_Low']
        symbol_df['Boll_Stoch_Diff'] = np.where(denom != 0, diff_boll / denom, 0)
        symbol_df['Indicator_Diff'] = symbol_df['Stochastic_Oscillator'] - symbol_df['Boll_Stoch_Diff']

        no_of_days = 30
        symbol_df[f'30_Days_Forward_Return'] = (symbol_df['Close'].shift(-no_of_days) - symbol_df['Close']) / symbol_df['Close'] * 100
        symbol_df['Daily_Return'] = symbol_df['Close'].pct_change()
        symbol_df['Return_Volatility'] = symbol_df['Daily_Return'].rolling(window=no_of_days).std()

        symbol_df = indicators.ichimoku_score(symbol_df)
        
        symbol_df['Typical_Price'] = (symbol_df['High'] + symbol_df['Low'] + symbol_df['Close']) / 3
        symbol_df['TP_Volume'] = symbol_df['Typical_Price'] * symbol_df['Volume']
        run_vol = symbol_df['Volume'].rolling(window=20).sum()
        run_tp_vol = symbol_df['TP_Volume'].rolling(window=20).sum()
        symbol_df['VWAP'] = np.where(run_vol != 0, run_tp_vol / run_vol, 0)

        # Drop excessive columns
        symbol_df.replace([np.inf, -np.inf], pd.NA, inplace=True)

    except Exception as e:
        write_line(f"Error processing symbol {symbol}: {e}")
        return pd.DataFrame()

    return symbol_df

def add_technical_indicators(df):
    """
    Applies indicator processing to the full dataframe using multiprocessing.
    """
    df_list = [df_group for _, df_group in df.groupby("Symbol")]
    num_cpus = max(1, os.cpu_count() - 1)
    
    results = []
    with ProcessPoolExecutor(max_workers=num_cpus) as executor:
        for res in executor.map(process_symbol_df, df_list):
            results.append(res)
    
    if results:
        return pd.concat(results, ignore_index=True)
    else:
        return pd.DataFrame()


