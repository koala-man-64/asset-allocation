"""
Technical Indicator calculations for Feature Importance module.
Contains stateless, pure functions for computing market indicators.
"""
import numpy as np
import pandas as pd



def calculate_macd(dataframe, short_period=12, long_period=26, signal_period=9):
    short_ema = dataframe['Close'].ewm(span=short_period, adjust=False).mean()
    long_ema = dataframe['Close'].ewm(span=long_period, adjust=False).mean()
    dataframe['MACD'] = short_ema - long_ema
    dataframe['Signal_Line'] = dataframe['MACD'].ewm(span=signal_period, adjust=False).mean()
    return dataframe['Signal_Line']

def calculate_bollinger_bands(dataframe, period=20, num_of_std=2):
    rolling_mean = dataframe['Close'].rolling(window=period).mean()
    rolling_std = dataframe['Close'].rolling(window=period).std()
    dataframe['Bollinger_High'] = rolling_mean + (rolling_std * num_of_std)
    dataframe['Bollinger_Low'] = rolling_mean - (rolling_std * num_of_std)
    # Avoid division by zero
    diff = dataframe['Bollinger_High'] - dataframe['Bollinger_Low']
    dataframe['Bollinger_Close'] = np.where(diff != 0, (dataframe['Close'] - dataframe['Bollinger_Low']) / diff, 0)
    dataframe['Bollinger_Range'] = np.where(dataframe['Close'] != 0, diff / dataframe['Close'], 0)
    return dataframe

def calculate_obv(dataframe):
    df = dataframe.copy()
    df['Daily_Return'] = df['Close'].diff()
    df['Volume_Direction'] = np.where(df['Daily_Return'] > 0, df['Volume'], -df['Volume'])
    df['OBV'] = df['Volume_Direction'].rolling(window=26, min_periods=1).sum()
    df['Avg_Volume'] = df['Volume'].rolling(window=26, min_periods=1).mean()
    df['OBV_Adjusted'] = np.where(df['Avg_Volume'] != 0, df['OBV'] / df['Avg_Volume'], 0)
    return df['OBV_Adjusted']

def calculate_adl(dataframe):
    df = dataframe.copy()
    high_low_range = df['High'] - df['Low']
    high_low_range.replace(0, 1, inplace=True)
    mfm = ((df['Close'] - df['Low']) - (df['High'] - df['Close'])) / high_low_range
    avg_volume = df['Volume'].rolling(window=26, min_periods=1).mean()
    
    # Check for zero avg_volume to avoid division by zero
    normalized_volume = np.where(avg_volume != 0, df['Volume'] / avg_volume, 0)
    mfv = mfm * normalized_volume
    df['ADL'] = mfv.cumsum()
    return df['ADL']

def calculate_rsi(dataframe, period=14):
    delta = dataframe['Close'].pct_change()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    dataframe['RSI'] = 100 - (100 / (1 + rs))
    return dataframe['RSI']

def calculate_stochastic_oscillator(dataframe, period=20):
    low_min = dataframe['Low'].rolling(window=period).min()
    high_max = dataframe['High'].rolling(window=period).max()
    
    diff = high_max - low_min
    dataframe['Stochastic_High'] = dataframe['High'].rolling(window=period).max()
    dataframe['Stochastic_Low'] = dataframe['Low'].rolling(window=period).min()
    
    dataframe['Stochastic_Oscillator'] = np.where(diff != 0, (dataframe['Close'] - low_min) / diff, 0)
    dataframe['Stochastic_Oscillator_Range'] = np.where(dataframe['Close'] != 0, diff / dataframe['Close'], 0)
    return dataframe

def calculate_cci(dataframe, period=20):
    # Log transform inputs safely (avoid log(<=0))
    # For robust code, ensure positive values or handle NaNs
    df_log = dataframe[['High', 'Low', 'Close']].copy()
    for col in df_log.columns:
        df_log[col] = np.log(df_log[col].replace(0, np.nan))
    
    TP = (df_log['High'] + df_log['Low'] + df_log['Close']) / 3
    TP_rolling_mean = TP.rolling(window=period).mean()
    TP_rolling_std = TP.rolling(window=period).std()
    
    CCI = (TP - TP_rolling_mean) / (0.015 * TP_rolling_std)
    return CCI

def standardize_macd(dataframe):
    macd = dataframe['MACD']
    macd_mean = macd.rolling(window=26).mean()
    macd_std = macd.rolling(window=26).std()
    dataframe['Standardized_MACD'] = (macd - macd_mean) / macd_std
    return dataframe['Standardized_MACD']

def ichimoku_score(df, tenkan_len=9, kijun_len=26, senkou_span_b_len=52, displacement=26):
    df = df.copy()
    df['tenkan'] = (df['High'].rolling(window=tenkan_len).max() + df['Low'].rolling(window=tenkan_len).min()) / 2.0
    df['kijun'] = (df['High'].rolling(window=kijun_len).max() + df['Low'].rolling(window=kijun_len).min()) / 2.0
    
    df['spanA'] = ((df['tenkan'] + df['kijun']) / 2.0).shift(displacement)
    
    roll_high_b = df['High'].rolling(window=senkou_span_b_len).max()
    roll_low_b = df['Low'].rolling(window=senkou_span_b_len).min()
    df['spanB'] = ((roll_high_b + roll_low_b) / 2.0).shift(displacement)
    
    df['chikou'] = df['Close'].shift(-displacement)
    
    df['Ichimoku Score'] = 0
    # Calculations
    df.loc[df['tenkan'] > df['kijun'], 'Ichimoku Score'] += 1
    df.loc[df['tenkan'] < df['kijun'], 'Ichimoku Score'] -= 1
    
    # Cloud check
    cloud_max = df[['spanA', 'spanB']].max(axis=1)
    cloud_min = df[['spanA', 'spanB']].min(axis=1)
    df.loc[df['Close'] > cloud_max, 'Ichimoku Score'] += 2
    df.loc[df['Close'] < cloud_min, 'Ichimoku Score'] -= 2
    
    df.loc[df['spanA'] > df['spanB'], 'Ichimoku Score'] += 1
    df.loc[df['spanA'] < df['spanB'], 'Ichimoku Score'] -= 1
    
    df.loc[df['Close'] > df['chikou'], 'Ichimoku Score'] += 1
    df.loc[df['Close'] < df['chikou'], 'Ichimoku Score'] -= 1
    
    return df
