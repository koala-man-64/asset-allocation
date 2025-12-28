import pandas as pd
import numpy as np
import ta

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
    range_val = df['High'] - df['Low']
    range_val.replace(0, 1, inplace=True)  # Replace zeros in the range to avoid division by zero
    mfm = ((df['Close'] - df['Low']) - (df['High'] - df['Close'])) / range_val

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

def is_greater_than_past_n_days(df, n=5, percentage_increase=0.0):
    """
    Check if the latest close is greater than the average of the past n closes by a certain percentage.
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
    """
    if len(df) < n + 1:  # +1 to include the latest day
        raise ValueError(f"Dataframe has fewer than {n + 1} rows.")

    # Get the latest close
    latest_close = df.iloc[-1]['Low']

    # Calculate the thresholds for each of the past n days
    thresholds = df.iloc[-(n + 1):-1]['Close'] * (1 - percentage_increase)

    # Check if the latest close is greater than all of the thresholds
    return (latest_close < thresholds).all()

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
    """
    Adds the weighted returns and variance for a provided list of symbols and weights
    """
    import copy
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

def is_weekend(date):
    return date.weekday() >= 5

def calculate_percent_change(df, start_date, end_date):
    start_open = df.loc[df['Date'] == start_date, 'Open'].values[0]
    end_close = df.loc[df['Date'] == end_date, 'Close'].values[0]

    percent_change = (end_close - start_open) / start_open

    return percent_change.round(4).astype(float)

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

def has_anomaly(df, target_cols = [], threshold=3):
    """
    Returns a tuple containing a boolean indicating if there are any anomalies
    and a DataFrame of rows where anomalies occur based on the z-score method
    for numeric columns in the dataset.
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
