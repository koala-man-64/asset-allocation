# Import necessary libraries
import pandas as pd
import crawler_lib as cl
from datetime import datetime, timedelta
import os
import numpy as np
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import cpu_count
sys.path.insert(1, 'G:/My Drive/Python/AAA_500')
import aaa_500_lib as alb
from itertools import product, repeat
import dask.dataframe as dd
from datetime import date

def has_date_named_column(df) -> bool:
    """
    Returns True if any column name in df can be parsed as a date.
    """
    # Try parsing each column name; unparseable names become NaT
    parsed = pd.to_datetime(df.columns, errors='coerce', infer_datetime_format=True)
    # If any parsed stamp is not NaT, at least one column name is date‐like
    return parsed.notna().any()

# Function to fetch financial data (Replace this with your actual fetching logic)
def fetch_financial_data(symbol, columns_to_keep):
    """
    Fetch financial data for a given stock symbol.
    """
    # Your logic to fetch valuation, income statement, balance sheet, and cash flow data
    valuation_path = f'G:\My Drive\Python\Common\Yahoo\Valuation\{symbol}_quarterly_valuation_measures.csv'
    income_statement_path = f'G:\My Drive\Python\Common\Yahoo\Income Statement\{symbol}_quarterly_financials.csv'
    balance_sheet_path = f'G:\My Drive\Python\Common\Yahoo\Balance Sheet\{symbol}_quarterly_balance-sheet.csv'
    cash_flow_path = f'G:\My Drive\Python\Common\Yahoo\Cash Flow\{symbol}_quarterly_cash-flow.csv'
    
    valuation_df, income_statement_df, balance_sheet_df, cash_flow_df = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    if os.path.exists(valuation_path):
        valuation_df = pd.read_csv(valuation_path, header=0)
        valuation_df.columns = valuation_df.columns.str.strip().str.lower()
        if has_date_named_column(valuation_df):
            transpose_yahoo_dataframe(valuation_df)
        # valuation_df = combine_clean_and_ffill(pd.read_csv(valuation_path, header=0), key_cols=['Symbol','Date'])
        # valuation_df = transpose_financial_data(, symbol)
    if os.path.exists(income_statement_path):
        income_statement_df = pd.read_csv(income_statement_path, header=0)
        income_statement_df.columns = income_statement_df.columns.str.strip().str.lower()
        #income_statement_df = transpose_financial_data(pd.read_csv(income_statement_path, header=0), symbol)
    if os.path.exists(balance_sheet_path):
        balance_sheet_df = pd.read_csv(balance_sheet_path, header=0)
        balance_sheet_df.columns = balance_sheet_df.columns.str.strip().str.lower()
        #balance_sheet_df = transpose_financial_data(pd.read_csv(balance_sheet_path, header=0), symbol)
    if os.path.exists(cash_flow_path):
        cash_flow_df = pd.read_csv(cash_flow_path, header=0)
        cash_flow_df.columns = cash_flow_df.columns.str.strip().str.lower()
        #cash_flow_df = transpose_financial_data(pd.read_csv(cash_flow_path, header=0), symbol)
        
    return valuation_df, income_statement_df, balance_sheet_df, cash_flow_df

def parallel_apply(df_chunk):
    return df_chunk.apply(roll_to_closest_quarter_end, axis=1, args=(df_chunk,))

def determine_periodicity(df, date_col):
    # Calculate differences between consecutive dates
    date_diff_col = date_col+'_diff'
    df[date_diff_col] = df[date_col].diff().dt.days

    # Infer frequency
    if df[date_diff_col].nunique() == 1 and df[date_diff_col].iloc[1] == 1:
        frequency = 'Daily'
    elif df[date_diff_col].median() >= 28 and df[date_diff_col].median() <= 31:
        frequency = 'Monthly'
    elif df[date_diff_col].median() > 80 and df[date_diff_col].median() < 100:
        frequency = 'Quarterly'
    elif df[date_diff_col].median() > 360:
        frequency = 'Annual'
    else:
        frequency = 'Unknown'
    return frequency

# Function to process each stock symbol
def process_stock(symbol):
    
    while True:
        try:
            cl.write_line(f'Processing {symbol}')
            
            # validate symbol
            if symbol == 'AAM':
                d = ''
            
            columns = ['Symbol', 'Date', 'Market Cap', 'Enterprise Value', 'P/E Ratio', 'Forward P/E', 'Free Cash Flow', 
                    'Operating Cash Flow', 'Gross Profit', 'Operating Expense', 'Net Income', 'Total Revenue', 'Cash from Operations', 'Total Assets']
            # Fetch financial data
            valuation_df, income_statement_df, balance_sheet_df, cash_flow_df = fetch_financial_data(symbol, columns)
            if len(income_statement_df) == 0:
                return pd.DataFrame()
            
            # Add new calculated columns
            # Net Income from Last Year
            # Step 1: Convert 'date' to datetime if it's not already
            # income_statement_df['date'] = pd.to_datetime(income_statement_df['date'])
            # balance_sheet_df['date'] = pd.to_datetime(balance_sheet_df['date'])

            # Step 2: Sort the DataFrame by 'symbol' and 'date' to ensure correct group shifting
            income_statement_df.sort_values(by=['symbol', 'date'], inplace=True)
            balance_sheet_df.sort_values(by=['symbol', 'date'], inplace=True)

            # Step 3: Group by 'symbol' and shift the 'netincome' column within each group
            period = 4
            if 'netincome' in income_statement_df.columns:
                income_statement_df['netincome_last_year'] = (
                    income_statement_df
                    .groupby('symbol')['netincome']
                    .shift(period)
                    .fillna(0)
                )
                
            # Gross Margin
            costofrevenue_col = ''
            if 'costofrevenue' in income_statement_df.columns:
                costofrevenue_col = 'costofrevenue'
            elif 'noninterestexpense' in income_statement_df.columns:
                costofrevenue_col = 'noninterestexpense'
            elif 'operatingexpense' in income_statement_df.columns:
                costofrevenue_col = 'operatingexpense'
            elif 'totalexpenses' in income_statement_df.columns:
                costofrevenue_col = 'totalexpenses'
            elif 'expense' in income_statement_df.columns or 'cost' in income_statement_df.columns:
                costofrevenue_col = ''
            else:
                if symbol not in ['NMFC', 'PFLT', 'ARCC', 'PNNT', 'FDUS', 'PGP', 'SABA', 'SCM', 'PSEC', 'GAIN', 'GBDC', 'HRZN', 'SVVC', 'WHF', 'HASI', 'MAIN']:
                    costofrevenue_col = ''
                
            income_statement_df['grossmargin'] = 0
            income_statement_df['grossmargin_last_year'] = 0
            if costofrevenue_col:
                # compute the ratio
                income_statement_df['grossmargin'] = (
                    income_statement_df['totalrevenue'] 
                    / income_statement_df[costofrevenue_col]
                )
                # shift it by `period`, filling any new gaps with 0
                income_statement_df['grossmargin_last_year'] = (
                    income_statement_df
                    .groupby('symbol')['grossmargin']
                    .shift(period)
                    .fillna(0)
                )
                
            # 'Long-term Debt' / 'Total Assets' 
            # periodicity = determine_periodicity(balance_sheet_df, 'date')
            period = 4
            
            # 1) Figure out which debt column to use (or None if neither exists)
            if 'longtermdebt' in balance_sheet_df.columns:
                debt_col = 'longtermdebt'
            elif 'totaldebt' in balance_sheet_df.columns:
                debt_col = 'totaldebt'
            else:
                debt_col = None

            # 2) Only compute & attach ROA/LTDRatio if we actually have a debt column
            if debt_col:
                # ROA
                balance_sheet_df['roa'] = (
                    balance_sheet_df[debt_col] 
                    / balance_sheet_df['totalassets']
                )
                balance_sheet_df['roa_last_year'] = (
                    balance_sheet_df
                    .groupby('symbol')['roa']
                    .shift(period)
                ).fillna(0)

                # LTD Ratio (guarding against divide-by-zero)
                ratio = balance_sheet_df[debt_col] / balance_sheet_df['totaldebt']
                balance_sheet_df['ltdratio'] = (
                    ratio.replace([np.inf, -np.inf], np.nan)
                        .fillna(0)
                )
                balance_sheet_df['ltdratio_last_year'] = (
                    balance_sheet_df
                    .groupby('symbol')['ltdratio']
                    .shift(period)
                ).fillna(0)


            # Current Ratio
            currentasset_col = ''
            if 'currentassets' in balance_sheet_df.columns:
                currentasset_col = 'currentassets'
            elif 'totalassets' in balance_sheet_df.columns:
                currentasset_col = 'totalassets'
            else:
                if symbol not in []:
                    currentasset_col = ''
            # 1) Determine which liability column to use (or leave empty)
            currentliability_col = ''
            if 'currentliabilities' in balance_sheet_df.columns:
                currentliability_col = 'currentliabilities'
            elif 'totalliabilities' in balance_sheet_df.columns:
                currentliability_col = 'totalliabilities'
            elif 'totalliabilitiesnetminorityinterest' in balance_sheet_df.columns:
                currentliability_col = 'totalliabilitiesnetminorityinterest'
            # else: leave currentliability_col == ''

            # 2) Only compute current ratio & its “last year” if we have both sides of the ratio
            if currentasset_col and currentliability_col:
                balance_sheet_df['currentratio'] = (
                    balance_sheet_df[currentasset_col]
                    / balance_sheet_df[currentliability_col]
                )
                balance_sheet_df['currentratio_last_year'] = (
                    balance_sheet_df
                    .groupby('symbol')['currentratio']
                    .shift(period)
                    .fillna(0)
                )
            # if either asset or liability col is missing, we skip adding currentratio entirely

            # 3) Compute average total assets only if totalassets exists
            if 'totalassets' in balance_sheet_df.columns:
                balance_sheet_df['average_total_assets'] = (
                    balance_sheet_df.groupby('symbol')['totalassets']
                                    .shift(1)
                    + balance_sheet_df['totalassets']
                ) / 2

                # 4) Only compute asset turnover if we can pull in totalrevenue
                if 'totalrevenue' in income_statement_df.columns:
                    # merge in the revenue column (left‐join preserves all balance‐sheet rows)
                    balance_sheet_df = pd.merge(
                        balance_sheet_df,
                        income_statement_df[['symbol', 'date', 'totalrevenue']],
                        on=['symbol', 'date'],
                        how='left'
                    )
                    balance_sheet_df['asset_turnover_ratio'] = (
                        balance_sheet_df['totalrevenue']
                        / balance_sheet_df['average_total_assets']
                    )
                    balance_sheet_df['asset_turnover_ratio_last_year'] = (
                        balance_sheet_df
                        .groupby('symbol')['asset_turnover_ratio']
                        .shift(period)
                        .fillna(0)
                    )
            
            # Ensure the 'date' column in all DataFrames is in datetime format
            balance_sheet_df['date'] = pd.to_datetime(balance_sheet_df['date'])
            income_statement_df['date'] = pd.to_datetime(income_statement_df['date'])
            cash_flow_df['date'] = pd.to_datetime(cash_flow_df['date'])

            # First merge: df1 and df2
            merged_df_12 = pd.merge(balance_sheet_df, income_statement_df, on=['date', 'symbol'], how='inner')

            # Second merge: merge the result with df3
            final_merged_df = pd.merge(merged_df_12, cash_flow_df, on=['date', 'symbol'], how='inner')
                
            
            # # Define the direction for each metric: 1 for higher is better, -1 for lower is better
            # directions = {
            #     'Market Cap': 1,
            #     'Enterprise Value': 1,
            #     'P/E Ratio': -1,
            #     'Forward P/E': -1,
            #     'Operating Cash Flow': 1,
            #     'Gross Profit': 1,
            #     'Operating Expense': -1,
            #     'Net Income': 1,
            #     'Total Revenue': 1,
            #     'Free Cash Flow': 1,
            #     'Cash from Operations': 1
            # }
            # # Extract the required columns and apply directions
            # metrics_df = pd.DataFrame({
            #     'Date': income_statement_df['date'] if 'date' in income_statement_df.columns else np.nan,
            #     'Symbol': symbol,
            #     'Market Cap': valuation_df['marketcap'] * directions['Market Cap'] if 'marketcap' in valuation_df.columns else np.nan,
            #     'Enterprise Value': valuation_df['enterprisevalue'] * directions['Enterprise Value'] if 'enterprisevalue' in valuation_df.columns else np.nan,
            #     'P/E Ratio': valuation_df['peratio'] * directions['P/E Ratio'] if 'peratio' in valuation_df.columns else np.nan,
            #     'Forward P/E': valuation_df['forwardperatio'] * directions['Forward P/E'] if 'forwardperatio' in valuation_df.columns else np.nan,
            #     'Operating Cash Flow': cash_flow_df['operatingcashflow'] * directions['Operating Cash Flow'] if 'operatingcashflow' in cash_flow_df.columns else np.nan,
            #     'Gross Profit': income_statement_df['grossprofit'] * directions['Gross Profit'] if 'grossprofit' in income_statement_df.columns else np.nan,
            #     'Operating Expense': income_statement_df['operatingexpense'] * directions['Operating Expense'] if 'operatingexpense' in income_statement_df.columns else np.nan,
            #     'Net Income': income_statement_df['netincome'] * directions['Net Income'] if 'netincome' in income_statement_df.columns else np.nan,
            #     'Total Revenue': income_statement_df['totalrevenue'] * directions['Total Revenue'] if 'totalrevenue' in income_statement_df.columns else np.nan,
            #     'Free Cash Flow': cash_flow_df['freecashflow'] * directions['Free Cash Flow'] if 'freecashflow' in cash_flow_df.columns else np.nan,
            #     'Cash from Operations': cash_flow_df['cashflowfromcontinuingoperatingactivities'] * directions['Cash from Operations'] if 'cashflowfromcontinuingoperatingactivities' in cash_flow_df.columns else np.nan
            # })
            
            # Pitroski F-Score
            """
            PROFITABILITY SCORES:
            ---------------------
            1. Positive net income compared to last year (1 point):
            - A company earns 1 point if its net income is positive and higher than the previous year's net income.

            2. Positive operating cash flow in the current year (1 point):
            - A company earns 1 point if it has a positive operating cash flow in the current year.

            3. Higher return on assets (ROA) in the current period compared to the ROA in the previous year (1 point):
            - A company earns 1 point if its ROA in the current period is higher than the ROA in the previous year.

            4. Cash flow from operations greater than ROA (1 point):
            - A company earns 1 point if its cash flow from operations is greater than its ROA.

            BALANCE SHEET HEALTH SCORES:
            ----------------------------
            1. Lower ratio of long-term debt in the current period compared to the value in the previous year (1 point):
            - A company earns 1 point if its ratio of long-term debt in the current period is lower than in the previous year.

            2. Higher current ratio this year compared to the previous year (1 point):
            - A company earns 1 point if its current ratio this year is higher than in the previous year.

            3. No new shares were issued in the last year (1 point):
            - A company earns 1 point if no new shares were issued in the last year.

            OPERATING EFFICIENCY SCORES:
            -----------------------------
            1. A higher gross margin compared to the previous year (1 point):
            - A company earns 1 point if its gross margin in the current period is higher than in the previous year.

            2. A higher asset turnover ratio compared to the previous year (1 point):
            - A company earns 1 point if its asset turnover ratio in the current period is higher than in the previous year.
            """
            
            # start with the same index
            df_fa = pd.DataFrame(index=final_merged_df.index)

            # helper: return the column if present, otherwise a Series of fill values
            def safe(col, df=final_merged_df, fill=0):
                if col in df.columns:
                    return df[col]
                else:
                    return pd.Series(fill, index=df.index)

            # Date: if missing, default to NaT
            if 'date' in final_merged_df.columns:
                df_fa['Date'] = final_merged_df['date']
            else:
                df_fa['Date'] = pd.Series(pd.NaT, index=final_merged_df.index)

            df_fa['Symbol'] = symbol

            df_fa['NetIncome>LastYear'] = (
                (safe('netincome') > safe('netincome_last_year'))
                .astype(int)
            )

            df_fa['OperatingCashFlow>0'] = (
                (safe('operatingcashflow') > 0)
                .astype(int)
            )

            df_fa['ROA>LastYear'] = (
                (safe('roa') > safe('roa_last_year'))
                .astype(int)
            )

            df_fa['OperatingCashFlow>ROA'] = (
                (safe('operatingcashflow') > (safe('roa') * safe('totalassets')))
                .astype(int)
            )

            df_fa['LongTermDebtRatio<LastYear'] = (
                (safe('ltdratio') < safe('ltdratio_last_year'))
                .astype(int)
            )

            df_fa['CurrentRatio>LastYear'] = (
                (safe('currentratio') > safe('currentratio_last_year'))
                .astype(int)
            )

            df_fa['GrossMargin>LastYear'] = (
                (safe('grossmargin') > safe('grossmargin_last_year'))
                .astype(int)
            )

            df_fa['AssetTurnover>LastYear'] = (
                (safe('asset_turnover_ratio') > safe('asset_turnover_ratio_last_year'))
                .astype(int)
            )

            # market cap from valuation_df, defaulting to 0-series if missing
            if 'marketcap' in valuation_df.columns:
                df_fa['Market Cap'] = valuation_df['marketcap']
            else:
                df_fa['Market Cap'] = pd.Series(0, index=final_merged_df.index)

            # Sum them up
            df_fa['F-Score'] = (df_fa['NetIncome>LastYear'] +
                                    df_fa['OperatingCashFlow>0'] +
                                    df_fa['ROA>LastYear'] +
                                    df_fa['OperatingCashFlow>ROA'] +
                                    df_fa['LongTermDebtRatio<LastYear'] +
                                    df_fa['CurrentRatio>LastYear'] +
                                    df_fa['GrossMargin>LastYear'] +
                                    df_fa['AssetTurnover>LastYear'])
            
            df_fa = df_fa.dropna(subset=['Date'])
            # return df_fa[['Date', 'Symbol', 'F-Score', 'Market Cap']]
            return df_fa
        except Exception as e:
            cl.write_line(f'ERROR: {str(e)}')
            if "unsupported operand" in str(e):
                new_row = {"Ticker": symbol}
                cl.append_to_csv('G:\My Drive\Python\Common/blacklist_financial.csv', pd.DataFrame([new_row]))


def transpose_yahoo_dataframe(df: pd.DataFrame, ticker) -> None:
    """
    Read a CSV export from Yahoo Finance, transpose it, compute percent changes,
    and overwrite the original file with the transformed data.

    Parameters
    ----------
    filepath : str
        Path to the CSV file containing the Yahoo Finance table.
    **read_csv_kwargs :
        Additional kwargs to pass to pd.read_csv (e.g. sep, header, encoding).

    Returns
    -------
    None
    """
    # Set the 'name' column as the index
    df.set_index("name", inplace=True)

    # Drop rows where all elements are NaN
    df.dropna(how='all', inplace=True)

    # Drop 'ttm' column if present
    if 'ttm' in df.columns:
        df.drop(columns=['ttm'], inplace=True)

    # Remove commas and cast to float
    df = df.replace(',', '', regex=True).astype(float)

    # Replace any remaining NA/NaN with 0
    df.fillna(0, inplace=True)

    # Transpose so dates become the index
    df_transposed = df.T

    # Convert the index to datetime
    df_transposed.index = pd.to_datetime(df_transposed.index)

    # Compute percent change
    # df_transposed = df_transposed.pct_change() * 100
    df_transposed['symbol'] = ticker
    
def combine_clean_and_ffill(
    df: pd.DataFrame,
    key_cols: list[str] = None
) -> pd.DataFrame:
    """
    1) Coalesce any duplicate-named non-key columns by first non-null
    2) Strip commas/tabs, cast to float
    3) Sort ascending by Date (and Symbol if in key_cols)
    4) Forward-fill numeric cols per Symbol (or globally)
    5) Fill remaining NaNs with 0

    key_cols: columns to leave untouched (eg ['Symbol','Date'])
    """
    if key_cols is None:
        key_cols = []

    # 1. Identify duplicate names (excluding keys)
    dup_names = df.columns[df.columns.duplicated()].unique()
    dup_names = [n for n in dup_names if n not in key_cols]

    # 2. Coalesce & clean each duplicate group
    for name in dup_names:
        block = df[name]
        # coalesce left→right
        combined = block.bfill(axis=1).iloc[:, 0]
        # strip commas/tabs & cast
        no_commas = combined.astype(str).str.replace(r'[,\t]', '', regex=True)
        numeric = pd.to_numeric(no_commas, errors='coerce')
        # temporarily leave NaNs in place
        df[name] = numeric

    # 3. Drop extra columns, keeping first occurrence
    df = df.loc[:, ~df.columns.duplicated()]

    # 4. Ensure Date is datetime & sort
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        sort_keys = ['Symbol', 'Date'] if 'Symbol' in df.columns else ['Date']
        df = df.sort_values(sort_keys)

    # 5. Forward-fill numeric columns
    #    detect numeric cols as those not in key_cols
    num_cols = [c for c in df.columns if c not in key_cols]
    if 'Symbol' in df.columns:
        df[num_cols] = df.groupby('Symbol')[num_cols].ffill()
    else:
        df[num_cols] = df[num_cols].ffill()

    # 6. Finally fill any remaining NaNs with 0
    df[num_cols] = df[num_cols].fillna(0)

    return df

# Custom function to roll date to closest quarter-end
def roll_to_closest_quarter_end(row, df):
    
    # Create an instance of QuarterEnd
    quarter_end = pd.offsets.QuarterEnd()
    symbol = row['Symbol']
    date = row['Date']
    # Roll forward and backward to find closest quarter-end dates
    roll_forward_date = quarter_end.rollforward(date)
    roll_backward_date = quarter_end.rollback(date)
    
    # Check which one is closer
    closest_date = roll_forward_date if (roll_forward_date - date) <= (date - roll_backward_date) else roll_backward_date

    # Check for existing data for the rolled-back date and symbol
    if closest_date == roll_backward_date:
        exists = df[(df['Date'] == roll_backward_date) & (df['Symbol'] == symbol)].shape[0]
        if exists:
            closest_date = roll_forward_date  # If it already exists, roll forward instead
            
    # Update the row's date to the closest quarter-end date
    row['Date'] = closest_date
            
    return row

def check_and_insert(symbol_data, date_df):
    # Retrieve the unique symbol from symbol_data
    symbol = str(symbol_data['Symbol'].unique()[0])
    # Log message indicating that filling operation for financial data is about to start
    cl.write_line(f'FFill financial data for {symbol}...')
    # Merge symbol_data with date_df on 'Date' column using left join to maintain all rows of date_df
    merged_df = pd.merge(date_df, symbol_data, on='Date', how='left')
    # Identify columns to forward fill (ffill), excluding 'Date' and 'Symbol'
    cols_to_ffill = merged_df.columns.difference(['Date', 'Symbol'])

    # Check if there are any NaNs in the columns to fill
    if merged_df[cols_to_ffill].isnull().any().any():
        # If NaNs are present, use the latest values from symbol_data to fill them
        latest_values = symbol_data.iloc[-1][cols_to_ffill]
        for col in cols_to_ffill:
            merged_df[col] = merged_df[col].fillna(latest_values[col])

    # Ensure the 'Symbol' column in merged_df is set to the current symbol
    merged_df['Symbol'] = symbol
    # Return specific columns of merged_df including financial and market data
    return merged_df#[['Date', 'Symbol', 'FundamentalRank', 'OverallRank', 'Market Cap Category', 'Sector']]

def calculate_rank_for_symbol(symbol, date, df):
    """
    Calculate the TechnicalRank for a given symbol and date.
    """
    cl.write_line(f'Calculating rank for {symbol} - {date}')
    technical_rank = df[df['Symbol'] == symbol]['TechnicalRank'].iloc[0]
    return date, symbol, technical_rank

def update_rank_in_df(result_df, updates):
    """
    Update the result_df with the calculated TechnicalRank for each symbol.
    """
    for date, symbol, rank in updates:
        result_df.loc[(result_df['Date'] == date) & (result_df['Symbol'] == symbol), 'TechnicalRank'] = rank
    return result_df

# Function to categorize market cap
def categorize_market_cap(value):
    if value < 300e6:
        return 'Micro'
    elif 300e6 <= value < 2e9:
        return 'Small'
    elif 2e9 <= value < 10e9:
        return 'Mid'
    else:
        return 'Large'
    
def calculate_piotroski_score(row):
    score = 0
    score += 1 if row['Net Income'] > 0 else 0 # income_df
    score += 1 if row['ROA'] > 0 else 0 # 'Net Income' / 'Total Assets' --> infcome_df and balance_sheet_df
    score += 1 if row['Operating Cash Flow'] > 0 else 0 # cash_flow_df
    score += 1 if row['Operating Cash Flow'] > row['Net Income Last Year'] else 0 # cash_flow_df and income_df NOTE: need to add Net Income Last Year to income_df
    score += 1 if row['Long Term Debt to Assets'] < row['Long Term Debt to Assets Last Year'] else 0 # 'Long-term Debt' / 'Total Assets' # balance_sheet_df NOTE: need to add ratio, and ratio for last year
    score += 1 if row['Current Ratio'] > row['Current Ratio Last Year'] else 0 # 'Current Assets' / 'Current Liabilities'
    # score += 1 if row['New Shares Issued'] == 0 else 0
    score += 1 if row['Gross Margin'] > row['Gross Margin Last Year'] else 0 # Total Revenue / Cost of revenue
    score += 1 if row['Asset Turnover'] > row['Asset Turnover Last Year'] else 0 # Net Sales / Average Total Assets
    return score

if __name__ == '__main__':

    # load symbols
    blacklist_file = "G:\My Drive\Python\Common/blacklist_earnings.csv"
    df_symbols = alb.get_symbols()
    df_remove2 = pd.read_csv(blacklist_file)

    if len(df_remove2) > 0:
        # 3. Extract unique symbols from each “remove” DataFrame
        symbols_to_remove = set(df_remove2['Ticker'].unique())

        # 4. Filter your main df to exclude those symbols
        df_symbols = df_symbols[~df_symbols['Symbol'].isin(symbols_to_remove)]

        # 5. (Optional) Reset the index if you like a clean 0…N index
        df_symbols = df_symbols.reset_index(drop=True)  
    ma_tickers = pd.read_csv('G:\My Drive\Python\Common/market_analysis_tickers.csv')
    symbols = df_symbols[~df_symbols['Symbol'].isin(ma_tickers['Symbol'])]['Symbol'].to_list()
    
    # For debugging
    # debug_symbols = df_symbols.sample(n=5)['Symbol'].to_list()
    # df_symbols = df_symbols[df_symbols['Symbol'].isin(debug_symbols)]
    # symbols = df_symbols[~df_symbols['Symbol'].isin(ma_tickers['Symbol'])].sample(n=5)['Symbol'].to_list()

    # Init objects for overall analysis
    df_analysis_results = pd.DataFrame()
    final_ranks_df = pd.DataFrame()
    fundamental_analysis_results = []
          
    # Perform fundamental analysis on n_cores 
    n_cores = int(cpu_count() * .75)
    with ProcessPoolExecutor(max_workers=n_cores) as executor:
        fundamental_analysis_results = list(executor.map(process_stock, symbols))

    # Concatenate all the resulting DataFrames
    df_analysis_results = pd.concat(fundamental_analysis_results, ignore_index=True)

    # Convert 'Date' to datetime format if it's not already
    df_analysis_results['Date'] = pd.to_datetime(df_analysis_results['Date'])

    # Split DataFrame into chunks
    num_splits = n_cores  # Number of splits depends on your number of cores
    df_analysis_results_split = np.array_split(df_analysis_results, num_splits)

    # Adjust dates so they all row forward to the nearest quarter
    with ProcessPoolExecutor(max_workers=n_cores) as executor:
        result_chunks = list(executor.map(parallel_apply, df_analysis_results_split))
        
    # Now, combine the chunks back together
    df_analysis_results = pd.concat(result_chunks)
    
    # Fix index, add market cap and sector, store df_analysis_results to csv
    df_analysis_results.reset_index(drop=True, inplace=True)
    df_analysis_results['Market Cap Category'] = df_analysis_results['Market Cap'].apply(categorize_market_cap)
    df_analysis_results.drop(columns=['Market Cap'], inplace=True)
    df_analysis_results = pd.merge(df_analysis_results, df_symbols[['Symbol', 'Sector']], on='Symbol', how='left')
    df_analysis_results.to_csv('G:\My Drive\Python\Common/df_financial_analysis_results.csv', index=False)