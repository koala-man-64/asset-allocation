import subprocess
import os
import os.path as path
import warnings
import pandas as pd
import sys
import numpy as np
import datetime
warnings.filterwarnings('ignore')

"""
    Order of execution
        1. G:/My Drive/Python/AAA_500/aaa_500_main.py
        2. G:/My Drive/Python/SchwabCrawler/yahoo_crawler.py
        3. G:/My Drive/Python/FeatureImportance/pca_analysis_code.py
        4. G:/My Drive/Python/SchwabCrawler/fundamentals_analyzer.py
        5. G:/My Drive/Python/SchwabCrawler/asset_allocation_analysis.py

"""

def write_line(msg):
    '''
    Print a line to the console w/ a timestamp
    Parameters:
        str:
    '''
    sys.stdout.write('\r' + ' ' * 120 + '\r')
    sys.stdout.flush()
    ct = datetime.datetime.now()
    ct = ct.strftime('%Y-%m-%d %H:%M:%S')
    print('{}: {}'.format(ct, msg))


# Resolve duplicate columns


# Function to merge two DataFrames with forward filling








# Paths to the Python scripts you want to execute
paths_to_scripts = [
    'G:/My Drive/Python/AAA_500/aaa_500_main.py', # gathers price data; G:\My Drive\Python\AAA_500\Data\df_combined.csv
    'G:/My Drive/Python/SchwabCrawler/yahoo_crawler.py', # gathers financial data; G:\My Drive\Python\SchwabCrawler\Data\df_analysis_results_ranked.csv
    'G:\My Drive\Python\PriceTargetAnalysis\price_target_exploration.py', # retrieves price target data; G:\My Drive\Python\PriceTargetAnalysis\Data\df_price_targets.csv
    'G:\My Drive\Python\SchwabCrawler\earnings_scraper.py', # retrieves earnings data; G:\My Drive\Python\EarningsScraper\Data\df_earnings.csv    
    'G:/My Drive/Python/SchwabCrawler/fundamentals_analyzer.py', # performs technical analysis on financial data; G:\My Drive\Python\AssetAllocation\Data\df_analysis_results.csv
    # 'G:/My Drive/Python/FeatureImportance/pca_analysis_code.py', # performs technical analysis on price data; G:\My Drive\Python\FeatureImportance\Data\df_with_indicators.csv
    # 'G:/My Drive/Python/SchwabCrawler/asset_allocation_analysis.py' # aggregate price and financial data, rank and write to csv as single dataframe for ingestion by power bi
]

# Store the original working directoryb
original_cwd = os.getcwd()

# Iterate through each script path
for script_path in paths_to_scripts:
    # Get the directory and name of the script
    script_dir = path.dirname(script_path)
    script_name = path.basename(script_path)

    approval = input(f'Process {script_name} ([y]|n)?')

    if approval == 'n':
        continue

    # Change the current working directory to the script's directory
    os.chdir(script_dir)
    write_line(f"Executing {script_name}")
    try:
        # Execute the script
        result = subprocess.run(['python', script_path])#, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        write_line(f"Execution of {script_name} succeeded, output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        write_line(f"Execution of {script_name} failed: {e}")

    # Change back to the original working directory
    os.chdir(original_cwd)
