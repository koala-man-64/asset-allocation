import sys
import os
import asyncio
import warnings
import datetime
# Try to import local modules, handled gracefully if dependencies missing for placeholders
try:
    from scripts.market_data import core as a500lb
    from scripts.market_data import main as aaa_500_main
except ImportError as e:
    print(f"Warning: Could not import aaa_500 modules: {e}")

# Placeholder imports for when other scripts are populated
# from scripts.schwab_crawler import yahoo_crawler 
# from scripts.price_target_analysis import price_target_exploration
# from scripts.schwab_crawler import earnings_scraper
# from scripts.schwab_crawler import fundamentals_analyzer
# from scripts.feature_importance import pca_analysis_code
# from scripts.schwab_crawler import asset_allocation_analysis

warnings.filterwarnings('ignore')

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

def run_aaa_500():
    write_line("Starting AAA 500 Main Script")
    try:
        # Replicating logic from aaa_500_main.py __main__ block
        # Assuming a500lb and aaa_500_main are available
        df_symbols = a500lb.get_symbols()
        asyncio.run(aaa_500_main.main_async())
        write_line("AAA 500 Main Script execution completed.")
    except Exception as e:
        write_line(f"AAA 500 Main Script execution failed: {e}")
        import traceback
        traceback.print_exc()

def run_placeholder(script_name):
    # This function handles scripts that are still placeholders or not yet modularized
    import subprocess
    script_path = None
    if script_name == 'yahoo_crawler':
         script_path = os.path.join('scripts', 'schwab_crawler', 'yahoo_crawler.py')
    elif script_name == 'price_target_exploration':
         script_path = os.path.join('scripts', 'price_target_analysis', 'price_target_exploration.py')
    elif script_name == 'earnings_scraper':
         script_path = os.path.join('scripts', 'schwab_crawler', 'earnings_scraper.py')
    elif script_name == 'fundamentals_analyzer':
         script_path = os.path.join('scripts', 'schwab_crawler', 'fundamentals_analyzer.py')
    
    if script_path and os.path.exists(script_path):
        write_line(f"Executing placeholder script: {script_name}")
        try:
             subprocess.run([sys.executable, script_path])
        except Exception as e:
             write_line(f"Error running {script_name}: {e}")
    else:
        write_line(f"Script {script_name} not found or not mapped.")

def main():
    write_line("Starting Asset Allocation Script Runner")
    
    # 1. AAA 500 Main
    approval = input('Process aaa_500_main ([y]|n)? ')
    if approval != 'n':
        run_aaa_500()

    # 2. Yahoo Crawler
    approval = input('Process yahoo_crawler ([y]|n)? ')
    if approval != 'n':
        run_placeholder('yahoo_crawler')

    # 3. Price Target Exploration
    approval = input('Process price_target_exploration ([y]|n)? ')
    if approval != 'n':
        run_placeholder('price_target_exploration')

    # 4. Earnings Scraper
    approval = input('Process earnings_scraper ([y]|n)? ')
    if approval != 'n':
        run_placeholder('earnings_scraper')

    # 5. Fundamentals Analyzer
    approval = input('Process fundamentals_analyzer ([y]|n)? ')
    if approval != 'n':
        run_placeholder('fundamentals_analyzer')

    write_line("All scripts processed.")

if __name__ == "__main__":
    main()
