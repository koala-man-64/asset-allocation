import sys
import logging
from datetime import datetime
from typing import Dict, Any, List

import pandas as pd
import numpy as np
try:
    import dask.dataframe as dd
except ImportError:
    dd = None

from asset_allocation import config
from asset_allocation.data import storage
from asset_allocation.core import analysis, processing

logger = logging.getLogger(__name__)

# --- UI Helpers ---

def write_line(msg: str):
    """
    Print a message to the console with timestamp.
    Replica of the original behavior for familiarity.
    """
    # Clear line (generic implementation)
    sys.stdout.write('\r' + ' ' * 120 + '\r')
    sys.stdout.flush()
    
    ct = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'{config.COLOR_DATE}{ct}{config.COLOR_RESET}: {msg}')

def get_input_parameter(parameter_name: str, parameter_type: type, default_value: object, sample_values: list = []) -> object:
    msg = f"{config.COLOR_STRING}Enter {parameter_name}{config.COLOR_RESET} "
    if sample_values:
        msg += f"({', '.join(map(str, sample_values))})"
    msg += f" [Default: {config.COLOR_NUMBER}{default_value}{config.COLOR_RESET}]: "
    
    idx_str = input(msg)
    if idx_str == "":
        return default_value
    
    try:
        return parameter_type(idx_str)
    except Exception:
        write_line(f"Invalid input. Using default: {default_value}")
        return default_value

def prompt_user_dataframe(dataframes: Dict[str, Any], msg: str = "Select dataframe:") -> str:
    keys = list(dataframes.keys())
    if not keys:
        write_line("No dataframes available.")
        raise ValueError("No dataframes")
        
    print(msg)
    for i, k in enumerate(keys):
        print(f"  {i}: {k}")
        
    idx = get_input_parameter("index", int, 0)
    if 0 <= idx < len(keys):
        return keys[idx]
    return keys[0]

def prompt_user_column(ddf: Any, default: str) -> str:
    # Need to compute/fetch columns depending on dask or pandas
    if hasattr(ddf, 'columns'):
        cols = list(ddf.columns)
    else:
        # Fallback if unknown object
        return default
        
    print("Available columns:")
    for i, c in enumerate(cols):
        print(f"  {i}: {c}")
        
    idx = get_input_parameter("column index", int, -1)
    if 0 <= idx < len(cols):
        return cols[idx]
    
    # Try looking for default
    if default in cols:
        return default
    return cols[0]

# --- Handlers ---

def handle_add_sma(dataframes: Dict[str, Any]):
    name = prompt_user_dataframe(dataframes)
    df = dataframes[name]
    
    period = get_input_parameter("SMA Period", int, config.DEFAULT_SMA_PERIOD)
    col = prompt_user_column(df, "Close")
    
    write_line(f"Adding SMA {period} to {name} on {col}...")
    
    # Define wrapper for processing
    def _compute(pdf):
        return analysis.calculate_sma(pdf, col, period)
    
    # Update meta with new column
    new_col_name = f"SMA_{col}_{period}"
    meta = df._meta.assign(**{new_col_name: np.float64()})
    
    # Execute
    dataframes[name] = processing.apply_to_symbols(df, _compute, meta)
    write_line("SMA Added.")

def handle_add_range_percent(dataframes: Dict[str, Any]):
    name = prompt_user_dataframe(dataframes)
    df = dataframes[name]
    
    day_span = get_input_parameter("Days", int, 252)
    col = prompt_user_column(df, "Close")
    
    write_line(f"Adding Range% {day_span} to {name}...")
    
    def _compute(pdf):
        return analysis.calculate_range_percent(pdf, col, day_span)
        
    new_col_name = f"Range%_{col}_{day_span}"
    meta = df._meta.assign(**{new_col_name: np.float64()})
    
    dataframes[name] = processing.apply_to_symbols(df, _compute, meta)
    write_line("Range% Added.")

def handle_bollinger(dataframes: Dict[str, Any]):
    name = prompt_user_dataframe(dataframes)
    df = dataframes[name]
    
    period = get_input_parameter("Period", int, 20)
    mult = get_input_parameter("Std Mult", float, 2.0)
    col = prompt_user_column(df, "Close")
    
    write_line(f"Adding Bollinger Range%...")
    
    def _compute(pdf):
        return analysis.calculate_bollinger_range_pct(pdf, col, period, mult)
        
    # Construct expected col name for meta (simplified)
    k_str = f"{str(mult).rstrip('0').rstrip('.')}"
    new_col_name = f"BB_RangePct_{col}_{period}_{k_str}x"
    meta = df._meta.assign(**{new_col_name: np.float64()})
    
    dataframes[name] = processing.apply_to_symbols(df, _compute, meta)
    write_line("Bollinger Added.")

def handle_save(dataframes: Dict[str, Any]):
    # In original: store_pickle
    # We ask for a name
    name = input("Enter dataset name to save (leave empty for auto-generated): ")
    if not name:
        name = f"dataset_{datetime.now():%Y%m%d_%H%M%S}"
        
    path = config.DATA_DIR / name
    storage.save_dataset(dataframes, path)
    write_line(f"Saved to {path}")

def handle_load(dataframes: Dict[str, Any]):
    # List datasets
    datasets = storage.list_datasets(config.DATA_DIR)
    if not datasets:
        write_line("No datasets found.")
        return dataframes
        
    print("Available Datasets:")
    for i, d in enumerate(datasets):
        print(f"  {i}: {d}")
        
    idx = get_input_parameter("index", int, -1)
    if 0 <= idx < len(datasets):
        target = config.DATA_DIR / datasets[idx]
        write_line(f"Loading {target}...")
        loaded = storage.load_dataset(target)
        # Merge or replace? Original replace logic was complex (filters).
        # We will simply update/add to current dict
        dataframes.update(loaded)
        write_line(f"Loaded {len(loaded)} frames.")
    return dataframes

def handle_metrics(dataframes: Dict[str, Any]):
    name = prompt_user_dataframe(dataframes)
    df = dataframes[name]
    
    write_line("Computing Performance Metrics (This may take a while)...")
    
    # We need to compute!
    # Original logic is complex: EW, VW, etc.
    # For now, let's just do EW on the selected dataframe
    
    # 1. Daily Returns
    meta_daily = df._meta.assign(Daily_Return=np.float64())
    
    def _returns(pdf):
        return analysis.calculate_daily_returns(pdf, "Close", "Date", "Symbol")
        
    daily_returns = processing.apply_to_symbols(df, _returns, meta_daily)
    
    # 2. EW Series
    ew_series = daily_returns.groupby("Date")['Daily_Return'].mean().compute()
    
    # 3. Metrics
    metrics = analysis.calculate_series_metrics(ew_series, "EW Portfolio")
    
    print("\n--- Performance Metrics ---")
    for k, v in metrics.items():
        print(f"{k}: {v}")
    print("---------------------------\n")


# --- Main Menu ---

def main_loop():
    dataframes = {}
    
    # Try autoload (optional)
    # dataframes = handle_load({}) 
    
    options = [
        ("Load Dataframes", handle_load),
        ("Save Dataframes", handle_save),
        ("Add SMA", handle_add_sma),
        ("Add Range%", handle_add_range_percent),
        ("Add Bollinger", handle_bollinger),
        ("Performance Metrics", handle_metrics),
        ("Exit", None)
    ]
    
    while True:
        print("\n--- Asset Allocation Menu ---")
        for i, (label, _) in enumerate(options):
            print(f"[{i}] {label}")
            
        idx = get_input_parameter("Selection", int, -1)
        
        if 0 <= idx < len(options):
            label, func = options[idx]
            if label == "Exit":
                write_line("Goodbye!")
                break
            
            try:
                if label == "Load Dataframes":
                    dataframes = func(dataframes)
                else:
                    func(dataframes)
            except Exception as e:
                write_line(f"Error: {e}")
                logger.exception("Error in menu handler")
        else:
            write_line("Invalid selection.")
