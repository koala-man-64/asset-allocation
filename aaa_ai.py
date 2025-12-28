
import pandas as pd
import json
import datetime
import operator
import copy
import dask
import pandas_market_calendars as mcal
import numpy as np

from datetime import date, timedelta, datetime
import pickle

import dask.dataframe as dd
import traceback
import os
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
import multiprocessing
from typing import List, Union, Optional, Sequence
from pandas.api.types import is_float_dtype, is_datetime64_any_dtype
import warnings
import sys
# sys.path.insert(1, 'G:/My Drive/Python/Common')


# from stay_awake import StayAwake
warnings.filterwarnings('ignore')


COMMANDS = [
    "Evaluate Performance Metrics",
    "Print Performance Metrics",
    "Print Sample Dataset",
    "Print Symbol Summary",
    "Print Date Range",
    "Add Calculation",
    "Add Preset Filter",
    "Add Custom Filter",
    "Rank Column",
    "Add Weighted Expression",
    "Print Filters",
    "Clear Filters",
    "Save Filters",
    "Load Filters",
    "Materialize Dataframes",
    "Save Dataframes",
    "Load Dataframes",
    "Save Symbols",
    "Load Symbols",

    "Exit"
] 


#region Output funcitnos

def write_line(msg: str):
    """
    Print a line to the console w/ a timestamp
    Parameters:
        str:
    """
    ct = datetime.now()
    print("{}: {}".format(ct, msg))

def print_actions(dataframes):
    try:
        write_line("Print Filters selected.")
        # Print total number of stocks if available
        if 'df_symbols' in dataframes:
            total = len(dataframes['df_symbols'])
            write_line(f"Total number of stocks: {total}")
        # 1) Find available filter logs
        log_keys = [k for k in dataframes if k.endswith('_actions')]
        if not log_keys:
            write_line("No filter logs found.")
            return
        # 2) Display each log’s entries
        for key in log_keys:
            df_log = dataframes[key]
            write_line(f"Filter log: {key}")
            for idx, row in df_log.iterrows():
                write_line(f" {idx+1}. {row['action']} -> {row['prompt_answers_json']}")
    except Exception as e:
        write_line(f"Error in Print Filters: {e}")    
        traceback.print_exc()

def print_performance_metrics(dataframes):
    try:
        write_line("Executing: Print Performance Metrics")
        # TODO: use dataframes[...] as needed
    except Exception as e:
        write_line(f"Error in Print Performance Metrics: {e}")

def print_random_sample_by_columns(dataframes, sample_size=5):
    """
    Prompt the user to select one of the DataFrames in `dataframes`,
    then select columns by index, optionally filter to a specific date,
    and print a random sample of rows for those columns.

    Date filter behavior:
      - If a date-like column is detected (e.g., 'Date', 'obs_date', 'transaction_date'),
        the user is prompted to enter a YYYY-MM-DD date to filter on.
      - Press Enter to skip filtering and sample across all dates.
      - If no rows match the provided date, a message is shown and sampling proceeds
        across all dates.
    """
    import pandas as pd

    try:
        write_line("Print Random Sample selected.")

        # 1) Retrieve the selected DataFrame (as Dask or pandas)
        key = prompt_user_dataframe(dataframes)
        ddf = dataframes[key]

        # 2) List columns for the chosen DataFrame
        cols = list(ddf.columns)
        write_line("Available columns:")
        for i, col in enumerate(cols, start=1):
            write_line(f"  {i}. {col}")

        cols_choice = input("Select columns by comma-separated numbers (e.g. 1,3,5): ").strip()
        idxs = [s.strip() for s in cols_choice.split(",")]
        selected_cols = []
        for idx in idxs:
            if not (idx.isdigit() and 1 <= int(idx) <= len(cols)):
                write_line(f"Invalid column index '{idx}'. Aborting.")
                return
            selected_cols.append(cols[int(idx) - 1])

        # 3) Try to detect a date-like column to optionally filter on
        #    Primary candidates by name; if none match, skip date filtering.
        lowercase_cols = {c.lower(): c for c in cols}
        date_col = None
        for candidate in ("date", "obs_date", "transaction_date"):
            if candidate in lowercase_cols:
                date_col = lowercase_cols[candidate]
                break

        # 4) Compute to pandas (only needed columns to minimize memory)
        needed_cols = selected_cols.copy()
        if date_col and date_col not in needed_cols:
            needed_cols.append(date_col)

        pdf = ddf[needed_cols].compute() if hasattr(ddf, "compute") else ddf[needed_cols]
        if pdf.empty:
            write_line("No data available in those columns. Aborting.")
            return

        # 5) Optional date filter
        if date_col:
            write_line(f"Detected date column: '{date_col}'.")
            date_str = input("Enter a date to filter on (YYYY-MM-DD), or press Enter to skip: ").strip()
            if date_str:
                try:
                    target_date = pd.to_datetime(date_str).date()
                    # Normalize the date column to date
                    date_series = pd.to_datetime(pdf[date_col], errors="coerce").dt.date
                    mask = date_series == target_date
                    filtered = pdf.loc[mask]

                    if filtered.empty:
                        write_line(f"No rows found for {target_date} in '{date_col}'. Showing random sample across all dates.")
                    else:
                        pdf = filtered
                except Exception as ex:
                    write_line(f"Could not parse date '{date_str}' ({ex}). Proceeding without date filter.")

        # 6) Take a random sample
        out_df = pdf[selected_cols]
        if out_df.empty:
            write_line("No data available after filtering. Aborting.")
            return

        if len(out_df) >= sample_size:
            sample = out_df.sample(n=sample_size, random_state=42)
        else:
            sample = out_df

        # 7) Print the sample
        print(sample)

    except Exception as e:
        write_line(f"Error in Print Random Sample: {e}")
        return

def store_pickle(obj, base_dir):
    """fad
    Save a dict of DataFrames (Dask and/or pandas) as a dataset folder with a manifest.
    Prompts using the configured directory `base_dir` to either:
      - OVERWRITE an existing dataset (selected by number), or
      - Create a NEW dataset (default).

    Layout:
      base_dir/
        <dataset_name>/
          manifest.json
          <slug(key for dask)>/        (Parquet directory, Dask)
          <slug(key for pandas)>/data.parquet  (single file, pandas)
    """
    import os, re, json, shutil, time
    from datetime import datetime

    # lazy imports so this stays drop-in
    try:
        import dask.dataframe as dd
    except Exception:
        dd = None
    try:
        import pandas as pd
    except Exception:
        pd = None

    # -------- helpers --------
    def _is_dask_df(x) -> bool:
        return dd is not None and isinstance(x, dd.DataFrame)

    def _is_pandas_df(x) -> bool:
        return pd is not None and getattr(pd, "DataFrame", None) is not None and isinstance(x, pd.DataFrame)

    def _is_mixed_df_dict(x) -> bool:
        return isinstance(x, dict) and len(x) > 0 and all(_is_dask_df(v) or _is_pandas_df(v) for v in x.values())

    def _sanitize(name: str) -> str:
        return re.sub(r'[\\/:\*\?"<>\|]', "_", str(name)).strip()

    def _slug(name: str) -> str:
        return re.sub(r'[\\/:\*\?"<>\|\s]+', "_", str(name)).strip("_")

    def _list_datasets(root_dir: str):
        items = []
        if not os.path.exists(root_dir):
            return items
        for name in sorted(os.listdir(root_dir)):
            p = os.path.join(root_dir, name)
            if os.path.isdir(p) and os.path.exists(os.path.join(p, "manifest.json")):
                items.append(name)
        return items

    def _save_dataset(dct: dict, dataset_dir: str, *, engine="pyarrow", write_index=False):
        t0 = time.perf_counter()
        write_line(f"[save] Creating dataset directory: {dataset_dir}")
        os.makedirs(dataset_dir, exist_ok=True)

        manifest = {
            "type": "mixed_df_dict",
            "version": "1.0",
            "engine": engine,
            "created": datetime.now().isoformat(timespec="seconds"),
            "items": []
        }

        total = len(dct)
        write_line(f"[save] Preparing to write {total} item(s)")

        for idx, (key, obj_v) in enumerate(dct.items(), start=1):
            key_str = str(key)

            # # --- your existing filter retained; now logs when skipping ---
            # if key_str == "df_earnings_data":
            #     pass
            # else:
            #     write_line(f"[save][{idx}/{total}] Skipping key '{key_str}' (only saving 'df_earnings_data')")
            #     continue

            sub = _slug(key_str)
            subdir = os.path.join(dataset_dir, sub)
            if os.path.exists(subdir):
                write_line(f"[save][{idx}/{total}] Removing existing folder: {subdir}")
                shutil.rmtree(subdir)
            os.makedirs(subdir, exist_ok=True)

            # Dask
            if _is_dask_df(obj_v):
                write_line(f"[save][{idx}/{total}] Key: '{key_str}' (Dask) → {subdir}")
                write_line(f"[save][{idx}/{total}]  • columns={len(obj_v.columns)}  npartitions={obj_v.npartitions}")
                t1 = time.perf_counter()
                out_path = subdir  # write as a directory
                obj_v.to_parquet(out_path, engine=engine, write_index=write_index)
                dt = time.perf_counter() - t1
                write_line(f"[save][{idx}/{total}]  ✓ wrote Dask Parquet in {dt:0.2f}s")

                meta = obj_v._meta
                dtypes = {c: str(meta.dtypes[c]) for c in meta.columns}
                manifest["items"].append({
                    "key": key_str,
                    "frame_type": "dask",
                    "path": sub,                # directory
                    "columns": list(meta.columns),
                    "dtypes": dtypes
                })

            # pandas
            elif _is_pandas_df(obj_v):
                write_line(f"[save][{idx}/{total}] Key: '{key_str}' (pandas) → {subdir}/data.parquet")
                try:
                    nrows = len(obj_v)
                except Exception:
                    nrows = "?"
                write_line(f"[save][{idx}/{total}]  • columns={len(obj_v.columns)}  rows={nrows}")
                t1 = time.perf_counter()
                file_path = os.path.join(subdir, "data.parquet")
                obj_v.to_parquet(file_path, engine=engine, index=write_index)
                dt = time.perf_counter() - t1
                write_line(f"[save][{idx}/{total}]  ✓ wrote pandas Parquet in {dt:0.2f}s")

                dtypes = {c: str(obj_v.dtypes[c]) for c in obj_v.columns}
                manifest["items"].append({
                    "key": key_str,
                    "frame_type": "pandas",
                    "path": f"{sub}/data.parquet",  # file
                    "columns": list(obj_v.columns),
                    "dtypes": dtypes
                })
            else:
                raise TypeError(f"Value for key '{key_str}' is not a Dask or pandas DataFrame")

        # Write manifest
        mpath = os.path.join(dataset_dir, "manifest.json")
        write_line(f"[save] Writing manifest → {mpath}")
        with open(mpath, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)

        write_line(f"[save] Done. Wrote {len(manifest['items'])} item(s). Elapsed {time.perf_counter() - t0:0.2f}s")

    # -------- main --------
    try:
        write_line(f"[start] store_pickle(base_dir='{base_dir}')")

        if not _is_mixed_df_dict(obj):
            write_line("ERROR: store_pickle expects a dict whose values are Dask and/or pandas DataFrames.")
            return

        # summary of incoming dict
        vals = list(obj.values())
        dask_cnt = sum(1 for v in vals if _is_dask_df(v))
        pandas_cnt = sum(1 for v in vals if _is_pandas_df(v))
        write_line(f"[check] items={len(vals)}  dask={dask_cnt}  pandas={pandas_cnt}")

        root = base_dir
        if not os.path.exists(root):
            write_line(f"[fs] Creating base_dir: {root}")
        os.makedirs(root, exist_ok=True)

        # List existing datasets and prompt
        existing = _list_datasets(root)
        write_line(f"[scan] Found {len(existing)} dataset(s) in {root}")
        if existing:
            write_line(f"Datasets in {root}:")
            for i, name in enumerate(existing, start=1):
                write_line(f"  {i}. {name}")

        sel = input("Enter number to OVERWRITE a dataset, or press Enter to create NEW: ").strip()

        if sel.isdigit() and 1 <= int(sel) <= len(existing):
            dataset_name = existing[int(sel) - 1]
            dataset_dir = os.path.join(root, dataset_name)
            write_line(f"[select] Will OVERWRITE dataset: {dataset_name}")
            ans = input(f"About to overwrite '{dataset_name}'. Continue? [y/N]: ").strip().lower()
            if ans != "y":
                write_line("[abort] User declined overwrite.")
                return
        else:
            suggested = f"dataset_{datetime.now():%Y%m%d_%H%M%S}"
            typed = input(f"New dataset folder name [{suggested}]: ").strip()
            dataset_name = _sanitize(typed) or suggested
            dataset_dir = os.path.join(root, dataset_name)
            write_line(f"[select] Will CREATE new dataset: {dataset_name}")

        write_line(f"[go] Saving dataset -> {dataset_dir}")
        _save_dataset(obj, dataset_dir)
        write_line("[done] Saved mixed DataFrame dataset successfully.")

    except Exception as e:
        write_line(f"ERROR: {e}")
        traceback.print_exc()     

def load_pickle(base_dir) -> object:
    """
    Load a dataset created by `store_pickle` (mixed Dask/pandas dict).
    Prompts in the configured directory `base_dir` to select which dataset to load.

    Returns:
      dict[str, (dask.dataframe.DataFrame | pandas.DataFrame)]
      preserving each item’s original type.
    """
    import os, json
    from datetime import datetime

    try:
        import dask.dataframe as dd
    except Exception:
        dd = None
    try:
        import pandas as pd
    except Exception:
        pd = None

    def _list_datasets(root_dir: str):
        items = []
        if not os.path.exists(root_dir):
            return items
        for name in sorted(os.listdir(root_dir)):
            p = os.path.join(root_dir, name)
            if os.path.isdir(p) and os.path.exists(os.path.join(p, "manifest.json")):
                items.append(name)
        return items

    def _load_dataset(dataset_dir: str):
        manifest_path = os.path.join(dataset_dir, "manifest.json")
        if not os.path.exists(manifest_path):
            raise FileNotFoundError(f"manifest.json not found in {dataset_dir}")
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)

        engine = manifest.get("engine", "pyarrow")
        result = {}

        for item in manifest.get("items", []):
            key = item["key"]
            ftype = item.get("frame_type")
            rel_path = item["path"]
            abs_path = os.path.join(dataset_dir, rel_path)

            if ftype == "dask":
                if dd is None:
                    raise RuntimeError("dask.dataframe is required to load Dask items.")
                # Path is a directory dataset
                result[key] = dd.read_parquet(abs_path, engine=engine)

            elif ftype == "pandas":
                if pd is None:
                    raise RuntimeError("pandas is required to load pandas items.")
                # Path is a single Parquet file
                result[key] = pd.read_parquet(abs_path, engine=engine)

            else:
                # Fallback: try to infer by filesystem (dir->dask, file->pandas)
                if os.path.isdir(abs_path):
                    if dd is None:
                        raise RuntimeError("dask.dataframe is required to load this directory dataset.")
                    result[key] = dd.read_parquet(abs_path, engine=engine)
                else:
                    if pd is None:
                        raise RuntimeError("pandas is required to load this file dataset.")
                    result[key] = pd.read_parquet(abs_path, engine=engine)

        return result

    try:
        root = base_dir
        if not os.path.exists(root):
            write_line(f"Path does not exist: {root}")
            return None

        # If they passed a dataset folder directly
        if os.path.isdir(root) and os.path.exists(os.path.join(root, "manifest.json")):
            write_line(f"Loading dataset - {root}")
            return _load_dataset(root)

        datasets = _list_datasets(root)
        if not datasets:
            write_line(f"No datasets found in: {root}")
            return None

        write_line(f"Datasets in {root}:")
        for i, name in enumerate(datasets, start=1):
            p = os.path.join(root, name)
            mtime = datetime.fromtimestamp(os.path.getmtime(p)).strftime("%Y-%m-%d %H:%M:%S")
            write_line(f"  {i}. {name}  (modified {mtime})")

        choice = input("Select dataset by number: ").strip()
        if not (choice.isdigit() and 1 <= int(choice) <= len(datasets)):
            write_line("Invalid selection. Aborting.")
            return None

        dataset_dir = os.path.join(root, datasets[int(choice) - 1])
        write_line(f"Loading dataset - {dataset_dir}")
        return _load_dataset(dataset_dir)

    except Exception as e:
        write_line(f"ERROR: {e}")
        return None

def print_symbol_summary(dataframes):
    """
    Prompt the user to select one of the DataFrames in `dataframes`
    and print out the min and max values of its 'Date' column.
    Returns (min_date, max_date) if successful, else None.
    """
    try:
        write_line("Print Symbol Summary selected.")
        # 1) List available DataFrames
        keys = list(dataframes.keys())
        write_line("Available DataFrames:")
        for i, key in enumerate(keys, start=1):
            write_line(f"  {i}. {key}")
        choice = input("Select DataFrame by number: ").strip()
        if not (choice.isdigit() and 1 <= int(choice) <= len(keys)):
            write_line("Invalid selection. Aborting.")
            return

        # 2) Retrieve and normalize to Dask DataFrame
        key = keys[int(choice) - 1]
        ddf = dataframes[key]
        
        # 1) Compute the raw counts per sector
        counts = ddf.groupby('Sector').size().compute().rename("count")

        # 2) Turn into a DataFrame so we can add more columns
        summary = counts.to_frame()

        # 3) Calculate percent of total
        total = summary['count'].sum()
        summary['percent'] = (summary['count'] / total * 100).round(2)

        print(summary)

    except Exception as e:
        write_line(f"Error in Print Symbol Summary: {e}")
        return

#endregion

#region Input functions

def prompt_user_dataframe(dataframes, msg: str = None):
    # 1) List available DataFrames
    keys = list(dataframes.keys())
    write_line("Available DataFrames:")
    for i, key in enumerate(keys, start=1):
        write_line(f"  {i}. {key}")
    if msg is None:
        msg = "Select DataFrame by number: "
    choice = input(msg).strip()
    if not (choice.isdigit() and 1 <= int(choice) <= len(keys)):
        write_line("Invalid selection. Aborting.")
        return

    # 2) Retrieve the selected DataFrame (as Dask or pandas)
    key = keys[int(choice) - 1]
    return key

def prompt_user_command():
    write_line("Please enter the number of one of the following commands:")
    for idx, cmd in enumerate(COMMANDS, start=1):
        write_line(f"  {idx}. {cmd}")
    choice = input("Enter number: ").strip()
    if not choice.isdigit():
        write_line("Invalid input. Please enter a number.")
        return None
    idx = int(choice)
    if 1 <= idx <= len(COMMANDS):
        return COMMANDS[idx - 1]
    else:
        write_line(f"Invalid selection: {idx}. Please try again.")
        return None

def prompt_user_column(ddf, default, message="Select the column number to apply calculation: ") -> str:
    columns = list(ddf.columns)
    print("Available columns:")
    for i, col in enumerate(columns, 1):
        print(f"  {i}. {col}")
    try:
        col_choice = int(input(message).strip())
        if not (1 <= col_choice <= len(columns)):
            if default:
                print(f"Invalid choice. Defaulting to '{default}'.")
                target_col = default
            else:
                raise ValueError
        else:
            target_col = columns[col_choice - 1]
    except ValueError:
        if default:
            print(f"Invalid input. Defaulting to '{default}'.")
            target_col = default
        
    return target_col

def get_input_parameter(parameter_name: str, parameter_type: type, default_value: object, sample_values: list=[]):
    
    try:     
        input_param = input(f"Enter the {parameter_name} parameter (e.g., {str(sample_values)}): ").strip()
        if len(input_param) == 0:
            print(f"Invalid {parameter_name}. Using default of {str(default_value)}.")
            input_param = default_value
    except ValueError:
        print(f"Invalid input. Using default of {str(default_value)}.")
        
    if parameter_type == type(int):   
        try:
            input_param = int(input_param)
        except Exception as e:
            input_param = str(input_param)
    elif parameter_type == type(float):
        try:
            input_param = float(input_param)
        except Exception as e:
            input_param = str(input_param)
    else:
        input_param = str(input_param)
    return input_param
 
def get_date_range(dataframes):
    """
    Prompt the user to select one of the DataFrames in `dataframes`
    and print out the min and max values of its 'Date' column.
    Returns (min_date, max_date) if successful, else None.
    """
    try:
        write_line("Get Date Range selected.")
        # 1) List available DataFrames
        keys = list(dataframes.keys())
        write_line("Available DataFrames:")
        for i, key in enumerate(keys, start=1):
            write_line(f"  {i}. {key}")
        choice = input("Select DataFrame by number: ").strip()
        if not (choice.isdigit() and 1 <= int(choice) <= len(keys)):
            write_line("Invalid selection. Aborting.")
            return

        # 2) Retrieve and normalize to Dask DataFrame
        key = keys[int(choice) - 1]
        df = dataframes[key]
        if isinstance(df, pd.DataFrame):
            df = dd.from_pandas(df, npartitions=4)

        # 3) Check for 'Date' column
        if 'Date' not in df.columns:
            write_line(f"No 'Date' column found in '{key}'.")
            return

        # 4) Compute min and max in one pass
        min_dt, max_dt = dask.compute(df['Date'].min(), df['Date'].max())

        # 5) Display the results
        write_line(f"Date range for '{key}': {min_dt.date()} → {max_dt.date()}")
        return min_dt, max_dt

    except Exception as e:
        write_line(f"Error in Get Date Range: {e}")
        return

#endregion

#region Calcuations
 
def add_sma(dataframes, prompt_answers=None):
    """
    Adds a Simple Moving Average (SMA) column to the Dask DataFrame
    (default 'df_price_analysis'), computed per Symbol and sorted by Date.

    Tracks/reuses in `prompt_answers`:
      - dataframe  : which DF to use (default 'df_price_analysis')
      - period     : SMA window (int)
      - target_col : source column (e.g., 'Close')
    """
    import json
    import numpy as np
    import pandas as pd
    import dask.dataframe as dd

    # ---- prompt state ----
    if prompt_answers is None:
        prompt_answers = {}

    # 1) Pick dataframe (default + track)
    dataframe_name = prompt_answers.get('dataframe', 'df_price_analysis')
    if dataframe_name not in dataframes:
        raise KeyError(f"DataFrame '{dataframe_name}' not found in dataframes.")
    prompt_answers['dataframe'] = dataframe_name
    ddf = dataframes[dataframe_name]

    # 2) SMA period (reuse or prompt)
    if 'period' in prompt_answers:
        try:
            period = int(prompt_answers['period'])
        except Exception:
            raise ValueError(f"Invalid period in prompt_answers: {prompt_answers['period']}")
    else:
        period = int(get_input_parameter('SMA period', type(int), 20, [20]))
        prompt_answers['period'] = period

    # 3) Target column (reuse or prompt)
    if 'target_col' in prompt_answers:
        target_col = prompt_answers['target_col']
    else:
        target_col = prompt_user_column(ddf, 'Close')
        prompt_answers['target_col'] = target_col

    # ---- parameters / names ----
    date_col   = 'Date'
    symbol_col = 'Symbol'
    col_name   = f"SMA_{target_col}_{period}"

    # ---- basic checks ----
    columns = list(ddf.columns)
    needed = {symbol_col, date_col, target_col}
    missing = needed - set(columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    # ---- ensure Date is datetime (Dask-safe) ----
    ddf = ddf.assign(**{date_col: dd.to_datetime(ddf[date_col], errors="coerce")})

    # ---- per-group compute (pandas fn) ----
    def _compute_sma(pdf: pd.DataFrame) -> pd.DataFrame:
        write_line(f'Computing {col_name} for {str(pdf[symbol_col].iloc[0])}')
        pdf = pdf.sort_values(date_col, kind="mergesort")
        s = pd.to_numeric(pdf[target_col], errors='coerce')
        pdf[col_name] = s.rolling(window=period).mean().round(2)
        return pdf

    # ---- meta with new column dtype ----
    meta = ddf._meta.assign(**{col_name: np.float64()})

    # ---- shuffle -> groupby.apply -> tidy ----
    out = (
        ddf.shuffle(symbol_col)
           .groupby(symbol_col)
           .apply(_compute_sma, meta=meta, include_groups=True,)
           .reset_index(drop=True)
           .map_partitions(lambda pdf: pdf.sort_values([symbol_col, date_col], kind="mergesort"))
    )

    # ---- update dict ----
    dataframes[dataframe_name] = out

    # ---- minimal action log (compact + dedup by prompt snapshot) ----
    try:
        log_key = "df_actions"
        entry = {
            "timestamp": pd.Timestamp.now(),
            "action": "add_sma",
            "dataframe": dataframe_name,
            "added_columns": [col_name],
            "prompt_answers_json": json.dumps(prompt_answers, default=str, sort_keys=True),
        }
        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
        else:
            df_log = dataframes[log_key]
            if "prompt_answers_json" not in df_log.columns:
                df_log["prompt_answers_json"] = ""
            if not (df_log["prompt_answers_json"] == entry["prompt_answers_json"]).any():
                df_log.loc[len(df_log)] = entry
    except Exception:
        # logging should never break the computation
        pass

    # ---- return ----
    return dataframes
      
def add_range_percent(dataframes, prompt_answers=None):
    """
    Adds a rolling % position within the (low..high) range over a day window:
      Range% = (value - rolling_low) / (rolling_high - rolling_low) * 100
    Computed per Symbol, sorted by Date.

    Tracks all prompts in `prompt_answers`:
      - dataframe  : which DF to use (defaults to 'df_price_analysis')
      - day_span   : rolling window (int)
      - target_col : price/metric column to evaluate (e.g., 'Close')
    """
    import json
    import numpy as np
    import pandas as pd
    import dask.dataframe as dd

    # ---- prompt state ----
    if prompt_answers is None:
        prompt_answers = {}

    # 1) Pick dataframe (default to df_price_analysis, but track it)
    dataframe_name = prompt_answers.get('dataframe', 'df_price_analysis')
    if dataframe_name not in dataframes:
        raise KeyError(f"DataFrame '{dataframe_name}' not found in dataframes.")
    prompt_answers['dataframe'] = dataframe_name
    ddf = dataframes[dataframe_name]

    # 2) Day span (use provided or prompt)
    if 'day_span' in prompt_answers:
        day_span = int(prompt_answers['day_span'])
    else:
        # default=252, allowed=[252] (matches your helper style)
        day_span = get_input_parameter('Day span', type(int), 252, [252])
        day_span = int(day_span)
        prompt_answers['day_span'] = day_span

    # 3) Target column (use provided or prompt)
    if 'target_col' in prompt_answers:
        target_col = prompt_answers['target_col']
    else:
        target_col = prompt_user_column(ddf, 'Close')
        prompt_answers['target_col'] = target_col

    # ---- parameters / names ----
    date_col   = 'Date'
    symbol_col = 'Symbol'
    col_name   = f"Range%_{target_col}_{day_span}"   # final added column

    # ---- basic checks ----
    columns = list(ddf.columns)
    needed = {symbol_col, date_col, target_col}
    missing = needed - set(columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    # ---- ensure Date is datetime (Dask-safe) ----
    ddf = ddf.assign(**{date_col: dd.to_datetime(ddf[date_col], errors="coerce")})

    # ---- per-group compute (pandas fn) ----
    def _compute_period_range(pdf: pd.DataFrame) -> pd.DataFrame:
        # stable sort within group
        pdf = pdf.sort_values(date_col, kind="mergesort")

        # rolling high/low on the target column
        s = pd.to_numeric(pdf[target_col], errors='coerce')
        roll_high = s.rolling(window=day_span, min_periods=1).max()
        roll_low  = s.rolling(window=day_span, min_periods=1).min()

        denom = (roll_high - roll_low).replace(0, np.nan)
        pct = ((s - roll_low) / denom) * 100.0

        pdf[col_name] = (
            pct.fillna(0.0)
               .clip(lower=0.0, upper=100.0)
               .astype(np.float64)
               .round(2)
        )
        return pdf

    # ---- meta with new column dtype ----
    meta = ddf._meta.assign(**{col_name: np.float64()})

    # ---- shuffle -> groupby.apply -> tidy ----
    out = (
        ddf.shuffle(symbol_col)
           .groupby(symbol_col)
           .apply(_compute_period_range, meta=meta, include_groups=True,)
           .reset_index(drop=True)
           .map_partitions(lambda pdf: pdf.sort_values([symbol_col, date_col], kind="mergesort"))
    )

    # ---- update dict ----
    dataframes[dataframe_name] = out

    # ---- minimal action log (compact + dedup by prompt snapshot) ----
    try:
        log_key = "df_actions"
        entry = {
            "timestamp": pd.Timestamp.now(),
            "action": "add_range_percent",
            "dataframe": dataframe_name,
            "added_columns": [col_name],
            "prompt_answers_json": json.dumps(prompt_answers, default=str, sort_keys=True),
        }
        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
        else:
            df_log = dataframes[log_key]
            if "prompt_answers_json" not in df_log.columns:
                df_log["prompt_answers_json"] = ""
            if not (df_log["prompt_answers_json"] == entry["prompt_answers_json"]).any():
                df_log.loc[len(df_log)] = entry
    except Exception as _:
        # logging should never crash the core computation
        pass

    # ---- return ----
    return dataframes

def add_rank_by_date(dataframes, prompt_answers=None):
    """
    Adds a per-Date ranking for a chosen column, across Symbols, to a Dask DataFrame.

    Tracks/reuses in `prompt_answers` (similar style to add_ema):
      - dataframe    : which DF to use (default 'df_price_analysis')
      - target_col   : column to rank (default 'Close')
      - rank_mode    : 'number' or 'percentage' (default 'number')
      - rank_order   : 'descending' or 'ascending' (default 'descending')
                       (also accepts boolean 'ascending' for convenience)
      - round_digits : rounding for percentage mode (default 2)

    Behavior:
      - Groups by Date only (ranking across all Symbols for that Date).
      - Rank "best" depends on rank_order:
          * descending -> larger values are better
          * ascending  -> smaller values are better
      - Adds one new column:
          RankNum_<target_col>   OR   RankPct_<target_col>
      - Leaves NaNs in the chosen column unranked (NaN in the rank column).
      - Ensures Date is datetime, uses stable mergesort ordering for determinism.

    Returns:
      - The updated `dataframes` dict (replaces the chosen dataframe with the ranked copy).
      - Appends a compact entry to dataframes['df_actions'] (if present/creatable).

    Notes:
      - Dask-only operations (shuffle -> groupby.apply with include_groups=True).
      - Ties resolved with method='min' (e.g., 1,1,3,...).
    """
    import json
    import numpy as np
    import pandas as pd
    import dask.dataframe as dd

    # ---- prompt state ----
    if prompt_answers is None:
        prompt_answers = {}

    # ---- 1) Pick dataframe (reuse or default like add_ema) ----
    dataframe_name = prompt_answers.get('dataframe', 'df_price_analysis')
    if dataframe_name not in dataframes:
        df_options = sorted([k for k, v in dataframes.items() if hasattr(v, 'columns')])
        if not df_options:
            raise KeyError("No suitable DataFrames found in `dataframes`.")
        default_df = df_options[0]
        dataframe_name = get_input_parameter('DataFrame to rank', str, default_df, df_options)
    prompt_answers['dataframe'] = dataframe_name
    ddf = dataframes[dataframe_name]

    # ---- constants / columns ----
    date_col   = 'Date'
    symbol_col = 'Symbol'

    # ---- 2) Target column (reuse or prompt like add_ema) ----
    if 'target_col' in prompt_answers:
        target_col = str(prompt_answers['target_col'])
    else:
        target_col = prompt_user_column(ddf, default=None, message='Select column to rank: ')
        prompt_answers['target_col'] = target_col

    # ---- 3) Rank mode (reuse or prompt) ----
    if 'rank_mode' in prompt_answers:
        rank_mode = str(prompt_answers['rank_mode']).strip().lower()
    else:
        rank_mode = get_input_parameter('Rank mode', str, 'number', ['number', 'percentage']).lower()
        prompt_answers['rank_mode'] = rank_mode

    # ---- 3a) Rank order (NEW): 'descending' or 'ascending' ----
    # Accept either boolean 'ascending' or string 'rank_order' in prompt_answers.
    if 'ascending' in prompt_answers:
        ascending = bool(prompt_answers['ascending'])
        rank_order = 'ascending' if ascending else 'descending'
        prompt_answers['rank_order'] = rank_order
    else:
        rank_order = prompt_answers.get('rank_order')
        if not rank_order:
            rank_order = get_input_parameter('Rank order', str, 'descending', ['descending', 'ascending']).lower()
            prompt_answers['rank_order'] = rank_order
        ascending = (rank_order == 'ascending')

    # ---- 3b) Optional rounding for percentage mode ----
    if rank_mode == 'percentage':
        if 'round_digits' in prompt_answers:
            try:
                round_digits = int(prompt_answers['round_digits'])
            except Exception:
                raise ValueError(f"Invalid round_digits in prompt_answers: {prompt_answers['round_digits']}")
        else:
            round_digits = int(get_input_parameter('Round digits (percentage)', int, 2, [0, 1, 2, 3, 4]))
            prompt_answers['round_digits'] = round_digits
    else:
        round_digits = None  # not used

    # ---- checks ----
    cols = set(map(str, ddf.columns))
    needed = {date_col, symbol_col, target_col}
    missing = needed - cols
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    # ---- ensure Date is datetime (Dask-safe) ----
    ddf = ddf.assign(**{date_col: dd.to_datetime(ddf[date_col], errors="coerce")})

    # ---- output column name ----
    col_name = f"RankPct_{target_col}" if rank_mode == 'percentage' else f"RankNum_{target_col}"

    # ---- optional progress line ----
    try:
        write_line(f"Ranking '{target_col}' by {date_col} as {rank_mode} ({rank_order}) on DataFrame '{dataframe_name}'...")
    except Exception:
        pass

    # ---- pandas-side per-date ranking ----
    def _rank_one_date(pdf: pd.DataFrame) -> pd.DataFrame:
        pdf = pdf.copy()
        vals = pd.to_numeric(pdf[target_col], errors='coerce')
        notna_mask = vals.notna()
        n = int(notna_mask.sum())

        if n == 0:
            pdf[col_name] = np.nan
            return pdf

        # numeric ranks: 1 = "best" according to 'ascending'
        #   ascending=True  -> smaller values rank 1 (best)
        #   ascending=False -> larger values rank 1 (best)
        ranks = vals.rank(ascending=ascending, method='min')  # float dtype

        if rank_mode == 'percentage':
            # 100 = best (rank 1), 0 = worst (rank n); works for either direction
            denom = (n - 1) if n > 1 else 1
            pct = ((n - ranks) / denom) * 100.0
            if round_digits is not None:
                pct = pct.round(round_digits)
            pdf[col_name] = pct.where(notna_mask, np.nan)
        else:
            pdf[col_name] = ranks.where(notna_mask, np.nan)

        return pdf

    # ---- meta with new column dtype (float covers both NaN + ints) ----
    meta = ddf._meta.assign(**{col_name: np.float64()})

    # ---- shuffle -> groupby(Date).apply -> stable sort ----
    out = (
        ddf.shuffle(date_col)
           .groupby(date_col)
           .apply(_rank_one_date, meta=meta, include_groups=True)
           .reset_index(drop=True)
           .map_partitions(lambda pdf: pdf.sort_values([date_col, symbol_col], kind="mergesort"))
    )

    # ---- update dict ----
    dataframes[dataframe_name] = out

    # ---- minimal action log (compact + dedup by prompt snapshot) ----
    try:
        log_key = "df_actions"
        entry = {
            "timestamp": pd.Timestamp.now(),
            "action": "add_rank_by_date",
            "dataframe": dataframe_name,
            "added_columns": [col_name],
            "prompt_answers_json": json.dumps(prompt_answers, default=str, sort_keys=True),
        }
        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
        else:
            df_log = dataframes[log_key]
            if "prompt_answers_json" not in df_log.columns:
                df_log["prompt_answers_json"] = ""
            if not (df_log["prompt_answers_json"] == entry["prompt_answers_json"]).any():
                df_log.loc[len(df_log)] = entry
    except Exception:
        pass

    return dataframes


def add_weighted_expression(dataframes, prompt_answers=None):
    """
    Build a new column from weighted operations across existing columns:
      expr = (w1*col1) (op2) (w2*col2) (op3) (w3*col3) ...

    Tracks/reuses in `prompt_answers` (aligned with add_ema style):
      - dataframe    : which DF to use (default 'df_price_analysis')
      - out_col      : name of the new column (default 'Expr_Custom')
      - steps        : optional pre-seeded list of dicts:
                       [{'col': 'Close', 'weight': 1.0},
                        {'op': '+', 'col': 'Volume', 'weight': 0.5}, ...]
      - round_digits : optional int to round the final result (e.g., 2)

    Notes:
      - Each referenced column is coerced to numeric (non-numeric -> NaN).
      - Division by zero is treated as NaN.
      - Final dtype is float64.
      - Compact action entry appended to dataframes['df_actions'].

    Returns:
      Updated `dataframes` (with the chosen dataframe replaced by the new one).
    """
    import json
    import numpy as np
    import pandas as pd
    import dask.dataframe as dd

    # ---- prompt state ----
    if prompt_answers is None:
        prompt_answers = {}

    # ---- 1) Pick dataframe (reuse or default like add_ema) ----
    dataframe_name = prompt_answers.get('dataframe', '')
    if dataframe_name == '':
        dataframe_name = prompt_user_dataframe(dataframes, 'Selected dataframe to apply weigthed expression: ')
    prompt_answers['dataframe'] = dataframe_name
    ddf = dataframes[dataframe_name]

    # ---- helpers ----
    def _to_numeric(series: dd.Series) -> dd.Series:
        return series.map_partitions(pd.to_numeric, errors='coerce')

    def _norm_op(token: str) -> str:
        t = (token or '').strip().lower()
        aliases = {
            '+': '+', 'plus': '+', 'add': '+',
            '-': '-', 'minus': '-', 'sub': '-',
            '*': '*', 'x': '*', 'times': '*', 'multiply': '*', 'mul': '*',
            '/': '/', 'divide': '/', 'div': '/',
        }
        return aliases.get(t, '')

    # ---- 2) Output column name (reuse or prompt) ----
    out_col = str(prompt_answers.get('out_col')) if 'out_col' in prompt_answers else None
    if not out_col:
        out_col = get_input_parameter('Name for new column', str, 'Expr_Custom', [])
        prompt_answers['out_col'] = out_col

    # ---- 3) Build steps (reuse pre-seeded or interactively prompt) ----
    steps = prompt_answers.get('steps')
    if not (isinstance(steps, list) and steps):
        steps = []
        first_col = prompt_user_column(ddf, default='Close')
        w1 = get_input_parameter('Weight for first column', float, 1.0, [1.0, 0.5, 2.0, -1.0])
        steps.append({'col': first_col, 'weight': float(w1)})

        while True:
            op = get_input_parameter(
                "Operation (+, -, *, /) (leave blank to finish)",
                str, '', ['+', '-', '*', '/', 'plus', 'minus', 'multiply', 'divide', '']
            )
            op = _norm_op(op)
            if not op:
                break
            next_col = prompt_user_column(ddf, '')
            w = get_input_parameter(f'Weight for {next_col}', float, 1.0, [1.0, 0.5, 2.0, -1.0])
            steps.append({'op': op, 'col': next_col, 'weight': float(w)})
        prompt_answers['steps'] = steps

    # ---- 3a) Optional rounding (reuse or skip) ----
    round_digits = None
    if 'round_digits' in prompt_answers and prompt_answers['round_digits'] is not None:
        try:
            round_digits = int(prompt_answers['round_digits'])
        except Exception:
            raise ValueError(f"Invalid round_digits in prompt_answers: {prompt_answers['round_digits']}")

    # ---- validate referenced columns ----
    cols_set = set(map(str, ddf.columns))
    needed_cols = {s['col'] for s in steps}
    missing = needed_cols - cols_set
    if missing:
        raise KeyError(f"Missing referenced columns: {missing}")
    if not steps:
        raise ValueError("No steps provided to build the expression.")
    if 'col' not in steps[0]:
        raise ValueError("First step must specify a 'col'.")

    # ---- 4) Build expression lazily in Dask ----
    try:
        write_line(f"Building expression for {out_col} with {len(steps)} step(s)…")
    except Exception:
        pass

    expr = _to_numeric(ddf[steps[0]['col']]) * float(steps[0].get('weight', 1.0))

    for s in steps[1:]:
        op = s.get('op', '')
        col = s['col']
        w = float(s.get('weight', 1.0))
        rhs = _to_numeric(ddf[col]) * w

        if op == '+':
            expr = expr + rhs
        elif op == '-':
            expr = expr - rhs
        elif op == '*':
            expr = expr * rhs
        elif op == '/':
            rhs_safe = rhs.map_partitions(lambda ser: ser.replace(0, np.nan))
            expr = expr / rhs_safe
        else:
            raise ValueError(f"Unsupported operation: {op}")

    # Coerce to float64 and clean inf -> NaN
    expr = expr.astype('float64')
    expr = expr.map_partitions(lambda s: s.replace([np.inf, -np.inf], np.nan))
    if round_digits is not None:
        # dask Series.round supports decimals
        expr = expr.round(int(round_digits))

    # Attach new column
    meta = ddf._meta.assign(**{out_col: np.float64()})
    ddf_new = dd.concat([ddf, expr.rename(out_col)], axis=1, ignore_unknown_divisions=True)
    ddf_new = ddf_new.astype({out_col: 'float64'})

    # ---- 5) Persist back + log ----
    dataframes[dataframe_name] = ddf_new

    try:
        # human-readable formula for the log
        if steps:
            formula = f"({steps[0].get('weight', 1.0)}*{steps[0]['col']})"
            for s in steps[1:]:
                sym = s.get('op', '?')
                formula = f"{formula} {sym} ({s.get('weight', 1.0)}*{s['col']})"
        else:
            formula = ""

        entry = {
            "timestamp": pd.Timestamp.now(),
            "action": "add_weighted_expression",
            "dataframe": dataframe_name,
            "added_columns": [out_col],
            "formula": formula,
            "prompt_answers_json": json.dumps(prompt_answers, default=str, sort_keys=True),
        }
        log_key = "df_actions"
        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
        else:
            df_log = dataframes[log_key]
            if "prompt_answers_json" not in df_log.columns:
                df_log["prompt_answers_json"] = ""
            if not (df_log["prompt_answers_json"] == entry["prompt_answers_json"]).any():
                df_log.loc[len(df_log)] = entry
    except Exception:
        # logging must never break the flow
        pass

    try:
        write_line(f"Added column '{out_col}'.")
    except Exception:
        pass

    return dataframes

def save_unique_symbols(dataframes, prompt_answers=None):
    """
    Saves a unique, sorted list of symbols from the `df_symbols` dataframe's
    'Symbol' column to the local Symbols/ folder.

    Options via prompt_answers (all optional):
      - dataframe   : which DF key to read from (default 'df_symbols')
      - symbol_col  : column name for symbols (default 'Symbol')
      - folder      : folder to write to (default 'Symbols')
      - filename    : filename to write (default 'symbols.csv')  <-- will be prompted if missing
      - append_date : bool; if True => append YYYYMMDD to filename (default False)

    Returns: dict with {'path': <written_path>, 'count': <n_symbols>}
    """
    import os
    import re
    import json
    import pandas as pd

    try:
        import dask.dataframe as dd  # optional; only needed if df is Dask
        _has_dask = True
    except Exception:
        _has_dask = False

    if prompt_answers is None:
        prompt_answers = {}

    # ---- parameters / defaults ----
    df_key     = prompt_answers.get('dataframe', 'df_symbols')
    symbol_col = prompt_answers.get('symbol_col', 'Symbol')
    folder     = prompt_answers.get('folder', 'Symbols')
    append_dt  = bool(prompt_answers.get('append_date', False))

    # ---- prompt for filename if not provided ----
    def _sanitize_filename(name: str) -> str:
        # Remove characters not allowed on common filesystems
        name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', '_', name.strip())
        # Prevent empty/just dots
        if not name or set(name) == {"."}:
            name = "symbols.csv"
        # Ensure .csv extension
        root, ext = os.path.splitext(name)
        if ext.lower() != ".csv":
            name = f"{root}.csv"
        return name

    default_filename = "symbols.csv"
    filename = prompt_answers.get('filename')
    if not filename:
        try:
            user_in = input(f"Enter filename to save (default: {default_filename}): ").strip()
        except Exception:
            user_in = ""  # non-interactive environments
        filename = _sanitize_filename(user_in or default_filename)
    else:
        filename = _sanitize_filename(str(filename))

    if df_key not in dataframes:
        raise KeyError(f"'{df_key}' not found in dataframes.")

    df_any = dataframes[df_key]

    # ---- extract symbol series robustly for Pandas or Dask ----
    if _has_dask and isinstance(df_any, dd.DataFrame):
        if symbol_col not in df_any.columns:
            raise KeyError(f"Column '{symbol_col}' not found in {df_key}.")
        series = df_any[symbol_col].dropna().astype(str).drop_duplicates().compute()
        symbols = sorted(series.unique())
    else:
        if symbol_col not in df_any.columns:
            raise KeyError(f"Column '{symbol_col}' not in {df_key}.")
        series = df_any[symbol_col].dropna().astype(str)
        symbols = sorted(pd.unique(series))

    # ---- ensure destination and filename ----
    os.makedirs(folder, exist_ok=True)
    if append_dt:
        base, ext = os.path.splitext(filename)
        date_str = pd.Timestamp.now().strftime("%Y%m%d")
        filename = f"{base}_{date_str}{ext or '.csv'}"
    path = os.path.join(folder, filename)

    # ---- write CSV ----
    pd.DataFrame({'Symbol': symbols}).to_csv(path, index=False)

    # ---- minimal action log ----
    try:
        entry = {
            "timestamp": pd.Timestamp.now(),
            "action": "save_unique_symbols",
            "dataframe": df_key,
            "rows_written": len(symbols),
            "file_path": path,
            "prompt_answers_json": json.dumps(prompt_answers, default=str, sort_keys=True),
        }
        log_key = "df_actions"
        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
        else:
            df_log = dataframes[log_key]
            if "prompt_answers_json" not in df_log.columns:
                df_log["prompt_answers_json"] = ""
            if not (df_log["prompt_answers_json"] == entry["prompt_answers_json"]).any():
                df_log.loc[len(df_log)] = entry
    except Exception:
        pass

    return {"path": path, "count": len(symbols)}


def load_symbols_list(dataframes, prompt_answers=None):
    """
    Loads a saved symbols CSV from Symbols/ (default) back into memory.

    Options via prompt_answers (all optional):
      - folder    : folder to read from (default 'Symbols')
      - filename  : specific filename to read (default: auto-pick newest *.csv)
      - target_df : key to store a DataFrame with the loaded symbols
                    (default 'df_symbols_loaded')

    Behavior:
      - If 'filename' isn't provided, it finds the most recently modified CSV
        in the folder and loads that.
      - Returns {'symbols': <list[str]>, 'path': <file_path>, 'count': <int>}
      - Also stores a DataFrame with a single 'Symbol' column under target_df.
    """
    import os
    import glob
    import json
    import pandas as pd

    if prompt_answers is None:
        prompt_answers = {}

    folder     = prompt_answers.get('folder', 'Symbols')
    filename   = prompt_answers.get('filename')  # may be None
    target_key = prompt_answers.get('target_df', 'df_symbols_loaded')

    # ---- pick file ----
    if filename:
        path = os.path.join(folder, filename)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
    else:
        # newest CSV in folder
        pattern = os.path.join(folder, "*.csv")
        candidates = glob.glob(pattern)
        if not candidates:
            raise FileNotFoundError(f"No CSV files found in folder: {folder}")

        # Sort newest → oldest for convenience
        candidates.sort(key=os.path.getmtime, reverse=True)

        def _fmt_time(ts: float) -> str:
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")

        def _fmt_size(bytes_: int) -> str:
            # human-ish, but keep it simple
            kb = bytes_ / 1024
            if kb < 1024:
                return f"{kb:.1f} KB"
            return f"{kb/1024:.2f} MB"

        print("\nChoose a file to load:")
        for i, p in enumerate(candidates, 1):
            mtime = os.path.getmtime(p)
            size = os.path.getsize(p)
            print(f"  {i}. {os.path.basename(p):<40}  { _fmt_time(mtime) }  { _fmt_size(size) }")

        # Prompt for index
        default_index = 1
        try:
            raw = input(f"\nEnter index 1–{len(candidates)} (default {default_index}): ").strip()
        except Exception:
            raw = ""  # non-interactive environments fall back to default

        if raw == "":
            idx = default_index - 1
        else:
            try:
                idx = int(raw) - 1
                if not (0 <= idx < len(candidates)):
                    raise ValueError
            except ValueError:
                raise ValueError(f"Invalid selection: {raw!r}. Expected 1–{len(candidates)}.")

        path = candidates[idx]


    # ---- read and normalize ----
    df = pd.read_csv(path)
    if 'Symbol' not in df.columns:
        # tolerate different casing or first column unnamed
        cols_lower = {c.lower(): c for c in df.columns}
        if 'symbol' in cols_lower:
            df = df.rename(columns={cols_lower['symbol']: 'Symbol'})
        else:
            # fallback: assume first column is symbols
            first = df.columns[0]
            df = df.rename(columns={first: 'Symbol'})

    df['Symbol'] = df['Symbol'].dropna().astype(str)
    symbols = sorted(pd.unique(df['Symbol']))

    # ---- store in dict ----
    dataframes[target_key] = pd.DataFrame({'Symbol': symbols})

    # ---- minimal action log ----
    try:
        entry = {
            "timestamp": pd.Timestamp.now(),
            "action": "load_symbols_list",
            "file_path": path,
            "loaded_rows": len(symbols),
            "stored_as": target_key,
            "prompt_answers_json": json.dumps(prompt_answers, default=str, sort_keys=True),
        }
        log_key = "df_actions"
        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
        else:
            df_log = dataframes[log_key]
            if "prompt_answers_json" not in df_log.columns:
                df_log["prompt_answers_json"] = ""
            if not (df_log["prompt_answers_json"] == entry["prompt_answers_json"]).any():
                df_log.loc[len(df_log)] = entry
    except Exception:
        pass

    
    if len(symbols) > 0:
        dataframes = apply_symbol_filter(dataframes, symbols)

    return dataframes

def add_ema(dataframes, prompt_answers=None):
    """
    Adds an Exponential Moving Average (EMA) column to a Dask DataFrame
    (default 'df_price_analysis'), computed per Symbol and sorted by Date.

    Tracks/reuses in `prompt_answers`:
      - dataframe  : which DF to use (default 'df_price_analysis')
      - period     : EMA window (int; default prompt 20 with options [10,20,50])
      - target_col : source column (e.g., 'Close')
    """
    import json
    import numpy as np
    import pandas as pd
    import dask.dataframe as dd

    # ---- prompt state ----
    if prompt_answers is None:
        prompt_answers = {}

    # 1) Pick dataframe (default + track)
    if 'dataframe' in prompt_answers:
        dataframe_name = prompt_answers['dataframe']    
    else:
        dataframe_name = prompt_user_dataframe(dataframes, 'Select dataframe to calculate the EMA in: ')  
    prompt_answers['dataframe'] = dataframe_name
    ddf = dataframes[dataframe_name]

    # 2) EMA period (reuse or prompt)
    if 'period' in prompt_answers:
        try:
            period = int(prompt_answers['period'])
        except Exception:
            raise ValueError(f"Invalid period in prompt_answers: {prompt_answers['period']}")
    else:
        period = int(get_input_parameter('EMA period', type(int), 20, [10, 20, 50]))
        prompt_answers['period'] = period

    # 3) Target column (reuse or prompt)
    if 'target_col' in prompt_answers:
        target_col = prompt_answers['target_col']
    else:
        target_col = prompt_user_column(ddf, 'Close')
        prompt_answers['target_col'] = target_col

    # ---- parameters / names ----
    date_col   = 'Date'
    symbol_col = 'Symbol'
    col_name   = f"EMA_{target_col}_{period}"

    # ---- basic checks ----
    columns = list(ddf.columns)
    needed = {symbol_col, date_col, target_col}
    missing = needed - set(columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    # ---- ensure Date is datetime (Dask-safe) ----
    ddf = ddf.assign(**{date_col: dd.to_datetime(ddf[date_col], errors="coerce")})

    # ---- per-group compute (pandas fn) ----
    def _compute_ema(pdf: pd.DataFrame) -> pd.DataFrame:
        write_line(f'Calculating {col_name} for {str(pdf[symbol_col].iloc[0])}')
        pdf = pdf.sort_values(date_col, kind="mergesort")
        s = pd.to_numeric(pdf[target_col], errors='coerce')
        pdf[col_name] = s.ewm(span=period, adjust=False).mean().round(2)
        return pdf

    # ---- meta with new column dtype ----
    meta = ddf._meta.assign(**{col_name: np.float64()})

    # ---- shuffle -> groupby.apply -> tidy ----
    out = (
        ddf.shuffle(symbol_col)
           .groupby(symbol_col)
           .apply(_compute_ema, meta=meta, include_groups=True,)
           .reset_index(drop=True)
           .map_partitions(lambda pdf: pdf.sort_values([symbol_col, date_col], kind="mergesort"))
    )

    # ---- update dict ----
    dataframes[dataframe_name] = out

    # ---- minimal action log (compact + dedup by prompt snapshot) ----
    try:
        log_key = "df_actions"
        entry = {
            "timestamp": pd.Timestamp.now(),
            "action": "add_ema",
            "dataframe": dataframe_name,
            "added_columns": [col_name],
            "prompt_answers_json": json.dumps(prompt_answers, default=str, sort_keys=True),
        }
        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
        else:
            df_log = dataframes[log_key]
            if "prompt_answers_json" not in df_log.columns:
                df_log["prompt_answers_json"] = ""
            if not (df_log["prompt_answers_json"] == entry["prompt_answers_json"]).any():
                df_log.loc[len(df_log)] = entry
    except Exception:
        # logging should never break the computation
        pass

    # ---- return ----
    return dataframes

def add_bollinger_bands(dataframes, prompt_answers=None):
    """
    Adds Bollinger Band columns to a Dask DataFrame (default 'df_price_analysis'),
    computed per Symbol and sorted by Date.

    Tracks/reuses in `prompt_answers`:
      - dataframe   : which DF to use (default prompt)
      - period      : rolling window (int; default 20; options [10,20,50])
      - std_mult    : standard-deviation multiple k (float; default 2.0; options [1.5,2.0,2.5])
      - target_col  : source column (e.g., 'Close')
      - add_pctb    : include %B column (bool; default True)
      - add_bandw   : include BandWidth column (bool; default True)
      - lag_days    : non-negative integer; shift outputs forward by this many rows per Symbol
                      (default 0 = no lag). Value at t then equals indicator from t - lag_days.

    Formulas:
      Middle = SMA(period)
      Std    = rolling std (ddof=1)
      Upper  = Middle + k * Std
      Lower  = Middle - k * Std
      %B     = (Price - Lower) / (Upper - Lower)
      BandWidth = (Upper - Lower) / Middle
    """
    import json
    import numpy as np
    import pandas as pd
    import dask.dataframe as dd

    if prompt_answers is None:
        prompt_answers = {}

    # 1) Pick dataframe
    if 'dataframe' in prompt_answers:
        dataframe_name = prompt_answers['dataframe']
    else:
        dataframe_name = prompt_user_dataframe(dataframes, 'Select dataframe to calculate Bollinger Bands in: ')
    prompt_answers['dataframe'] = dataframe_name
    ddf = dataframes[dataframe_name]

    # 2) Period
    if 'period' in prompt_answers:
        period = int(prompt_answers['period'])
    else:
        period = int(get_input_parameter('Bollinger period', type(int), 20, [10, 20, 50]))
        prompt_answers['period'] = period

    # 3) Std multiple k
    if 'std_mult' in prompt_answers:
        std_mult = float(prompt_answers['std_mult'])
    else:
        std_mult = float(get_input_parameter('Std multiple (k)', float, 2.0, [1.5, 2.0, 2.5]))
        prompt_answers['std_mult'] = std_mult

    # 4) Target column
    if 'target_col' in prompt_answers:
        target_col = prompt_answers['target_col']
    else:
        target_col = prompt_user_column(ddf, 'Close')
        prompt_answers['target_col'] = target_col

    # 5) Optional outputs
    if 'add_pctb' in prompt_answers:
        add_pctb = bool(prompt_answers['add_pctb'])
    else:
        add_pctb = True
        prompt_answers['add_pctb'] = add_pctb

    if 'add_bandw' in prompt_answers:
        add_bandw = bool(prompt_answers['add_bandw'])
    else:
        add_bandw = True
        prompt_answers['add_bandw'] = add_bandw

    # 6) Lag (days/rows)
    if 'lag_days' in prompt_answers:
        lag_days = int(prompt_answers['lag_days'])
    else:
        lag_days = int(get_input_parameter('Lag days (shift outputs forward)', type(int), 0, [0, 1, 2, 5]))
        prompt_answers['lag_days'] = lag_days
    if lag_days < 0:
        raise ValueError("lag_days must be >= 0")

    # ---- parameters / names ----
    date_col   = 'Date'
    symbol_col = 'Symbol'
    k_str = f"{str(std_mult).rstrip('0').rstrip('.')}"  # "2" or "1.5"

    col_mid  = f"BB_Middle_{target_col}_{period}"
    col_up   = f"BB_Upper_{target_col}_{period}_{k_str}x"
    col_low  = f"BB_Lower_{target_col}_{period}_{k_str}x"
    col_pctb = f"BB_PctB_{target_col}_{period}_{k_str}x"
    col_bw   = f"BB_BandWidth_{target_col}_{period}_{k_str}x"

    # ---- checks ----
    columns = list(ddf.columns)
    needed = {'Symbol', 'Date', target_col}
    missing = needed - set(columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    # ---- ensure Date is datetime (Dask-safe) ----
    ddf = ddf.assign(**{date_col: dd.to_datetime(ddf[date_col], errors="coerce")})

    # ---- per-group compute (pandas fn) ----
    def _compute_bbands(pdf: pd.DataFrame) -> pd.DataFrame:
        sym = str(pdf[symbol_col].iloc[0]) if not pdf.empty else "<NA>"
        # write_line(f'Calculating Bollinger Bands for {sym}')
        pdf = pdf.sort_values(date_col, kind="mergesort")

        s = pd.to_numeric(pdf[target_col], errors='coerce')

        mid = s.rolling(window=period, min_periods=period).mean()
        std = s.rolling(window=period, min_periods=period).std(ddof=1)
        up  = mid + std_mult * std
        low = mid - std_mult * std

        # Optional %B and BandWidth BEFORE lag
        pctb = bw = None
        if add_pctb:
            denom = (up - low)
            with np.errstate(divide='ignore', invalid='ignore'):
                pctb = (s - low) / denom
        if add_bandw:
            with np.errstate(divide='ignore', invalid='ignore'):
                bw = (up - low) / mid

        # Apply lag by shifting outputs forward (aligning value at t with t - lag_days)
        if lag_days:
            mid = mid.shift(lag_days)
            up  = up.shift(lag_days)
            low = low.shift(lag_days)
            if pctb is not None:
                pctb = pctb.shift(lag_days)
            if bw is not None:
                bw = bw.shift(lag_days)

        pdf[col_mid] = mid.round(4)
        pdf[col_up]  = up.round(4)
        pdf[col_low] = low.round(4)
        if add_pctb:
            pdf[col_pctb] = pctb.astype(float)
        if add_bandw:
            pdf[col_bw] = bw.astype(float)

        return pdf

    # ---- meta with new columns ----
    meta_add = {
        col_mid:  np.float64(),
        col_up:   np.float64(),
        col_low:  np.float64(),
    }
    if add_pctb:
        meta_add[col_pctb] = np.float64()
    if add_bandw:
        meta_add[col_bw] = np.float64()
    meta = ddf._meta.assign(**meta_add)

    # ---- shuffle -> groupby.apply -> tidy ----
    out = (
        ddf.shuffle(symbol_col)
           .groupby(symbol_col)
           .apply(_compute_bbands, meta=meta, include_groups=True)
           .reset_index(drop=True)
           .map_partitions(lambda pdf: pdf.sort_values([symbol_col, date_col], kind="mergesort"))
    )

    dataframes[dataframe_name] = out

    # ---- compact action log ----
    try:
        log_key = "df_actions"
        entry = {
            "timestamp": pd.Timestamp.now(),
            "action": "add_bollinger_bands",
            "dataframe": dataframe_name,
            "added_columns": [col_mid, col_up, col_low] +
                             ([col_pctb] if add_pctb else []) +
                             ([col_bw] if add_bandw else []),
            "prompt_answers_json": json.dumps(prompt_answers, default=str, sort_keys=True),
        }
        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
        else:
            df_log = dataframes[log_key]
            if "prompt_answers_json" not in df_log.columns:
                df_log["prompt_answers_json"] = ""
            if not (df_log["prompt_answers_json"] == entry["prompt_answers_json"]).any():
                df_log.loc[len(df_log)] = entry
    except Exception:
        pass

    return dataframes


def add_dv(dataframes, prompt_answers=None):
    """
    Adds a Dollar Volume column to a Dask DataFrame (default 'df_price_analysis').
    Dollar Volume = <price_col> * <volume_col>

    Tracks/reuses in `prompt_answers`:
      - dataframe   : which DF to use (default 'df_price_analysis')
      - price_col   : source price column (default 'Close')
      - volume_col  : source volume column (default 'Volume')
      - out_col     : output column name (default 'Dollar_Volume')
      - round_digits: rounding digits (int; default 2)
    """
    import json
    import pandas as pd
    import dask.dataframe as dd

    if prompt_answers is None:
        prompt_answers = {}

    # 1) Pick dataframe (default + track)
    dataframe_name = prompt_answers.get('dataframe', 'df_price_analysis')
    if dataframe_name not in dataframes:
        raise KeyError(f"DataFrame '{dataframe_name}' not found in dataframes.")
    prompt_answers['dataframe'] = dataframe_name
    ddf = dataframes[dataframe_name]

    price_col = 'Close'
    volume_col = 'Volume'
    out_col = 'Dollar_Volume'
    round_digits = 2

    # 3) Basic checks
    needed = {price_col, volume_col}
    missing = needed - set(ddf.columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    # 4) Compute Dollar Volume (coerce numeric to be safe)
    s_price = dd.to_numeric(ddf[price_col], errors='coerce')
    s_vol   = dd.to_numeric(ddf[volume_col], errors='coerce')
    dv = (s_price * s_vol)
    if isinstance(round_digits, int) and round_digits >= 0:
        dv = dv.round(round_digits)

    ddf = ddf.assign(**{out_col: dv})
    dataframes[dataframe_name] = ddf

    # 5) Minimal action log (compact + dedup by prompt snapshot)
    try:
        log_key = "df_actions"
        entry = {
            "timestamp": pd.Timestamp.now(),
            "action": "add_dv",
            "dataframe": dataframe_name,
            "added_columns": [out_col],
            "prompt_answers_json": json.dumps(prompt_answers, default=str, sort_keys=True),
        }
        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
        else:
            df_log = dataframes[log_key]
            if "prompt_answers_json" not in df_log.columns:
                df_log["prompt_answers_json"] = ""
            if not (df_log["prompt_answers_json"] == entry["prompt_answers_json"]).any():
                df_log.loc[len(df_log)] = entry
    except Exception:
        # logging should never break the computation
        pass

    # 6) Return
    return dataframes

def add_macd(dataframes, prompt_answers=None):
    """
    Adds MACD, Signal, and Histogram columns to a Dask DataFrame
    (default 'df_price_analysis'), computed per Symbol and sorted by Date.

    Tracks/reuses in `prompt_answers`:
      - dataframe   : which DF to use (default 'df_price_analysis')
      - target_col  : source column (e.g., 'Close')
      - short_span  : short EMA span (int; default 12)
      - long_span   : long  EMA span (int; default 26)
      - signal_span : signal EMA span (int; default 9)
    """
    import json
    import numpy as np
    import pandas as pd
    import dask.dataframe as dd

    if prompt_answers is None:
        prompt_answers = {}

    # 1) Pick dataframe (default + track)
    if 'dataframe' in prompt_answers:
        dataframe_name = prompt_answers['dataframe']
    else:
        dataframe_name = prompt_user_dataframe(dataframes, 'Select dataframe to apply MACD: ')
    prompt_answers['dataframe'] = dataframe_name
    ddf = dataframes[dataframe_name]
    target_col = 'Close'

    # 3) Spans (reuse or prompt)
    def _get_int(pa_key, label, default_val, options):
        if pa_key in prompt_answers:
            return int(prompt_answers[pa_key])
        val = int(get_input_parameter(label, type(int), default_val, options))
        prompt_answers[pa_key] = val
        return val

    short_span  = _get_int('short_span',  'Short span',  12, [12])
    long_span   = _get_int('long_span',   'Long span',   26, [26])
    signal_span = _get_int('signal_span', 'Signal span',  9, [9])

    # ---- parameters / names ----
    date_col   = 'Date'
    symbol_col = 'Symbol'
    macd_col   = f"MACD_{target_col}_{short_span}_{long_span}"
    signal_col = f"MACD_{target_col}_Signal_{signal_span}"
    hist_col   = f"MACD_{target_col}_Hist_{short_span}_{long_span}_{signal_span}"
    calculation_name = f"MACD_{target_col} - {short_span}, {long_span}, {signal_span}"

    # ---- basic checks ----
    columns = list(ddf.columns)
    needed = {symbol_col, date_col, target_col}
    missing = needed - set(columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    # ---- ensure Date is datetime (Dask-safe) ----
    ddf = ddf.assign(**{date_col: dd.to_datetime(ddf[date_col], errors="coerce")})

    # ---- per-group compute (pandas fn) ----
    def _compute_macd(pdf: pd.DataFrame) -> pd.DataFrame:
        write_line(f'Calculating {calculation_name} for {str(pdf[symbol_col].iloc[0])}')
        pdf = pdf.sort_values(date_col, kind="mergesort")
        s = pd.to_numeric(pdf[target_col], errors='coerce')
        short_ema = s.ewm(span=short_span, adjust=False).mean()
        long_ema  = s.ewm(span=long_span,  adjust=False).mean()
        macd = short_ema - long_ema
        signal = macd.ewm(span=signal_span, adjust=False).mean()
        pdf[macd_col]   = macd.round(4)
        pdf[signal_col] = signal.round(4)
        pdf[hist_col]   = (macd - signal).round(4)
        return pdf

    # ---- meta with new column dtypes ----
    meta = ddf._meta.assign(**{
        macd_col:   np.float64(),
        signal_col: np.float64(),
        hist_col:   np.float64(),
    })

    # ---- shuffle -> groupby.apply -> tidy ----
    out = (
        ddf.shuffle(symbol_col)
           .groupby(symbol_col)
           .apply(_compute_macd, meta=meta, include_groups=True,)
           .reset_index(drop=True)
           .map_partitions(lambda pdf: pdf.sort_values([symbol_col, date_col], kind="mergesort"))
    )

    # ---- update dict ----
    dataframes[dataframe_name] = out

    # ---- minimal action log (compact + dedup by prompt snapshot) ----
    try:
        log_key = "df_actions"
        entry = {
            "timestamp": pd.Timestamp.now(),
            "action": "add_macd",
            "dataframe": dataframe_name,
            "added_columns": [macd_col, signal_col, hist_col],
            "prompt_answers_json": json.dumps(prompt_answers, default=str, sort_keys=True),
        }
        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
        else:
            df_log = dataframes[log_key]
            if "prompt_answers_json" not in df_log.columns:
                df_log["prompt_answers_json"] = ""
            if not (df_log["prompt_answers_json"] == entry["prompt_answers_json"]).any():
                df_log.loc[len(df_log)] = entry
    except Exception:
        # logging should never break the computation
        pass

    # ---- return ----
    return dataframes

def materialize_dataframe(dataframes, prompt_answers=None):
    """
    Materialize a dataframe from the `dataframes` dict by either:
      - persisting it on the Dask cluster (stays a Dask object), and/or
      - computing it to a local pandas object and storing alongside.

    Menus:
      - Dataframe selection supports number or name.
      - Action selection supports number (1/2/3), full name, or single-letter alias.

    Tracks/reuses in `prompt_answers`:
      - dataframe          : name OR number (1-based) in `dataframes` (default 'df_price_analysis')
      - action             : 'persist' | 'compute' | 'both'  (default 'persist')
      - out_name_persist   : key to store persisted Dask object (default = dataframe)
      - out_name_pandas    : key to store computed pandas object (default = f"{dataframe}_pd")
      - wait_for_persist   : bool, wait on cluster after persist (default True)

    Returns:
      The same `dataframes` dict, updated in-place.
    """
    import json
    import pandas as pd
    import dask.dataframe as dd

    # ---------- helpers ----------
    def _log(msg):
        try:
            write_line(msg)  # your logger, if available
        except Exception:
            pass

    def _ask(name, typ, default, options=None):
        # Uses your get_input_parameter if present; otherwise returns default
        try:
            return get_input_parameter(name, typ, default, options)
        except Exception:
            return default

    def _desc(obj):
        try:
            if isinstance(obj, dd.DataFrame):
                return f"dask.DataFrame nparts={obj.npartitions}"
            if isinstance(obj, dd.Series):
                return f"dask.Series nparts={obj.npartitions}"
        except Exception:
            pass
        if isinstance(obj, pd.DataFrame):
            return "pandas.DataFrame"
        if isinstance(obj, pd.Series):
            return "pandas.Series"
        return type(obj).__name__

    def _normalize_action(sel, default_action='persist'):
        """
        Accepts:
          - number: 1 (persist), 2 (compute), 3 (both)
          - name: 'persist', 'compute', 'both'
          - alias: 'p', 'c', 'b'
        Returns normalized action string.
        """
        mapping_num = {1: 'persist', 2: 'compute', 3: 'both'}
        mapping_alias = {
            'p': 'persist',
            'persist': 'persist',
            '1': 'persist',
            'c': 'compute',
            'compute': 'compute',
            '2': 'compute',
            'b': 'both',
            'both': 'both',
            '3': 'both',
        }
        if isinstance(sel, int):
            return mapping_num.get(sel, default_action)
        s = str(sel).strip().lower()
        return mapping_alias.get(s, default_action)

    if prompt_answers is None:
        prompt_answers = {}

    # ---------- dataframe menu ----------
    names = list(dataframes.keys())
    default_df = prompt_answers.get('dataframe', 'df_price_analysis')
    default_idx = names.index(default_df) + 1 if default_df in names else 1

    _log("Available dataframes:")
    for i, name in enumerate(names, 1):
        info = _desc(dataframes[name])
        prefix = "->" if i == default_idx else "  "
        _log(f"{prefix} {i}. {name} [{info}]")

    # Selection (number or name)
    if 'dataframe' in prompt_answers:
        sel_df = prompt_answers['dataframe']
    else:
        sel_df = _ask("Select dataframe (enter number or name)", str, str(default_idx), options=None)

    if isinstance(sel_df, int) or (isinstance(sel_df, str) and sel_df.isdigit()):
        idx = int(sel_df) - 1
        if idx < 0 or idx >= len(names):
            raise KeyError(f"Selection {sel_df} out of range 1..{len(names)}.")
        dataframe_name = names[idx]
    else:
        dataframe_name = str(sel_df)
        if dataframe_name not in dataframes:
            raise KeyError(f"DataFrame '{dataframe_name}' not found in dataframes.")

    prompt_answers['dataframe'] = dataframe_name
    obj = dataframes[dataframe_name]
    is_dask_df = isinstance(obj, (dd.DataFrame, dd.Series))

    # ---------- action menu (now with numbers) ----------
    actions = [('persist', 'Persist on workers'),
               ('compute', 'Compute to pandas (local)'),
               ('both', 'Persist & Compute')]
    default_action = prompt_answers.get('action', 'persist')
    try:
        default_action_idx = [a for a, _ in actions].index(default_action) + 1
    except ValueError:
        default_action_idx = 1

    _log("Choose action:")
    for i, (a, desc) in enumerate(actions, 1):
        prefix = "->" if i == default_action_idx else "  "
        _log(f"{prefix} {i}. {a:<7} - {desc}")

    if 'action' in prompt_answers:
        sel_action = prompt_answers['action']
    else:
        sel_action = _ask("Action (1=persist, 2=compute, 3=both or name/alias)", str, str(default_action_idx), options=None)

    action = _normalize_action(sel_action, default_action)
    prompt_answers['action'] = action

    # ---------- output names & wait flag ----------
    out_name_persist = prompt_answers.get('out_name_persist', dataframe_name)
    out_name_pandas  = prompt_answers.get('out_name_pandas', f"{dataframe_name}_pd")
    wait_for_persist = bool(prompt_answers.get('wait_for_persist', True))
    prompt_answers['out_name_persist'] = out_name_persist
    prompt_answers['out_name_pandas']  = out_name_pandas
    prompt_answers['wait_for_persist'] = wait_for_persist

    # ---------- do the work ----------
    persisted_obj = None
    computed_obj  = None

    if action in ('persist', 'both'):
        if is_dask_df:
            _log(f"Persisting '{dataframe_name}' -> '{out_name_persist}'")
            persisted_obj = obj.persist()
            if wait_for_persist:
                try:
                    from dask.distributed import wait, get_client
                    _ = get_client()
                    wait(persisted_obj)
                except Exception:
                    pass
            dataframes[out_name_persist] = persisted_obj
        else:
            _log(f"'{dataframe_name}' is already a pandas object; 'persist' is a no-op.")
            dataframes[out_name_persist] = obj

    if action in ('compute', 'both'):
        _log(f"Computing '{dataframe_name}' -> '{out_name_pandas}' (pandas)")
        if is_dask_df:
            computed_obj = obj.compute()
        else:
            computed_obj = obj.copy() if hasattr(obj, "copy") else obj
        dataframes[out_name_pandas] = computed_obj

    # ---------- action log ----------
    try:
        entry = {
            "timestamp": pd.Timestamp.now(),
            "action": "materialize_dataframe",
            "dataframe": dataframe_name,
            "is_dask_input": bool(is_dask_df),
            "did_persist": bool(persisted_obj is not None) or (action == 'persist' and not is_dask_df),
            "did_compute": bool(computed_obj is not None) or (action == 'compute' and not is_dask_df),
            "out_name_persist": out_name_persist,
            "out_name_pandas": out_name_pandas,
            "prompt_answers_json": json.dumps(prompt_answers, default=str, sort_keys=True),
        }
        log_key = "df_actions"
        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
        else:
            df_log = dataframes[log_key]
            if "prompt_answers_json" not in df_log.columns:
                df_log["prompt_answers_json"] = ""
            if not (df_log["prompt_answers_json"] == entry["prompt_answers_json"]).any():
                dataframes[log_key].loc[len(dataframes[log_key])] = entry
    except Exception:
        pass

    return dataframes

def add_atr(dataframes, prompt_answers=None):
    """
    Adds an Average True Range (ATR) column to the Dask DataFrame (default 'df_price_analysis'),
    computed per Symbol and sorted by Date, using Wilder's smoothing.

    Tracks/reuses in `prompt_answers`:
      - dataframe    : which DF to use (default 'df_price_analysis')
      - period       : ATR window (int; default 14)
      - high_col     : source high column (default prompt -> 'High')
      - low_col      : source low column  (default prompt -> 'Low')
      - close_col    : source close col   (default prompt -> 'Close')
      - out_col      : optional explicit output column name (default f'ATR_{period}')
      - round_digits : rounding digits (int; default 2)
    """
    import json
    import numpy as np
    import pandas as pd
    import dask.dataframe as dd

    # ---- prompt state ----
    if prompt_answers is None:
        prompt_answers = {}

    # 1) Pick dataframe (default + track)
    if 'dataframe' in prompt_answers:
        dataframe_name = prompt_answers['dataframe']
    else:
        dataframe_name = prompt_user_dataframe(dataframes, "Select dataframe to add ATR to: " )
    prompt_answers['dataframe'] = dataframe_name
    ddf = dataframes[dataframe_name]

    # 2) ATR period (reuse or prompt; default 14)
    if 'period' in prompt_answers:
        try:
            period = int(prompt_answers['period'])
        except Exception:
            raise ValueError(f"Invalid period in prompt_answers: {prompt_answers['period']}")
    else:
        period = int(get_input_parameter('ATR period', type(int), 14, [14]))
        prompt_answers['period'] = period

    high_col = 'High'
    low_col = 'Low'
    close_col = 'Close'

    # 4) Output name + rounding
    if 'out_col' in prompt_answers and prompt_answers['out_col']:
        col_name = prompt_answers['out_col']
    else:
        col_name = f"ATR_{period}"
        prompt_answers['out_col'] = col_name

    round_digits = int(prompt_answers.get('round_digits', 2))
    prompt_answers['round_digits'] = round_digits

    # ---- constants ----
    date_col   = 'Date'
    symbol_col = 'Symbol'

    # ---- basic checks ----
    columns = list(ddf.columns)
    needed = {symbol_col, date_col, high_col, low_col, close_col}
    missing = needed - set(columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    # ---- ensure Date is datetime (Dask-safe) ----
    ddf = ddf.assign(**{date_col: dd.to_datetime(ddf[date_col], errors="coerce")})

    # ---- per-group compute (pandas fn) ----
    def _compute_atr(pdf: pd.DataFrame) -> pd.DataFrame:
        sym = str(pdf[symbol_col].iloc[0]) if len(pdf) else "<empty>"
        write_line(f'Computing {col_name} for {sym}')
        pdf = pdf.sort_values(date_col, kind="mergesort")

        hi = pd.to_numeric(pdf[high_col], errors='coerce')
        lo = pd.to_numeric(pdf[low_col], errors='coerce')
        cl = pd.to_numeric(pdf[close_col], errors='coerce')

        prev_close = cl.shift(1)

        # True Range: max(H-L, |H-prevC|, |L-prevC|)
        tr_components = pd.concat(
            [(hi - lo).abs(),
             (hi - prev_close).abs(),
             (lo - prev_close).abs()],
            axis=1
        )
        tr = tr_components.max(axis=1, skipna=True)

        # Wilder's ATR via EWM with alpha=1/period; first ATR appears at index >= period
        atr = tr.ewm(alpha=(1.0 / period), adjust=False, min_periods=period).mean()

        pdf[col_name] = atr.round(round_digits)
        return pdf

    # ---- meta with new column dtype ----
    meta = ddf._meta.assign(**{col_name: np.float64()})

    # ---- shuffle -> groupby.apply -> tidy ----
    out = (
        ddf.shuffle(symbol_col)
           .groupby(symbol_col)
           .apply(_compute_atr, meta=meta, include_groups=True)
           .reset_index(drop=True)
           .map_partitions(lambda pdf: pdf.sort_values([symbol_col, date_col], kind="mergesort"))
    )

    # ---- update dict ----
    dataframes[dataframe_name] = out

    # ---- minimal action log (compact + dedup by prompt snapshot) ----
    try:
        log_key = "df_actions"
        entry = {
            "timestamp": pd.Timestamp.now(),
            "action": "add_atr",
            "dataframe": dataframe_name,
            "added_columns": [col_name],
            "prompt_answers_json": json.dumps(prompt_answers, default=str, sort_keys=True),
        }
        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
        else:
            df_log = dataframes[log_key]
            if "prompt_answers_json" not in df_log.columns:
                df_log["prompt_answers_json"] = ""
            if not (df_log["prompt_answers_json"] == entry["prompt_answers_json"]).any():
                df_log.loc[len(df_log)] = entry
    except Exception:
        # logging should never break the computation
        pass

    return dataframes

def add_crossover_proximity(dataframes, prompt_answers=None):
    """
    Adds bullish/bearish crossover proximity columns to a Dask DataFrame
    (default 'df_price_analysis'), computed per Symbol and sorted by Date.

    Tracks/reuses in `prompt_answers`:
      - dataframe   : which DF to use (default 'df_price_analysis')
      - bullish_col : column for bullish side of comparison
      - bearish_col : column for bearish side of comparison
    """
    import json
    import numpy as np
    import pandas as pd
    import dask.dataframe as dd

    # ---- prompt state ----
    if prompt_answers is None:
        prompt_answers = {}

    # 1) Pick dataframe (default + track)
    if 'dataframe' in prompt_answers:
        dataframe_name = prompt_answers['dataframe']
    else:
        dataframe_name = prompt_user_dataframe(dataframes, 'Select dataframe to calculate crossover proximity on: ')
    prompt_answers['dataframe'] = dataframe_name
    ddf = dataframes[dataframe_name]

    # 2) Choose columns (reuse or prompt)
    if 'bullish_col' in prompt_answers:
        bullish_col = prompt_answers['bullish_col']
    else:
        write_line('Choose bullish indicator')
        bullish_col = prompt_user_column(ddf, None)
        prompt_answers['bullish_col'] = bullish_col

    if 'bearish_col' in prompt_answers:
        bearish_col = prompt_answers['bearish_col']
    else:
        write_line('Choose bearish indicator')
        bearish_col = prompt_user_column(ddf, None)
        prompt_answers['bearish_col'] = bearish_col

    # ---- parameters / names ----
    date_col   = 'Date'
    symbol_col = 'Symbol'
    calc_bull  = f'Bullish_Crossover_Proximity - {bullish_col}, {bearish_col}'
    calc_bear  = f'Bearish_Crossover_Proximity - {bullish_col}, {bearish_col}'
    last_bull  = f'Last_Bullish_Crossover_Date - {bullish_col}, {bearish_col}'
    last_bear  = f'Last_Bearish_Crossover_Date - {bullish_col}, {bearish_col}'
    calculation_name = f'Crossover Proximity - {bullish_col}, {bearish_col}'

    # ---- basic checks ----
    columns = list(ddf.columns)
    needed = {symbol_col, date_col, bullish_col, bearish_col}
    missing = needed - set(columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    # ---- ensure Date is datetime (Dask-safe) ----
    ddf = ddf.assign(**{date_col: dd.to_datetime(ddf[date_col], errors="coerce")})

    # ---- per-group compute (pandas fn) ----
    def _compute_crossover_proximity(pdf: pd.DataFrame) -> pd.DataFrame:
        write_line(f'Calculating {calculation_name} for {str(pdf[symbol_col].iloc[0])}')
        pdf = pdf.sort_values(date_col, kind="mergesort")

        # Coerce indicators to numeric to avoid string-comparison bugs
        b_up = pd.to_numeric(pdf[bullish_col], errors='coerce')
        b_dn = pd.to_numeric(pdf[bearish_col], errors='coerce')

        # 1) Identify crossovers
        bull_x = (b_up > b_dn) & (b_up.shift(1) <= b_dn.shift(1))
        bear_x = (b_up < b_dn) & (b_up.shift(1) >= b_dn.shift(1))

        # 2) Forward-fill last event date
        pdf[date_col] = pd.to_datetime(pdf[date_col], errors='coerce')
        pdf[last_bull] = pd.Series(pd.NaT, index=pdf.index, dtype='datetime64[ns]')
        pdf[last_bear] = pd.Series(pd.NaT, index=pdf.index, dtype='datetime64[ns]')
        pdf[last_bull] = pdf[last_bull].where(~bull_x, pdf[date_col]).ffill()
        pdf[last_bear] = pdf[last_bear].where(~bear_x, pdf[date_col]).ffill()

        # 3) Days since each event (float for consistency across gaps)
        pdf[calc_bull] = (pdf[date_col] - pdf[last_bull]).dt.days.astype('float64')
        pdf[calc_bear] = (pdf[date_col] - pdf[last_bear]).dt.days.astype('float64')
        pdf = pdf.drop(columns=[last_bull, last_bear])
        return pdf

    # ---- meta with new column dtypes ----
    # Use NaT scalars for datetime64[ns] in meta; float64 for proximities
    meta = ddf._meta.assign(**{
        calc_bull: np.float64(),
        calc_bear: np.float64()
    })

    # ---- shuffle -> groupby.apply -> tidy ----
    out = (
        ddf.shuffle(symbol_col)
           .groupby(symbol_col)
           .apply(_compute_crossover_proximity, meta=meta, include_groups=True,)
           .reset_index(drop=True)
           .map_partitions(lambda pdf: pdf.sort_values([symbol_col, date_col], kind="mergesort"))
    )

    # ---- update dict ----
    dataframes[dataframe_name] = out

    # ---- minimal action log (compact + dedup by prompt snapshot) ----
    try:
        log_key = "df_actions"
        entry = {
            "timestamp": pd.Timestamp.now(),
            "action": "add_crossover_proximity",
            "dataframe": dataframe_name,
            "added_columns": [calc_bull, calc_bear, last_bull, last_bear],
            "prompt_answers_json": json.dumps(prompt_answers, default=str, sort_keys=True),
        }
        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
        else:
            df_log = dataframes[log_key]
            if "prompt_answers_json" not in df_log.columns:
                df_log["prompt_answers_json"] = ""
            if not (df_log["prompt_answers_json"] == entry["prompt_answers_json"]).any():
                df_log.loc[len(df_log)] = entry
    except Exception:
        # logging should never break the computation
        pass

    # ---- return ----
    return dataframes

def add_vwap(dataframes):
    import pandas as pd
    try:
        import dask.dataframe as dd
    except Exception:
        dd = None

    # tiny helper: if any index level name collides with a column name -> reset (drop=True)
    def _reset_drop_if_indexname_in_columns(_df):
        idx = getattr(_df, "index", None)
        if idx is None:
            return _df
        names = []
        if hasattr(idx, "names") and idx.names:
            names = [n for n in idx.names if n is not None]
        elif hasattr(idx, "name") and idx.name is not None:
            names = [idx.name]
        if any(n in getattr(_df, "columns", []) for n in names):
            return _df.reset_index(drop=True)
        return _df

    # Prompt the user for the VWAP period
    try:
        period = int(input("Enter the VWAP rolling period (e.g., 20): ").strip())
        if period <= 0:
            print("Invalid period. Using default of 20.")
            period = 20
    except ValueError:
        print("Invalid input. Using default of 20.")
        period = 20

    dataframe_name = 'df_price_analysis'
    df = dataframes[dataframe_name]
    col_name = f"VWAP_{period}"

    # ensure df doesn’t have key-name collisions between index and columns
    df = _reset_drop_if_indexname_in_columns(df)

    is_dask = dd is not None and isinstance(df, dd.DataFrame)
    groupby_col = 'Symbol' if 'Symbol' in df.columns else None
    date_col = 'Date' if 'Date' in df.columns else None

    # --- DASK PATH (no known-divisions rolling; per-group apply + merge) ---
    if is_dask:
        to_num = lambda s: dd.to_numeric(s, errors="coerce")
        high = to_num(df['High']); low = to_num(df['Low']); close = to_num(df['Close']); vol = to_num(df['Volume'])
        tp = (high + low + close) / 3
        tpv = tp * vol

        cols = [c for c in [groupby_col] if c]
        tmp = df[cols].assign(_tpv=tpv, _vol=vol)
        if date_col:
            tmp = tmp.assign(**{date_col: dd.to_datetime(df[date_col], errors='coerce')})

        def _vwap_pdf(pdf: pd.DataFrame) -> pd.DataFrame:
            if date_col and date_col in pdf.columns:
                pdf = pdf.sort_values(date_col, kind="mergesort")
            numer = pd.to_numeric(pdf['_tpv'], errors='coerce').rolling(window=period, min_periods=period).sum()
            denom = pd.to_numeric(pdf['_vol'],  errors='coerce').rolling(window=period, min_periods=period).sum()
            pdf['_vwap'] = (numer / denom).astype('float64')
            keep = []
            if groupby_col and groupby_col in pdf.columns: keep.append(groupby_col)
            if date_col and date_col in pdf.columns:       keep.append(date_col)
            keep.append('_vwap')
            return pdf[keep]

        # meta for apply
        meta_cols = {'_vwap': pd.Series(dtype='float64')}
        if groupby_col: meta_cols[groupby_col] = pd.Series(dtype=df[groupby_col].dtype if hasattr(df[groupby_col], "dtype") else "object")
        if date_col:    meta_cols[date_col]    = pd.Series(dtype="datetime64[ns]")
        ordered = {}
        if groupby_col: ordered[groupby_col] = meta_cols[groupby_col]
        if date_col:    ordered[date_col]    = meta_cols[date_col]
        ordered['_vwap'] = meta_cols['_vwap']
        meta_df = pd.DataFrame(ordered)

        if groupby_col:
            rolled = (
                tmp.shuffle(groupby_col)
                   .groupby(groupby_col)
                   .apply(_vwap_pdf, meta=meta_df)
            )
        else:
            grp = '_grp__'
            tmp = tmp.assign(**{grp: 1})
            meta_df2 = pd.DataFrame({'_vwap': pd.Series(dtype='float64')})
            rolled = (
                tmp.shuffle(grp)
                   .groupby(grp)
                   .apply(lambda pdf: _vwap_pdf(pdf.drop(columns=[grp], errors='ignore')),
                          meta=meta_df2)
            )

        # make sure the rolled side also has no index/column name collisions
        rolled = _reset_drop_if_indexname_in_columns(rolled)

        # Ensure Date dtype matches before merge
        if date_col:
            df = df.assign(**{date_col: dd.to_datetime(df[date_col], errors='coerce')})

        if groupby_col and date_col:
            df = df.merge(rolled.rename(columns={'_vwap': col_name}), on=[groupby_col, date_col], how='left')
        elif groupby_col:
            df = df.merge(rolled.rename(columns={'_vwap': col_name}), on=[groupby_col], how='left')
        elif date_col:
            df = df.merge(rolled.rename(columns={'_vwap': col_name}), on=[date_col], how='left')
        else:
            df = df.merge(rolled.rename(columns={'_vwap': col_name})[[col_name]],
                          left_index=True, right_index=True, how='left')

        df = df.assign(**{col_name: dd.to_numeric(df[col_name], errors='coerce').round(2)})

    # --- PANDAS PATH ---
    else:
        gdf = df.copy()
        if date_col:
            gdf[date_col] = pd.to_datetime(gdf[date_col], errors='coerce')
        tp = (pd.to_numeric(gdf['High'], errors='coerce')
              + pd.to_numeric(gdf['Low'], errors='coerce')
              + pd.to_numeric(gdf['Close'], errors='coerce')) / 3.0
        tpv = tp * pd.to_numeric(gdf['Volume'], errors='coerce')

        if groupby_col:
            def _vw(pdf: pd.DataFrame) -> pd.Series:
                if date_col and date_col in pdf.columns:
                    pdf = pdf.sort_values(date_col, kind="mergesort")
                numer = ((pd.to_numeric(pdf['High'], errors='coerce')
                         + pd.to_numeric(pdf['Low'], errors='coerce')
                         + pd.to_numeric(pdf['Close'], errors='coerce')) / 3.0
                         * pd.to_numeric(pdf['Volume'], errors='coerce')).rolling(window=period, min_periods=period).sum()
                denom = pd.to_numeric(pdf['Volume'], errors='coerce').rolling(window=period, min_periods=period).sum()
                return (numer / denom)
            df[col_name] = gdf.groupby(groupby_col, group_keys=False).apply(_vw).round(2)
        else:
            numer = tpv.rolling(window=period, min_periods=period).sum()
            denom = pd.to_numeric(gdf['Volume'], errors='coerce').rolling(window=period, min_periods=period).sum()
            df[col_name] = (numer / denom).round(2)

    dataframes[dataframe_name] = df
    return dataframes

def get_performance_metrics(dataframes):
    try:
        write_line("Get Performance Metrics with Dask selected.")
        
        # 1. Verify input
        if 'df_price_analysis' not in dataframes:
            write_line("Error: 'df_price_analysis' not found. Aborting calculation.")
            return
        
        # 2. Convert pandas → Dask
        ddf_price_analysis = dataframes['df_price_analysis']
        min_date, max_date = dask.compute(
            ddf_price_analysis['Date'].min(),
            ddf_price_analysis['Date'].max()
        )
                
        # 5. Prompt for a valid date range
        while True:
            start_str = input(f"Enter start date (YYYY‑MM‑DD) between {min_date} and {max_date}: ")
            end_str   = input(f"Enter   end date (YYYY‑MM‑DD) between {min_date} and {max_date}: ")
            try:
                start_ts = pd.to_datetime(start_str)
                end_ts   = pd.to_datetime(end_str)
                if not (min_date <= start_ts <= end_ts <= max_date):
                    raise ValueError
                break
            except Exception:
                print("⚠️ Invalid—use YYYY‑MM‑DD and stay within the shown window.")
        
        write_line(f"Calculating metrics from {start_ts.date()} to {end_ts.date()}…")
        
        # 6. Filter to the selected window
        win = ddf_price_analysis[(ddf_price_analysis['Date'] >= start_ts) & (ddf_price_analysis['Date'] <= end_ts)]
        
        # 7. Compute daily returns
        win['daily_return'] = (win['Close'] - win['Open']) / win['Open']
        
        # 8. Parallel group‑by aggregations
        cum_returns = win.groupby('Symbol')['daily_return'] \
                         .apply(lambda x: (1 + x).prod() - 1,
                                meta=('cumulative_return','f8'), include_groups=True,)
        volatilities = win.groupby('Symbol')['daily_return'].std().rename('volatility')
        
        # 9. Assemble and materialize
        metrics_ddf = dd.concat([cum_returns, volatilities], axis=1)
        metrics_df  = metrics_ddf.compute()
        
        # 10. Build summary (back in pandas)
        summary_df = pd.DataFrame([
            {'metric': 'avg_cumulative_return', 'symbol': None, 'value': metrics_df['cumulative_return'].mean()},
            {'metric': 'avg_volatility',        'symbol': None, 'value': metrics_df['volatility'].mean()},
            {'metric': 'max_cumulative_return', 'symbol': metrics_df['cumulative_return'].idxmax(), 'value': metrics_df['cumulative_return'].max()},
            {'metric': 'min_cumulative_return', 'symbol': metrics_df['cumulative_return'].idxmin(), 'value': metrics_df['cumulative_return'].min()},
            {'metric': 'most_volatile',         'symbol': metrics_df['volatility'].idxmax(),        'value': metrics_df['volatility'].max()},
            {'metric': 'least_volatile',        'symbol': metrics_df['volatility'].idxmin(),        'value': metrics_df['volatility'].min()},
        ])
        
        # 11. Store results
        dataframes['df_performance_metrics'] = metrics_df
        dataframes['df_performance_summary'] = summary_df
        
        write_line(f"Finished: {len(metrics_df)} symbols from {start_ts.date()} → {end_ts.date()}.")
    
    except Exception as e:
        write_line(f"Error in Get Performance Metrics: {e}")

def compute_symbol_averages(
    df,
    value_cols: Union[str, Sequence[str]],
    start_date: Optional[Union[str, date, datetime]] = None,
    end_date:   Optional[Union[str, date, datetime]] = None,
    date_col:   str = 'Date',
    symbol_col: str = 'Symbol',
    reset_index: bool = False,
):
    """
    Computes per-symbol averages of `value_cols` over an optional date window.
    Works with pandas.DataFrame or dask.dataframe.DataFrame.
    If start_date or end_date is None, that side isn't applied.
    Returns same *type* as input (pandas/dask).
    """
    # normalize columns
    cols = [value_cols] if isinstance(value_cols, str) else list(value_cols)

    # detect dask vs pandas
    try:
        import dask.dataframe as dd
        is_dask = isinstance(df, dd.DataFrame)
    except Exception:
        dd = None
        is_dask = False

    # build datetime series without mutating df (stay Dask-native if Dask)
    if is_dask:
        dates = dd.to_datetime(df[date_col], errors="coerce")  # no .compute()
    else:
        dates = pd.to_datetime(df[date_col], errors="coerce")

    # boolean mask (use isna()/notnull() for widest compatibility)
    mask = (~dates.isna()) if hasattr(dates, "isna") else dates.notnull()

    if start_date is not None:
        start = pd.Timestamp(start_date)
        mask = mask & (dates >= start)
    if end_date is not None:
        end = pd.Timestamp(end_date)
        mask = mask & (dates <= end)

    # filter rows (avoids divisions requirement)
    filtered = df[mask]

    # group and average
    out = filtered.groupby(symbol_col)[cols].mean()

    if reset_index and not is_dask:
        out = out.reset_index()

    return out
    
#endregion
    
#region Dataframe functions    

def _gb_apply(gb, func, meta):
    """Call groupby.apply with include_groups=True when supported (pandas ≥2.2)."""
    try:
        return gb.apply(func, meta=meta, include_groups=True)   # future-proof + silences warning
    except TypeError:
        # older pandas that doesn't accept include_groups
        return gb.apply(func, meta=meta)

def evaluate_symbols_performance(dataframes, prompt_answers=None):
    """
    Filter by a prompted/remembered date range and compute portfolio metrics
    for a collection of Symbols. Produces both Equal-Weighted (EW) and
    Value-Weighted (VW) results (VW uses MarketCap or EnterpriseValue).

    Tracks/reuses in `prompt_answers`:
      - dataframe         : price DF to use (default 'df_price_analysis')
      - start_date        : inclusive (YYYY-MM-DD)
      - end_date          : inclusive (YYYY-MM-DD)
      - risk_free_rate    : annual RF as decimal (default 0.0)
      - fundamentals_df   : DF with weights (default auto-detect or 'df_fundamentals')
      - weight_col        : 'MarketCap' (default) or 'EnterpriseValue'

    Writes/updates (side effects on `dataframes` dict):
      - dataframes['df_portfolio_daily'] : Date, EqualWeighted, ValueWeighted, N_Symbols
      - dataframes['df_range_metrics']   : one summary row per run
      - dataframes['df_actions']         : compact action log (dedup by prompt snapshot)

    External helpers expected to exist in the calling codebase:
      - write_line(msg: str) -> None
          Simple logger/print wrapper used throughout for traceability.
      - get_input_parameter(prompt, type_, default, choices) -> Any
          Interactive input function used only when prompt_answers is missing values.
      - _gb_apply(grouped, func, meta) -> dask.DataFrame
          Thin wrapper around Dask's groupby.apply that accepts a pandas function
          and a `meta` specification to keep the Dask graph typed and schedulable.

    Notes on Dask patterns used here:
      - We keep everything lazy in Dask until we intentionally `.compute()` to
        materialize results needed for pandas-only metrics and outputs.
      - For groupby.apply we always provide `meta` to avoid dask-expr ambiguity.
      - We ensure sort stability with `kind="mergesort"` wherever order matters.
    """
    import json
    import numpy as np
    import pandas as pd
    import dask.dataframe as dd

    write_line("▶ evaluate_symbols_performance: start")

    # ---------------- prompt state ----------------
    # Ensure we always have a dict to read/write "remembered" parameters.
    if prompt_answers is None:
        prompt_answers = {}
        write_line("  • prompt_answers not provided; starting fresh dict")

    # 1) Price dataframe
    # Pull a named DF (defaults to 'df_price_analysis') from the shared `dataframes` bag.
    price_df_name = prompt_answers.get('dataframe', 'df_price_analysis')
    if price_df_name not in dataframes:
        write_line(f"  ✖ DataFrame '{price_df_name}' not in dataframes")
        raise KeyError(f"DataFrame '{price_df_name}' not found in dataframes.")
    prompt_answers['dataframe'] = price_df_name
    ddf = dataframes[price_df_name]
    write_line(f"  • Using price DF: {price_df_name} (type={type(ddf).__name__})")

    # Validate presence of key columns required for return calculations.
    date_col, symbol_col, close_col = 'Date', 'Symbol', 'Close'
    needed = {date_col, symbol_col, close_col}
    missing = needed - set(ddf.columns)
    if missing:
        write_line(f"  ✖ Missing required columns in price DF: {missing}")
        raise KeyError(f"Missing required columns in price DF: {missing}")

    # Ensure Date is datetime for robust filtering & grouping.
    # Use Dask-safe conversion; invalid values become NaT rather than throwing.
    ddf = ddf.assign(**{date_col: dd.to_datetime(ddf[date_col], errors='coerce')})
    write_line("  • Ensured Date column is datetime (coerce errors → NaT)")

    # 2) Choose/remember start/end dates
    # We try to infer the data's overall min/max to suggest sensible defaults.
    try:
        df_min_dt = ddf[date_col].min().compute()
        df_max_dt = ddf[date_col].max().compute()
        write_line(f"  • Data Date range: {str(df_min_dt)[:10]} → {str(df_max_dt)[:10]}")
    except Exception as e:
        # If the global min/max fail (e.g., dataset empty), we fall back to prompts.
        df_min_dt, df_max_dt = None, None
        write_line(f"  ! Could not compute global date range; proceeding with prompts. Error: {e}")

    # Use provided dates in prompt_answers if present; otherwise prompt with defaults.
    if 'start_date' in prompt_answers:
        start_date = pd.to_datetime(str(prompt_answers['start_date']), errors='coerce')
        write_line(f"  • start_date from prompt_answers: {start_date.date() if pd.notna(start_date) else 'NaT'}")
    else:
        default_start = df_min_dt.date().isoformat() if pd.notna(df_min_dt) else "2000-01-01"
        start_date_str = get_input_parameter("Start date (YYYY-MM-DD)", str, default_start, [default_start])
        start_date = pd.to_datetime(start_date_str, errors='coerce')
        prompt_answers['start_date'] = start_date.date().isoformat() if pd.notna(start_date) else default_start
        write_line(f"  • start_date selected: {prompt_answers['start_date']}")

    if 'end_date' in prompt_answers:
        end_date = pd.to_datetime(str(prompt_answers['end_date']), errors='coerce')
        write_line(f"  • end_date from prompt_answers: {end_date.date() if pd.notna(end_date) else 'NaT'}")
    else:
        default_end = df_max_dt.date().isoformat() if pd.notna(df_max_dt) else pd.Timestamp.today().date().isoformat()
        end_date_str = get_input_parameter("End date (YYYY-MM-DD)", str, default_end, [default_end])
        end_date = pd.to_datetime(end_date_str, errors='coerce')
        prompt_answers['end_date'] = end_date.date().isoformat() if pd.notna(end_date) else default_end
        write_line(f"  • end_date selected: {prompt_answers['end_date']}")

    # Basic date sanity checks (parse success and ordering).
    if pd.isna(start_date) or pd.isna(end_date):
        write_line("  ✖ Could not parse start/end dates")
        start_date = df_min_dt
        end_date = df_max_dt

    if start_date > end_date:
        # Normalize caller input (swap if reversed).
        write_line("  • start_date > end_date; swapping")
        start_date, end_date = end_date, start_date
        prompt_answers['start_date'] = start_date.date().isoformat()
        prompt_answers['end_date'] = end_date.date().isoformat()

    # 3) Risk-free rate
    # Either use provided annual RF or prompt for it. Stored as decimal (e.g., 0.02 = 2%).
    if 'risk_free_rate' in prompt_answers:
        rf_annual = float(prompt_answers['risk_free_rate'])
        write_line(f"  • risk_free_rate (annual): {rf_annual}")
    else:
        rf_annual = float(get_input_parameter("Annual risk-free rate (e.g., 0.02 for 2%)", float, 0.0, [0.0]))
        prompt_answers['risk_free_rate'] = rf_annual
        write_line(f"  • risk_free_rate chosen: {rf_annual}")

    # 4) Fundamentals / weights DF (optional)
    # We attempt to auto-detect a fundamentals DF with a weight-like column.
    weight_col_default = 'MarketCap'
    fundamentals_df_name = prompt_answers.get('fundamentals_df')

    if not fundamentals_df_name:
        # Heuristic: pick the first DF in `dataframes` containing 'MarketCap'
        for k, v in dataframes.items():
            if isinstance(v, (pd.DataFrame, dd.DataFrame)) and 'MarketCap' in getattr(v, 'columns', []):
                fundamentals_df_name = k
                break
        # Fallback to a conventional name if present.
        if not fundamentals_df_name and 'df_fundamentals' in dataframes:
            fundamentals_df_name = 'df_fundamentals'
    prompt_answers['fundamentals_df'] = fundamentals_df_name
    write_line(f"  • fundamentals DF: {fundamentals_df_name if fundamentals_df_name else 'none (VW will be skipped)'}")

    # Preferred weight column; will be validated against actual columns later.
    # weight_col = prompt_answers.get('weight_col', weight_col_default)
    # write_line(f"  • preferred weight_col: {weight_col}")

    # ---------------- filter date range ----------------
    # Lazy filter on Date so all downstream work is scoped to the requested window.
    fdf = ddf[(ddf[date_col] >= start_date) & (ddf[date_col] <= end_date)]
    try:
        # Compute an exact count to assert there's data.
        n_rows = int(fdf.shape[0].compute())
        write_line(f"  • filtered rows in range [{start_date.date()} → {end_date.date()}]: {n_rows:,}")
        if n_rows == 0:
            raise ValueError
    except Exception:
        # Fallback size check for older/newer Dask variants.
        if fdf.size.compute() == 0:
            write_line("  ✖ No rows in selected date range")
            raise ValueError(f"No rows found in range {start_date.date()} to {end_date.date()}.")

    # ---------------- daily returns per symbol ----------------
    write_line("  • computing per-symbol daily returns (groupby.apply)…")

    def _daily_returns(pdf: pd.DataFrame) -> pd.DataFrame:
        """
        pandas function (runs per-partition per-group under Dask):
        - Sort by Date (stable) to ensure pct_change uses chronological order.
        - Coerce 'Close' to numeric (invalid → NaN) and compute pct_change.
        - Return a slim DF to keep memory and shuffle costs low.
        """
        pdf = pdf.sort_values(date_col, kind="mergesort")  # stable sort
        c = pd.to_numeric(pdf[close_col], errors='coerce')
        r = c.pct_change().round(3)  # daily returns rounded to 3 decimals
        return pd.DataFrame({
            symbol_col: pdf[symbol_col].values,
            date_col:   pdf[date_col].values,
            'Daily_Return': r.values
        })

    # `meta` tells Dask the output schema of the apply for planning.
    meta_daily = fdf._meta[[symbol_col, date_col]].assign(Daily_Return=np.float64())
    daily_by_symbol = _gb_apply(fdf.groupby(symbol_col), _daily_returns, meta_daily).reset_index(drop=True)

    # Equal-Weighted (EW): simple cross-sectional mean of symbol-day returns.
    ew_series = daily_by_symbol.groupby(date_col)['Daily_Return'].mean()
    # Also track how many symbols contributed each day (for context/plotting).
    n_syms_daily = daily_by_symbol.groupby(date_col)['Daily_Return'].count()
    write_line("  • scheduled EW series and per-day symbol counts")

    # ---------------- value-weighted series (optional) ----------------
    # VW requires a weight per (Symbol, Date). We pick a weight column and forward-fill it.
    vw_series, weights_ok = None, False
    # if fundamentals_df_name and fundamentals_df_name in dataframes:
    #     write_line(f"  • preparing VW series from {fundamentals_df_name}")
    #     wdf_any = dataframes[fundamentals_df_name]
    #     # Normalize fundamentals DF to Dask no matter what the input type is.
    #     if isinstance(wdf_any, pd.DataFrame):
    #         write_line("    - fundamentals is pandas; converting to Dask lazily")
    #         wdf = dd.from_pandas(wdf_any, npartitions=max(1, len(wdf_any) // 250_000 or 1))
    #     else:
    #         wdf = wdf_any

    #     # Ensure Date type for the weights DF as well.
    #     wdf = wdf.assign(**{date_col: dd.to_datetime(wdf[date_col], errors='coerce')})

    #     # Pick an actual weight column that exists.
    #     available = set(wdf.columns)
    #     chosen_weight_col = (weight_col if weight_col in available
    #                          else ('MarketCap' if 'MarketCap' in available
    #                                else ('EnterpriseValue' if 'EnterpriseValue' in available else None)))

    #     if not chosen_weight_col:
    #         write_line("    - no usable weight column found; skipping VW")
    #     else:
    #         prompt_answers['weight_col'] = chosen_weight_col
    #         write_line(f"    - using weight column: {chosen_weight_col}")

    #         # Keep only what we need to reduce shuffle payload.
    #         wdf_slim = wdf[[symbol_col, date_col, chosen_weight_col]].rename(columns={chosen_weight_col: 'Weight'})

    #         def _ffill_weight(pdf: pd.DataFrame) -> pd.DataFrame:
    #             """
    #             For each symbol, sort by date and forward-fill Weight within that symbol.
    #             This handles sparse fundamentals snapshots (e.g., quarterly data).
    #             """
    #             pdf = pdf.sort_values(date_col, kind="mergesort")
    #             pdf['Weight'] = pd.to_numeric(pdf['Weight'], errors='coerce').ffill()
    #             return pdf[[symbol_col, date_col, 'Weight']]

    #         meta_w = wdf_slim._meta[[symbol_col, date_col]].assign(Weight=np.float64())
    #         wdf_ff = _gb_apply(wdf_slim.groupby(symbol_col), _ffill_weight, meta_w).reset_index(drop=True)

    #         # Join weights onto the daily returns; weights may still need ffill post-merge
    #         # (e.g., if there were dates in returns without a same-day fundamentals row).
    #         merged = daily_by_symbol.merge(wdf_ff, on=[symbol_col, date_col], how='left')

    #         def _ffill_weight_postmerge(pdf: pd.DataFrame) -> pd.DataFrame:
    #             """
    #             After the merge, ensure Weight remains forward-filled within each symbol.
    #             """
    #             pdf = pdf.sort_values(date_col, kind="mergesort")
    #             pdf['Weight'] = pd.to_numeric(pdf['Weight'], errors='coerce').ffill()
    #             return pdf

    #         meta_post = merged._meta.assign(Weight=np.float64())
    #         merged_ff = _gb_apply(merged.groupby(symbol_col), _ffill_weight_postmerge, meta=meta_post).reset_index(drop=True)

    #         # Compute weighted returns and weight totals per day, then divide.
    #         merged_ff = merged_ff.assign(
    #             WeightNum = dd.to_numeric(merged_ff['Weight'], errors='coerce'),
    #             WR = merged_ff['Daily_Return'] * dd.to_numeric(merged_ff['Weight'], errors='coerce')
    #         )
    #         sums = merged_ff.groupby(date_col)[['WR', 'WeightNum']].sum()
    #         vw_series = (sums['WR'] / sums['WeightNum']).rename('Daily_Return')
    #         weights_ok = True
    #         write_line("    - VW series scheduled")
    # else:
    #     write_line("  • fundamentals DF not found; VW metrics will be NaN")

    # ---------------- collect to pandas for metrics ----------------
    # We now materialize the EW series to pandas for downstream stats that use pandas ops.
    ew = ew_series.dropna().compute().sort_index()
    write_line(f"    - EW trading days: {len(ew):,}")

    # Collect per-day symbol counts to pandas and align to EW's index (dates).
    # (Do this *after* compute so reindex is a pandas op, avoiding Dask Series quirks.)
    write_line("  • collecting per-day symbol counts to pandas…")
    n_syms_pd = n_syms_daily.compute().sort_index()        # pandas Series now
    n_syms_pd = n_syms_pd.reindex(ew.index).fillna(0).astype(int)
    write_line(f"    - EW trading days: {len(ew):,}")

    # If VW exists, collect it as well and ensure it's a pandas Series indexed by Date.
    if vw_series is not None:
        write_line("  • collecting VW series to pandas…")
        vw_pd = vw_series.dropna().compute()          # may be Series or 1-col DataFrame
        if hasattr(vw_pd, "columns"):                 # DataFrame case; squeeze to Series
            if "Daily_Return" in vw_pd.columns and vw_pd.shape[1] == 1:
                vw_pd = vw_pd["Daily_Return"]
            else:
                vw_pd = vw_pd.squeeze()
        vw = vw_pd.sort_index()
    else:
        vw = pd.Series(dtype="float64")

    # ---------------- helper to compute metrics ----------------
    # Convert annual RF to a daily equivalent (assuming ~252 trading days).
    TRADING_DAYS = 252.0
    rf_daily = (1.0 + rf_annual)**(1.0 / TRADING_DAYS) - 1.0

    def _metrics_from_series(s: pd.Series, label: str):
        """
        Compute standard return/risk metrics from a daily-return Series:
        - cumulative return / annualized return
        - annualized volatility
        - Sharpe (using daily RF) and Sortino (downside stdev)
        - max drawdown and Calmar (ann return / |maxDD|)
        - hit-rate, best/worst day, period coverage

        Returns a dict with namespaced keys using the provided `label`.
        """

        write_line(f"→ [{label}] starting metrics computation")

        if s is None or s.empty:
            write_line(f"  • {label}: empty → metrics will be NaN")
            return {
                f"{label}_cum_return": np.nan,
                f"{label}_ann_return": np.nan,
                f"{label}_ann_vol": np.nan,
                f"{label}_sharpe": np.nan,
                f"{label}_sortino": np.nan,
                f"{label}_max_drawdown": np.nan,
                f"{label}_calmar": np.nan,
                f"{label}_hit_rate": np.nan,
                f"{label}_best_day": np.nan,
                f"{label}_worst_day": np.nan,
                f"{label}_trading_days": 0,
                f"{label}_start_used": pd.NaT,
                f"{label}_end_used": pd.NaT,
            }

        # Basic diagnostics before cleaning
        n_total = int(s.shape[0])
        n_nan = int(s.isna().sum())
        n_posinf = int(np.isposinf(s).sum())
        n_neginf = int(np.isneginf(s).sum())
        write_line(
            f"  • {label}: count={n_total}, NaN={n_nan}, +inf={n_posinf}, -inf={n_neginf}, "
            f"start={s.index.min()}, end={s.index.max()}"
        )

        # Replace infinities with 0 (preserve NaN)
        s = s.replace([np.inf, -np.inf], 0)
        write_line(f"  • {label}: replaced ±inf with 0")

        # Size and compounding math
        n_days = int(s.shape[0])
        cum_return = float((1.0 + s).prod() - 1.0)
        ann_return = float((1.0 + cum_return) ** (TRADING_DAYS / max(n_days, 1)) - 1.0)
        write_line(
            f"  • {label}: cum_return={cum_return:.6f}, ann_return={ann_return:.6f}, days={n_days}"
        )

        # Risk/ratio metrics
        mean_daily = float(s.mean())
        vol_daily = float(s.std(ddof=0))  # population stdev for daily returns
        ann_vol = float(vol_daily * np.sqrt(TRADING_DAYS)) if np.isfinite(vol_daily) else np.nan
        write_line(
            f"  • {label}: mean_daily={mean_daily:.6f}, vol_daily={vol_daily:.6f}, ann_vol={ann_vol:.6f}"
            if np.isfinite(ann_vol) else
            f"  • {label}: mean_daily={mean_daily:.6f}, vol_daily={vol_daily}, ann_vol=NaN"
        )

        sharpe = float(((mean_daily - rf_daily) / vol_daily) * np.sqrt(TRADING_DAYS)) if vol_daily > 0 else np.nan
        write_line(
            f"  • {label}: rf_daily={rf_daily:.8f}, sharpe={sharpe:.6f}" if np.isfinite(sharpe) else
            f"  • {label}: rf_daily={rf_daily:.8f}, sharpe=NaN (vol_daily ≤ 0)"
        )

        # Sortino: only penalize downside (returns below risk-free)
        downside = np.minimum(0.0, s - rf_daily)
        n_downside = int((s - rf_daily < 0).sum())
        downside_std = float(pd.Series(downside).std(ddof=0))
        sortino = float(((mean_daily - rf_daily) / downside_std) * np.sqrt(TRADING_DAYS)) if downside_std > 0 else np.nan
        write_line(
            f"  • {label}: downside_days={n_downside}, downside_std={downside_std:.6f}, sortino={sortino:.6f}"
            if np.isfinite(sortino) else
            f"  • {label}: downside_days={n_downside}, downside_std={downside_std}, sortino=NaN"
        )

        # Drawdown metrics require an equity curve (cumprod of 1+r)
        equity = (1.0 + s).cumprod()
        rolling_max = equity.cummax()
        drawdown = equity / rolling_max - 1.0
        if not drawdown.empty:
            max_drawdown_val = float(drawdown.min())
            max_dd_date = drawdown.idxmin()
        else:
            max_drawdown_val = np.nan
            max_dd_date = pd.NaT
        calmar = float(ann_return / abs(max_drawdown_val)) if (max_drawdown_val is not None and max_drawdown_val < 0) else np.nan
        write_line(
            f"  • {label}: max_drawdown={max_drawdown_val:.6f} at {max_dd_date}, calmar={calmar:.6f}"
            if np.isfinite(max_drawdown_val) else
            f"  • {label}: max_drawdown=NaN, calmar=NaN"
        )

        # Final summary line (kept from your original)
        write_line(
            f"    - {label}: cum={cum_return:.4f}, ann={ann_return:.4f}, vol(ann)={ann_vol:.4f}, "
            f"sharpe={sharpe:.3f}, sortino={sortino:.3f}, maxDD={max_drawdown_val:.4f}, days={n_days}"
            if np.isfinite(max_drawdown_val) else
            f"    - {label}: cum={cum_return:.4f}, ann={ann_return:.4f}, vol(ann)={ann_vol:.4f}, "
            f"sharpe={sharpe:.3f}, sortino={sortino:.3f}, maxDD=NaN, days={n_days}"
        )

        # Package metrics with consistent rounding
        return {
            f"{label}_cum_return": round(cum_return, 6),
            f"{label}_ann_return": round(ann_return, 6),
            f"{label}_ann_vol": round(ann_vol, 6),
            f"{label}_sharpe": round(sharpe, 6),
            f"{label}_sortino": round(sortino, 6),
            f"{label}_max_drawdown": round(max_drawdown_val, 6) if np.isfinite(max_drawdown_val) else np.nan,
            f"{label}_calmar": round(calmar, 6) if np.isfinite(calmar) else np.nan,
            f"{label}_hit_rate": round(float((s > 0).mean()), 6),
            f"{label}_best_day": round(float(s.max()), 6),
            f"{label}_worst_day": round(float(s.min()), 6),
            f"{label}_trading_days": int(n_days),
            f"{label}_start_used": s.index.min(),
            f"{label}_end_used": s.index.max(),
        }

    write_line("  • computing EW metrics…")
    ew_metrics = _metrics_from_series(ew, "ew_portfolio")
    write_line("  • computing VW metrics…")
    vw_metrics = _metrics_from_series(vw if weights_ok else None, "vw_portfolio")

    # ---------------- per-symbol total returns + winner stats ----------------
    write_line("  • computing per-symbol total returns and winner stats…")

    def _symbol_total_return(pdf: pd.DataFrame) -> pd.Series:
        """
        For each symbol's slice within the date window:
        - Sort chronologically, coerce Close to numeric, drop NaNs.
        - Compute simple period return using first/last good prices.
        """
        pdf = pdf.sort_values(date_col, kind="mergesort")
        c = pd.to_numeric(pdf[close_col], errors='coerce').dropna()
        ret = np.nan if c.empty or c.iloc[0] == 0 else (c.iloc[-1] / c.iloc[0] - 1.0)
        return pd.Series({'Total_Return': ret})

    # Materialize per-symbol totals (a small table) to pandas for aggregation.
    sym_rets = _gb_apply(fdf.groupby(symbol_col), _symbol_total_return, meta={'Total_Return': 'float64'}).reset_index().compute()

    # Aggregate symbol-level outcomes (count, average, median, % winners).
    n_syms = int(sym_rets[symbol_col].nunique()) if not sym_rets.empty else 0
    avg_symbol_total_ret = float(sym_rets['Total_Return'].mean(skipna=True)) if not sym_rets.empty else np.nan
    median_symbol_total_ret = float(sym_rets['Total_Return'].median(skipna=True)) if not sym_rets.empty else np.nan
    winners = int((sym_rets['Total_Return'] > 0).sum()) if not sym_rets.empty else 0
    pct_winners = float(winners / n_syms) if n_syms > 0 else np.nan
    write_line(f"    - symbols={n_syms}, winners={winners} ({pct_winners:.2%} if not NaN)")

    # Value-weighted winner share (how much of starting portfolio weight sits in eventual winners).
    vw_winner_weight_share = np.nan
    # if weights_ok and not sym_rets.empty and not ew.empty:
    #     try:
    #         start_used = ew.index.min()  # Align to the EW series' start for a consistent "starting portfolio".
    #         write_line("  • computing VW winner weight share at start_used…")

    #         # Normalize fundamentals DF type and ensure Date is datetime.
    #         wdf_start_any = dataframes[prompt_answers['fundamentals_df']]
    #         if isinstance(wdf_start_any, dd.DataFrame):
    #             wdf_start = wdf_start_any
    #         else:
    #             wdf_start = dd.from_pandas(wdf_start_any, npartitions=1)
    #         wdf_start = wdf_start.assign(**{date_col: dd.to_datetime(wdf_start[date_col], errors='coerce')})

    #         # Keep only rows at or before the start date; we'll pick the latest snapshot per symbol.
    #         wdf_start = wdf_start[wdf_start[date_col] <= start_used]

    #         # Normalize a Weight column name if not already present.
    #         if 'Weight' not in wdf_start.columns:
    #             chosen = prompt_answers.get('weight_col', 'MarketCap')
    #             if chosen in wdf_start.columns:
    #                 wdf_start = wdf_start.rename(columns={chosen: 'Weight'})
    #             elif 'EnterpriseValue' in wdf_start.columns:
    #                 wdf_start = wdf_start.rename(columns={'EnterpriseValue': 'Weight'})
    #         wdf_start = wdf_start[[symbol_col, date_col, 'Weight']]

    #         def _latest_before(pdf: pd.DataFrame) -> pd.DataFrame:
    #             """
    #             Within each symbol, take the last row (closest at/before start_used),
    #             then return a tiny (Symbol, Weight) slice.
    #             """
    #             pdf = pdf.sort_values(date_col, kind='mergesort')
    #             pdf = pdf.tail(1)
    #             return pdf[[symbol_col, 'Weight']]

    #         meta_lb = wdf_start._meta[[symbol_col]].assign(Weight=np.float64())
    #         start_weights = _gb_apply(wdf_start.groupby(symbol_col), _latest_before, meta=meta_lb).compute()

    #         # Merge weights with outcome and compute share of weight in winners.
    #         tmp = sym_rets.merge(start_weights, on=symbol_col, how='left')
    #         tmp['Weight'] = pd.to_numeric(tmp['Weight'], errors='coerce')
    #         pos_weight = float(tmp.loc[tmp['Total_Return'] > 0, 'Weight'].sum())
    #         all_weight = float(tmp['Weight'].sum())
    #         vw_winner_weight_share = (pos_weight / all_weight) if all_weight > 0 else np.nan
    #         write_line(f"    - VW winner weight share: {vw_winner_weight_share:.4f} (NaN if insufficient weights)")
    #     except Exception as e:
    #         # Non-fatal: keep metrics calculation resilient.
    #         write_line(f"    ! Failed VW winner weight share calc: {e}")

    # ---------------- assemble metrics row ----------------
    # One tidy record summarizing the run (covers EW, VW, and per-symbol aggregates).
    metrics = {
        "run_at": pd.Timestamp.now(),
        "dataframe": price_df_name,
        "start_date": pd.to_datetime(prompt_answers['start_date']),
        "end_date": pd.to_datetime(prompt_answers['end_date']),
        "symbols": n_syms,
        "risk_free_rate_annual": rf_annual,

        # EW series metrics
        **ew_metrics,
        # VW series metrics (NaN if not available)
        **vw_metrics,

        # per-symbol aggregates
        "avg_symbol_total_return": round(avg_symbol_total_ret, 6),
        "median_symbol_total_return": round(median_symbol_total_ret, 6),
        "winners": winners,
        "pct_winners": round(pct_winners, 6) if np.isfinite(pct_winners) else np.nan,
        "vw_winner_weight_share": round(vw_winner_weight_share, 6) if np.isfinite(vw_winner_weight_share) else np.nan,
        "fundamentals_df": fundamentals_df_name,
        "weight_col": prompt_answers.get('weight_col'),
    }
    write_line("  • assembled metrics row")

    # ---------------- save portfolio daily table (for plotting) ----------------
    # Build a daily table aligned to EW dates, with symbol counts, and optionally VW.
    daily_index = ew.index
    df_daily = pd.DataFrame({
        'Date': daily_index,
        'EqualWeighted': ew.values,
        'N_Symbols': n_syms_pd.reindex(daily_index).fillna(0).astype(int).values
    })
    if vw_series is not None and not vw.empty:
        # Merge VW by Date; rename Date consistently to 'Date'.
        df_daily = df_daily.merge(
            vw.rename('ValueWeighted').to_frame().reset_index().rename(columns={date_col: 'Date'}),
            on='Date', how='left'
        )
    else:
        df_daily['ValueWeighted'] = np.nan

    # Persist the daily series (plot-friendly).
    dataframes['df_portfolio_daily'] = df_daily
    write_line(f"  • wrote df_portfolio_daily: {len(df_daily):,} rows")

    # ---------------- append metrics row ----------------
    # Append-or-create a compact cumulative table of run-level summaries.
    metrics_key = "df_range_metrics"
    if metrics_key not in dataframes:
        dataframes[metrics_key] = pd.DataFrame([metrics])
        write_line("  • created df_range_metrics with first row")
    else:
        dataframes[metrics_key].loc[len(dataframes[metrics_key])] = metrics
        write_line(f"  • appended row to df_range_metrics (now {len(dataframes[metrics_key])} rows)")

    # ---------------- action log ----------------
    # Deduplicate by the serialized prompt snapshot so repeated identical runs don't spam the log.
    try:
        log_key = "df_actions"
        entry = {
            "timestamp": pd.Timestamp.now(),
            "action": "evaluate_symbols_performance",
            "dataframe": price_df_name,
            "added_columns": [],  # none added to source DF
            "prompt_answers_json": json.dumps(prompt_answers, default=str, sort_keys=True),
        }
        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
            write_line("  • created df_actions log")
        else:
            df_log = dataframes[log_key]
            if "prompt_answers_json" not in df_log.columns:
                df_log["prompt_answers_json"] = ""
            if not (df_log["prompt_answers_json"] == entry["prompt_answers_json"]).any():
                df_log.loc[len(df_log)] = entry
                write_line("  • appended action log entry (dedup by prompt snapshot)")
            else:
                write_line("  • action log unchanged (duplicate prompt snapshot)")
    except Exception as e:
        # Logging is best-effort and must never break the computation.
        write_line(f"  ! action log failed (non-fatal): {e}")

    # Put back any updated refs (e.g., if we reassigned ddf earlier).
    dataframes[price_df_name] = ddf

    write_line("▶ evaluate_symbols_performance: done")
    return dataframes


def get_float_column_range(df: pd.DataFrame, col: str):
    """
    If df[col] exists and is a float dtype, prints its min, max, and range.
    """
    if col not in df.columns:
        print(f"Column '{col}' not found.")
        return
    if not is_float_dtype(df[col]):
        print(f"Column '{col}' is not float dtype (found {df[col].dtype}).")
        return
    # dropna so NaNs don’t spoil the min/max
    series = df[col].dropna()
    minimum = series.min().compute()
    maximum = series.max().compute()
    value_range = maximum - minimum

    # print(f"Column '{col}':")
    # print(f"  Min   = {minimum}")
    # print(f"  Max   = {maximum}")
    # print(f"  Range = {value_range}")
    return minimum, maximum
    
def gather_df_metadata(df_dict, date_col=None):
    """
    Collects metadata for each Dask DataFrame in `df_dict`, with tracing prints.

    Parameters
    ----------
    df_dict : dict of dask.DataFrame
    date_col : str or None, default None

    Returns
    -------
    metadata : dict
    """
    print("▶ Starting metadata gathering (Dask)")

    metadata = {}
    overall_min = None
    overall_max = None

    for name, df in df_dict.items():
        print(f"\n⟳ Processing '{name}'")
        
        # columns
        cols = df.columns.tolist()
        print(f"   • Columns: {cols}")

        info = {
            'columns'  : cols
        }

        # date range
        if date_col and date_col in df.columns:
            print(f"   → Computing date range on '{date_col}'")
            dates = dd.to_datetime(df[date_col], errors='coerce')
            min_d = dates.min().compute()
            max_d = dates.max().compute()
            print(f"     • min_date = {min_d}, max_date = {max_d}")
            info.update(min_date=min_d, max_date=max_d)

            if overall_min is None or min_d < overall_min:
                overall_min = min_d
                print(f"     ↳ New overall_min_date = {overall_min}")
            if overall_max is None or max_d > overall_max:
                overall_max = max_d
                print(f"     ↳ New overall_max_date = {overall_max}")

        metadata[name] = info

    # attach overall date span if applicable
    if date_col:
        print(f"\n✔ Overall date span: {overall_min} → {overall_max}")
        metadata['_overall_min_date_'] = overall_min
        metadata['_overall_max_date_'] = overall_max

    print("▶ Finished metadata gathering\n")
    return metadata

def infer_column_types(file_path, nrows=1000):
    """
    Inspect the first `nrows` of the CSV and infer types for Dask-friendly reading.

    Strategy (to avoid per-partition dtype drift):
      - Mark date-like columns as 'object' (text). We'll parse them AFTER loading.
      - For other columns, try float; else leave as 'object'.

    Returns:
      parse_dates: []  (intentionally empty; we parse later)
      dtype_map  : dict of {col -> 'float64' or 'object'}
    """
    import pandas as pd

    sample = pd.read_csv(file_path, nrows=nrows, low_memory=False)
    parse_dates = []  # leave empty on purpose
    dtype_map = {}

    if 'hist' in file_path:
        pass  # keep your existing hook

    date_like_keys = ("date", "datetime", "timestamp", "time")

    for col in sample.columns:
        lname = col.lower()

        # Treat date-like columns as text now; we'll normalize later.
        if any(k in lname for k in date_like_keys):
            dtype_map[col] = 'object'
            continue

        # Keep your explicit non-float guardrails
        if col in ['Sector', 'Industry', 'Date']:
            dtype_map[col] = 'object'
            continue

        # Float check: consider it numeric if most non-null entries are numeric
        ser_num = pd.to_numeric(sample[col], errors='coerce')
        non_null = sample[col].notna().sum()
        numeric_ok = ser_num.notna().sum()
        if non_null and (numeric_ok / max(non_null, 1) >= 0.9):
            dtype_map[col] = 'float64'
        else:
            dtype_map[col] = 'object'

    return parse_dates, dtype_map

def read_csv_dask_with_inferred_types(path, nrows=1000):
    """
    Read CSV into a Dask DataFrame using inferred dtypes, then
    uniformly parse any date-like columns (keeps time-of-day).

    - Avoids `parse_dates` at read time (prevents mismatched dtypes).
    - Parses afterwards with `utc=True` and drops tz to naive UTC.
    """
    import dask.dataframe as dd
    import pandas as pd

    parse_dates, dtype_map = infer_column_types(path, nrows)

    # Always read date-like fields as text/object to avoid per-partition drift
    df = dd.read_csv(
        path,
        dtype=dtype_map,
        assume_missing=True,   # safer for mixed int/float columns
        # on_bad_lines="skip", # uncomment if you want to skip malformed lines
    )

    # Identify date-like columns (same logic as in infer)
    date_like_keys = ("date", "datetime", "timestamp", "time")
    date_cols = [c for c in df.columns if any(k in c.lower() for k in date_like_keys)]

    if date_cols:
        def _parse_dates(pdf: pd.DataFrame, cols):
            for c in cols:
                s = pd.to_datetime(pdf[c], errors="coerce", utc=True)  # accepts date-only & date-time
                # if tz-aware, convert to UTC then drop tz to get naive UTC timestamps
                if pd.api.types.is_datetime64tz_dtype(s):
                    s = s.dt.tz_convert("UTC").dt.tz_localize(None)
                else:
                    # already naive (e.g., no tz info); leave as-is
                    pass
                pdf[c] = s
            return pdf

        df = df.map_partitions(_parse_dates, date_cols)

    return df

def select_dataframe(df_dict):
    """
    Prompt the user to choose one DataFrame from df_dict.

    Returns
    -------
    (name, df) : tuple
        The key name and the pandas or Dask DataFrame selected.
    """
    keys = list(df_dict.keys())
    print("Available DataFrames:")
    for i, name in enumerate(keys, start=1):
        print(f"  {i}. {name}")
    while True:
        choice = input("Enter the number or name of the DataFrame to select: ").strip()
        # numeric selection
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(keys):
                selected = keys[idx]
                break
        # name selection
        elif choice in df_dict:
            selected = choice
            break
        print("⛔ Invalid selection – please try again.")
    print(f"\n✅ You selected '{selected}'\n")
    return selected, df_dict[selected]
    
def add_calculation(dataframes, prompt_answers=None, persist=False):
    """
    Interactive filter on a dict of Dask DataFrames.

    Modes:
      1) Direct: select one column, list its unique values, pick by index.
      2) Relative: sort by a column then take top N rows or top N%.
    """
    try:
        write_line("Add Calculation selected.")
        available_calculations = [
            "Simple Moving Average",
            "Exponential Moving Average",
            "Dollar Volume",
            "MACD",
            "Crossover Proximity",
            # "VWAP",
            "Period Range %",
            "ATR",
            # "Filter Trading Days"
            "Bollinger Band",
            "Bollinger Range %",
            "Breakout Flag"
        ]
        
        # Display menu
        print("Select a calculation:")
        for i, calc in enumerate(available_calculations, start=1):
            print(f"  {i}. {calc}")

        # Prompt for choice
        choice = input("Enter number: ").strip()

        # Validate and get selection
        if choice.isdigit() and 1 <= int(choice) <= len(available_calculations):
            selected_calc = available_calculations[int(choice) - 1]
            print(f"You selected: {selected_calc}")
        else:
            print("Invalid choice.")
            selected_calc = None
            
        # Call the appropriate function
        if selected_calc == "Simple Moving Average":
            dataframes = add_sma(dataframes)
        elif selected_calc == "Exponential Moving Average":
            dataframes = add_ema(dataframes)
        elif selected_calc == "Dollar Volume":
            dataframes = add_dv(dataframes)
        elif selected_calc == "VWAP":
            dataframes = add_vwap(dataframes)
        elif selected_calc ==  "MACD":
            dataframes = add_macd(dataframes)
        elif selected_calc == "Crossover Proximity":
            dataframes = add_crossover_proximity(dataframes)
        elif selected_calc == "Period Range %":
            dataframes = add_range_percent(dataframes)
        elif selected_calc == "ATR":
            dataframes = add_atr(dataframes)
        elif selected_calc == "Bollinger Band":
            dataframes = add_bollinger_bands(dataframes)
        elif selected_calc == "Bolling Range %":
            dataframes = add_bollinger_range_pct(dataframes)
        elif selected_calc == 'Breakout Flag':
            dataframes = add_breakout_flag(dataframes)
        # elif selected_calc == 'Filter Trading Days':
        #     dataframes = filter_us_trading_days_ddf(dataframes)
        else:
            print(f"No function implemented for {selected_calc} yet.")
            return dataframes
        # if persist:
        #     dataframes = persist_dataframes(dataframes)        
    except Exception as e:
        write_line(f"Error in Add Calculation: {e}")
    
    return dataframes

def add_bollinger_range_pct(dataframes, prompt_answers=None):
    """
    Adds a single column with the Bollinger band range as a percentage of price.

      Range% = ((UpperBand - LowerBand) / Price) * (100 if as_percent else 1)

    Bands are computed per Symbol from the target_col (default 'Close'):
      Middle = SMA(period)
      Std    = rolling std (ddof=1)
      Upper  = Middle + k * Std
      Lower  = Middle - k * Std

    Tracks/reuses in `prompt_answers`:
      - dataframe   : which DF to use (default prompt)
      - period      : rolling window (int; default 20; options [10,20,50])
      - std_mult    : std-dev multiple k (float; default 2.0; options [1.5,2.0,2.5])
      - target_col  : price column for bands AND denominator (default prompt 'Close')
      - lag_days    : non-negative int; shift result forward by this many rows (default 0)
      - as_percent  : bool; True -> 0..100, False -> 0..1 (default True)

    Output column example:
      BB_RangePct_Close_20_2x[_Lag2]  (float64)

    Notes:
      - Uses sample std (ddof=1). Switch to ddof=0 for population std if desired.
      - `lag_days` semantics: value at t equals indicator computed on data up to t - lag_days.
    """
    import json
    import numpy as np
    import pandas as pd
    import dask.dataframe as dd

    if prompt_answers is None:
        prompt_answers = {}

    # ---- 1) Pick dataframe ----
    if 'dataframe' in prompt_answers:
        dataframe_name = prompt_answers['dataframe']
    else:
        dataframe_name = prompt_user_dataframe(dataframes, 'Select dataframe to calculate Bollinger Range% in: ')
    prompt_answers['dataframe'] = dataframe_name
    ddf = dataframes[dataframe_name]

    # ---- 2) Period ----
    if 'period' in prompt_answers:
        period = int(prompt_answers['period'])
    else:
        period = int(get_input_parameter('Bollinger period', type(int), 20, [10, 20, 50]))
        prompt_answers['period'] = period

    # ---- 3) Std multiple k ----
    if 'std_mult' in prompt_answers:
        std_mult = float(prompt_answers['std_mult'])
    else:
        std_mult = float(get_input_parameter('Std multiple (k)', float, 2.0, [1.5, 2.0, 2.5]))
        prompt_answers['std_mult'] = std_mult

    # ---- 4) Target column (also denominator) ----
    if 'target_col' in prompt_answers:
        target_col = prompt_answers['target_col']
    else:
        target_col = prompt_user_column(ddf, 'Close')
        prompt_answers['target_col'] = target_col

    # ---- 5) Lag days ----
    if 'lag_days' in prompt_answers:
        lag_days = int(prompt_answers['lag_days'])
    else:
        lag_days = int(get_input_parameter('Lag days (shift outputs forward)', type(int), 0, [0, 1, 2, 5]))
        prompt_answers['lag_days'] = lag_days
    if lag_days < 0:
        raise ValueError("lag_days must be >= 0")

    # ---- 6) As percent (0..100) or ratio (0..1) ----
    if 'as_percent' in prompt_answers:
        as_percent = bool(prompt_answers['as_percent'])
    else:
        as_percent = True
        prompt_answers['as_percent'] = as_percent

    # ---- parameters / names ----
    date_col   = 'Date'
    symbol_col = 'Symbol'
    k_str = f"{str(std_mult).rstrip('0').rstrip('.')}"  # "2" or "1.5"

    col_name = f"BB_RangePct_{target_col}_{period}_{k_str}x"
    if lag_days:
        col_name += f"_Lag{lag_days}"

    # ---- checks ----
    columns = list(ddf.columns)
    needed = {symbol_col, date_col, target_col}
    missing = needed - set(columns)
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    # ---- ensure Date is datetime (Dask-safe) ----
    ddf = ddf.assign(**{date_col: dd.to_datetime(ddf[date_col], errors="coerce")})

    # ---- per-group compute (pandas fn) ----
    def _compute_range_pct(pdf: pd.DataFrame) -> pd.DataFrame:
        sym = str(pdf[symbol_col].iloc[0]) if not pdf.empty else "<NA>"
        write_line(f'Calculating BB Range% for {sym}')
        pdf = pdf.sort_values(date_col, kind="mergesort")

        s = pd.to_numeric(pdf[target_col], errors='coerce')

        mid = s.rolling(window=period, min_periods=period).mean()
        std = s.rolling(window=period, min_periods=period).std(ddof=1)
        up  = mid + std_mult * std
        low = mid - std_mult * std

        rng = up - low  # band width in price units

        with np.errstate(divide='ignore', invalid='ignore'):
            pct = rng / s
            if as_percent:
                pct = pct * 100.0

        # Apply lag (shift forward so value at t uses info from t - lag_days)
        if lag_days:
            pct = pct.shift(lag_days)

        pdf[col_name] = pct.astype(float)
        return pdf

    # ---- meta with new column ----
    meta = ddf._meta.assign(**{col_name: np.float64()})

    # ---- shuffle -> groupby.apply -> tidy ----
    out = (
        ddf.shuffle(symbol_col)
           .groupby(symbol_col)
           .apply(_compute_range_pct, meta=meta, include_groups=True)
           .reset_index(drop=True)
           .map_partitions(lambda pdf: pdf.sort_values([symbol_col, date_col], kind="mergesort"))
    )

    # ---- update dict ----
    dataframes[dataframe_name] = out

    # ---- compact action log ----
    try:
        log_key = "df_actions"
        entry = {
            "timestamp": pd.Timestamp.now(),
            "action": "add_bollinger_range_pct",
            "dataframe": dataframe_name,
            "added_columns": [col_name],
            "prompt_answers_json": json.dumps(prompt_answers, default=str, sort_keys=True),
        }
        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
        else:
            df_log = dataframes[log_key]
            if "prompt_answers_json" not in df_log.columns:
                df_log["prompt_answers_json"] = ""
            if not (df_log["prompt_answers_json"] == entry["prompt_answers_json"]).any():
                df_log.loc[len(df_log)] = entry
    except Exception:
        pass

    return dataframes

from pathlib import Path
import dask.dataframe as dd

def write_dask_df_to_csv(
    ddf: dd.DataFrame,
    path: str,
    single_file: bool = False,
    index: bool = False,
    compute: bool = True,
    **to_csv_kwargs
):
    """
    Write a Dask DataFrame to CSV.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        The Dask DataFrame to write.
    path : str
        If single_file=False:
            - Should include a wildcard, e.g. 'output/part-*.csv', OR
            - A directory; then 'part-*.csv' is appended automatically.
        If single_file=True:
            - Full path to target file, e.g. 'output/data.csv'.
    single_file : bool, default False
        If True, attempt to write to a single CSV file (requires
        ddf to have known divisions and may be slower).
    index : bool, default False
        Whether to write the index to the CSV.
    compute : bool, default True
        If True, trigger the write immediately and return the result.
        If False, return a Dask Delayed object you can compute later.
    **to_csv_kwargs :
        Any extra kwargs passed to ddf.to_csv (e.g. header, sep, na_rep).

    Returns
    -------
    result :
        - If compute=True: list of written file paths (or a single path).
        - If compute=False: Dask Delayed object.
    """
    path_obj = Path(path)

    if single_file:
        # Ensure parent directory exists
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        result = ddf.to_csv(
            str(path_obj),
            single_file=True,
            index=index,
            compute=compute,
            **to_csv_kwargs,
        )
    else:
        # If user passed a directory, append a pattern like part-*.csv
        if path_obj.suffix == "":
            path_obj.mkdir(parents=True, exist_ok=True)
            final_path = path_obj / "part-*.csv"
        else:
            final_path = path_obj

        result = ddf.to_csv(
            str(final_path),
            index=index,
            compute=compute,
            **to_csv_kwargs,
        )

    return result


def print_bad_dates_dask(ddf, col="Date", fmt="%Y-%m-%d %H:%M:%S", sample=50):
    s = ddf[col].astype("string").str.strip()
    parsed = dd.to_datetime(s, format=fmt, errors="coerce")
    bad_mask = parsed.isna() & s.notna() & (s != "")

    # counts
    bad_ct   = bad_mask.sum().compute()
    total_ct = s.size.compute()
    print(f"Bad dates: {bad_ct} / {total_ct} (format={fmt})")

    if bad_ct:
        # values + counts
        vc = s[bad_mask].value_counts().compute()
        print("\nTop offending values:")
        print(vc.head(25))

        # sample rows with their original values
        print("\nSample offending rows:")
        print(s[bad_mask].head(sample).compute())
   
def persist_dataframes(dataframes, prompt_answers=None):
    """
    Prompt to persist selected Dask DataFrames in `dataframes`.

    Uses `prompt_answers` if provided:
      - persist_all: bool
      - persist_keys: list[str] (names of dataframes to persist)
      - repartition: {"mode": "none" | "n" | "size", "value": int|float}
        - mode="n"   -> value = number of partitions
        - mode="size"-> value = MB per partition
    """

    def _parse_index_list(s, max_len):
        # e.g. "1,3,5" -> [1,3,5], 1-based to 0-based
        idxs = []
        for part in s.split(","):
            part = part.strip()
            if not part:
                continue
            if not part.isdigit():
                return None
            i = int(part)
            if not (1 <= i <= max_len):
                return None
            idxs.append(i - 1)
        return sorted(set(idxs))

    try:
        if prompt_answers is None:
            prompt_answers = {}

        # 1) Gather Dask DataFrames only
        dask_keys = [k for k, v in dataframes.items() if isinstance(v, dd.DataFrame)]
        if not dask_keys:
            write_line("No Dask DataFrames found to persist. Nothing to do.")
            return dataframes

        write_line("Dask DataFrames available for persistence:")
        for i, k in enumerate(dask_keys, 1):
            try:
                nparts = dataframes[k].npartitions
            except Exception:
                nparts = "?"
            write_line(f"  {i}. {k}  (partitions: {nparts})")

        # 2) Decide which to persist (all vs select)
        if 'persist_all' not in prompt_answers and 'persist_keys' not in prompt_answers:
            choice = input("Persist ALL? [y/N]: ").strip().lower()
            if choice == "y":
                prompt_answers['persist_all'] = True
            else:
                prompt_answers['persist_all'] = False
                picks = input("Enter numbers to persist (comma-separated), or blank to cancel: ").strip()
                if not picks:
                    write_line("No selection made. Aborting persist.")
                    return dataframes
                idxs = _parse_index_list(picks, len(dask_keys))
                if idxs is None:
                    write_line("Invalid selection. Aborting persist.")
                    return dataframes
                prompt_answers['persist_keys'] = [dask_keys[i] for i in idxs]

        if prompt_answers.get('persist_all', False):
            selected_keys = dask_keys
        else:
            selected_keys = prompt_answers.get('persist_keys', [])
            if not selected_keys:  # still empty?
                write_line("No dataframes selected. Aborting persist.")
                return dataframes

        # 3) Optional repartition prompt
        if 'repartition' not in prompt_answers:
            write_line("Repartition before persisting?")
            write_line("  1) No")
            write_line("  2) By number of partitions")
            write_line("  3) By size (MB) per partition")
            r = input("Choose 1/2/3: ").strip()
            if r == "2":
                val = input("Target number of partitions (int): ").strip()
                prompt_answers['repartition'] = {"mode": "n", "value": int(val)}
            elif r == "3":
                val = input("Target size per partition in MB (e.g., 64): ").strip()
                prompt_answers['repartition'] = {"mode": "size", "value": float(val)}
            else:
                prompt_answers['repartition'] = {"mode": "none", "value": None}

        # 4) Apply optional repartitioning
        rep = prompt_answers['repartition']
        to_persist = []
        for k in selected_keys:
            df = dataframes[k]
            if rep["mode"] == "n" and rep["value"]:
                df = df.repartition(npartitions=int(rep["value"]))
                write_line(f"{k}: repartitioned to {int(rep['value'])} partitions.")
            elif rep["mode"] == "size" and rep["value"]:
                df = df.repartition(partition_size=f"{float(rep['value'])}MB")
                write_line(f"{k}: repartitioned to ~{float(rep['value'])}MB per partition.")
            # collect for concurrent persist
            to_persist.append(df)

        # 5) Persist concurrently and assign back
        write_line("Persisting selected dataframes...")
        persisted = dask.persist(*to_persist)  # returns tuple in same order
        for k, df_p in zip(selected_keys, persisted):
            dataframes[k] = df_p
            try:
                nparts = df_p.npartitions
            except Exception:
                nparts = "?"
            write_line(f"{k} persisted. (partitions: {nparts})")

        write_line("Done.")
        return dataframes

    except Exception as e:
        write_line(f"Error in persist_dataframes: {e}")
        return dataframes

def coerce_date_only_dask(ddf, col='Date'):
    s = ddf[col].astype(str).str.strip().str.slice(0, 10)  # keep YYYY-MM-DD
    out = dd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
    return ddf.assign(**{col: out})

def get_unfiltered_dataframes():
    # Set file paths for data sourcesd
    file_paths = {
        'symbols': 'G:/My Drive/Python/Common/stock_tickers.csv',
        'price_data': 'G:/My Drive/Python/Common/get_historical_data_output.csv',
        'financial_data': 'G:/My Drive/Python/Common/df_financial_analysis_results.csv',
        'valuation_data': 'G:/My Drive/Python/Common/Valuation_Combined.csv',
        'price_target_data': 'G:/My Drive/Python/Common/df_price_targets.csv',
        'earnings_data': 'G:/My Drive/Python/Common/all_earnings_data.csv',
    }

    # Read in the dataframes
    df_symbols = read_csv_dask_with_inferred_types(file_paths["symbols"])
    df_symbols['URL'] = 'https://finviz.com/quote.ashx?t=' + df_symbols['Symbol'].astype(str).str.strip().str.upper()
    df_price_analysis = read_csv_dask_with_inferred_types(file_paths["price_data"])
    # df_earnings_data       = read_csv_dask_with_inferred_types(file_paths["earnings_data"])
    df_valuation_data       = read_csv_dask_with_inferred_types(file_paths["valuation_data"])
   
    # Set dict of all dataframes
    df_dict = {
        "df_symbols": df_symbols,
        "df_price_analysis": df_price_analysis,#[['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']],
        # "df_earnings_data": df_earnings_data[['Date', 'Symbol', 'Next Earnings Date', 'Previous Earnings Date', 'Last Earnings Performance', 'Days Until Next Earnings']],
        # "df_balance_sheet_data": df_balance_sheet_data,
        # "df_income_statement_data": df_income_statement_data,
        # "df_cash_flow_data": df_cash_flow_data,
        "df_valuation_data": df_valuation_data,
        # "df_price_target_data": df_price_target_data,
    }
    
    df_dict = filter_us_trading_days_ddf(df_dict)

    # For debugging
    # df_dict = apply_symbol_filter(df_dict, ['AAPL', 'FB'])

    return df_dict

def add_breakout_flag(dataframes, prompt_answers=None):
    """
    Identify post-squeeze breakout days using your existing helpers.
    Creates boolean flags + a simple score per row (per Symbol):

      SqueezeFlag   : BB_Range% below its EMA by a factor (low volatility)
      BandBreakFlag : %B > 1 + epsilon (close breaks above the upper BB)
      RangeSpikeFlag: Day's range% > multiple of EMA(range%)
      RelVolFlag    : DollarVolume > multiple of EMA(DollarVolume)
      MomentumFlag  : (optional) MACD momentum positive (hist or line>signal)
      BreakoutFlag  : SqueezeFlag & BandBreakFlag & RangeSpikeFlag & RelVolFlag & MomentumFlag?
      BreakoutScore : Simple numeric for ranking

    Parameters via `prompt_answers` (with sensible defaults):
      - dataframe           : default 'df_price_analysis'
      - bb_period           : 20
      - std_mult            : 2.0
      - lag_days            : 1            # to avoid lookahead
      - squeeze_ema_period  : 100
      - squeeze_mult        : 0.70         # BB_Range% < 0.70 * EMA(BB_Range%) → squeeze
      - band_break_eps      : 0.0          # require %B > 1 + eps
      - range_ma_period     : 20
      - range_mult          : 1.50         # Range% > 1.5× EMA(Range%)
      - dv_ma_period        : 20
      - dv_mult             : 1.50         # DollarVolume > 1.5× EMA(DollarVolume)
      - use_macd            : True
      - macd_mode           : 'hist>0'     # or 'line>signal'
      - target_col          : 'Close'      # for BB calculations

    Output columns:
      - SqueezeFlag, BandBreakFlag, RangeSpikeFlag, RelVolFlag, MomentumFlag
      - BreakoutFlag (uint8), BreakoutScore (float)

    Notes:
      - Tries to be resilient to missing helper columns (computes what it needs).
      - Minimal conversions to keep Dask-friendly vector ops.
    """
    import json
    import numpy as np
    import pandas as pd
    import dask.dataframe as dd

    if prompt_answers is None:
        prompt_answers = {}

    # ---------- read parameters ----------
    def _pa(key, default): return prompt_answers.get(key, default)

    df_name            = _pa('dataframe', 'df_price_analysis')
    bb_period          = int(_pa('bb_period', 20))
    std_mult           = float(_pa('std_mult', 2.0))
    lag_days           = int(_pa('lag_days', 1))
    squeeze_ema_period = int(_pa('squeeze_ema_period', 100))
    squeeze_mult       = float(_pa('squeeze_mult', 0.70))
    band_break_eps     = float(_pa('band_break_eps', 0.0))
    range_ma_period    = int(_pa('range_ma_period', 20))
    range_mult         = float(_pa('range_mult', 1.50))
    dv_ma_period       = int(_pa('dv_ma_period', 20))
    dv_mult            = float(_pa('dv_mult', 1.50))
    use_macd           = bool(_pa('use_macd', True))
    macd_mode          = _pa('macd_mode', 'hist>0')
    target_col         = _pa('target_col', 'Close')

    # ---------- fetch DF & normalize Date ----------
    ddf = dataframes[df_name]
    date_col, sym_col = 'Date', 'Symbol'
    ddf = ddf.assign(**{date_col: dd.to_datetime(ddf[date_col], errors='coerce')})

    # ---------- ensure prerequisites via your helpers ----------
    # 1) Bollinger Range% (normalized band width)
    k_str = f"{str(std_mult).rstrip('0').rstrip('.')}"
    bb_range_col = f"BB_RangePct_{target_col}_{bb_period}_{k_str}x"
    if lag_days:
        bb_range_col += f"_Lag{lag_days}"
    if bb_range_col not in ddf.columns:
        dataframes = add_bollinger_range_pct(dataframes, {
            'dataframe': df_name, 'period': bb_period, 'std_mult': std_mult,
            'target_col': target_col, 'lag_days': lag_days, 'as_percent': True
        })
        ddf = dataframes[df_name]

    # 2) EMA of Range% for squeeze baseline
    ema_bb_range_col = f"EMA_{bb_range_col}_{squeeze_ema_period}"
    if ema_bb_range_col not in ddf.columns:
        dataframes = add_ema(dataframes, {
            'dataframe': df_name, 'period': squeeze_ema_period, 'target_col': bb_range_col
        })
        ddf = dataframes[df_name]

    # 3) Full Bollinger Bands (need %B for band break)
    pctb_col = f"BB_PctB_{target_col}_{bb_period}_{k_str}x"
    up_col   = f"BB_Upper_{target_col}_{bb_period}_{k_str}x"
    if pctb_col not in ddf.columns or up_col not in ddf.columns:
        dataframes = add_bollinger_bands(dataframes, {
            'dataframe': df_name, 'period': bb_period, 'std_mult': std_mult,
            'target_col': target_col, 'add_pctb': True, 'add_bandw': False, 'lag_days': lag_days
        })
        ddf = dataframes[df_name]

    # 4) Day's range% (if you already have add_range_percent, great; else compute quickly)
    # Try to locate an existing range% column
    range_pct_col = None
    for c in ddf.columns:
        s = str(c).lower()
        # if 'range' in s and '%' in s:
        if s == 'DayRangePct_from_OHLC':
            range_pct_col = c
            break
    if range_pct_col is None:
        # Create a simple intraday range% from OHLC
        tmp_col = f"DayRangePct_from_OHLC"
        if tmp_col not in ddf.columns:
            ddf[tmp_col] = ( (ddf['High'] - ddf['Low']) / ddf[target_col] ) * 100.0
        range_pct_col = tmp_col
    dataframes[df_name] = ddf
    
    # EMA of range% for spike baseline
    ema_range_col = f"EMA_{range_pct_col}_{range_ma_period}"
    if ema_range_col not in ddf.columns:
        dataframes = add_ema(dataframes, {
            'dataframe': df_name, 'period': range_ma_period, 'target_col': range_pct_col
        })
        ddf = dataframes[df_name]

    # 5) Dollar Volume + EMA(DV) for relative-volume filter
    # Try add_dv if no DV-like column exists
    dv_candidates = [c for c in ddf.columns if str(c).lower() in ('dollarvolume','dollar_volume','dv')]
    if not dv_candidates:
        dataframes = add_dv(dataframes)  # assumes default df & column name
        ddf = dataframes[df_name]
        dv_candidates = [c for c in ddf.columns if str(c).lower() in ('dollarvolume','dollar_volume','dv')]
    dataframes[df_name] = ddf
    
    dv_col = dv_candidates[0]
    ema_dv_col = f"EMA_{dv_col}_{dv_ma_period}"
    if ema_dv_col not in ddf.columns:
        dataframes = add_ema(dataframes, {
            'dataframe': df_name, 'period': dv_ma_period, 'target_col': dv_col
        })
        ddf = dataframes[df_name]

    # 6) Optional MACD confirmation
    macd_hist_col = None
    macd_line_col = macd_sig_col = None
    if use_macd:
        # Make sure MACD exists
        macd_like = [c.lower() for c in ddf.columns]
        need_macd = not any('macd' in s for s in macd_like)
        if need_macd:
            dataframes = add_macd(dataframes, {'dataframe': df_name})
            ddf = dataframes[df_name]
        # Try to find MACD columns
        for c in ddf.columns:
            cl = str(c).lower()
            if 'macd' in cl and 'hist' in cl:
                macd_hist_col = c
            elif 'macd' in cl and '_signal' in cl:#cl.endswith('_signal') or ('macd_signal' in cl):
                macd_sig_col = c
            elif 'macd' in cl :#and ('line' in cl or cl.endswith('_macd')):
                macd_line_col = c

    # ---------- compute flags ----------
    # Squeeze: BB_Range% below EMA by factor
    squeeze = (ddf[bb_range_col] < (squeeze_mult * ddf[ema_bb_range_col]))

    # Band break: %B > 1 + eps  (close outside the upper band)
    band_break = (ddf[pctb_col] > (1.0 + band_break_eps))

    # Range spike: day range% exceeds multiple of its EMA
    range_spike = (ddf[range_pct_col] > (range_mult * ddf[ema_range_col]))

    # Relative volume spike: DV > multiple of EMA(DV)
    rel_vol = (ddf[dv_col] > (dv_mult * ddf[ema_dv_col]))

    # Momentum confirmation
    if use_macd and macd_hist_col is not None and macd_mode == 'hist>0':
        mom = (ddf[macd_hist_col] > 0)
    elif use_macd and macd_line_col is not None and macd_sig_col is not None and macd_mode == 'line>signal':
        mom = (ddf[macd_line_col] > ddf[macd_sig_col])
    else:
        mom = True  # if MACD not available, don't block

    # Combine
    breakout_flag = (squeeze & band_break & range_spike & rel_vol & mom)

    # Score (simple, monotonic): multiply a few normalized pieces
    # (1 + pct above thresholds) to rank the strongest breaks on a date
    eps = 1e-9
    score = (
        ( (ddf[pctb_col] - 1.0).clip(lower=0) + eps )
        * ( (ddf[range_pct_col] / (ddf[ema_range_col] + eps)).clip(lower=0) )
        * ( (ddf[dv_col]     / (ddf[ema_dv_col] + eps)).clip(lower=0) )
    )

    ddf = ddf.assign(
        SqueezeFlag     = squeeze.astype('uint8'),
        BandBreakFlag   = band_break.astype('uint8'),
        RangeSpikeFlag  = range_spike.astype('uint8'),
        RelVolFlag      = rel_vol.astype('uint8'),
        MomentumFlag    = (mom.astype('uint8') if isinstance(mom, dd.Series) else (dd.from_array(np.ones(len(ddf), dtype='uint8')) if mom is True else mom)),
        BreakoutFlag    = breakout_flag.astype('uint8'),
        BreakoutScore   = score.astype('float64'),
    )

    # Persist back
    dataframes[df_name] = ddf

    # Log
    try:
        log_key = "df_actions"
        entry = {
            "timestamp": pd.Timestamp.now(),
            "action": "add_breakout_flag",
            "dataframe": df_name,
            "added_columns": ["SqueezeFlag","BandBreakFlag","RangeSpikeFlag","RelVolFlag","MomentumFlag","BreakoutFlag","BreakoutScore"],
            "prompt_answers_json": json.dumps({
                'dataframe': df_name, 'bb_period': bb_period, 'std_mult': std_mult, 'lag_days': lag_days,
                'squeeze_ema_period': squeeze_ema_period, 'squeeze_mult': squeeze_mult,
                'band_break_eps': band_break_eps, 'range_ma_period': range_ma_period, 'range_mult': range_mult,
                'dv_ma_period': dv_ma_period, 'dv_mult': dv_mult, 'use_macd': use_macd, 'macd_mode': macd_mode,
                'target_col': target_col
            }, default=str, sort_keys=True),
        }
        import pandas as pd
        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
        else:
            df_log = dataframes[log_key]
            if "prompt_answers_json" not in df_log.columns:
                df_log["prompt_answers_json"] = ""
            if not (df_log["prompt_answers_json"] == entry["prompt_answers_json"]).any():
                df_log.loc[len(df_log)] = entry
    except Exception:
        pass

    return dataframes


def apply_dataframes_actions(dataframes):
    """
    Applies all filters stored in dataframes['df_actions'] by calling add_filter.
    Wraps each filter application in a try/except and logs progress.
    """
    try:
        # 1) Check for filters DataFrame
        if 'df_actions' not in dataframes:
            write_line("No filters found in dataframes. Nothing to apply.")
            return

        df_actions = dataframes['df_actions']
        total = len(df_actions)
        write_line(f"Starting to apply {total} filter(s)...")

        # 2) Iterate through each filter row
        for idx, row in df_actions.iterrows():
            action = row['action']
            try:                
                write_line(f"[{idx+1}/{total}] Calling {action} action: {row.to_dict()}")
                if action == 'add_custom_filter':
                    dataframes = add_custom_filter(dataframes, prompt_answers=json.loads(row['prompt_answers_json'])) 
                elif action == "add_ema":
                    dataframes  = add_ema(dataframes, prompt_answers=json.loads(row['prompt_answers_json']))
                elif action == "add_dv":
                    dataframes = add_dv(dataframes)       
                elif action == 'add_macd':
                    dataframes = add_macd(dataframes, prompt_answers=json.loads(row['prompt_answers_json']))  
                elif action == 'add_crossover_proximity':
                    dataframes = add_crossover_proximity(dataframes, prompt_answers=json.loads(row['prompt_answers_json']))
                elif action == 'add_range_percent':
                    dataframes = add_range_percent(dataframes, prompt_answers=json.loads(row['prompt_answers_json']))
                elif action == 'add_atr' or action == 'add_adr':
                    dataframes = add_atr(dataframes, prompt_answers=json.loads(row['prompt_answers_json']))
                elif action == 'add_rank_by_date':
                    dataframes = add_rank_by_date(dataframes, prompt_answers=json.loads(row['prompt_answers_json']))
                elif action == 'add_weighted_expression':
                    dataframes = add_weighted_expression(dataframes, prompt_answers=json.loads(row['prompt_answers_json']))
                elif action == 'add_bollinger_bands':
                    dataframes = add_bollinger_bands(dataframes, prompt_answers=json.loads(row['prompt_answers_json']))
                elif action == 'add_bollinger_range_pct':
                    dataframes = add_bollinger_range_pct(dataframes, prompt_answers=json.loads(row['prompt_answers_json']))
                elif action == 'add_breakout_flag':
                    dataframes = add_breakout_flag(dataframes, prompt_answers=json.loads(row['prompt_answers_json']))
                else:
                    write_line(f"Warning: {action} not implemented.")
                write_line(f"[{idx+1}/{total}] Action applied successfully.")
                
                # Prompt user to materialize
                user_input = input("Materialize? [y|N]: ")
                if user_input == 'y':
                    dataframes = materialize_dataframe(dataframes)
                    
            except Exception as inner_e:
                write_line(f"[{idx+1}/{total}] Error applying filter: {inner_e}")

        write_line("Finished applying all filters.")

    except Exception as e:
        # Catch any unexpected errors in the overall process
        write_line(f"Error in apply_dataframes_actions: {e}")
    return dataframes

#endregion 
  
#region Filter functions

def filter_symbols_by_columns(
    ddf,                          # Dask or pandas DataFrame
    col1: str,
    op: str,                      # one of '=', '==', '!=', '>', '>=', '<', '<='
    col2: str,
    start_date: Union[str, pd.Timestamp, None],
    end_date:   Union[str, pd.Timestamp, None],
) -> List[str]:
    """
    For each Symbol, compute the average value per DATE within [start_date, end_date],
    then average those daily values across the range (so each date has equal weight).
    Return Symbols where <avg(col1)> (op) <avg(col2)> holds.
    Works with Dask or pandas. Final list is computed to Python.
    """

    # 1) parse/normalize dates
    start_ts = pd.Timestamp(start_date) if start_date is not None else None
    end_ts   = pd.Timestamp(end_date)   if end_date   is not None else None

    # 2) ensure Date is datetime64[ns] (works for both pandas & dask)
    ddf = ddf.assign(Date=ddf["Date"].astype("datetime64[ns]"))

    # 3) filter by date window (inclusive bounds)
    if start_ts is not None:
        ddf = ddf[ddf["Date"] >= start_ts]
    if end_ts is not None:
        ddf = ddf[ddf["Date"] <= end_ts]

    # 4) coerce target columns to numeric (NaNs ignored by mean)
    #    use map_partitions(pd.to_numeric) if this is a Dask Series
    def _to_numeric(series):
        try:
            # If it's a Dask Series, prefer map_partitions to avoid eager compute
            import dask.dataframe as dd  # noqa: F401
            if hasattr(series, "map_partitions"):
                return series.map_partitions(pd.to_numeric, errors="coerce",
                                             meta=(series.name, "float64"))
        except Exception:
            pass
        return pd.to_numeric(series, errors="coerce")

    ddf[col1] = _to_numeric(ddf[col1])
    ddf[col2] = _to_numeric(ddf[col2])

    # 5) average *by date* first so each date counts once per Symbol,
    #    then average across dates to get a single per-Symbol average
    per_day = ddf.groupby(["Symbol", "Date"])[[col1, col2]].mean()
    per_symbol = per_day.groupby("Symbol")[[col1, col2]].mean()

    # 6) operator mapping
    ops = {
        "=":  operator.eq,
        "==": operator.eq,
        "!=": operator.ne,
        ">":  operator.gt,
        ">=": operator.ge,
        "<":  operator.lt,
        "<=": operator.le,
    }
    if op not in ops:
        raise ValueError(f"Unsupported comparison operator: {op!r}")
    cmp_func = ops[op]

    # 7) apply comparison and return matching Symbols
    matched = per_symbol[cmp_func(per_symbol[col1], per_symbol[col2])]

    # Dask -> pandas; pandas -> no-op
    try:
        matched = matched.compute()
    except Exception:
        pass

    return list(matched.index)

def filter_symbols_by_average(
    df: pd.DataFrame,
    selected_col: str,
    selected_cmp: str,
    threshold: Union[str, int],
    start_date: Union[str, date],
    end_date:   Union[str, date],
    date_col:   str = 'Date',
    symbol_col: str = 'Symbol'
) -> List[str]:
    """
    Filters symbols whose average of `selected_col` over the given date range
    satisfies the comparison against `threshold`.
    """
    
    # 1) get the averages
    if str == type(threshold):
        avg_df = compute_symbol_averages(
            df, [selected_col, threshold], start_date, end_date, date_col, symbol_col
        )
    else:
        avg_df = compute_symbol_averages(
            df, selected_col, start_date, end_date, date_col, symbol_col
        )

    # 2) map comparison operator to function
    ops = {
        '=':  operator.eq,
        '==': operator.eq,
        '!=': operator.ne,
        '>':  operator.gt,
        '>=': operator.ge,
        '<':  operator.lt,
        '<=': operator.le,
    }
    if selected_cmp not in ops:
        raise ValueError(f"Unsupported comparison operator: {selected_cmp!r}")
    cmp_func = ops[selected_cmp]

    # 3) apply filter
    if type(threshold) == type(float):
        mask = cmp_func(avg_df[selected_col], threshold)
    else:
        mask = cmp_func(avg_df[selected_col], threshold)
        

    # 4) return matching symbols
    # avg_df = avg_df.reset_index()
    # return list(avg_df['Symbol'])
    return list(avg_df[mask].reset_index()["Symbol"].compute())
      
           
def clear_filters(dataframes):
    try:
        write_line("Clear Filters selected.")
        dataframes = get_unfiltered_dataframes()
        if 'df_actions' in dataframes.keys():
            del dataframes['df_actions']
        write_line(f"Dataframes cleared.")
    except Exception as e:
        write_line(f"Error in Remove Filter: {e}")
    return dataframes

def add_custom_filter(dataframes, prompt_answers=None):
    """
    Interactive filter on a dict of Dask DataFrames.

    Modes:
      1) Direct: select one column, list its unique values, pick by index (or numeric compare).
      2) Relative: sort by a numeric column then take top N rows or top N%.
    
    Behavior:
      - Tracks ALL user inputs in `prompt_answers` (mutated in-place).
      - If keys already exist in `prompt_answers`, they are used and no prompt is shown.
      - Writes a log row to `dataframes['df_actions']` with a JSON snapshot of prompt_answers.
    """
    try:
        import json
        import pandas as pd
        import dask.dataframe as dd
        from datetime import datetime
        from pandas.api.types import is_datetime64_any_dtype  # via pd.api.* is fine too
        # Use pandas API so it works for both pandas & dask dtypes
        is_numeric_dtype = pd.api.types.is_numeric_dtype

        write_line("Add Custom Filter selected.")
        description = ""
        symbols = []

        # Ensure we have a dict we can mutate
        if prompt_answers is None:
            prompt_answers = {}

        # —————— Select DataFrame ——————
        df_keys = list(dataframes.keys())
        if 'dataframe' not in prompt_answers:
            write_line("Available dataframes:")
            for i, key in enumerate(df_keys, 1):
                write_line(f"  {i}. {key}")
            choice = input("Select dataframe by number: ").strip()
            if not (choice.isdigit() and 1 <= int(choice) <= len(df_keys)):
                write_line("Invalid selection. Aborting filter.")
                return dataframes
            prompt_answers['dataframe'] = df_keys[int(choice) - 1]

        key = prompt_answers['dataframe']
        df = dataframes[key]

        # —————— Select Column ——————
        sel_col = 'target_col'
        if 'target_col' in prompt_answers:
            sel_col = 'target_col'
        else:
            sel_col = 'column'
        if sel_col not in prompt_answers:
            cols = list(df.columns)
            write_line("Columns available to filter:")
            for i, col_name in enumerate(cols, 1):
                write_line(f"  {i}. {col_name}")
            col_choice = input("Select column by number: ").strip()
            if not (col_choice.isdigit() and 1 <= int(col_choice) <= len(cols)):
                write_line("Invalid column. Aborting filter.")
                return dataframes
            prompt_answers[sel_col] = cols[int(col_choice) - 1]

        target_col = prompt_answers[sel_col]

        # —————— Branch by dtype ——————
        # normalize dtype checks to work with Dask/pandas
        col_dtype = df[target_col].dtype

        # ===== 1) NUMERIC =====
        if is_numeric_dtype(col_dtype):
            # Filter mode
            if 'mode' not in prompt_answers:
                print("Select filter type: 1. Direct  2. Relative")
                mode_choice = input("Enter 1 or 2: ").strip()
                if mode_choice == "1":
                    prompt_answers['mode'] = "Direct"
                elif mode_choice == "2":
                    prompt_answers['mode'] = "Relative"
                else:
                    write_line("Invalid filter type. Aborting.")
                    return dataframes

            # ---------- Direct mode ----------
            if prompt_answers['mode'] == "Direct":
                # comparison operator
                comparison_filters = ["=", ">", ">=", "<", "<=", "!="]
                if 'comparison_operator' not in prompt_answers:
                    print("Comparison filters available:")
                    for i, op in enumerate(comparison_filters, 1):
                        print(f"  {i}. {op}")
                    cmp_choice = input("Select comparison by number: ").strip()
                    if not (cmp_choice.isdigit() and 1 <= int(cmp_choice) <= len(comparison_filters)):
                        write_line("Invalid comparison. Aborting filter.")
                        return dataframes
                    prompt_answers['comparison_operator'] = comparison_filters[int(cmp_choice) - 1]

                selected_cmp = prompt_answers['comparison_operator']

                # compare to column or value
                if 'compare_to' not in prompt_answers:
                    print("Select compare target: 1. Column  2. Value")
                    cv_choice = input("Enter 1 or 2: ").strip()
                    if cv_choice == "1":
                        prompt_answers['compare_to'] = "column"
                    elif cv_choice == "2":
                        prompt_answers['compare_to'] = "value"
                    else:
                        write_line("Invalid choice. Aborting filter.")
                        return dataframes

                # Use provided dates in prompt_answers if present; otherwise prompt with defaults.
                df_min_dt = df['Date'].min().compute()                    
                df_max_dt = df['Date'].max().compute()
                if 'start_date' in prompt_answers:
                    start_date = pd.to_datetime(str(prompt_answers['start_date']), errors='coerce')
                    write_line(f"  • start_date from prompt_answers: {start_date.date() if pd.notna(start_date) else 'NaT'}")
                else:
                    default_start = df_min_dt.date().isoformat() if pd.notna(df_min_dt) else "2000-01-01"
                    start_date_str = get_input_parameter("Start date (YYYY-MM-DD)", str, default_start, [df_min_dt, df_max_dt])
                    start_date = pd.to_datetime(start_date_str, errors='coerce')
                    prompt_answers['start_date'] = start_date.date().isoformat() if pd.notna(start_date) else default_start
                    write_line(f"  • start_date selected: {prompt_answers['start_date']}")

                if 'end_date' in prompt_answers:
                    end_date = pd.to_datetime(str(prompt_answers['end_date']), errors='coerce')
                    write_line(f"  • end_date from prompt_answers: {end_date.date() if pd.notna(end_date) else 'NaT'}")
                else:
                    default_end = df_max_dt.date().isoformat() if pd.notna(df_max_dt) else pd.Timestamp.today().date().isoformat()
                    end_date_str = get_input_parameter("End date (YYYY-MM-DD)", str, default_end, [df_min_dt, df_max_dt])
                    end_date = pd.to_datetime(end_date_str, errors='coerce')
                    prompt_answers['end_date'] = end_date.date().isoformat() if pd.notna(end_date) else default_end
                    write_line(f"  • end_date selected: {prompt_answers['end_date']}")


                # --- compare to COLUMN
                if prompt_answers['compare_to'] == "column":
                    if 'second_column' not in prompt_answers:
                        cols = list(df.columns)
                        print("Columns available to compare against:")
                        for i, cn in enumerate(cols, 1):
                            print(f"  {i}. {cn}")
                        col2_choice = input("Select column by number: ").strip()
                        if not (col2_choice.isdigit() and 1 <= int(col2_choice) <= len(cols)):
                            print("Invalid column. Aborting filter.")
                            return dataframes
                        prompt_answers['second_column'] = cols[int(col2_choice) - 1]

                    col2 = prompt_answers['second_column']
                    symbols = filter_symbols_by_columns(df, target_col, selected_cmp, col2, start_date, end_date)
                    dataframes = apply_symbol_filter(dataframes, symbols)
                    description = f"{key} - {target_col} {selected_cmp} {col2} (column compare)"

                # --- compare to VALUE
                else:
                    # min/max range (use helper if you have it)
                    try:
                        min_val, max_val = get_float_column_range(df, target_col)
                    except Exception:
                        # Fallback: compute directly
                        min_val = df[target_col].min().compute()
                        max_val = df[target_col].max().compute()

                    # value
                    if 'value' not in prompt_answers:
                        value_input = input(
                            f"Enter a value for filter “{target_col} {selected_cmp} …” "
                            f"(must be between {min_val} and {max_val}): "
                        ).strip()
                        try:
                            prompt_answers['value'] = float(value_input)
                        except ValueError:
                            write_line(f"❌ “{value_input}” is not a valid number. Aborting filter.")
                            return dataframes
                    else:
                        # ensure it's numeric
                        try:
                            prompt_answers['value'] = float(prompt_answers['value'])
                        except Exception:
                            write_line("Provided 'value' is not numeric. Aborting filter.")
                            return dataframes



                    symbols = filter_symbols_by_average(
                        df,
                        selected_col=target_col,
                        selected_cmp=selected_cmp,
                        threshold=prompt_answers['value'],
                        start_date=start_date,
                        end_date=end_date
                    )
                    dataframes = apply_symbol_filter(dataframes, symbols)
                    description = f"{key} - {target_col} {selected_cmp} {prompt_answers['value']}"

            # ---------- Relative mode ----------
            else:
                # sort_direction
                if 'sort_direction' not in prompt_answers:
                    write_line("Sort direction: 1. Ascending  2. Descending")
                    asc_choice = input("Enter 1 or 2: ").strip()
                    prompt_answers['sort_direction'] = 'asc' if asc_choice == "1" else 'desc'
                asc = (prompt_answers['sort_direction'] == 'asc')

                # method: percent or rows
                if 'n_or_percent' not in prompt_answers:
                    write_line("Select method: 1. Top N percent  2. Top N rows")
                    method_choice = input("Enter 1 or 2: ").strip()
                    prompt_answers['n_or_percent'] = 'percent' if method_choice == "1" else 'rows'

                # value: either percent (0-100) or N
                if 'value' not in prompt_answers:
                    if prompt_answers['n_or_percent'] == 'percent':
                        pct = float(input("Enter percent (e.g. 10): ").strip())
                        prompt_answers['value'] = pct
                    else:
                        n = int(input("Enter N rows: ").strip())
                        prompt_answers['value'] = n
                else:
                    # normalize to float for percent, int for rows
                    if prompt_answers['n_or_percent'] == 'percent':
                        prompt_answers['value'] = float(prompt_answers['value'])
                    else:
                        prompt_answers['value'] = int(prompt_answers['value'])

                # Use provided dates in prompt_answers if present; otherwise prompt with defaults.
                if 'start_date' in prompt_answers:
                    start_date = pd.to_datetime(str(prompt_answers['start_date']), errors='coerce')
                    write_line(f"  • start_date from prompt_answers: {start_date.date() if pd.notna(start_date) else 'NaT'}")
                else:
                    df_min_dt = df['Date'].min().compute()
                    default_start = df_min_dt.date().isoformat() if pd.notna(df_min_dt) else "2000-01-01"
                    start_date_str = get_input_parameter("Start date (YYYY-MM-DD)", str, default_start, [default_start])
                    start_date = pd.to_datetime(start_date_str, errors='coerce')
                    prompt_answers['start_date'] = start_date.date().isoformat() if pd.notna(start_date) else default_start
                    write_line(f"  • start_date selected: {prompt_answers['start_date']}")

                if 'end_date' in prompt_answers:
                    end_date = pd.to_datetime(str(prompt_answers['end_date']), errors='coerce')
                    write_line(f"  • end_date from prompt_answers: {end_date.date() if pd.notna(end_date) else 'NaT'}")
                else:
                    df_max_dt = df['Date'].max().compute()
                    default_end = df_max_dt.date().isoformat() if pd.notna(df_max_dt) else pd.Timestamp.today().date().isoformat()
                    end_date_str = get_input_parameter("End date (YYYY-MM-DD)", str, default_end, [default_end])
                    end_date = pd.to_datetime(end_date_str, errors='coerce')
                    prompt_answers['end_date'] = end_date.date().isoformat() if pd.notna(end_date) else default_end
                    write_line(f"  • end_date selected: {prompt_answers['end_date']}")
                    
                # compute averages & sort
                symbol_averages = compute_symbol_averages(df, target_col, start_date, end_date)
                symbol_averages = symbol_averages.sort_values('avg_value', ascending=asc)

                total = symbol_averages.shape[0]
                if prompt_answers['n_or_percent'] == 'percent':
                    pct = max(0.0, min(100.0, float(prompt_answers['value'])))
                    k = max(1, int(total * (pct / 100.0)))
                    desc_piece = f"top {int(pct)}%"
                else:
                    k = max(1, int(prompt_answers['value']))
                    desc_piece = f"top {k}"

                selected_symbols = symbol_averages.iloc[:k]['Symbol'].tolist()
                dataframes = apply_symbol_filter(dataframes, selected_symbols)
                description = f"{key} - {target_col} ({'asc' if asc else 'desc'}) - {desc_piece}"

        # ===== 2) DATETIME =====
        elif is_datetime64_any_dtype(col_dtype):
            # Use provided dates in prompt_answers if present; otherwise prompt with defaults.
            if 'start_date' in prompt_answers:
                start_date = pd.to_datetime(str(prompt_answers['start_date']), errors='coerce')
                write_line(f"  • start_date from prompt_answers: {start_date.date() if pd.notna(start_date) else 'NaT'}")
            else:
                df_min_dt = df['Date'].min().compute()
                default_start = df_min_dt.date().isoformat() if pd.notna(df_min_dt) else "2000-01-01"
                start_date_str = get_input_parameter("Start date (YYYY-MM-DD)", str, default_start, [default_start])
                start_date = pd.to_datetime(start_date_str, errors='coerce')
                prompt_answers['start_date'] = start_date.date().isoformat() if pd.notna(start_date) else default_start
                write_line(f"  • start_date selected: {prompt_answers['start_date']}")

            if 'end_date' in prompt_answers:
                end_date = pd.to_datetime(str(prompt_answers['end_date']), errors='coerce')
                write_line(f"  • end_date from prompt_answers: {end_date.date() if pd.notna(end_date) else 'NaT'}")
            else:
                df_max_dt = df['Date'].max().compute()
                default_end = df_max_dt.date().isoformat() if pd.notna(df_max_dt) else pd.Timestamp.today().date().isoformat()
                end_date_str = get_input_parameter("End date (YYYY-MM-DD)", str, default_end, [default_end])
                end_date = pd.to_datetime(end_date_str, errors='coerce')
                prompt_answers['end_date'] = end_date.date().isoformat() if pd.notna(end_date) else default_end
                write_line(f"  • end_date selected: {prompt_answers['end_date']}")

            dataframes = apply_date_filter(dataframes, start_date, end_date)
            description = f"{key} - {target_col} Date Range: {start_date} to {end_date}"

        # ===== 3) STRING / CATEGORICAL =====
        else:
            # value = list of selected categories (strings)
            if 'value' not in prompt_answers:
                import os, glob, csv as _csv
                import pandas as pd

                # Ask if the user wants to load values from a CSV
                yn = input("Load values from a CSV in 'Data' folder (IN)? (y/N): ").strip().lower()
                use_csv = (yn == 'y')

                if use_csv:
                    # --- List CSVs in the Data folder and select by index ---
                    csv_dir = prompt_answers.get('csv_dir', 'Data/Filters')
                    paths = sorted([p for p in glob.glob(os.path.join(csv_dir, "*.csv")) if os.path.isfile(p)],
                                   key=lambda p: (os.path.basename(p).lower(), p.lower()))
                    if not paths:
                        write_line(f"No CSV files found in '{csv_dir}'. Falling back to manual selection.")
                        use_csv = False
                    else:
                        write_line("CSV files found:")
                        for i, p in enumerate(paths, 1):
                            write_line(f"  {i}. {p}")
                        if len(paths) == 1:
                            chosen_path = paths[0]
                            write_line(f"Only one CSV found; selected: {chosen_path}")
                        else:
                            sel = input("Select CSV by number: ").strip()
                            if not (sel.isdigit() and 1 <= int(sel) <= len(paths)):
                                write_line("Invalid CSV selection. Falling back to manual selection.")
                                use_csv = False
                            else:
                                chosen_path = paths[int(sel) - 1]

                if use_csv:
                    # --- Choose delimiter (use provided or sniff) ---
                    sep = (str(prompt_answers.get('csv_sep', '')).strip() or None)
                    if sep is None:
                        try:
                            with open(chosen_path, "r", encoding="utf-8", errors="ignore") as f:
                                sample = f.read(10000)
                            dialect = _csv.Sniffer().sniff(sample, delimiters=[",",";","\t","|"])
                            sep = dialect.delimiter
                        except Exception:
                            sep = ","

                    # --- Peek and choose a column ---
                    try:
                        tmp = pd.read_csv(chosen_path, sep=sep, nrows=5, dtype=str, engine="python")
                    except Exception as e:
                        write_line(f"❌ Could not read CSV: {chosen_path}  Error: {e}. Falling back to manual selection.")
                        use_csv = False

                if use_csv:
                    cols_list = list(tmp.columns)
                    if not cols_list:
                        write_line("CSV has no columns. Falling back to manual selection.")
                        use_csv = False
                    else:
                        write_line(f"Columns in CSV ({chosen_path}):")
                        for i, cn in enumerate(cols_list, 1):
                            write_line(f"  {i}. {cn}")
                        csel = input("Select CSV column by number: ").strip()
                        try:
                            csel_i = int(csel) - 1
                            csv_col = cols_list[csel_i]
                        except Exception:
                            write_line("Invalid CSV column selection. Falling back to manual selection.")
                            use_csv = False

                if use_csv:
                    # --- Load full column and normalize string values ---
                    try:
                        csv_df = pd.read_csv(chosen_path, sep=sep, dtype=str, engine="python")
                    except Exception as e:
                        write_line(f"❌ Failed to load CSV fully: {e}. Falling back to manual selection.")
                        use_csv = False

                if use_csv:
                    ser = csv_df[csv_col].astype(str)
                    if bool(prompt_answers.get('csv_strip', True)):
                        ser = ser.str.strip()
                    if bool(prompt_answers.get('csv_lower', False)):
                        ser = ser.str.lower()
                    selected = ser[ser.notna() & (ser != "")].unique().tolist()
                    write_line(f"Loaded {len(selected)} unique value(s) from {chosen_path}:{csv_col} (sep='{sep}')")
                    prompt_answers['value'] = selected
                    prompt_answers['comparison_operator'] = "IN"
                else:
                    # --- Original manual picker (fallback) ---
                    unique_vals = df[target_col].dropna().unique().compute().tolist()
                    write_line(f"Unique values in '{target_col}':")
                    for idx, val in enumerate(unique_vals, 1):
                        write_line(f"  {idx}. {val!r}")
                    picks = input("Select value indices (comma-separated): ").strip()
                    try:
                        idxs = [int(x) - 1 for x in picks.split(",")]
                        selected = [unique_vals[i] for i in idxs]
                    except Exception:
                        write_line("Invalid indices. Aborting filter.")
                        return dataframes
                    prompt_answers['value'] = selected
                    prompt_answers['comparison_operator'] = "IN"
            else:
                selected = prompt_answers['value']
                # allow JSON string or comma-separated string
                if isinstance(selected, str):
                    try:
                        parsed = json.loads(selected)
                        if isinstance(parsed, list):
                            selected = parsed
                    except Exception:
                        if ',' in selected:
                            selected = [s.strip() for s in selected.split(',') if s.strip()]
                        else:
                            selected = [selected]
                elif not isinstance(selected, list):
                    selected = [str(selected)]
                prompt_answers['value'] = selected
                prompt_answers['comparison_operator'] = "IN"

            # Apply
            comparison_operator = "IN"
            prompt_answers['comparison_operator'] = comparison_operator
            df_filt = df[df[target_col].isin(prompt_answers['value'])]
            write_line(f"Filtering {target_col!r} ∈ {prompt_answers['value']!r}")

            # If you want to reduce all dataframes to only the selected symbols:
            if 'Symbol' in df_filt.columns:
                try:
                    symbols = df_filt['Symbol'].dropna().unique().compute().tolist()
                except Exception:
                    symbols = df_filt['Symbol'].compute().dropna().unique().tolist()
                dataframes = apply_symbol_filter(dataframes, symbols)

            description = f"{key} - {target_col} - IN - {prompt_answers['value']!r}"

        # —————— Log the filter ——————
        log_key = "df_actions"
        entry = {
            'timestamp': pd.Timestamp.now(),
            'dataframe': key,
            'action': 'add_custom_filter',
            'description': description,
            'prompt_answers_json': json.dumps(prompt_answers, default=str, sort_keys=True),
        }

        if log_key not in dataframes:
            dataframes[log_key] = pd.DataFrame([entry])
            write_line(f"Created prompt log '{log_key}' with first entry.")
        else:
            df_log = dataframes[log_key]
            # Dedup based on exact prompt_answers snapshot; create col if missing
            if 'prompt_answers_json' not in df_log.columns:
                df_log['prompt_answers_json'] = ""
            if (df_log['prompt_answers_json'] == entry['prompt_answers_json']).any():
                write_line("An identical prompt_answers entry already exists. Skipping log append.")
            else:
                df_log.loc[len(df_log)] = entry
                write_line(f"Appended new entry to filter log '{log_key}'.")
        # dataframes = persist_dataframes(dataframes)
        return dataframes

    except Exception as e:
        write_line(f"Error in Add Filter: {e}")
        return dataframes
    
def add_preset_filter(dataframes):
    
    # Prompt user for date range to apply filter
    choice = input("Select last N number of days to apply filter: ").strip()
    if not (choice.isdigit()):
        write_line("Invalid selection. Aborting date filter.")
        return
    days_back = int(choice)
    
    # ==================== HIGH RISK, HIGH REWARD ====================
    # Strategy 1: Volatile Momentum Breakout
    # - High recent volatility combined with bullish technical signals (CCI and MACD).
    # Criteria:
    #   - Return_Volatility in top 10%
    #   - CCI > +100 (strong bullish momentum)
    #   - MACD_Signal_Line crossover positive recently (MACD > Signal_Line)
    #   - Bollinger_Range in top 20% (suggesting breakout moves)
    end_date = dataframes['df_price_data']['Date'].max().compute()
    start_date = end_date - timedelta(days=days_back)
    prompt_answers = [
        {
            'dataframe': 'df_price_data',
            'start_date': start_date,
            'end_date': end_date,
            'column': 'Return_Volatility',
            'mode': 'Relative',
            'sort_direction': 'desc',
            'n_or_percent': 'percent',
            'value': 10        
        },
        {
            'dataframe': 'df_price_data',
            'start_date': start_date,
            'end_date': end_date,
            'column': 'CCI',
            'mode': 'Direct',
            'comparison_operator': '>=',
            'value': 10        
        }
    ]


    # Strategy 2: Oversold Reversal Candidates
    # - Stocks sharply below recent moving averages, potentially due for rapid mean-reversion.
    # Criteria:
    #   - 20_Day_MA_Diff% in bottom 20% (oversold relative to 20-day MA)
    #   - Stochastic_Oscillator_Range < 20 (indicating oversold conditions)
    #   - Return_Volatility in top 20% (large moves expected)

    # Strategy 3: Volatile Mid-Range Swing Trades
    # - Stocks with high volatility trading in middle of their annual range, implying potential explosive moves either direction.
    # Criteria:
    #   - 52_Week_Range_Percent between 30%-70%
    #   - Return_Volatility in top 20%
    #   - Bollinger_Range in top 20% (wide bands indicating volatility)

    # ==================== MEDIUM RISK, MEDIUM REWARD ====================

    # Strategy 1: Trend Stability Selection
    # - Moderately volatile stocks showing stable trends and moderate momentum indicators.
    # Criteria:
    #   - Standardized_MACD between -1 and 1 (moderate momentum)
    #   - Return_Volatility in 40%-60% range
    #   - Bollinger_Range moderate (40%-60%, stable volatility)
    #   - Ichimoku Score neutral to slightly bullish

    # Strategy 2: Controlled Pullbacks in Uptrends
    # - Stocks recently retracing slightly within steady upward trends.
    # Criteria:
    #   - 20_Day_MA_Diff% slightly negative but within moderate range (middle 40%-60%)
    #   - CCI between 0 and -100 (moderate pullback)
    #   - Return_Volatility in moderate range (middle 40%-60%)

    # Strategy 3: Mid-Channel Range Trading
    # - Stocks consistently oscillating within a predictable range.
    # Criteria:
    #   - Bollinger_Close near 0 (mid-channel price)
    #   - Bollinger_Range moderate (middle 40%-60%)
    #   - MACD near signal line (indicating neutral momentum)

    # ==================== LOW RISK, LOW REWARD ====================

    # Strategy 1: Defensive, Low-Volatility Leaders
    # - Stocks trading consistently near their highs with minimal volatility.
    # Criteria:
    #   - 52_Week_Range_Percent above 80% (near 52-week highs)
    #   - Return_Volatility bottom 10%
    #   - VWAP_Diff bottom 20% (consistently trading near VWAP)
    #   - Ichimoku Score consistently positive

    # Strategy 2: Stable Dividend-like Performers
    # - Stable, predictable price movements resembling bond-like consistency.
    # Criteria:
    #   - Daily_Return volatility bottom 20%
    #   - Diff%_Minus_RiskFree bottom 20%-40% (slightly above risk-free rate)
    #   - 20_Day_STD_Diff% bottom 10% (low volatility around 20-day average)

    # Strategy 3: Low-Volatility Consolidation
    # - Stocks trading sideways with very low volatility, potentially breaking out slowly over time.
    # Criteria:
    #   - Bollinger_Range bottom 20% (narrow bands indicating consolidation)
    #   - Return_Volatility bottom 20%
    #   - Standardized_MACD close to zero (minimal momentum changes)

    return dataframes       
       
def apply_date_filter(dataframes, start_date, end_date):
    """
    Filters each Dask DataFrame in `dataframes` to only include rows
    where 'Date' is between start_date and end_date (inclusive).

    Parameters
    ----------
    dataframes : dict[str, dd.DataFrame]
        A dict of named Dask DataFrames.
    start_date, end_date : str or datetime-like
        The date range bounds (inclusive). Can be strings like "2025-01-01"
        or pandas.Timestamp, etc.

    Returns
    -------
    dict[str, dd.DataFrame]
        The same dict, but with each qualifying DataFrame replaced by its filtered version.
    """
    # Normalize the bounds
    start = pd.to_datetime(start_date)
    end   = pd.to_datetime(end_date)

    for key, df in dataframes.items():
        if isinstance(df, dd.DataFrame) and 'Date' in df.columns:
            # Ensure 'Date' is datetime
            df = df.assign(Date=dd.to_datetime(df['Date']))

            # Build and apply mask
            mask = (df['Date'] >= start) & (df['Date'] <= end)
            filtered = df[mask]

            # If you’ll be doing a lot of downstream work on each filtered DF,
            # you could persist it here:
            # filtered = filtered.persist()

            dataframes[key] = filtered

    return dataframes
       
def apply_symbol_filter(dataframes, symbols):
    """
    Filter every DataFrame in `dataframes` down to the provided `symbols`.

    Fixes for "Unalignable boolean Series provided as indexer":
      - Normalize `symbols` to a plain Python LIST (then SET) — never a Dask object.
      - For Dask DataFrames, build the mask *inside* each partition via map_partitions,
        so the boolean indexer is created from the same partition (no cross-graph alignment).
      - If 'Symbol' is an index instead of a column, filter via index.isin(...) accordingly.
      - Maintain a small canonical `df_symbols` as a **pandas** DataFrame to avoid dask-expr len()/opt issues.
    """
    # --- lightweight logger that degrades to print ---
    def _log(msg):
        try:
            write_line(msg)
        except Exception:
            try:
                print(msg)
            except Exception:
                pass

    # Optional dask import; work even if Dask isn't installed
    try:
        import dask
        import dask.dataframe as dd
    except Exception:
        dask = None
        dd = None

    import pandas as pd

    # --- normalize symbols to a plain Python list (de-duped, order-preserving) ---
    def _to_symbol_list(obj):
        # Dask collections -> compute to pandas
        if dd is not None:
            if isinstance(obj, dd.DataFrame):
                obj = obj['Symbol'] if 'Symbol' in obj.columns else obj.iloc[:, 0]
            if isinstance(obj, (dd.Series, dd.Index)):
                obj = obj.dropna().unique()
                obj = dask.compute(obj)[0]  # -> pandas Index/Series
            elif hasattr(obj, "compute") and not isinstance(obj, pd.DataFrame):
                # delayed/scalar
                obj = obj.compute()

        # Pandas objects
        if isinstance(obj, pd.DataFrame):
            obj = obj['Symbol'] if 'Symbol' in obj.columns else obj.iloc[:, 0]
            obj = obj.dropna().unique()
        elif isinstance(obj, (pd.Series, pd.Index)):
            obj = obj.dropna().unique()

        # Convert array-like to list
        if hasattr(obj, "tolist"):
            obj = obj.tolist()

        # Scalar -> list
        if not isinstance(obj, (list, tuple, set)):
            obj = [obj]

        # De-dupe, preserve order
        seen, out = set(), []
        for x in obj:
            if x is None:
                continue
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    sym_list = _to_symbol_list(symbols)
    sym_set = set(sym_list)

    # # Keep a tiny canonical symbols frame as **pandas** to avoid dask-expr mixing elsewhere
    # try:
    #     dataframes['df_symbols'] = pd.DataFrame({'Symbol': sym_list})
    # except Exception as e:
    #     _log(f"Warning: could not update df_symbols: {e}")

    # Early exit: if no symbols, filter to empty where applicable
    if not sym_list:
        _log("No symbols provided; filtering matching DataFrames to 0 rows.")
        for key, df in list(dataframes.items()):
            try:
                if hasattr(df, "columns"):
                    if 'Symbol' in getattr(df, "columns", []):
                        # maintain schema with zero rows
                        dataframes[key] = df.head(0) if hasattr(df, "head") else df.iloc[0:0]
                    elif getattr(getattr(df, "index", None), "name", None) == 'Symbol':
                        dataframes[key] = df.head(0) if hasattr(df, "head") else df.iloc[0:0]
            except Exception as e:
                _log(f"  • Skipped {key}: {e}")
        return dataframes

    # Partition-safe filter functions (create mask inside each partition)
    def _filter_pdf_by_col(pdf, symbols_set):
        # pdf is a pandas DataFrame partition
        return pdf[pdf['Symbol'].isin(symbols_set)]

    def _filter_pdf_by_index(pdf, symbols_set):
        return pdf[pdf.index.isin(symbols_set)]

    # Filter each DF that has a 'Symbol' column or index
    for key, df in list(dataframes.items()):
        try:
            has_cols = hasattr(df, "columns")
            if not has_cols:
                continue

            has_symbol_col = 'Symbol' in df.columns
            has_symbol_index = getattr(getattr(df, "index", None), "name", None) == 'Symbol'

            if not (has_symbol_col or has_symbol_index):
                continue

            # ---- Dask path: build mask inside each partition to avoid alignment issues ----
            if dd is not None and isinstance(df, dd.DataFrame):
                if has_symbol_col:
                    filtered = df.map_partitions(_filter_pdf_by_col, sym_set, meta=df._meta)
                else:
                    filtered = df.map_partitions(_filter_pdf_by_index, sym_set, meta=df._meta)

                dataframes[key] = filtered
                continue

            # ---- Pandas path ----
            if has_symbol_col:
                dataframes[key] = df[df['Symbol'].isin(sym_set)]
            else:
                # 'Symbol' as index
                dataframes[key] = df[df.index.isin(sym_set)]

        except Exception as e:
            _log(f"  • Primary filter failed for {key} due to: {e}")
            # Last-resort fallback: compute to pandas then filter
            try:
                pdf = df.compute() if hasattr(df, "compute") else df
                if 'Symbol' in pdf.columns:
                    dataframes[key] = pdf[pdf['Symbol'].isin(sym_set)]
                else:
                    dataframes[key] = pdf[pdf.index.isin(sym_set)]
            except Exception as e2:
                _log(f"    × Could not filter {key}: {e2}")

    return dataframes

def filter_us_trading_days_ddf(dataframes: dict, date_col: str = "Date", calendar: str = "XNYS") -> dd.DataFrame:
    ddf = dataframes['df_price_analysis']
    
    # 1) Ensure datetime and drop NaT upfront (avoids weird min/max cases)
    ddf = ddf.assign(**{date_col: dd.to_datetime(ddf[date_col], errors="coerce")}).dropna(subset=[date_col])

    # 2) Compute data span once (single graph evaluation)
    start_ts, end_ts = dd.compute(ddf[date_col].min(), ddf[date_col].max())

    # 3) Get the valid NYSE *session* days for that span
    nyse = mcal.get_calendar(calendar)
    valid = nyse.valid_days(start_date=str(start_ts.date()), end_date=str(end_ts.date()))
    vd = pd.DatetimeIndex(valid).tz_localize(None)  # make tz-naive just in case
    valid_dates = set(vd.date)  # set of datetime.date for fast membership

    # 4) Partition-wise filter (avoids full materialization)
    def _keep_sessions(part: pd.DataFrame) -> pd.DataFrame:
        s = pd.to_datetime(part[date_col], errors="coerce")
        mask = s.dt.date.isin(valid_dates)
        return part.loc[mask]

    dataframes['df_price_analysis'] = ddf.map_partitions(_keep_sessions, meta=ddf._meta)
    return dataframes
        
def save_filters(dataframes, prompt_answers=None):
    """
    Save a *selected subset* of log rows (default: df_filters, else df_actions) to a pickle file.

    Prompts:
      - Which source to use (auto: df_filters if present; else df_actions)
      - Which rows to save (by index; supports ranges like '1,3,5-8')
      - Overwrite existing .pkl or create a new one

    Optional prompt_answers keys to skip prompts:
      - save_source: 'df_filters' or 'df_actions'
      - save_indices: e.g. '1,3,5-8' or [0,2,4] (0-based or 1-based accepted for strings)
      - overwrite_choice: int index (1-based) of existing file to overwrite
      - new_filename: 'my_filters.pkl'
      - confirm_overwrite: 'y'/'n'
    """
    import os
    import pickle
    import pandas as pd

    def _pick_source(df_map, pa):
        # prefer df_filters if present
        default = 'df_filters' if 'df_filters' in df_map else 'df_actions'
        if default not in df_map:
            return None
        src = pa.get('save_source', default) if pa else default
        if src not in df_map:
            # fallback to the other if available
            alt = 'df_actions' if src == 'df_filters' else 'df_filters'
            return alt if alt in df_map else None
        return src

    def _parse_index_list(s, max_len):
        """
        Accepts '1,3,5-8' (1-based) or '0,2,4' as ints list.
        Returns sorted unique 0-based indices or None if invalid.
        """
        if isinstance(s, list):
            # assume already 0-based ints
            idxs = []
            for i in s:
                try:
                    i = int(i)
                except Exception:
                    return None
                if not (0 <= i < max_len):
                    return None
                idxs.append(i)
            return sorted(set(idxs))

        s = (s or "").strip()
        if not s:
            return None
        out = set()
        for part in s.split(","):
            part = part.strip()
            if "-" in part:
                a, b = part.split("-", 1)
                if not (a.strip().isdigit() and b.strip().isdigit()):
                    return None
                start = int(a.strip())
                end   = int(b.strip())
                # interpret as 1-based if any index >=1 and <=max_len
                # normalize to 0-based
                if start >= 1 and end >= 1:
                    start -= 1
                    end   -= 1
                if start > end:
                    start, end = end, start
                if start < 0 or end >= max_len:
                    return None
                out.update(range(start, end + 1))
            else:
                if not part.isdigit():
                    return None
                i = int(part)
                # normalize 1-based to 0-based if within [1..max_len]
                if 1 <= i <= max_len:
                    i -= 1
                if not (0 <= i < max_len):
                    return None
                out.add(i)
        return sorted(out)

    try:
        if prompt_answers is None:
            prompt_answers = {}

        # 0) pick source df
        source_key = _pick_source(dataframes, prompt_answers)
        if source_key is None:
            write_line("No filters/actions log found (need 'df_filters' or 'df_actions').")
            return

        df_log = dataframes[source_key]
        if not isinstance(df_log, pd.DataFrame) or df_log.empty:
            write_line(f"No rows found in {source_key}. Nothing to save.")
            return

        df_log = df_log.reset_index(drop=True)

        # 1) list rows to the user
        write_line(f"Saving from: {source_key}  (rows: {len(df_log)})")
        # Display a concise preview
        preview_cols = []
        for c in ['timestamp', 'description', 'action', 'dataframe', 'added_columns']:
            if c in df_log.columns:
                preview_cols.append(c)
        if not preview_cols:
            preview_cols = list(df_log.columns[:5])  # fallback

        for i, row in df_log[preview_cols].iterrows():
            # compact line
            parts = []
            for c in preview_cols:
                val = row[c]
                if isinstance(val, list):
                    val = ", ".join(map(str, val))[:120]
                elif isinstance(val, str):
                    val = val.replace("\n", " ")[:120]
                parts.append(f"{c}={val}")
            write_line(f"  {i+1}. " + " | ".join(parts))

        # 2) choose which rows to save
        if 'save_indices' in prompt_answers:
            idxs = _parse_index_list(prompt_answers['save_indices'], len(df_log))
            if idxs is None or len(idxs) == 0:
                write_line("Invalid or empty 'save_indices' in prompt_answers. Save canceled.")
                return
        else:
            picks = input("Enter row numbers/ranges to save (e.g., 1,3,5-8): ").strip()
            idxs = _parse_index_list(picks, len(df_log))
            if idxs is None or len(idxs) == 0:
                write_line("No valid selection. Save canceled.")
                return

        df_selected = df_log.iloc[idxs].copy()
        write_line(f"Selected {len(df_selected)} row(s) to save.")

        # 3) choose target file (list existing .pkl)
        import os
        dir_path = os.path.join('Data', 'Filters')
        os.makedirs(dir_path, exist_ok=True)
        existing = [f for f in os.listdir(dir_path)
                    if f.lower().endswith('.pkl') and os.path.isfile(os.path.join(dir_path, f))]
        existing.sort()

        overwrite_index = None
        if existing:
            write_line("Existing filter files:")
            for i, fname in enumerate(existing, 1):
                write_line(f"  {i}. {fname}")

        if 'overwrite_choice' in prompt_answers:
            oc = prompt_answers['overwrite_choice']
            if isinstance(oc, str) and oc.strip().isdigit():
                oc = int(oc.strip())
            if isinstance(oc, int) and 1 <= oc <= len(existing):
                overwrite_index = oc
        else:
            choice = input("Enter number to OVERWRITE, or press Enter to save as a NEW file: ").strip()
            if choice and choice.isdigit():
                choice = int(choice)
                if 1 <= choice <= len(existing):
                    overwrite_index = choice

        # 4) resolve target path
        if overwrite_index:
            target_file = existing[overwrite_index - 1]
            full_path = os.path.join(dir_path, target_file)
            confirm = prompt_answers.get('confirm_overwrite') or input(
                f"Overwrite '{target_file}'? [y/N]: "
            ).strip().lower()
            if confirm != 'y':
                write_line("Save canceled.")
                return
        else:
            filename = prompt_answers.get('new_filename') or input(
                "Enter NEW filename (e.g. my_filters.pkl): "
            ).strip()
            if not filename:
                write_line("Save canceled: no filename provided.")
                return
            if not filename.lower().endswith('.pkl'):
                filename += '.pkl'
            full_path = os.path.join(dir_path, filename)
            if os.path.exists(full_path):
                confirm = prompt_answers.get('confirm_overwrite') or input(
                    f"'{filename}' already exists. Overwrite? [y/N]: "
                ).strip().lower()
                if confirm != 'y':
                    write_line("Save canceled.")
                    return

        # 5) actually save only the selected rows
        with open(full_path, 'wb') as f:
            pickle.dump(df_selected, f)

        write_line(f"Saved {len(df_selected)} row(s) from {source_key} -> {full_path}")

    except Exception as e:
        write_line(f"Error in Save Filters: {e}")
        return


        
def load_filters(dataframes, prompt_answers=None):
    """
    Loads a filters/actions DataFrame from a pickle in Data/Filters and then:
      1) Clear all dataframes and load fresh, OR
      2) Merge/apply to existing dataframes.

    Prompts (skipped if present in prompt_answers):
      - load_source: directory path (default 'Data/Filters')
      - load_latest: bool -> auto-pick most recently modified .pkl
      - load_choice: index (1-based) of file to load OR exact filename string
      - post_load_action: '1' or '2' (or 'clear'/'apply')
    """
    import os
    import pickle
    import pandas as pd

    if prompt_answers is None:
        prompt_answers = {}

    # 0) Resolve directory
    dir_path = prompt_answers.get('load_source', os.path.join('Data', 'Filters'))
    os.makedirs(dir_path, exist_ok=True)

    # 1) Find .pkl files
    existing = [f for f in os.listdir(dir_path) if f.lower().endswith('.pkl') and os.path.isfile(os.path.join(dir_path, f))]
    existing.sort()
    if not existing:
        write_line(f"No filter files found in {dir_path}.")
        return dataframes

    # Helper: choose file from prompt_answers or interactively
    def _choose_filename(pa, files, base_dir):
        # a) load_latest
        if pa.get('load_latest', False):
            # Pick most recently modified
            latest = max(files, key=lambda fn: os.path.getmtime(os.path.join(base_dir, fn)))
            return latest

        # b) explicit load_choice (index or filename)
        if 'load_choice' in pa:
            choice = str(pa['load_choice']).strip()
            # If it's a digit, treat as index (1-based)
            if choice.isdigit():
                idx = int(choice)
                if 1 <= idx <= len(files):
                    return files[idx - 1]
            # Otherwise treat as filename
            if choice in files:
                return choice
            # If provided 'load_filename'
        if 'load_filename' in pa and pa['load_filename'] in files:
            return pa['load_filename']

        # c) Interactive selection
        write_line("Available filter files:")
        for idx, fname in enumerate(files, start=1):
            write_line(f"  {idx}. {fname}")
        choice = input(f"Select file to load (1–{len(files)}): ").strip()
        if not (choice.isdigit() and 1 <= int(choice) <= len(files)):
            return None
        return files[int(choice) - 1]

    filename = _choose_filename(prompt_answers, existing, dir_path)
    if not filename:
        write_line("Invalid selection. Aborting load.")
        return dataframes

    full_path = os.path.join(dir_path, filename)

    # 2) Load pickle
    try:
        write_line(f"Loading filters from {full_path}…")
        with open(full_path, 'rb') as f:
            loaded = pickle.load(f)
        if not isinstance(loaded, pd.DataFrame):
            write_line("Loaded object is not a pandas DataFrame. Aborting load.")
            return dataframes
        write_line(f"Filters loaded from {full_path} (rows: {len(loaded)})")
    except Exception as e:
        write_line(f"Error in Load Filters: {e}")
        return dataframes

    # 3) Decide post-load action
    def _normalize_action(x):
        x = str(x).strip().lower()
        if x in {"1", "clear", "fresh"}:
            return "clear"
        if x in {"2", "apply", "merge"}:
            return "apply"
        return None

    action = _normalize_action(prompt_answers.get('post_load_action'))
    if action is None:
        write_line("What would you like to do next?")
        write_line("  1. Clear all existing dataframes and load filters fresh")
        write_line("  2. Apply loaded filters to existing dataframes")
        action_in = input("Enter 1 or 2: ").strip()
        action = _normalize_action(action_in)

    if action == "clear":
        # Reset to base, then set df_actions = loaded and apply
        dataframes = get_unfiltered_dataframes()
        dataframes['df_actions'] = loaded.reset_index(drop=True)
        dataframes = apply_dataframes_actions(dataframes)
        write_line("All dataframes cleared. Loaded and applied filters into dataframes['df_actions'].")

    elif action == "apply":
        # Merge with existing df_actions (or initialize)
        if 'df_actions' in dataframes and isinstance(dataframes['df_actions'], pd.DataFrame):
            df_existing = dataframes['df_actions'].reset_index(drop=True)
            df_merged = pd.concat([df_existing, loaded.reset_index(drop=True)], ignore_index=True)

            # Convert list cells to tuples so drop_duplicates can hash them
            for col in df_merged.columns:
                if df_merged[col].apply(lambda x: isinstance(x, list)).any():
                    df_merged[col] = df_merged[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)

            df_merged.drop_duplicates(keep='last', inplace=True)

            # Convert tuples back to lists
            for col in df_merged.columns:
                if df_merged[col].apply(lambda x: isinstance(x, tuple)).any():
                    df_merged[col] = df_merged[col].apply(lambda x: list(x) if isinstance(x, tuple) else x)

            dataframes['df_actions'] = df_merged.reset_index(drop=True)
            write_line(f"Filter log now has {len(df_merged)} unique entries.")
        else:
            write_line("No existing filter log found; initializing with loaded filters.")
            dataframes = get_unfiltered_dataframes()
            dataframes['df_actions'] = loaded.reset_index(drop=True)
            write_line(f"Filter log initialized with {len(loaded)} entries.")

        dataframes = apply_dataframes_actions(dataframes)
        write_line("Applied loaded filters to your existing dataframes.")

    else:
        write_line("Invalid choice. No further action taken.")
        return dataframes

    return dataframes


#endregion


def logic_loop(dataframes: dict):
    while True:
        cmd = prompt_user_command()
        if cmd == "Evaluate Performance Metrics":
            dataframes = evaluate_symbols_performance(dataframes)
            # get_performance_metrics(dataframes)
        elif cmd == "Add Calculation":
            dataframes = add_calculation(dataframes)
        elif cmd == "Rank Column":
            dataframes = add_rank_by_date(dataframes)
        elif cmd == "Add Preset Filter":
            dataframes = add_preset_filter(dataframes)
        elif cmd == "Add Weighted Expresion":
            dataframes = add_weighted_expression(dataframes)
        elif cmd == "Print Sample Dataset":
            print_random_sample_by_columns(dataframes, 20)
        elif cmd == "Print Performance Metrics":
            print_performance_metrics(dataframes)
        elif cmd == "Pause to view dataframe":
            input("Place breakpoint and press enter...")
            input("Press enter to continue...")            
        elif cmd == "Print Symbol Summary":
            print_symbol_summary(dataframes)
        elif cmd == "Print Date Range":
            get_date_range(dataframes)
        elif cmd == "Add Custom Filter":
            dataframes = add_custom_filter(dataframes)
        elif cmd == "Add Weighted Expression":
            dataframes = add_weighted_expression(dataframes)
        elif cmd == "Clear Filters":            
            dataframes = clear_filters(dataframes)
        elif cmd == "Print Filters":
            print_actions(dataframes)
        elif cmd == "Save Filters":
            save_filters(dataframes)
        elif cmd == "Load Filters":
            dataframes = load_filters(dataframes)
        elif cmd == "Save Dataframes":
            store_pickle(dataframes, "Data/")
        elif cmd == "Load Dataframes":
            dataframes = load_pickle("Data/")
        elif cmd == "Materialize Dataframes":
            dataframes = materialize_dataframe(dataframes)
        elif cmd == 'Save Symbols':
            dataframes = save_unique_symbols(dataframes)
        elif cmd == 'Load Symbols':
            dataframes = load_symbols_list(dataframes)

        elif cmd == "Exit":
            write_line("Exiting program. Goodbye!")
            break
        else:
            write_line(f"Unknown command: '{cmd}'. Please try again.")

def main():
    df_dict = get_unfiltered_dataframes()
    logic_loop(df_dict)
    
if __name__ == "__main__":
    
    # with StayAwake():
    main()
