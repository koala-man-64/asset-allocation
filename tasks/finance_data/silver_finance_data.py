from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd
from io import BytesIO
import re
import json

from core import core as mdc
from core import delta_core
from tasks.finance_data import config as cfg
from core.pipeline import DataPaths
from tasks.common.watermarks import check_blob_unchanged, load_watermarks, save_watermarks
from tasks.common.silver_contracts import (
    ContractViolation,
    align_to_existing_schema,
    assert_no_unexpected_mixed_empty,
    log_contract_violation,
    normalize_date_column,
    require_non_empty_frame,
)

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)


@dataclass(frozen=True)
class BlobProcessResult:
    blob_name: str
    silver_path: Optional[str]
    status: str  # ok|skipped|failed
    error: Optional[str] = None


_KEY_NORMALIZER = re.compile(r"[^a-z0-9]+")


def _normalize_key(name: Any) -> str:
    return _KEY_NORMALIZER.sub("", str(name).strip().lower())


def _try_parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None

    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"none", "null", "nan", "n/a", "na", "-"}:
        return None

    text = text.replace(",", "")
    try:
        return float(text)
    except Exception:
        return None


def _get_first_float(payload: dict[str, Any], candidates: list[str]) -> Optional[float]:
    normalized = {_normalize_key(k): v for k, v in payload.items()}
    for candidate in candidates:
        value = normalized.get(_normalize_key(candidate))
        parsed = _try_parse_float(value)
        if parsed is not None:
            return parsed
    return None


def _load_close_prices(ticker: str) -> pd.DataFrame:
    """
    Load close price series for valuation approximation.

    Preference order:
      1) Silver Delta market table (full history when available)
      2) Bronze market CSV (often compact)
    """
    market_path = DataPaths.get_market_data_path(ticker.replace(".", "-"))
    df = delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, market_path, columns=["Date", "Close"])
    if df is not None and not df.empty:
        if "Close" not in df.columns and "close" in df.columns:
            df = df.rename(columns={"close": "Close"})
        if "Date" not in df.columns and "date" in df.columns:
            df = df.rename(columns={"date": "Date"})

        if {"Date", "Close"}.issubset(df.columns):
            out = df[["Date", "Close"]].copy()
            out["Date"] = pd.to_datetime(out["Date"], errors="coerce", utc=True).dt.tz_convert(None)
            out["Close"] = pd.to_numeric(out["Close"], errors="coerce")
            out = out.dropna(subset=["Date", "Close"]).sort_values("Date").reset_index(drop=True)
            if not out.empty:
                return out

    try:
        raw_bytes = mdc.read_raw_bytes(f"market-data/{ticker}.csv", client=bronze_client)
        df_csv = pd.read_csv(BytesIO(raw_bytes))
        if "Close" not in df_csv.columns and "close" in df_csv.columns:
            df_csv = df_csv.rename(columns={"close": "Close"})
        if "Date" not in df_csv.columns and "date" in df_csv.columns:
            df_csv = df_csv.rename(columns={"date": "Date"})
        if {"Date", "Close"}.issubset(df_csv.columns):
            out = df_csv[["Date", "Close"]].copy()
            out["Date"] = pd.to_datetime(out["Date"], errors="coerce", utc=True).dt.tz_convert(None)
            out["Close"] = pd.to_numeric(out["Close"], errors="coerce")
            out = out.dropna(subset=["Date", "Close"]).sort_values("Date").reset_index(drop=True)
            return out
    except Exception:
        return pd.DataFrame()

    return pd.DataFrame()


def _build_valuation_timeseries_from_overview(payload: dict[str, Any], *, ticker: str) -> pd.DataFrame:
    """
    Alpha Vantage does not provide a full historical valuation time series. We approximate it by
    scaling "current" ratios from `OVERVIEW` by historical close prices.

    This yields a best-effort daily valuation series that is consistent on the latest close date
    (i.e., latest date matches the overview ratio).
    """
    df_prices = _load_close_prices(ticker)

    market_cap_now = _get_first_float(payload, ["MarketCapitalization", "Market Cap", "MarketCap"])
    pe_now = _get_first_float(payload, ["PERatio", "P/E", "PE Ratio"])
    forward_pe_now = _get_first_float(payload, ["ForwardPE", "Forward P/E", "Forward PE"])
    ev_ebitda_now = _get_first_float(payload, ["EVToEBITDA", "EV/EBITDA", "EV To EBITDA"])
    ev_revenue_now = _get_first_float(payload, ["EVToRevenue", "EV/Revenue", "EV To Revenue"])
    shares_outstanding_now = _get_first_float(payload, ["SharesOutstanding", "Shares Outstanding"])
    ebitda_now = _get_first_float(payload, ["EBITDA"])

    if df_prices is None or df_prices.empty:
        today = datetime.now(timezone.utc).date().isoformat()
        row: dict[str, Any] = {"Date": today, "Symbol": ticker}
        if market_cap_now is not None:
            row["market_cap"] = market_cap_now
        if pe_now is not None:
            row["pe_ratio"] = pe_now
        if forward_pe_now is not None:
            row["forward_pe"] = forward_pe_now
        if ev_ebitda_now is not None:
            row["ev_ebitda"] = ev_ebitda_now
        if ev_revenue_now is not None:
            row["ev_revenue"] = ev_revenue_now
        if shares_outstanding_now is not None:
            row["shares_outstanding"] = shares_outstanding_now
        if ebitda_now is not None:
            row["ebitda"] = ebitda_now

        df = pd.DataFrame([row])
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True).dt.tz_convert(None)
        return df

    close_now = float(df_prices["Close"].iloc[-1])
    if close_now <= 0:
        return pd.DataFrame()

    scale = df_prices["Close"] / close_now

    shares_outstanding = shares_outstanding_now
    if shares_outstanding is None and market_cap_now is not None:
        shares_outstanding = market_cap_now / close_now

    out = pd.DataFrame({"Date": df_prices["Date"], "Symbol": ticker})

    if shares_outstanding is not None:
        out["shares_outstanding"] = float(shares_outstanding)

    if market_cap_now is not None:
        if shares_outstanding is not None:
            out["market_cap"] = df_prices["Close"] * float(shares_outstanding)
        else:
            out["market_cap"] = float(market_cap_now) * scale

    if pe_now is not None:
        out["pe_ratio"] = float(pe_now) * scale
    if forward_pe_now is not None:
        out["forward_pe"] = float(forward_pe_now) * scale
    if ev_ebitda_now is not None:
        out["ev_ebitda"] = float(ev_ebitda_now) * scale
    if ev_revenue_now is not None:
        out["ev_revenue"] = float(ev_revenue_now) * scale
    if ebitda_now is not None:
        out["ebitda"] = float(ebitda_now)

    out["Date"] = pd.to_datetime(out["Date"], errors="coerce", utc=True).dt.tz_convert(None)
    out = out.dropna(subset=["Date"]).sort_values(["Date"]).reset_index(drop=True)
    return out


def _read_finance_csv(raw_bytes: bytes) -> pd.DataFrame:
    """
    Read finance CSVs defensively.

    These legacy CSVs commonly include thousands separators and sparse cells.
    Reading everything as strings (and disabling default NA parsing) avoids mixed
    object columns like ["1,234", NaN] that later break Arrow/Delta writes.
    """
    return pd.read_csv(BytesIO(raw_bytes), dtype=str, keep_default_na=False)


def _read_finance_json(raw_bytes: bytes, *, ticker: str, suffix: str) -> pd.DataFrame:
    payload = json.loads(raw_bytes.decode("utf-8"))

    # Fundamentals endpoints return quarterlyReports/annualReports; OVERVIEW is a flat dict.
    if suffix in {"quarterly_balance-sheet", "quarterly_cash-flow", "quarterly_financials"}:
        reports = payload.get("quarterlyReports") or []
        if not isinstance(reports, list) or not reports:
            return pd.DataFrame()

        rows = []
        for item in reports:
            if not isinstance(item, dict):
                continue
            date_raw = item.get("fiscalDateEnding")
            if not date_raw:
                continue
            row = {"Date": str(date_raw).strip(), "Symbol": ticker}
            for k, v in item.items():
                if k == "fiscalDateEnding":
                    continue
                # Keep values as strings for downstream Delta writes (avoids mixed object types).
                row[str(k)] = "" if v is None else str(v)
            rows.append(row)

        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True).dt.tz_convert(None)
        df = df.dropna(subset=["Date"]).sort_values(["Date"]).reset_index(drop=True)
        return df

    # "Valuation" bucket: approximate a daily valuation series from OVERVIEW + historical closes.
    if suffix == "quarterly_valuation_measures" and isinstance(payload, dict):
        return _build_valuation_timeseries_from_overview(payload, ticker=ticker)

    # Default: store snapshot-style JSON payload on today's date.
    if isinstance(payload, dict):
        today = datetime.now(timezone.utc).date().isoformat()
        row = {"Date": today, "Symbol": ticker}
        for k, v in payload.items():
            if k in {"Symbol"}:
                continue
            row[str(k)] = "" if v is None else str(v)
        df = pd.DataFrame([row])
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True).dt.tz_convert(None)
        return df

    return pd.DataFrame()

def transpose_dataframe(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Transposes legacy finance CSV (Metrics in Rows, Dates in Columns)
    to (Dates in Rows, Metrics in Columns).
    """
    if 'name' in df.columns:
        df['name'] = df['name'].astype(str).str.strip()
    elif 'breakdown' in df.columns:
        df['breakdown'] = df['breakdown'].astype(str).str.strip()

    df = _rename_ttm_columns(df)

    if 'name' in df.columns:
        df = df.set_index('name')
    elif 'breakdown' in df.columns:
        df = df.set_index('breakdown')
    else:
        df = df.set_index(df.columns[0])
    
    df_t = df.transpose()
    df_t.index.name = 'Date'
    df_t = df_t.reset_index()
    df_t.columns.name = None
    df_t['Symbol'] = ticker
    return df_t


def _infer_date_format(columns: list[str]) -> str:
    for raw in columns:
        value = str(raw).strip()
        if not value:
            continue
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return "%Y-%m-%d"
        if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", value):
            return "%m/%d/%Y"
        if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2}", value):
            return "%m/%d/%y"
        if re.fullmatch(r"\d{4}/\d{1,2}/\d{1,2}", value):
            return "%Y/%m/%d"
    return "%m/%d/%Y"


def _rename_ttm_columns(df: pd.DataFrame) -> pd.DataFrame:
    ttm_cols = [col for col in df.columns if str(col).strip().lower() == "ttm"]
    if not ttm_cols:
        return df

    date_candidates = [
        col
        for col in df.columns
        if col not in ttm_cols and str(col).strip().lower() not in {"name", "breakdown"}
    ]
    date_format = _infer_date_format([str(c) for c in date_candidates])
    today_str = datetime.now(timezone.utc).date().strftime(date_format)

    out = df.copy()
    for col in ttm_cols:
        if today_str in out.columns:
            out = out.drop(columns=[col])
        else:
            out = out.rename(columns={col: today_str})
    return out

def _utc_today() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc).date())


def resample_daily_ffill(df: pd.DataFrame, *, extend_to: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """
    Resamples sparse dataframe to daily frequency using forward fill.
    """
    if 'Date' not in df.columns:
        return df
        
    df = df.copy()
    df['Date'] = pd.to_datetime(df['Date'], errors="coerce", utc=True).dt.tz_convert(None)
    df = df.dropna(subset=["Date"])
    if df.empty:
        return df

    df = df.set_index('Date')
    df = df.sort_index()
    
    # Resample and ffill
    # We must restrict to the known date range
    if df.empty:
        return df
        
    end = df.index.max()
    if extend_to is not None and extend_to > end:
        end = extend_to

    full_range = pd.date_range(start=df.index.min(), end=end, freq="D", name="Date")
    df_daily = df.reindex(full_range).ffill()
    
    return df_daily.reset_index()

def _try_get_delta_max_date(container: str, path: str) -> Optional[pd.Timestamp]:
    df = delta_core.load_delta(container, path, columns=["Date"])
    if df is None or df.empty or "Date" not in df.columns:
        return None

    dates = pd.to_datetime(df["Date"], errors="coerce")
    dates = dates.dropna()
    if dates.empty:
        return None
    return pd.Timestamp(dates.max()).normalize()


def _align_to_existing_schema(df: pd.DataFrame, container: str, path: str) -> pd.DataFrame:
    return align_to_existing_schema(df, container=container, path=path)


def process_blob(blob, *, desired_end: pd.Timestamp, watermarks: dict | None = None) -> BlobProcessResult:
    blob_name = blob['name'] 
    # expected: finance-data/Folder Name/ticker_suffix.csv
    # e.g. finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.csv
    
    parts = blob_name.split('/')
    if len(parts) < 3:
        return BlobProcessResult(blob_name=blob_name, silver_path=None, status="skipped")
        
    folder_name = parts[1]
    filename = parts[2]
    
    if not (filename.endswith(".csv") or filename.endswith(".json")):
        return BlobProcessResult(blob_name=blob_name, silver_path=None, status="skipped")
        
    # extract ticker
    # filename: ticker_suffix.csv
    # suffix starts with quarterly_...
    # split by first underscore? Tickers can have no underscores usually.
    # suffix is known: 
    known_suffixes = [
        "quarterly_balance-sheet", 
        "quarterly_valuation_measures", 
        "quarterly_cash-flow", 
        "quarterly_financials"
    ]
    
    suffix = None
    for s in known_suffixes:
        if filename.endswith(s + ".csv") or filename.endswith(s + ".json"):
            suffix = s
            break
            
    if not suffix:
        mdc.write_line(f"Skipping unknown file format: {filename}")
        return BlobProcessResult(blob_name=blob_name, silver_path=None, status="skipped")
        
    ticker = filename.replace(f"_{suffix}.csv", "").replace(f"_{suffix}.json", "")
    
    # Silver Path
    # Use DataPaths or manual? DataPaths uses folder name.
    # DataPaths.get_finance_path(folder_name, ticker, suffix)
    silver_path = DataPaths.get_finance_path(folder_name, ticker, suffix)

    if hasattr(cfg, "DEBUG_SYMBOLS") and cfg.DEBUG_SYMBOLS and ticker not in cfg.DEBUG_SYMBOLS:
        return BlobProcessResult(blob_name=blob_name, silver_path=silver_path, status="skipped")
    
    if watermarks is not None:
        unchanged, signature = check_blob_unchanged(blob, watermarks.get(blob_name))
        if unchanged:
            return BlobProcessResult(blob_name=blob_name, silver_path=silver_path, status="skipped")
    else:
        signature = {}
        bronze_lm = blob.get("last_modified")
        if bronze_lm is not None:
            bronze_ts = bronze_lm.timestamp()
            silver_lm = delta_core.get_delta_last_commit(cfg.AZURE_CONTAINER_SILVER, silver_path)
            if silver_lm and (silver_lm > bronze_ts):
                max_date = _try_get_delta_max_date(cfg.AZURE_CONTAINER_SILVER, silver_path)
                if max_date is not None and max_date >= desired_end:
                    return BlobProcessResult(blob_name=blob_name, silver_path=silver_path, status="skipped")

    mdc.write_line(f"Processing {ticker} {folder_name}...")
    
    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        if filename.endswith(".json"):
            df_raw = _read_finance_json(raw_bytes, ticker=ticker, suffix=suffix)
        else:
            df_raw = _read_finance_csv(raw_bytes)

        # Header-only or otherwise empty inputs occasionally appear in Bronze.
        if df_raw is None or df_raw.empty:
            mdc.write_warning(f"Skipping empty finance CSV: {blob_name}")
            return BlobProcessResult(blob_name=blob_name, silver_path=silver_path, status="skipped")

        # Legacy CSV requires transpose; AV JSON is already row-oriented by Date.
        if filename.endswith(".json"):
            df_clean = df_raw
        else:
            df_clean = transpose_dataframe(df_raw, ticker)

        try:
            df_clean = require_non_empty_frame(
                df_clean, context=f"finance preflight {blob_name}"
            )
            df_clean = normalize_date_column(
                df_clean,
                context=f"finance date parse {blob_name}",
                aliases=("Date", "date"),
                canonical="Date",
            )
            df_clean = assert_no_unexpected_mixed_empty(
                df_clean, context=f"finance date filter {blob_name}", alias="Date"
            )
        except ContractViolation as exc:
            log_contract_violation(f"finance preflight failed for {blob_name}", exc, severity="ERROR")
            return BlobProcessResult(
                blob_name=blob_name,
                silver_path=silver_path,
                status="failed",
                error=str(exc),
            )
        
        # Resample to daily frequency (forward fill)
        df_clean = resample_daily_ffill(df_clean, extend_to=desired_end)
        if df_clean is None or df_clean.empty:
            mdc.write_warning(f"No valid dated rows after cleaning/resample for {blob_name}; skipping.")
            return BlobProcessResult(
                blob_name=blob_name,
                silver_path=silver_path,
                status="skipped",
                error="No valid dated rows after cleaning/resample.",
            )
        
        # Write to Silver (Overwrite is fine for finance snapshots, or merge? 
        # Typically Finance Sheets are full snapshots. Replacing is safer for consistency, 
        # but if we want history of OLD financial restatements... 
        # Current logic: Overwrite/Upsert. store_delta defaults to append?
        # Let's use overwrite mode for now as Transposed data is simpler.
        # But wait, store_delta default implementation? 
        # Checking delta_core usage: usually it appends or overwrites. 
        # I'll check store_delta signature in next turn if needed.
        # Assuming overwrite for the partition/table is safest for now to avoid specific duplicates.
        
        df_clean = _align_to_existing_schema(df_clean, cfg.AZURE_CONTAINER_SILVER, silver_path)
        delta_core.store_delta(
            df_clean,
            cfg.AZURE_CONTAINER_SILVER,
            silver_path,
            mode="overwrite",
            schema_mode="merge",
        )
        mdc.write_line(f"Updated Silver {silver_path}")
        if watermarks is not None and signature:
            signature["updated_at"] = datetime.utcnow().isoformat()
            watermarks[blob_name] = signature
        return BlobProcessResult(blob_name=blob_name, silver_path=silver_path, status="ok")
        
    except Exception as e:
        mdc.write_error(f"Failed to process {blob_name}: {e}")
        return BlobProcessResult(blob_name=blob_name, silver_path=silver_path, status="failed", error=str(e))


def main() -> int:
    mdc.log_environment_diagnostics()
    
    mdc.write_line("Listing Bronze Finance files...")
    # Recursive list? list_blobs(name_starts_with="finance-data/") usually returns all nested.
    blobs = bronze_client.list_blob_infos(name_starts_with="finance-data/")

    watermarks = load_watermarks("bronze_finance_data")
    watermarks_dirty = False
    
    desired_end = _utc_today()

    results: list[BlobProcessResult] = []
    for blob in blobs:
        result = process_blob(blob, desired_end=desired_end, watermarks=watermarks)
        if result.status == "ok" and watermarks is not None:
            watermarks_dirty = True
        results.append(result)
        
    processed = sum(1 for r in results if r.status == "ok")
    skipped = sum(1 for r in results if r.status == "skipped")
    failed = sum(1 for r in results if r.status == "failed")
    mdc.write_line(f"Silver finance ingest complete: processed={processed}, skipped={skipped}, failed={failed}")
    if watermarks is not None and watermarks_dirty:
        save_watermarks("bronze_finance_data", watermarks)
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    from core.by_date_pipeline import run_partner_then_by_date
    from tasks.finance_data.materialize_silver_finance_by_date import (
        discover_year_months_from_data,
        main as by_date_main,
    )
    from tasks.common.job_trigger import trigger_next_job_from_env

    job_name = "silver-finance-job"
    exit_code = run_partner_then_by_date(
        job_name=job_name,
        partner_main=main,
        by_date_main=by_date_main,
        year_months_provider=discover_year_months_from_data,
    )
    if exit_code == 0:
        trigger_next_job_from_env()
    raise SystemExit(exit_code)
