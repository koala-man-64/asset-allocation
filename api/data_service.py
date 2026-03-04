from __future__ import annotations

import math
import json
from io import BytesIO
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from core import config as cfg
from core import core as mdc
from core import delta_core
from core.pipeline import DataPaths
from tasks.common import bronze_bucketing
from tasks.common import layer_bucketing


_FINANCE_SILVER_FOLDERS: dict[str, tuple[str, str]] = {
    "balance_sheet": ("Balance Sheet", "quarterly_balance-sheet"),
    "income_statement": ("Income Statement", "quarterly_financials"),
    "cash_flow": ("Cash Flow", "quarterly_cash-flow"),
    "valuation": ("Valuation", "quarterly_valuation_measures"),
}
_FINANCE_SUBDOMAIN_TO_REPORT_TYPE: dict[str, str] = {
    "balance_sheet": "balance_sheet",
    "income_statement": "income_statement",
    "cash_flow": "cash_flow",
    "valuation": "overview",
}


class DataService:
    """
    Service layer for accessing financial data from Delta Lake storage.
    Decouples API from direct pipeline script usage.
    """

    @staticmethod
    def _sanitize_json_value(value: Any) -> Any:
        if value is None:
            return None

        if isinstance(value, (bool, np.bool_)):
            return bool(value)

        if isinstance(value, (float, np.floating)):
            numeric = float(value)
            if not math.isfinite(numeric):
                return None
            return numeric

        if isinstance(value, (int, np.integer)):
            return int(value)

        if isinstance(value, dict):
            return {str(k): DataService._sanitize_json_value(v) for k, v in value.items()}

        if isinstance(value, list):
            return [DataService._sanitize_json_value(v) for v in value]

        if isinstance(value, tuple):
            return [DataService._sanitize_json_value(v) for v in value]

        return value

    @staticmethod
    def _df_to_records_json_safe(df: pd.DataFrame, *, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        if limit:
            df = df.head(int(limit))

        # Starlette's JSONResponse enforces RFC-compliant JSON and rejects NaN/Inf.
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.astype(object).where(pd.notnull(df), None)

        records: Any = df.to_dict(orient="records")
        sanitized = DataService._sanitize_json_value(records)
        return sanitized

    @staticmethod
    def _normalize_date_sort_direction(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip().lower()
        if not normalized:
            return None
        if normalized not in {"asc", "desc"}:
            raise ValueError("date sort direction must be 'asc' or 'desc'.")
        return normalized

    @staticmethod
    def _coerce_sortable_timestamp(value: Any) -> Optional[pd.Timestamp]:
        if value is None:
            return None
        try:
            parsed = pd.to_datetime(value, errors="coerce")
        except Exception:
            return None
        if pd.isna(parsed):
            return None
        if isinstance(parsed, pd.Timestamp):
            if parsed.tz is not None:
                parsed = parsed.tz_convert(None)
            return parsed
        try:
            ts = pd.Timestamp(parsed)
            if ts.tz is not None:
                ts = ts.tz_convert(None)
            return ts
        except Exception:
            return None

    @staticmethod
    def _detect_date_column_for_sort(rows: List[Dict[str, Any]]) -> Optional[str]:
        if not rows:
            return None

        columns: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            for key in row.keys():
                columns.add(str(key))

        if not columns:
            return None

        preferred: List[str] = []
        for candidate in ("date", "Date"):
            if candidate in columns:
                preferred.append(candidate)
        for column in sorted(columns):
            lowered = column.lower()
            if column in preferred:
                continue
            if "date" in lowered or lowered in {"datetime", "timestamp", "as_of", "asof"}:
                preferred.append(column)

        for column in preferred:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if DataService._coerce_sortable_timestamp(row.get(column)) is not None:
                    return column
        return None

    @staticmethod
    def _finalize_rows(
        rows: List[Dict[str, Any]],
        *,
        limit: Optional[int] = None,
        sort_by_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        out = list(rows or [])
        direction = DataService._normalize_date_sort_direction(sort_by_date)
        if direction and out:
            date_column = DataService._detect_date_column_for_sort(out)
            if date_column:
                valid: List[tuple[pd.Timestamp, Dict[str, Any]]] = []
                missing: List[Dict[str, Any]] = []
                for row in out:
                    ts = DataService._coerce_sortable_timestamp(row.get(date_column))
                    if ts is None:
                        missing.append(row)
                        continue
                    valid.append((ts, row))
                valid.sort(key=lambda item: item[0], reverse=(direction == "desc"))
                out = [item[1] for item in valid] + missing

        if limit is not None:
            out = out[: int(limit)]
        return out

    @staticmethod
    def _first_bronze_blob_path(
        client: Any,
        *,
        container: str,
        prefix: str,
        allowed_suffixes: tuple[str, ...],
    ) -> str:
        normalized = str(prefix or "").strip()
        if not normalized:
            raise ValueError("prefix is required to list Bronze blobs.")
        if not normalized.endswith("/"):
            normalized = normalized + "/"

        list_files = getattr(client, "list_files", None)
        if callable(list_files):
            names = [str(name) for name in list_files(name_starts_with=normalized)]
            candidates = [name for name in names if name.lower().endswith(allowed_suffixes)]
            if not candidates:
                raise FileNotFoundError(f"No Bronze blobs found under {container}/{normalized}")
            return sorted(candidates)[0]

        container_client = getattr(client, "container_client", None)
        if container_client is not None and hasattr(container_client, "list_blobs"):
            for blob in container_client.list_blobs(name_starts_with=normalized):
                name = getattr(blob, "name", None)
                if name and str(name).lower().endswith(allowed_suffixes):
                    return str(name)
            raise FileNotFoundError(f"No Bronze blobs found under {container}/{normalized}")

        raise FileNotFoundError("Storage client does not support listing Bronze blobs.")

    @staticmethod
    def _container_for_layer(layer: str) -> str:
        key = str(layer or "").strip().lower()
        if key == "silver":
            return cfg.AZURE_CONTAINER_SILVER
        if key == "gold":
            return cfg.AZURE_CONTAINER_GOLD
        if key == "bronze":
            return cfg.AZURE_CONTAINER_BRONZE
        raise ValueError(f"Unsupported layer: {layer!r}")

    @staticmethod
    def _read_bronze_alpha26_bucket(
        *,
        container: str,
        client: Any,
        domain_prefix: str,
        symbol: Optional[str] = None,
        report_type: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        resolved_symbol = str(symbol or "").strip().upper()
        resolved_report_type = str(report_type or "").strip().lower()
        if resolved_symbol:
            bucket = bronze_bucketing.bucket_letter(resolved_symbol)
            blob_path = bronze_bucketing.bucket_blob_path(domain_prefix, bucket)
        else:
            blob_path = DataService._first_bronze_blob_path(
                client,
                container=container,
                prefix=f"{str(domain_prefix).strip('/')}/buckets",
                allowed_suffixes=(".parquet",),
            )

        raw_bytes = mdc.read_raw_bytes(blob_path, client=client)
        if not raw_bytes:
            raise FileNotFoundError(f"Raw blob not found: {container}/{blob_path}")
        df = pd.read_parquet(BytesIO(raw_bytes))

        if resolved_symbol and "symbol" in df.columns:
            df = df[df["symbol"].astype(str).str.upper() == resolved_symbol]
        if resolved_report_type and "report_type" in df.columns:
            df = df[df["report_type"].astype(str).str.lower() == resolved_report_type]

        return DataService._df_to_records_json_safe(df, limit=limit)

    @staticmethod
    def _require_storage_client(container: str) -> Any:
        client = mdc.get_storage_client(container)
        if client is None:
            raise FileNotFoundError(
                f"Storage client unavailable for container={container!r}. "
                "Set Azure storage env vars to enable Delta table discovery."
            )
        return client

    @staticmethod
    def _discover_delta_table_paths(container: str, prefix: str) -> List[str]:
        normalized = str(prefix or "").strip().strip("/")
        if not normalized:
            raise ValueError("prefix is required to discover Delta tables.")

        client = DataService._require_storage_client(container)
        list_files = getattr(client, "list_files", None)
        if not callable(list_files):
            raise FileNotFoundError("Storage client does not support listing Delta table paths.")

        roots: set[str] = set()
        search_prefix = f"{normalized}/"
        for name in list_files(name_starts_with=search_prefix):
            text = str(name or "")
            marker = "/_delta_log/"
            if marker not in text:
                continue
            root = text.split(marker, 1)[0].strip("/")
            if root and root.startswith(search_prefix.rstrip("/")):
                roots.add(root)
        return sorted(roots)

    @staticmethod
    def _collect_delta_frames(
        container: str,
        paths: List[str],
        *,
        limit: Optional[int] = None,
        enrich: Optional[Any] = None,
    ) -> List[pd.DataFrame]:
        frames: List[pd.DataFrame] = []
        row_budget = int(limit) if limit is not None else None
        rows_collected = 0
        for path in paths:
            try:
                df = delta_core.load_delta(container, path)
            except Exception:
                continue
            if df is None or df.empty:
                continue
            if enrich is not None:
                df = enrich(df, path)
            frames.append(df)
            rows_collected += int(len(df))
            if row_budget is not None and rows_collected >= row_budget:
                break
        return frames

    @staticmethod
    def _frames_to_records(frames: List[pd.DataFrame], *, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        if not frames:
            return []
        merged = pd.concat(frames, ignore_index=True)
        return DataService._df_to_records_json_safe(merged, limit=limit)

    @staticmethod
    def _read_cross_section_from_prefix(container: str, prefix: str, *, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        paths = DataService._discover_delta_table_paths(container, prefix)
        frames = DataService._collect_delta_frames(container, paths, limit=limit)
        return DataService._frames_to_records(frames, limit=limit)

    @staticmethod
    def _read_silver_finance_regular(
        *,
        container: str,
        ticker: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        symbol = str(ticker or "").strip().upper()
        if symbol:
            bucket = layer_bucketing.bucket_letter(symbol)
            paths = [
                DataPaths.get_silver_finance_bucket_path(sub_domain, bucket)
                for sub_domain in _FINANCE_SILVER_FOLDERS.keys()
            ]
        else:
            paths = DataService._discover_delta_table_paths(container, "finance-data")

        def _enrich(df: pd.DataFrame, path: str) -> pd.DataFrame:
            out = df.copy()
            parts = str(path or "").split("/")
            sub_domain = parts[1] if len(parts) > 2 else ""
            if sub_domain and "sub_domain" not in out.columns:
                out["sub_domain"] = sub_domain
            if symbol and "symbol" in out.columns:
                out = out[out["symbol"].astype(str).str.upper() == symbol]
            return out

        frames = DataService._collect_delta_frames(container, paths, limit=limit, enrich=_enrich)
        return DataService._frames_to_records(frames, limit=limit)
    
    @staticmethod
    def get_data(
        layer: str, 
        domain: str, 
        ticker: Optional[str] = None,
        limit: Optional[int] = None,
        sort_by_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generic data retrieval for market, earnings, and price-target domains.

        Notes
        - Silver/Gold use Delta tables.
        - Bronze stores alpha26 bucket parquet files (`A..Z`) per domain.
        - Cross-sectional requests are assembled from bucketed Delta folders.
        """
        resolved_layer = str(layer or "").strip().lower()
        raw_domain = str(domain or "").strip().lower()
        resolved_domain = "price-target" if raw_domain in {"price-target", "price_target"} else raw_domain
        container = DataService._container_for_layer(resolved_layer)
        resolved_sort = DataService._normalize_date_sort_direction(sort_by_date)
        downstream_limit = None if resolved_sort else limit

        if resolved_layer == "bronze":
            rows = DataService._get_bronze_data(
                container=container,
                domain=resolved_domain,
                ticker=ticker,
                limit=downstream_limit,
            )
            return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)

        if resolved_domain.startswith("finance/"):
            _, _, sub_domain = resolved_domain.partition("/")
            if not sub_domain:
                raise ValueError("finance domain requires a sub-domain, e.g. finance/balance_sheet")
            return DataService.get_finance_data(
                resolved_layer,
                sub_domain,
                ticker=ticker,
                limit=limit,
                sort_by_date=resolved_sort,
            )

        is_silver = resolved_layer == "silver"
        is_gold = resolved_layer == "gold"
        if is_silver:
            layer_bucketing.silver_layout_mode()
        if is_gold:
            layer_bucketing.gold_layout_mode()
        symbol = str(ticker or "").strip().upper()

        if resolved_domain == "market":
            if symbol:
                path = (
                    DataPaths.get_silver_market_bucket_path(layer_bucketing.bucket_letter(symbol))
                    if is_silver
                    else DataPaths.get_gold_market_bucket_path(layer_bucketing.bucket_letter(symbol))
                )
                rows = DataService._read_delta(container, path, limit=None)
                rows = [row for row in rows if str(row.get("symbol", "")).strip().upper() == symbol]
                return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)
            prefix = "market-data/buckets" if is_silver else "market/buckets"
            rows = DataService._read_cross_section_from_prefix(container, prefix, limit=downstream_limit)
            return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)

        if resolved_domain == "finance":
            if is_silver:
                rows = DataService._read_silver_finance_regular(
                    container=container,
                    ticker=symbol or None,
                    limit=downstream_limit,
                )
                return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)
            if symbol:
                path = DataPaths.get_gold_finance_bucket_path(layer_bucketing.bucket_letter(symbol))
                rows = DataService._read_delta(container, path, limit=None)
                rows = [row for row in rows if str(row.get("symbol", "")).strip().upper() == symbol]
                return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)
            rows = DataService._read_cross_section_from_prefix(container, "finance/buckets", limit=downstream_limit)
            return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)

        if resolved_domain == "earnings":
            if symbol:
                path = (
                    DataPaths.get_silver_earnings_bucket_path(layer_bucketing.bucket_letter(symbol))
                    if is_silver
                    else DataPaths.get_gold_earnings_bucket_path(layer_bucketing.bucket_letter(symbol))
                )
                rows = DataService._read_delta(container, path, limit=None)
                rows = [row for row in rows if str(row.get("symbol", "")).strip().upper() == symbol]
                return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)
            prefix = f"{(getattr(cfg, 'EARNINGS_DATA_PREFIX', 'earnings-data') or 'earnings-data')}/buckets" if is_silver else "earnings/buckets"
            rows = DataService._read_cross_section_from_prefix(container, prefix, limit=downstream_limit)
            return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)

        if resolved_domain == "price-target":
            if symbol:
                path = (
                    DataPaths.get_silver_price_target_bucket_path(layer_bucketing.bucket_letter(symbol))
                    if is_silver
                    else DataPaths.get_gold_price_targets_bucket_path(layer_bucketing.bucket_letter(symbol))
                )
                rows = DataService._read_delta(container, path, limit=None)
                rows = [row for row in rows if str(row.get("symbol", "")).strip().upper() == symbol]
                return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)
            prefix = "price-target-data/buckets" if is_silver else "targets/buckets"
            rows = DataService._read_cross_section_from_prefix(container, prefix, limit=downstream_limit)
            return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)

        raise ValueError(f"Domain '{domain}' not supported on generic endpoint")

    @staticmethod
    def get_finance_data(
        layer: str,
        sub_domain: str,
        ticker: Optional[str] = None,
        limit: Optional[int] = None,
        sort_by_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Specialized retrieval for Finance data.
        """
        resolved_layer = str(layer or "").strip().lower()
        resolved_sub = str(sub_domain or "").strip().lower()
        container = DataService._container_for_layer(resolved_layer)
        resolved_sort = DataService._normalize_date_sort_direction(sort_by_date)
        downstream_limit = None if resolved_sort else limit

        if resolved_layer == "bronze":
            client = mdc.get_storage_client(container)
            if client is None:
                raise FileNotFoundError(
                    f"Storage client unavailable for container={container!r}. "
                    "Set Azure storage env vars to enable Bronze exploration."
                )

            if resolved_sub not in _FINANCE_SILVER_FOLDERS:
                raise ValueError(f"Unknown finance sub-domain: {sub_domain}")

            report_type = _FINANCE_SUBDOMAIN_TO_REPORT_TYPE.get(resolved_sub)
            if not report_type:
                raise ValueError(f"Unsupported finance sub-domain in alpha26 mode: {sub_domain}")
            rows = DataService._read_bronze_alpha26_bucket(
                container=container,
                client=client,
                domain_prefix="finance-data",
                symbol=ticker,
                report_type=report_type,
                limit=downstream_limit,
            )
            return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)
        
        if resolved_layer == "silver":
            if not ticker:
                raise ValueError("ticker is required for Silver finance data.")
            if resolved_sub not in _FINANCE_SILVER_FOLDERS:
                raise ValueError(f"Unknown finance sub-domain: {sub_domain}")

            layer_bucketing.silver_layout_mode()
            symbol = str(ticker).strip().upper()
            path = DataPaths.get_silver_finance_bucket_path(
                resolved_sub,
                layer_bucketing.bucket_letter(symbol),
            )
            rows = DataService._read_delta(container, path, limit=None)
            rows = [row for row in rows if str(row.get("symbol", "")).strip().upper() == symbol]
            return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)

        if resolved_layer == "gold":
            if not ticker:
                raise ValueError("ticker is required for Gold finance data.")
            layer_bucketing.gold_layout_mode()
            symbol = str(ticker).strip().upper()
            path = DataPaths.get_gold_finance_bucket_path(layer_bucketing.bucket_letter(symbol))
            rows = DataService._read_delta(container, path, limit=None)
            rows = [row for row in rows if str(row.get("symbol", "")).strip().upper() == symbol]
            return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)

        raise ValueError("Layer must be 'bronze', 'silver', or 'gold'.")

    @staticmethod
    def _get_bronze_data(
        *,
        container: str,
        domain: str,
        ticker: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        client = mdc.get_storage_client(container)
        if client is None:
            raise FileNotFoundError(
                f"Storage client unavailable for container={container!r}. "
                "Set Azure storage env vars to enable Bronze exploration."
            )

        symbol = str(ticker or "").strip().upper() if ticker else ""
        bronze_bucketing.bronze_layout_mode()

        if domain == "market":
            return DataService._read_bronze_alpha26_bucket(
                container=container,
                client=client,
                domain_prefix="market-data",
                symbol=symbol or None,
                limit=limit,
            )

        if domain == "earnings":
            prefix = getattr(cfg, "EARNINGS_DATA_PREFIX", "earnings-data") or "earnings-data"
            return DataService._read_bronze_alpha26_bucket(
                container=container,
                client=client,
                domain_prefix=prefix,
                symbol=symbol or None,
                limit=limit,
            )

        if domain == "finance":
            return DataService._read_bronze_alpha26_bucket(
                container=container,
                client=client,
                domain_prefix="finance-data",
                symbol=symbol or None,
                limit=limit,
            )

        if domain in {"price-target", "price_target"}:
            return DataService._read_bronze_alpha26_bucket(
                container=container,
                client=client,
                domain_prefix="price-target-data",
                symbol=symbol or None,
                limit=limit,
            )

        raise ValueError(f"Domain '{domain}' not supported on Bronze explorer endpoint")

    @staticmethod
    def _read_bronze_raw(
        container: str,
        blob_path: str,
        *,
        kind: str,
        limit: Optional[int] = None,
        client: Any = None,
    ) -> List[Dict[str, Any]]:
        if client is None:
            client = mdc.get_storage_client(container)
        if client is None:
            raise FileNotFoundError(
                f"Storage client unavailable for container={container!r}. "
                "Set Azure storage env vars to enable Bronze exploration."
            )

        raw_bytes = mdc.read_raw_bytes(blob_path, client=client)
        if not raw_bytes:
            raise FileNotFoundError(f"Raw blob not found: {container}/{blob_path}")

        df: pd.DataFrame
        kind_key = str(kind or "").strip().lower()
        if kind_key == "csv":
            df = pd.read_csv(BytesIO(raw_bytes))
        elif kind_key == "json":
            payload = json.loads(raw_bytes.decode("utf-8"))
            if isinstance(payload, list):
                df = pd.DataFrame(payload)
            elif isinstance(payload, dict):
                df = pd.DataFrame([payload])
            else:
                raise ValueError(f"Unsupported JSON payload type: {type(payload).__name__}")
        elif kind_key == "parquet":
            df = pd.read_parquet(BytesIO(raw_bytes))
        else:
            raise ValueError(f"Unsupported bronze kind={kind!r}")

        return DataService._df_to_records_json_safe(df, limit=limit)

    @staticmethod
    def _read_delta(container: str, path: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        try:
            df = delta_core.load_delta(container, path)
            if df is None:
                raise FileNotFoundError(f"Delta table not found: {container}/{path}")

            return DataService._df_to_records_json_safe(df, limit=limit)
        except Exception as e:
            # Log error
            raise FileNotFoundError(f"Failed to read data at {path}: {str(e)}")

    @staticmethod
    def _extract_finance_domain_rows(
        layer: str,
        domain: str,
        ticker: Optional[str],
        sample_rows: int,
    ) -> List[Dict[str, Any]]:
        normalized_domain = str(domain or "").strip().lower()
        if normalized_domain.startswith("finance/"):
            _, _, remainder = normalized_domain.partition("/")
            sub_domain = remainder.strip()
            if not sub_domain:
                raise ValueError("finance domain requires a sub-domain, e.g. finance/balance_sheet")
            return DataService.get_finance_data(layer, sub_domain, ticker, limit=sample_rows)
        return DataService.get_data(layer, domain, ticker, limit=sample_rows)

    @staticmethod
    def _format_number_bucket_edge(value: float) -> float:
        if value == int(value):
            return float(int(value))
        return float(np.round(value, 6))

    @staticmethod
    def get_column_profile(
        layer: str,
        domain: str,
        column: str,
        *,
        ticker: Optional[str] = None,
        bins: int = 20,
        sample_rows: int = 10000,
        top_values: int = 20,
    ) -> Dict[str, Any]:
        normalized_layer = str(layer or "").strip().lower()
        normalized_domain = str(domain or "").strip().lower()
        normalized_column = str(column or "").strip()

        if not normalized_layer:
            raise ValueError("layer is required.")
        if normalized_layer not in {"bronze", "silver", "gold"}:
            raise ValueError("Layer must be 'bronze', 'silver', or 'gold'.")
        if not normalized_domain:
            raise ValueError("domain is required.")
        if not normalized_column:
            raise ValueError("column is required.")

        resolved_ticker = None if ticker is None else str(ticker).strip().upper() or None
        resolved_bins = max(3, min(int(bins), 200))
        resolved_sample_rows = max(10, min(int(sample_rows), 100000))
        resolved_top_values = max(1, min(int(top_values), 200))

        rows = DataService._extract_finance_domain_rows(
            normalized_layer,
            normalized_domain,
            resolved_ticker,
            sample_rows=resolved_sample_rows,
        )
        if not rows:
            return {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "column": normalized_column,
                "kind": "string",
                "totalRows": 0,
                "nonNullCount": 0,
                "nullCount": 0,
                "sampleRows": resolved_sample_rows,
                "bins": [],
                "uniqueCount": 0,
                "duplicateCount": 0,
                "topValues": [],
            }

        df = pd.DataFrame(rows)
        if normalized_column not in df.columns:
            raise ValueError(f"Column '{normalized_column}' not found in sampled data.")

        series = df[normalized_column]
        total_rows = int(len(df))
        series_non_null = series.dropna()
        non_null_count = int(len(series_non_null))
        null_count = total_rows - non_null_count

        if non_null_count == 0:
            return {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "column": normalized_column,
                "kind": "string",
                "totalRows": total_rows,
                "nonNullCount": 0,
                "nullCount": null_count,
                "sampleRows": resolved_sample_rows,
                "bins": [],
                "uniqueCount": 0,
                "duplicateCount": 0,
                "topValues": [],
            }

        candidate_str = series_non_null.astype(str).str.strip()
        if candidate_str.empty:
            return {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "column": normalized_column,
                "kind": "string",
                "totalRows": total_rows,
                "nonNullCount": non_null_count,
                "nullCount": null_count,
                "sampleRows": resolved_sample_rows,
                "bins": [],
                "uniqueCount": 0,
                "duplicateCount": 0,
                "topValues": [],
            }

        parsed_date = pd.to_datetime(series_non_null, errors="coerce", utc=False)
        date_count = int(parsed_date.notna().sum())
        date_ratio = date_count / non_null_count if non_null_count else 0.0

        if date_ratio >= 0.7:
            date_vals = parsed_date.dropna().dt.to_period("M").astype(str)
            value_counts = date_vals.value_counts().sort_index()
            buckets = []
            for key, count in value_counts.items():
                buckets.append({
                    "label": str(key),
                    "count": int(count),
                })
            return {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "column": normalized_column,
                "kind": "date",
                "totalRows": total_rows,
                "nonNullCount": non_null_count,
                "nullCount": null_count,
                "sampleRows": resolved_sample_rows,
                "bins": buckets,
                "uniqueCount": int(date_vals.nunique()),
                "duplicateCount": int(non_null_count - date_vals.nunique()),
                "topValues": [],
            }

        numeric = pd.to_numeric(series_non_null, errors="coerce")
        numeric_count = int(numeric.notna().sum())
        numeric_ratio = numeric_count / non_null_count if non_null_count else 0.0

        if numeric_ratio >= 0.7 and numeric_count > 0:
            numeric_clean = numeric.replace([np.inf, -np.inf], np.nan).dropna()

            if numeric_clean.empty:
                kind = "string"
                return {
                    "layer": normalized_layer,
                    "domain": normalized_domain,
                    "column": normalized_column,
                    "kind": kind,
                    "totalRows": total_rows,
                    "nonNullCount": non_null_count,
                    "nullCount": null_count,
                    "sampleRows": resolved_sample_rows,
                    "bins": [],
                    "uniqueCount": 0,
                    "duplicateCount": 0,
                    "topValues": [],
                }

            if len(numeric_clean.unique()) == 1:
                value = DataService._format_number_bucket_edge(float(numeric_clean.iloc[0]))
                return {
                    "layer": normalized_layer,
                    "domain": normalized_domain,
                    "column": normalized_column,
                    "kind": "numeric",
                    "totalRows": total_rows,
                    "nonNullCount": non_null_count,
                    "nullCount": null_count,
                    "sampleRows": resolved_sample_rows,
                    "bins": [{"label": str(value), "count": int(len(numeric_clean)), "start": value, "end": value}],
                    "uniqueCount": 1,
                    "duplicateCount": int(non_null_count - 1),
                    "topValues": [],
                }

            try:
                bucketed = pd.cut(numeric_clean, bins=resolved_bins)
            except ValueError:
                unique_values = numeric_clean.drop_duplicates().sort_values()
                min_value = float(unique_values.min())
                max_value = float(unique_values.max())
                if min_value == max_value:
                    return {
                        "layer": normalized_layer,
                        "domain": normalized_domain,
                        "column": normalized_column,
                        "kind": "numeric",
                        "totalRows": total_rows,
                        "nonNullCount": non_null_count,
                        "nullCount": null_count,
                        "sampleRows": resolved_sample_rows,
                        "bins": [{"label": str(DataService._format_number_bucket_edge(min_value)), "count": int(len(numeric_clean)), "start": min_value, "end": max_value}],
                        "uniqueCount": int(unique_values.nunique()),
                        "duplicateCount": int(non_null_count - unique_values.nunique()),
                        "topValues": [],
                    }
                bucketed = pd.qcut(
                    numeric_clean,
                    q=min(20, int(numeric_clean.nunique())),
                    duplicates="drop"
                )
                buckets = bucketed.value_counts().sort_index()
            else:
                buckets = bucketed.value_counts().sort_index()

            payload = []
            for key, count in buckets.items():
                if isinstance(key, pd.Interval):
                    left = DataService._format_number_bucket_edge(float(key.left))
                    right = DataService._format_number_bucket_edge(float(key.right))
                    if isinstance(key.left, (int, float)) and isinstance(key.right, (int, float)) and key.left == key.right:
                        label = str(left)
                    else:
                        label = f"{left} to {right}"
                    payload.append(
                        {
                            "label": label,
                            "count": int(count),
                            "start": left,
                            "end": right,
                        }
                    )
                else:
                    value = DataService._format_number_bucket_edge(float(key))
                    payload.append({"label": str(value), "count": int(count), "start": value, "end": value})

            return {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "column": normalized_column,
                "kind": "numeric",
                "totalRows": total_rows,
                "nonNullCount": non_null_count,
                "nullCount": null_count,
                "sampleRows": resolved_sample_rows,
                "bins": payload,
                "uniqueCount": int(numeric_clean.nunique()),
                "duplicateCount": int(non_null_count - numeric_clean.nunique()),
                "topValues": [],
            }

        value_counts = candidate_str.value_counts()
        unique = int(value_counts.shape[0])
        top_n = value_counts.head(resolved_top_values)

        return {
            "layer": normalized_layer,
            "domain": normalized_domain,
            "column": normalized_column,
            "kind": "string",
            "totalRows": total_rows,
            "nonNullCount": non_null_count,
            "nullCount": null_count,
            "sampleRows": resolved_sample_rows,
            "bins": [],
            "uniqueCount": unique,
            "duplicateCount": int(non_null_count - unique),
            "topValues": [
                {"value": str(value), "count": int(count)} for value, count in top_n.items()
            ],
        }
