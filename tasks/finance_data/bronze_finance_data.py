from __future__ import annotations

import asyncio
import json
import os
import hashlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Optional

import pandas as pd

from core.massive_gateway_client import (
    MassiveGatewayClient,
    MassiveGatewayError,
    MassiveGatewayNotFoundError,
    MassiveGatewayRateLimitError,
)
from core import core as mdc
from core.pipeline import ListManager
from tasks.common.bronze_backfill_coverage import (
    load_coverage_marker,
    normalize_date,
    resolve_backfill_start_date,
    should_force_backfill,
    write_coverage_marker,
)
from tasks.common.run_manifests import create_bronze_finance_manifest
from tasks.common import bronze_bucketing
from tasks.common import domain_artifacts
from tasks.common.job_status import resolve_job_run_status
from tasks.finance_data import config as cfg


bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
common_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_COMMON)
list_manager = ListManager(bronze_client, "finance-data", auto_flush=False, allow_blacklist_updates=False)


REPORTS = [
    {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "report": "balance_sheet",
    },
    {
        "folder": "Cash Flow",
        "file_suffix": "quarterly_cash-flow",
        "report": "cash_flow",
    },
    {
        "folder": "Income Statement",
        "file_suffix": "quarterly_financials",
        "report": "income_statement",
    },
    {
        "folder": "Valuation",
        "file_suffix": "quarterly_valuation_measures",
        "report": "valuation",
    },
]


FINANCE_REPORT_STALE_DAYS = max(0, int(getattr(cfg, "MASSIVE_FINANCE_FRESH_DAYS", 7)))
_RECOVERY_MAX_ATTEMPTS = 3
_RECOVERY_SLEEP_SECONDS = 5.0
_DEFAULT_SHARED_FINANCE_LOCK = "finance-pipeline-shared"
_COVERAGE_DOMAIN = "finance"
_COVERAGE_PROVIDER = "massive"
_FINANCE_SCHEMA_VERSION = 2
_STATEMENT_TIMEFRAMES: tuple[str, ...] = ("quarterly", "annual")
_STATEMENT_QUERY_LIMIT = 100
_VALUATION_QUERY_LIMIT = 1
_BUCKET_COLUMNS = [
    "symbol",
    "report_type",
    "payload_json",
    "source_min_date",
    "source_max_date",
    "ingested_at",
    "payload_hash",
]


def _empty_coverage_summary() -> dict[str, int]:
    return {
        "coverage_checked": 0,
        "coverage_forced_refetch": 0,
        "coverage_marked_covered": 0,
        "coverage_marked_limited": 0,
        "coverage_skipped_limited_marker": 0,
    }


def _mark_coverage(
    *,
    symbol: str,
    backfill_start: date,
    status: str,
    earliest_available: Optional[date],
    coverage_summary: dict[str, int],
) -> None:
    try:
        write_coverage_marker(
            common_client=common_client,
            domain=_COVERAGE_DOMAIN,
            symbol=symbol,
            backfill_start=backfill_start,
            coverage_status=status,
            earliest_available=earliest_available,
            provider=_COVERAGE_PROVIDER,
        )
        if status == "covered":
            coverage_summary["coverage_marked_covered"] += 1
        elif status == "limited":
            coverage_summary["coverage_marked_limited"] += 1
    except Exception as exc:
        mdc.write_warning(f"Failed to write finance coverage marker for {symbol}: {exc}")


def _is_truthy(raw: str | None) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _parse_wait_timeout_seconds(raw: str | None, *, default: float) -> float | None:
    if raw is None:
        return default
    value = str(raw).strip()
    if not value:
        return default
    if value.lower() in {"none", "inf", "infinite", "forever"}:
        return None
    try:
        parsed = float(value)
    except Exception:
        return default
    return max(0.0, parsed)


def _validate_environment() -> None:
    if not cfg.AZURE_CONTAINER_BRONZE:
        raise ValueError("Environment variable 'AZURE_CONTAINER_BRONZE' is strictly required.")
    if not os.environ.get("ASSET_ALLOCATION_API_BASE_URL"):
        raise ValueError("Environment variable 'ASSET_ALLOCATION_API_BASE_URL' is strictly required.")
    if not (
        os.environ.get("ASSET_ALLOCATION_API_KEY")
        or os.environ.get("API_KEY")
        or _is_truthy(os.environ.get("ASSET_ALLOCATION_API_ALLOW_NO_AUTH"))
    ):
        raise ValueError("Environment variable 'ASSET_ALLOCATION_API_KEY' (or API_KEY) is strictly required.")


def _is_fresh(blob_last_modified: Optional[datetime], *, fresh_days: int) -> bool:
    if blob_last_modified is None:
        return False
    try:
        age = datetime.now(timezone.utc) - blob_last_modified
    except Exception:
        return False
    return age <= timedelta(days=max(0, fresh_days))


def _json_dumps_compact(payload: Any) -> str:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def _decode_payload_json(raw: Any) -> Optional[dict[str, Any]]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except Exception:
        return None
    if isinstance(parsed, dict):
        return parsed
    return None


def _parse_ingested_at(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _build_finance_bucket_row(
    *,
    symbol: str,
    report_type: str,
    payload: dict[str, Any],
    source_min_date: Optional[date],
    source_max_date: Optional[date],
) -> dict[str, Any]:
    payload_json = _json_dumps_compact(payload)
    payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    return {
        "symbol": str(symbol).strip().upper(),
        "report_type": str(report_type).strip().lower(),
        "payload_json": payload_json,
        "source_min_date": source_min_date.isoformat() if source_min_date is not None else None,
        "source_max_date": source_max_date.isoformat() if source_max_date is not None else None,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "payload_hash": payload_hash,
    }


def _load_alpha26_finance_row_map(*, symbols: set[str]) -> dict[tuple[str, str], dict[str, Any]]:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for bucket in bronze_bucketing.ALPHABET_BUCKETS:
        try:
            df = bronze_bucketing.read_bucket_parquet(
                client=bronze_client,
                prefix="finance-data",
                bucket=bucket,
            )
        except Exception:
            continue
        if df is None or df.empty:
            continue
        if "symbol" not in df.columns or "report_type" not in df.columns:
            continue
        for _, row in df.iterrows():
            symbol = str(row.get("symbol") or "").strip().upper()
            report_type = str(row.get("report_type") or "").strip().lower()
            if not symbol or not report_type:
                continue
            if symbols and symbol not in symbols:
                continue
            candidate = {
                "symbol": symbol,
                "report_type": report_type,
                "payload_json": row.get("payload_json"),
                "source_min_date": row.get("source_min_date"),
                "source_max_date": row.get("source_max_date"),
                "ingested_at": row.get("ingested_at"),
                "payload_hash": row.get("payload_hash"),
            }
            key = (symbol, report_type)
            existing = out.get(key)
            if existing is None:
                out[key] = candidate
                continue
            existing_ts = _parse_ingested_at(existing.get("ingested_at"))
            candidate_ts = _parse_ingested_at(candidate.get("ingested_at"))
            if existing_ts is None and candidate_ts is not None:
                out[key] = candidate
                continue
            if existing_ts is not None and candidate_ts is not None and candidate_ts >= existing_ts:
                out[key] = candidate
    return out


def _upsert_alpha26_finance_row(
    *,
    row_key: tuple[str, str],
    row: dict[str, Any],
    alpha26_rows: Optional[dict[tuple[str, str], dict[str, Any]]],
    alpha26_lock: Optional[threading.Lock],
) -> None:
    if alpha26_rows is None:
        return
    if alpha26_lock is not None:
        with alpha26_lock:
            alpha26_rows[row_key] = row
    else:
        alpha26_rows[row_key] = row


def _remove_alpha26_finance_row(
    *,
    row_key: tuple[str, str],
    alpha26_rows: Optional[dict[tuple[str, str], dict[str, Any]]],
    alpha26_lock: Optional[threading.Lock],
) -> None:
    if alpha26_rows is None:
        return
    if alpha26_lock is not None:
        with alpha26_lock:
            alpha26_rows.pop(row_key, None)
    else:
        alpha26_rows.pop(row_key, None)


def _write_alpha26_finance_buckets(
    alpha26_rows: dict[tuple[str, str], dict[str, Any]]
) -> tuple[int, Optional[str], Optional[int]]:
    bucket_frames = bronze_bucketing.empty_bucket_frames(_BUCKET_COLUMNS)
    if alpha26_rows:
        frame = pd.DataFrame(list(alpha26_rows.values()), columns=_BUCKET_COLUMNS)
    else:
        frame = pd.DataFrame(columns=_BUCKET_COLUMNS)

    if not frame.empty:
        frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
        frame["report_type"] = frame["report_type"].astype(str).str.strip().str.lower()
        frame["ingested_at_sort"] = pd.to_datetime(frame.get("ingested_at"), errors="coerce", utc=True)
        frame = frame.sort_values("ingested_at_sort").drop(columns=["ingested_at_sort"])
        frame = frame.drop_duplicates(subset=["symbol", "report_type"], keep="last").reset_index(drop=True)
        for bucket, part in bronze_bucketing.split_df_by_bucket(frame, symbol_column="symbol").items():
            bucket_frames[bucket] = part

    for bucket in bronze_bucketing.ALPHABET_BUCKETS:
        part = bucket_frames[bucket]
        if part is None or part.empty:
            part = pd.DataFrame(columns=_BUCKET_COLUMNS)
        bronze_bucketing.write_bucket_parquet(
            client=bronze_client,
            prefix="finance-data",
            bucket=bucket,
            df=part,
            codec=bronze_bucketing.alpha26_codec(),
        )
        try:
            domain_artifacts.write_bucket_artifact(
                layer="bronze",
                domain="finance",
                bucket=bucket,
                df=part,
                date_column="date",
                client=bronze_client,
                job_name="bronze-finance-job",
            )
        except Exception as exc:
            mdc.write_warning(f"Bronze finance metadata bucket artifact write failed bucket={bucket}: {exc}")

    symbols = sorted({str(key[0]).upper() for key in alpha26_rows.keys()})
    symbol_to_bucket = {symbol: bronze_bucketing.bucket_letter(symbol) for symbol in symbols}
    index_path = bronze_bucketing.write_symbol_index(domain="finance", symbol_to_bucket=symbol_to_bucket)
    column_count: Optional[int] = len(_BUCKET_COLUMNS)
    if index_path:
        try:
            payload = domain_artifacts.write_domain_artifact(
                layer="bronze",
                domain="finance",
                date_column="date",
                client=bronze_client,
                symbol_count_override=len(symbol_to_bucket),
                symbol_index_path=index_path,
                job_name="bronze-finance-job",
            )
            column_count = domain_artifacts.extract_column_count(payload)
        except Exception as exc:
            mdc.write_warning(f"Bronze finance metadata artifact write failed: {exc}")
    return len(symbol_to_bucket), index_path, column_count


def _delete_flat_finance_symbol_blobs() -> int:
    deleted = 0
    allowed_folders = {str(report.get("folder")) for report in REPORTS}
    for blob in bronze_client.list_blob_infos(name_starts_with="finance-data/"):
        name = str(blob.get("name") or "")
        if "/buckets/" in name:
            continue
        parts = name.strip("/").split("/")
        if len(parts) != 3:
            continue
        if parts[0] != "finance-data":
            continue
        if parts[1] not in allowed_folders:
            continue
        if not parts[2].endswith(".json"):
            continue
        try:
            bronze_client.delete_file(name)
            deleted += 1
        except Exception as exc:
            mdc.write_warning(f"Failed deleting flat finance blob {name}: {exc}")
    return deleted


def _parse_iso_date(raw: Any) -> Optional[date]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text[:10]).date()
    except Exception:
        return None


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "nan", "n/a", "na", "-", "not available"}:
        return None
    try:
        return float(text.replace(",", ""))
    except Exception:
        return None


def _get_nested_dict(payload: Any, *keys: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    for key in keys:
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _get_first_float(payload: dict[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        if key not in payload:
            continue
        parsed = _coerce_float(payload.get(key))
        if parsed is not None:
            return parsed
    return None


def _empty_finance_payload(report_name: str) -> dict[str, Any]:
    base = {
        "schema_version": _FINANCE_SCHEMA_VERSION,
        "provider": _COVERAGE_PROVIDER,
        "report_type": str(report_name).strip().lower(),
    }
    if report_name == "valuation":
        return {
            **base,
            "as_of": None,
            "market_cap": None,
            "pe_ratio": None,
        }
    return {
        **base,
        "rows": [],
    }


def _extract_statement_section(report_name: str, row: dict[str, Any]) -> dict[str, Any]:
    financials = _get_nested_dict(row, "financials")
    if report_name == "balance_sheet":
        return _get_nested_dict(row, "balance_sheet", "balanceSheet") or _get_nested_dict(
            financials,
            "balance_sheet",
            "balanceSheet",
        )
    if report_name == "income_statement":
        return _get_nested_dict(row, "income_statement", "incomeStatement") or _get_nested_dict(
            financials,
            "income_statement",
            "incomeStatement",
        )
    if report_name == "cash_flow":
        return _get_nested_dict(row, "cash_flow_statement", "cash_flow", "cashFlowStatement") or _get_nested_dict(
            financials,
            "cash_flow_statement",
            "cash_flow",
            "cashFlowStatement",
        )
    return {}


def _canonical_statement_row(report_name: str, row: dict[str, Any], *, timeframe: str) -> Optional[dict[str, Any]]:
    report_date = _parse_iso_date(
        row.get("period_end") or row.get("period_of_report_date") or row.get("date") or row.get("report_period")
    )
    if report_date is None:
        return None

    section = _extract_statement_section(report_name, row)
    if not section:
        return None

    if report_name == "balance_sheet":
        out = {
            "date": report_date.isoformat(),
            "timeframe": timeframe,
            "long_term_debt": _get_first_float(
                section,
                "long_term_debt_and_capital_lease_obligations",
                "long_term_debt",
                "long_term_debt_noncurrent",
            ),
            "total_assets": _get_first_float(section, "total_assets"),
            "current_assets": _get_first_float(section, "total_current_assets", "current_assets"),
            "current_liabilities": _get_first_float(
                section,
                "total_current_liabilities",
                "current_liabilities",
            ),
            "shares_outstanding": _get_first_float(
                section,
                "common_stock_shares_outstanding",
                "common_shares_outstanding",
                "ordinary_shares_number",
                "share_issued",
            ),
        }
    elif report_name == "income_statement":
        out = {
            "date": report_date.isoformat(),
            "timeframe": timeframe,
            "total_revenue": _get_first_float(section, "revenues", "revenue", "total_revenue"),
            "gross_profit": _get_first_float(section, "gross_profit"),
            "net_income": _get_first_float(
                section,
                "net_income_loss",
                "consolidated_net_income_loss",
                "net_income_loss_attributable_to_parent",
            ),
            "shares_outstanding": _get_first_float(
                section,
                "diluted_shares_outstanding",
                "basic_shares_outstanding",
                "weighted_average_shares",
            ),
        }
    elif report_name == "cash_flow":
        out = {
            "date": report_date.isoformat(),
            "timeframe": timeframe,
            "operating_cash_flow": _get_first_float(
                section,
                "net_cash_flow_from_operating_activities",
                "net_cash_from_operating_activities",
                "net_cash_provided_by_operating_activities",
            ),
        }
    else:
        return None

    metric_values = [value for key, value in out.items() if key not in {"date", "timeframe"}]
    if not any(value is not None for value in metric_values):
        return None
    return out


def _build_statement_payload(report_name: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        row_date = str(row.get("date") or "").strip()
        timeframe = str(row.get("timeframe") or "").strip().lower()
        if not row_date or not timeframe:
            continue
        deduped[(row_date, timeframe)] = row
    ordered_rows = [deduped[key] for key in sorted(deduped)]
    return {
        "schema_version": _FINANCE_SCHEMA_VERSION,
        "provider": _COVERAGE_PROVIDER,
        "report_type": str(report_name).strip().lower(),
        "rows": ordered_rows,
    }


def _build_valuation_payload(payload: dict[str, Any], *, report_name: str) -> dict[str, Any]:
    results = payload.get("results")
    latest_row: dict[str, Any] | None = None
    latest_date: Optional[date] = None
    if isinstance(results, list):
        for item in results:
            if not isinstance(item, dict):
                continue
            item_date = _parse_iso_date(item.get("date") or item.get("as_of"))
            if item_date is None:
                continue
            if latest_date is None or item_date > latest_date:
                latest_date = item_date
                latest_row = item
    elif isinstance(payload, dict):
        latest_row = payload
        latest_date = _parse_iso_date(payload.get("date") or payload.get("as_of"))

    market_cap = _get_first_float(latest_row or {}, "market_cap")
    pe_ratio = _get_first_float(latest_row or {}, "price_to_earnings", "pe_ratio")
    if latest_date is None or (market_cap is None and pe_ratio is None):
        return _empty_finance_payload(report_name)
    return {
        "schema_version": _FINANCE_SCHEMA_VERSION,
        "provider": _COVERAGE_PROVIDER,
        "report_type": report_name,
        "as_of": latest_date.isoformat(),
        "market_cap": market_cap,
        "pe_ratio": pe_ratio,
    }


def _fetch_massive_finance_payload(
    *,
    symbol: str,
    report_name: str,
    massive_client: MassiveGatewayClient,
) -> dict[str, Any]:
    if report_name == "valuation":
        payload = massive_client.get_finance_report(
            symbol=symbol,
            report="valuation",
            sort="date.desc",
            limit=_VALUATION_QUERY_LIMIT,
            pagination=False,
        )
        if not isinstance(payload, dict):
            raise MassiveGatewayError(
                "Unexpected Massive valuation response type.",
                payload={"symbol": symbol, "report": report_name},
            )
        return _build_valuation_payload(payload, report_name=report_name)

    rows: list[dict[str, Any]] = []
    for timeframe in _STATEMENT_TIMEFRAMES:
        payload = massive_client.get_finance_report(
            symbol=symbol,
            report=report_name,
            timeframe=timeframe,
            sort="period_end.asc",
            limit=_STATEMENT_QUERY_LIMIT,
            pagination=True,
        )
        if not isinstance(payload, dict):
            raise MassiveGatewayError(
                "Unexpected Massive statement response type.",
                payload={"symbol": symbol, "report": report_name, "timeframe": timeframe},
            )
        results = payload.get("results")
        if not isinstance(results, list):
            continue
        for item in results:
            if not isinstance(item, dict):
                continue
            canonical_row = _canonical_statement_row(report_name, item, timeframe=timeframe)
            if canonical_row is not None:
                rows.append(canonical_row)
    return _build_statement_payload(report_name, rows)


def _payload_report_dates(payload: dict[str, Any]) -> list[date]:
    report_type = str(payload.get("report_type") or "").strip().lower()
    if report_type == "valuation":
        valuation_date = _parse_iso_date(payload.get("as_of"))
        return [valuation_date] if valuation_date is not None else []

    rows = payload.get("rows")
    if not isinstance(rows, list):
        return []
    out: list[date] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_date = _parse_iso_date(row.get("date"))
        if row_date is not None:
            out.append(row_date)
    return out


def _apply_backfill_start_to_finance_payload(payload: dict[str, Any], *, backfill_start: Optional[date]) -> dict[str, Any]:
    if backfill_start is None:
        return payload

    report_type = str(payload.get("report_type") or "").strip().lower()
    if report_type == "valuation":
        as_of = _parse_iso_date(payload.get("as_of"))
        if as_of is not None and as_of < backfill_start:
            return _empty_finance_payload(report_type)
        return payload

    rows = payload.get("rows")
    if not isinstance(rows, list):
        return payload

    filtered_rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_date = _parse_iso_date(row.get("date"))
        if row_date is not None and row_date < backfill_start:
            continue
        filtered_rows.append(row)

    filtered_payload = dict(payload)
    filtered_payload["rows"] = filtered_rows
    return filtered_payload


def _extract_latest_finance_report_date(payload: dict[str, Any]) -> Optional[date]:
    dates = _payload_report_dates(payload)
    if not dates:
        return None
    return max(dates)


def _extract_source_earliest_finance_date(payload: dict[str, Any]) -> Optional[date]:
    dates = _payload_report_dates(payload)
    if not dates:
        return None
    return min(dates)


def _has_non_empty_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float, bool)):
        return True
    text = str(value).strip()
    if not text:
        return False
    return text.lower() not in {"none", "null", "nan", "n/a", "na", "-", "not available"}


def _is_canonical_finance_payload(payload: dict[str, Any], *, report_name: str) -> bool:
    return (
        isinstance(payload, dict)
        and payload.get("schema_version") == _FINANCE_SCHEMA_VERSION
        and str(payload.get("provider") or "").strip().lower() == _COVERAGE_PROVIDER
        and str(payload.get("report_type") or "").strip().lower() == str(report_name).strip().lower()
    )


def _is_empty_finance_payload(payload: dict[str, Any], *, report_name: str) -> bool:
    if not payload:
        return True

    payload_report_type = str(payload.get("report_type") or report_name).strip().lower()
    if not _is_canonical_finance_payload(payload, report_name=payload_report_type):
        return True

    if payload_report_type == "valuation":
        return not _has_non_empty_value(payload.get("as_of")) or not any(
            _has_non_empty_value(payload.get(key)) for key in ("market_cap", "pe_ratio")
        )

    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        return True
    for row in rows:
        if not isinstance(row, dict):
            continue
        if _has_non_empty_value(row.get("date")):
            return False
    return True

    return False


def fetch_and_save_raw(
    symbol: str,
    report: dict[str, str],
    massive_client: MassiveGatewayClient,
    *,
    backfill_start: Optional[date] = None,
    coverage_summary: Optional[dict[str, int]] = None,
    alpha26_mode: bool = True,
    alpha26_existing_row: Optional[dict[str, Any]] = None,
    alpha26_rows: Optional[dict[tuple[str, str], dict[str, Any]]] = None,
    alpha26_lock: Optional[threading.Lock] = None,
) -> bool:
    """
    Fetch a finance report via the API-hosted Massive gateway and store canonical v2 bytes in Bronze buckets.

    Returns True when a write occurred, False when skipped (fresh/no-op).
    """
    coverage_summary = coverage_summary if coverage_summary is not None else _empty_coverage_summary()
    if list_manager.is_blacklisted(symbol):
        return False

    if not alpha26_mode:
        raise ValueError("Bronze finance only supports alpha26 bucket mode.")

    report_name = report["report"]
    row_key = (str(symbol).strip().upper(), str(report_name).strip().lower())
    resolved_backfill_start = normalize_date(backfill_start)
    existing_payload: Optional[dict[str, Any]] = None
    existing_min_date: Optional[date] = None
    force_backfill = False
    existing_row = dict(alpha26_existing_row or {})

    try:
        if existing_row:
            existing_payload = _decode_payload_json(existing_row.get("payload_json"))
            existing_payload_current = isinstance(existing_payload, dict) and _is_canonical_finance_payload(
                existing_payload,
                report_name=report_name,
            )
            if resolved_backfill_start is not None:
                coverage_summary["coverage_checked"] += 1
                if existing_payload_current:
                    existing_min_date = _extract_source_earliest_finance_date(existing_payload or {})
                marker = load_coverage_marker(
                    common_client=common_client,
                    domain=_COVERAGE_DOMAIN,
                    symbol=symbol,
                )
                force_backfill, skipped_limited_marker = should_force_backfill(
                    existing_min_date=existing_min_date,
                    backfill_start=resolved_backfill_start,
                    marker=marker,
                )
                if skipped_limited_marker:
                    coverage_summary["coverage_skipped_limited_marker"] += 1
                if force_backfill:
                    coverage_summary["coverage_forced_refetch"] += 1
                elif existing_min_date is not None and existing_min_date <= resolved_backfill_start:
                    _mark_coverage(
                        symbol=symbol,
                        backfill_start=resolved_backfill_start,
                        status="covered",
                        earliest_available=existing_min_date,
                        coverage_summary=coverage_summary,
                    )
            ingested_at = _parse_ingested_at(existing_row.get("ingested_at"))
            if existing_payload_current and _is_fresh(ingested_at, fresh_days=FINANCE_REPORT_STALE_DAYS) and not force_backfill:
                list_manager.add_to_whitelist(symbol)
                return False
    except Exception:
        pass

    payload = _fetch_massive_finance_payload(
        symbol=symbol,
        report_name=report_name,
        massive_client=massive_client,
    )
    if _is_empty_finance_payload(payload, report_name=report_name):
        list_manager.add_to_blacklist(symbol)
        raise MassiveGatewayNotFoundError(
            f"Massive returned empty finance payload for {symbol} report={report_name}; "
            "automatic blacklist updates are disabled."
        )
    source_earliest = _extract_source_earliest_finance_date(payload)
    payload = _apply_backfill_start_to_finance_payload(payload, backfill_start=resolved_backfill_start)
    if resolved_backfill_start is not None and _is_empty_finance_payload(payload, report_name=report_name):
        if force_backfill:
            _mark_coverage(
                symbol=symbol,
                backfill_start=resolved_backfill_start,
                status="limited",
                earliest_available=source_earliest,
                coverage_summary=coverage_summary,
            )
        if existing_row:
            _remove_alpha26_finance_row(
                row_key=row_key,
                alpha26_rows=alpha26_rows,
                alpha26_lock=alpha26_lock,
            )
            mdc.write_line(
                f"No finance rows on/after {resolved_backfill_start.isoformat()} for {symbol} report={report_name}; "
                "removed alpha26 row."
            )
            list_manager.add_to_whitelist(symbol)
            return True
        list_manager.add_to_whitelist(symbol)
        return False

    if resolved_backfill_start is not None and force_backfill:
        marker_status = (
            "covered"
            if source_earliest is not None and source_earliest <= resolved_backfill_start
            else "limited"
        )
        _mark_coverage(
            symbol=symbol,
            backfill_start=resolved_backfill_start,
            status=marker_status,
            earliest_available=source_earliest,
            coverage_summary=coverage_summary,
        )

    if existing_payload is not None:
        if existing_payload == payload:
            list_manager.add_to_whitelist(symbol)
            return False
        if resolved_backfill_start is None:
            incoming_latest = _extract_latest_finance_report_date(payload)
            existing_latest = _extract_latest_finance_report_date(existing_payload)
            if (
                incoming_latest is not None
                and existing_latest is not None
                and incoming_latest <= existing_latest
            ):
                list_manager.add_to_whitelist(symbol)
                return False

    source_min = _extract_source_earliest_finance_date(payload)
    source_max = _extract_latest_finance_report_date(payload)
    bucket_row = _build_finance_bucket_row(
        symbol=symbol,
        report_type=report_name,
        payload=payload,
        source_min_date=source_min,
        source_max_date=source_max,
    )
    _upsert_alpha26_finance_row(
        row_key=row_key,
        row=bucket_row,
        alpha26_rows=alpha26_rows,
        alpha26_lock=alpha26_lock,
    )
    list_manager.add_to_whitelist(symbol)
    return True


def _format_failure_reason(exc: BaseException) -> str:
    reason_parts = [f"type={type(exc).__name__}"]
    status_code = getattr(exc, "status_code", None)
    if status_code is not None:
        reason_parts.append(f"status={status_code}")
    detail = getattr(exc, "detail", None)
    if detail:
        reason_parts.append(f"detail={str(detail)[:220]}")
    else:
        message = str(exc).strip()
        if message:
            reason_parts.append(f"message={message[:220]}")
    payload = getattr(exc, "payload", None)
    if isinstance(payload, dict):
        path = payload.get("path")
        if path:
            reason_parts.append(f"path={path}")
    return " ".join(reason_parts)


def _safe_close_massive_client(client: MassiveGatewayClient | None) -> None:
    if client is None:
        return
    try:
        client.close()
    except Exception:
        pass


class _ThreadLocalMassiveClientManager:
    def __init__(self, factory: Callable[[], MassiveGatewayClient] | None = None) -> None:
        self._factory = factory or MassiveGatewayClient.from_env
        self._lock = threading.Lock()
        self._generation = 0
        self._clients: dict[int, tuple[int, MassiveGatewayClient]] = {}

    def get_client(self) -> MassiveGatewayClient:
        thread_id = threading.get_ident()
        with self._lock:
            current = self._clients.get(thread_id)
            if current and current[0] == self._generation:
                return current[1]
            if current:
                _safe_close_massive_client(current[1])
            fresh_client = self._factory()
            self._clients[thread_id] = (self._generation, fresh_client)
            return fresh_client

    def reset_current(self) -> None:
        thread_id = threading.get_ident()
        with self._lock:
            current = self._clients.pop(thread_id, None)
        if current:
            _safe_close_massive_client(current[1])

    def close_all(self) -> None:
        with self._lock:
            for _, client in list(self._clients.values()):
                _safe_close_massive_client(client)
            self._clients.clear()


def _is_recoverable_massive_error(exc: BaseException) -> bool:
    if isinstance(exc, MassiveGatewayNotFoundError):
        return False

    if isinstance(exc, MassiveGatewayRateLimitError):
        return True

    if isinstance(exc, MassiveGatewayError):
        status_code = getattr(exc, "status_code", None)
        if status_code in {408, 429, 500, 502, 503, 504}:
            return True

        message = str(exc).strip().lower()
        transient_markers = (
            "timeout",
            "timed out",
            "connection",
            "server disconnected",
            "remoteprotocolerror",
            "readerror",
            "connecterror",
            "gateway unavailable",
        )
        return any(marker in message for marker in transient_markers)

    return False


def _process_symbol_with_recovery(
    symbol: str,
    client_manager: _ThreadLocalMassiveClientManager,
    *,
    backfill_start: Optional[date] = None,
    alpha26_mode: bool = False,
    alpha26_rows: Optional[dict[tuple[str, str], dict[str, Any]]] = None,
    alpha26_lock: Optional[threading.Lock] = None,
    max_attempts: int = _RECOVERY_MAX_ATTEMPTS,
    sleep_seconds: float = _RECOVERY_SLEEP_SECONDS,
) -> tuple[int, bool, list[tuple[str, BaseException]], dict[str, int]]:
    attempts = max(1, int(max_attempts))
    sleep_seconds = max(0.0, float(sleep_seconds))
    pending_reports = list(REPORTS)
    wrote = 0
    final_failures: list[tuple[str, BaseException]] = []
    coverage_summary = _empty_coverage_summary()

    for attempt in range(1, attempts + 1):
        next_pending: list[dict[str, str]] = []
        transient_failures: list[tuple[str, BaseException]] = []

        for report in pending_reports:
            report_name = str(report.get("report") or "unknown")
            try:
                massive_client = client_manager.get_client()
                call_kwargs: dict[str, Any] = {
                    "backfill_start": backfill_start,
                    "coverage_summary": coverage_summary,
                }
                if alpha26_mode:
                    alpha26_existing_row: Optional[dict[str, Any]] = None
                    if alpha26_rows is not None:
                        key = (str(symbol).strip().upper(), str(report_name).strip().lower())
                        if alpha26_lock is not None:
                            with alpha26_lock:
                                existing = alpha26_rows.get(key)
                        else:
                            existing = alpha26_rows.get(key)
                        if isinstance(existing, dict):
                            alpha26_existing_row = dict(existing)
                    call_kwargs.update(
                        {
                            "alpha26_mode": True,
                            "alpha26_existing_row": alpha26_existing_row,
                            "alpha26_rows": alpha26_rows,
                            "alpha26_lock": alpha26_lock,
                        }
                    )
                if fetch_and_save_raw(symbol, report, massive_client, **call_kwargs):
                    wrote += 1
            except MassiveGatewayNotFoundError as exc:
                return wrote, True, [(report_name, exc)], coverage_summary
            except BaseException as exc:
                if _is_recoverable_massive_error(exc) and attempt < attempts:
                    next_pending.append(report)
                    transient_failures.append((report_name, exc))
                else:
                    final_failures.append((report_name, exc))

        if not next_pending:
            return wrote, False, final_failures, coverage_summary

        report_labels = ",".join(sorted({name for name, _ in transient_failures})) or "unknown"
        mdc.write_warning(
            f"Transient Massive error for {symbol}; attempt {attempt}/{attempts} failed for report(s) "
            f"[{report_labels}]. Sleeping {sleep_seconds:.1f}s and retrying remaining reports."
        )
        client_manager.reset_current()
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        pending_reports = next_pending

    return wrote, False, final_failures, coverage_summary


async def main_async() -> int:
    mdc.log_environment_diagnostics()
    _validate_environment()

    list_manager.load()

    mdc.write_line("Fetching symbols...")
    df_symbols = mdc.get_symbols().dropna(subset=["Symbol"]).copy()

    symbols: list[str] = []
    for sym in df_symbols["Symbol"].astype(str).tolist():
        if "." in sym:
            continue
        if list_manager.is_blacklisted(sym):
            continue
        symbols.append(sym)
    symbols = list(dict.fromkeys(symbols))

    if hasattr(cfg, "DEBUG_SYMBOLS") and cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG: Restricting to {len(cfg.DEBUG_SYMBOLS)} symbols")
        symbols = [s for s in symbols if s in cfg.DEBUG_SYMBOLS]

    alpha26_mode = bronze_bucketing.is_alpha26_mode()
    if not alpha26_mode:
        raise RuntimeError("Bronze finance only supports alpha26 bucket mode.")

    symbol_set = {str(s).strip().upper() for s in symbols}
    alpha26_rows: dict[tuple[str, str], dict[str, Any]] = _load_alpha26_finance_row_map(symbols=symbol_set)
    alpha26_lock: Optional[threading.Lock] = threading.Lock()
    mdc.write_line(
        f"Loaded existing finance alpha26 seed rows: reports={len(alpha26_rows)} symbols={len(symbol_set)}."
    )

    mdc.write_line(f"Starting Massive Bronze Finance Ingestion for {len(symbols)} symbols...")

    client_manager = _ThreadLocalMassiveClientManager()
    backfill_start = resolve_backfill_start_date()
    if backfill_start is not None:
        mdc.write_line(f"Applying historical cutoff to bronze finance data: {backfill_start.isoformat()}")

    max_workers = max(
        1,
        int(
            getattr(
                cfg,
                "MASSIVE_MAX_WORKERS",
                32,
            )
        ),
    )
    executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="massive-finance")
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(max_workers)

    progress = {"processed": 0, "written": 0, "skipped": 0, "failed": 0, "blacklisted": 0}
    coverage_progress = _empty_coverage_summary()
    retry_next_run: set[str] = set()
    failure_counts: dict[str, int] = {}
    failure_examples: dict[str, str] = {}
    progress_lock = asyncio.Lock()

    def worker(symbol: str) -> tuple[int, bool, list[tuple[str, BaseException]], dict[str, int]]:
        return _process_symbol_with_recovery(
            symbol,
            client_manager,
            backfill_start=backfill_start,
            alpha26_mode=alpha26_mode,
            alpha26_rows=alpha26_rows if alpha26_mode else None,
            alpha26_lock=alpha26_lock if alpha26_mode else None,
        )

    async def record_failures(symbol: str, failures: list[tuple[str, BaseException]]) -> None:
        failure_reasons: list[str] = []
        async with progress_lock:
            progress["failed"] += 1
            retry_next_run.add(symbol)
            for report_name, exc in failures:
                failure_type = type(exc).__name__
                failure_reason = _format_failure_reason(exc)
                failure_reasons.append(f"report={report_name} {failure_reason}")
                failure_counts[failure_type] = failure_counts.get(failure_type, 0) + 1
                failure_examples.setdefault(
                    failure_type,
                    f"symbol={symbol} report={report_name} {failure_reason}",
                )
            failed_total = progress["failed"]

        # Sample detailed failures to avoid log flooding while still exposing root causes.
        if failed_total <= 20 or failed_total % 250 == 0:
            summary = " | ".join(failure_reasons[:4])
            mdc.write_warning(
                "Bronze finance symbol failure: symbol={symbol} total_failed={failed_total} details={summary}".format(
                    symbol=symbol,
                    failed_total=failed_total,
                    summary=summary,
                )
            )

    async def run_symbol(symbol: str) -> None:
        async with semaphore:
            try:
                wrote, blacklisted, failures, coverage_summary = await loop.run_in_executor(executor, worker, symbol)
                if blacklisted:
                    list_manager.add_to_blacklist(symbol)
                    should_log = False
                    async with progress_lock:
                        progress["blacklisted"] += 1
                        should_log = progress["blacklisted"] <= 20
                        for key in coverage_progress:
                            coverage_progress[key] += int(coverage_summary.get(key, 0) or 0)
                    if should_log:
                        report_name = failures[0][0] if failures else "unknown"
                        mdc.write_warning(
                            "Invalid finance payload for {symbol} report={report}; automatic blacklist updates "
                            "are disabled for job runs.".format(symbol=symbol, report=report_name)
                        )
                elif failures:
                    async with progress_lock:
                        for key in coverage_progress:
                            coverage_progress[key] += int(coverage_summary.get(key, 0) or 0)
                    await record_failures(symbol, failures)
                else:
                    async with progress_lock:
                        if wrote:
                            progress["written"] += 1
                        else:
                            progress["skipped"] += 1
                        for key in coverage_progress:
                            coverage_progress[key] += int(coverage_summary.get(key, 0) or 0)
            except Exception as exc:
                await record_failures(symbol, [("unknown", exc)])
            finally:
                async with progress_lock:
                    progress["processed"] += 1
                    if progress["processed"] % 250 == 0:
                        mdc.write_line(
                            "Bronze finance progress: processed={processed} written={written} skipped={skipped} "
                            "blacklisted={blacklisted} failed={failed}".format(**progress)
                        )

    try:
        await asyncio.gather(*(run_symbol(s) for s in symbols), return_exceptions=True)
    finally:
        try:
            executor.shutdown(wait=True, cancel_futures=False)
        except Exception:
            pass
        try:
            client_manager.close_all()
        except Exception:
            pass
        try:
            list_manager.flush()
        except Exception as exc:
            mdc.write_warning(f"Failed to flush whitelist/blacklist updates: {exc}")

    alpha26_column_count: Optional[int] = len(_BUCKET_COLUMNS)
    try:
        written_symbols, index_path, alpha26_column_count = _write_alpha26_finance_buckets(alpha26_rows)
        flat_deleted = _delete_flat_finance_symbol_blobs()
        mdc.write_line(
            "Bronze finance alpha26 buckets written: "
            f"symbols={written_symbols} index={index_path or 'n/a'} flat_deleted={flat_deleted}"
        )
    except Exception as exc:
        progress["failed"] += 1
        mdc.write_error(f"Bronze finance alpha26 bucket write failed: {exc}")

    if failure_counts:
        ordered = sorted(failure_counts.items(), key=lambda item: item[1], reverse=True)
        summary = ", ".join(f"{name}={count}" for name, count in ordered[:8])
        mdc.write_warning(f"Bronze finance failure summary: {summary}")
        for name, _ in ordered[:3]:
            example = failure_examples.get(name)
            if example:
                mdc.write_warning(f"Bronze finance failure example ({name}): {example}")
    if retry_next_run:
        preview = ", ".join(sorted(retry_next_run)[:50])
        suffix = " ..." if len(retry_next_run) > 50 else ""
        mdc.write_line(
            f"Retry-on-next-run candidates (not blacklisted): count={len(retry_next_run)} symbols={preview}{suffix}"
        )

    job_status, exit_code = resolve_job_run_status(
        failed_count=progress["failed"],
        warning_count=progress["blacklisted"],
    )
    mdc.write_line(
        "Bronze Massive finance ingest complete: processed={processed} written={written} skipped={skipped} "
        "blacklisted={blacklisted} failed={failed} coverage_checked={coverage_checked} "
        "coverage_forced_refetch={coverage_forced_refetch} coverage_marked_covered={coverage_marked_covered} "
        "coverage_marked_limited={coverage_marked_limited} coverage_skipped_limited_marker={coverage_skipped_limited_marker} "
        "job_status={job_status}".format(
            **progress,
            **coverage_progress,
            job_status=job_status,
        )
    )
    try:
        listed_blobs = bronze_client.list_blob_infos(name_starts_with="finance-data/")
        manifest = create_bronze_finance_manifest(
            producer_job_name="bronze-finance-job",
            listed_blobs=listed_blobs,
            metadata={
                "jobStatus": job_status,
                "processed": int(progress["processed"]),
                "written": int(progress["written"]),
                "skipped": int(progress["skipped"]),
                "blacklisted": int(progress["blacklisted"]),
                "failed": int(progress["failed"]),
                "column_count": alpha26_column_count,
                "provider": _COVERAGE_PROVIDER,
                "schema_version": _FINANCE_SCHEMA_VERSION,
            },
        )
        if manifest:
            mdc.write_line(
                "Bronze finance manifest published: runId={run_id} blobCount={blob_count} path={path}".format(
                    run_id=manifest.get("runId"),
                    blob_count=manifest.get("blobCount"),
                    path=manifest.get("manifestPath"),
                )
            )
    except Exception as exc:
        mdc.write_warning(f"Failed to publish bronze finance manifest: {exc}")
    return exit_code


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "bronze-finance-job"
    shared_lock_name = (os.environ.get("FINANCE_PIPELINE_SHARED_LOCK_NAME") or _DEFAULT_SHARED_FINANCE_LOCK).strip()
    shared_wait_timeout = _parse_wait_timeout_seconds(
        os.environ.get("BRONZE_FINANCE_SHARED_LOCK_WAIT_SECONDS"),
        default=0.0,
    )
    with mdc.JobLock(shared_lock_name, wait_timeout_seconds=shared_wait_timeout):
        with mdc.JobLock(job_name):
            ensure_api_awake_from_env(required=True)
            exit_code = main()
            if exit_code == 0:
                write_system_health_marker(layer="bronze", domain="finance", job_name=job_name)
                trigger_next_job_from_env()
            raise SystemExit(exit_code)
