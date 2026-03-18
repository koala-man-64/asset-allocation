from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

import pandas as pd

from core import core as mdc
from tasks.common import bronze_bucketing
from tasks.common import domain_artifacts
from tasks.common import run_manifests


@dataclass
class PublishResult:
    run_id: str
    data_prefix: str
    bucket_paths: list[dict[str, Any]]
    index_path: Optional[str]
    manifest_path: Optional[str]
    written_symbols: int
    total_bytes: int
    file_count: int


def _normalize_bucket_frames(
    *,
    bucket_frames: Dict[str, pd.DataFrame],
    bucket_columns: Iterable[str],
) -> dict[str, pd.DataFrame]:
    columns = [str(column) for column in bucket_columns]
    normalized: dict[str, pd.DataFrame] = {}
    for bucket in bronze_bucketing.ALPHABET_BUCKETS:
        frame = bucket_frames.get(bucket)
        if isinstance(frame, pd.DataFrame) and not frame.empty:
            normalized[bucket] = frame.copy()
        else:
            normalized[bucket] = pd.DataFrame(columns=columns)
    return normalized


def _aggregate_finance_subdomains(bucket_summaries: Iterable[dict[str, Any]]) -> Optional[dict[str, int]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for payload in bucket_summaries:
        if not isinstance(payload, dict):
            continue
        subdomains = payload.get("subdomains")
        if not isinstance(subdomains, dict):
            continue
        for key, value in subdomains.items():
            normalized_key = domain_artifacts.normalize_sub_domain(key)
            if normalized_key not in domain_artifacts.FINANCE_SUBDOMAINS or not isinstance(value, dict):
                continue
            grouped.setdefault(normalized_key, []).append(value)
    if not grouped:
        return None
    out: dict[str, int] = {}
    for key, payloads in grouped.items():
        summary = domain_artifacts.aggregate_summaries(payloads, date_column="date")
        out[key] = int(summary.get("symbolCount") or 0)
    return out or None


def publish_alpha26_bronze_domain(
    *,
    domain: str,
    root_prefix: str,
    bucket_frames: Dict[str, pd.DataFrame],
    bucket_columns: Iterable[str],
    date_column: Optional[str],
    symbol_to_bucket: Dict[str, str],
    storage_client: Any,
    job_name: str,
    run_id: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> PublishResult:
    normalized_domain = str(domain or "").strip().lower().replace("_", "-")
    normalized_root_prefix = str(root_prefix or "").strip().strip("/")
    normalized_run_id = str(run_id or "").strip()
    if not normalized_domain:
        raise ValueError("domain is required")
    if not normalized_root_prefix:
        raise ValueError("root_prefix is required")
    if not normalized_run_id:
        raise ValueError("run_id is required")

    run_prefix = f"{normalized_root_prefix}/runs/{normalized_run_id}"
    codec = bronze_bucketing.alpha26_codec()
    prepared_frames = _normalize_bucket_frames(bucket_frames=bucket_frames, bucket_columns=bucket_columns)
    bucket_paths: list[dict[str, Any]] = []
    bucket_summaries: list[dict[str, Any]] = []
    total_bytes = 0

    mdc.write_line(
        f"Bronze {normalized_domain} commit started: run_id={normalized_run_id} data_prefix={run_prefix}"
    )
    for bucket in bronze_bucketing.ALPHABET_BUCKETS:
        frame = prepared_frames[bucket]
        payload = frame.to_parquet(index=False, compression=codec)
        path = bronze_bucketing.bucket_blob_path(run_prefix, bucket)
        mdc.write_line(
            f"Bronze {normalized_domain} bucket write started: run_id={normalized_run_id} bucket={bucket} rows={len(frame)}"
        )
        mdc.store_raw_bytes(payload, path, client=storage_client)
        entry = {
            "bucket": bucket,
            "name": path,
            "size": len(payload),
        }
        bucket_paths.append(entry)
        total_bytes += len(payload)
        bucket_summaries.append(
            domain_artifacts.summarize_frame(frame, domain=normalized_domain, date_column=date_column)
        )
        mdc.write_line(
            "Bronze {domain} bucket write completed: run_id={run_id} bucket={bucket} rows={rows} bytes={bytes}".format(
                domain=normalized_domain,
                run_id=normalized_run_id,
                bucket=bucket,
                rows=len(frame),
                bytes=len(payload),
            )
        )

    index_path = bronze_bucketing.write_symbol_index(
        domain=normalized_domain,
        symbol_to_bucket=symbol_to_bucket,
    )
    aggregate_summary = domain_artifacts.aggregate_summaries(
        bucket_summaries,
        symbol_count_override=len(symbol_to_bucket),
        date_column=date_column,
    )
    manifest_metadata = {
        **dict(metadata or {}),
        **aggregate_summary,
        "fileCount": len(bucket_paths),
        "totalBytes": total_bytes,
    }
    finance_subfolder_counts = _aggregate_finance_subdomains(bucket_summaries)
    if finance_subfolder_counts:
        manifest_metadata["financeSubfolderSymbolCounts"] = finance_subfolder_counts

    manifest_result = run_manifests.create_bronze_alpha26_manifest(
        domain=normalized_domain,
        producer_job_name=job_name,
        data_prefix=run_prefix,
        bucket_paths=bucket_paths,
        index_path=index_path,
        metadata=manifest_metadata,
        run_id=normalized_run_id,
    )
    manifest_path = str((manifest_result or {}).get("manifestPath") or "").strip() or None

    for entry in bucket_paths:
        bucket = str(entry.get("bucket") or "").strip().upper()
        if not bucket:
            continue
        domain_artifacts.write_bucket_artifact(
            layer="bronze",
            domain=normalized_domain,
            bucket=bucket,
            df=prepared_frames[bucket],
            date_column=date_column,
            client=storage_client,
            job_name=job_name,
            job_run_id=normalized_run_id,
            run_id=normalized_run_id,
            manifest_path=manifest_path,
            active_data_prefix=run_prefix,
            data_path=str(entry.get("name") or "").strip() or None,
        )

    domain_artifacts.write_domain_artifact(
        layer="bronze",
        domain=normalized_domain,
        date_column=date_column,
        client=storage_client,
        symbol_count_override=len(symbol_to_bucket),
        symbol_index_path=index_path,
        job_name=job_name,
        job_run_id=normalized_run_id,
        run_id=normalized_run_id,
        manifest_path=manifest_path,
        active_data_prefix=run_prefix,
        total_bytes_override=total_bytes,
        file_count_override=len(bucket_paths),
    )
    mdc.write_line(
        "Bronze {domain} commit completed: run_id={run_id} data_prefix={prefix} manifest_path={manifest_path} "
        "written_symbols={written_symbols} index_path={index_path}".format(
            domain=normalized_domain,
            run_id=normalized_run_id,
            prefix=run_prefix,
            manifest_path=manifest_path or "n/a",
            written_symbols=len(symbol_to_bucket),
            index_path=index_path or "n/a",
        )
    )
    return PublishResult(
        run_id=normalized_run_id,
        data_prefix=run_prefix,
        bucket_paths=bucket_paths,
        index_path=index_path,
        manifest_path=manifest_path,
        written_symbols=len(symbol_to_bucket),
        total_bytes=total_bytes,
        file_count=len(bucket_paths),
    )
