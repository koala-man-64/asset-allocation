from __future__ import annotations

import csv
import io
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from core.blob_storage import BlobStorageClient
from core import delta_core
from deltalake import DeltaTable

logger = logging.getLogger("asset_allocation.monitoring.domain_metadata")


LayerKey = str
DomainKey = str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_key(value: str) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def _require_container(env_name: str) -> str:
    raw = os.environ.get(env_name)
    container = raw.strip() if raw else ""
    if not container:
        raise ValueError(f"Missing required environment variable: {env_name}")
    return container


def _layer_container_env(layer: LayerKey) -> str:
    layer_key = _normalize_key(layer)
    if layer_key == "bronze":
        return "AZURE_CONTAINER_BRONZE"
    if layer_key == "silver":
        return "AZURE_CONTAINER_SILVER"
    if layer_key == "gold":
        return "AZURE_CONTAINER_GOLD"
    if layer_key == "platinum":
        return "AZURE_CONTAINER_PLATINUM"
    raise ValueError(f"Unsupported layer: {layer}")


def _delta_table_path(layer: LayerKey, domain: DomainKey) -> Optional[str]:
    layer_key = _normalize_key(layer)
    domain_key = _normalize_key(domain)

    if layer_key == "silver":
        return {
            "market": "market-data-by-date",
            "finance": "finance-data-by-date",
            "earnings": "earnings-data-by-date",
            "price-target": "price-target-data-by-date",
        }.get(domain_key)

    if layer_key == "gold":
        return {
            "market": "market_by_date",
            "finance": "finance_by_date",
            "earnings": "earnings_by_date",
            "price-target": "targets_by_date",
        }.get(domain_key)

    return None


def _blob_prefix(layer: LayerKey, domain: DomainKey) -> Optional[str]:
    layer_key = _normalize_key(layer)
    domain_key = _normalize_key(domain)

    if layer_key == "bronze":
        if domain_key in {"market", "finance", "earnings"}:
            return f"{domain_key}-data/"
        if domain_key == "price-target":
            return "price-target-data/"
        if domain_key == "platinum":
            return "platinum/"
    if layer_key == "platinum":
        return "platinum/"
    return None


def _whitelist_path(domain: DomainKey) -> Optional[str]:
    domain_key = _normalize_key(domain)
    if domain_key in {"market", "finance", "earnings"}:
        return f"{domain_key}-data/whitelist.csv"
    if domain_key == "price-target":
        return "price-target-data/whitelist.csv"
    return None


def _ticker_listing_prefix(layer: LayerKey, domain: DomainKey) -> Optional[str]:
    layer_key = _normalize_key(layer)
    domain_key = _normalize_key(domain)

    if layer_key == "silver":
        if domain_key == "market":
            return "market-data/"
        if domain_key == "finance":
            return "finance-data/"
        if domain_key == "earnings":
            return "earnings-data/"
        if domain_key == "price-target":
            return "price-target-data/"

    if layer_key == "gold":
        if domain_key == "market":
            return "market/"
        if domain_key == "finance":
            return "finance/"
        if domain_key == "earnings":
            return "earnings/"
        if domain_key == "price-target":
            return "targets/"

    return None


def _extract_ticker_from_blob_name(layer: LayerKey, domain: DomainKey, blob_name: str) -> Optional[str]:
    layer_key = _normalize_key(layer)
    domain_key = _normalize_key(domain)
    parts = str(blob_name).strip("/").split("/")

    if layer_key == "silver" and domain_key == "market":
        # market-data/<ticker>/_delta_log/<file>
        if len(parts) >= 4 and parts[0] == "market-data" and parts[2] == "_delta_log":
            return parts[1].strip() or None
        return None

    if layer_key == "gold" and domain_key == "market":
        # market/<ticker>/_delta_log/<file>
        if len(parts) >= 4 and parts[0] == "market" and parts[2] == "_delta_log":
            return parts[1].strip() or None
        return None

    if layer_key == "silver" and domain_key == "earnings":
        # earnings-data/<ticker>/_delta_log/<file>
        if len(parts) >= 4 and parts[0] == "earnings-data" and parts[2] == "_delta_log":
            return parts[1].strip() or None
        return None

    if layer_key == "gold" and domain_key == "earnings":
        # earnings/<ticker>/_delta_log/<file>
        if len(parts) >= 4 and parts[0] == "earnings" and parts[2] == "_delta_log":
            return parts[1].strip() or None
        return None

    if layer_key == "silver" and domain_key == "price-target":
        # price-target-data/<ticker>/_delta_log/<file>
        if len(parts) >= 4 and parts[0] == "price-target-data" and parts[2] == "_delta_log":
            return parts[1].strip() or None
        return None

    if layer_key == "gold" and domain_key == "price-target":
        # targets/<ticker>/_delta_log/<file>
        if len(parts) >= 4 and parts[0] == "targets" and parts[2] == "_delta_log":
            return parts[1].strip() or None
        return None

    if layer_key == "gold" and domain_key == "finance":
        # finance/<ticker>/_delta_log/<file>
        if len(parts) >= 4 and parts[0] == "finance" and parts[2] == "_delta_log":
            return parts[1].strip() or None
        return None

    if layer_key == "silver" and domain_key == "finance":
        # finance-data/<folder>/<ticker>_<suffix>/_delta_log/<file>
        if len(parts) >= 5 and parts[0] == "finance-data" and parts[3] == "_delta_log":
            table_name = parts[2].strip()
            if "_" not in table_name:
                return None
            ticker = table_name.split("_", 1)[0].strip()
            return ticker or None
        return None

    return None


def _parse_whitelist_symbols(blob_bytes: Optional[bytes]) -> Optional[int]:
    if not blob_bytes:
        return None
    text = blob_bytes.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text))
    symbols: set[str] = set()
    for row in reader:
        if not row:
            continue
        raw = str(row[0]).strip()
        if not raw:
            continue
        lowered = raw.lower()
        if lowered in {"symbol", "ticker", "tickers"}:
            continue
        symbols.add(raw.replace(".", "-"))
    return len(symbols)


def _count_symbols_from_listing(
    client: BlobStorageClient,
    *,
    layer: LayerKey,
    domain: DomainKey,
    prefix: str,
    max_scanned_blobs: int,
) -> Tuple[Optional[int], bool]:
    tickers: set[str] = set()
    scanned = 0
    truncated = False

    try:
        blobs = client.container_client.list_blobs(name_starts_with=prefix)
        for blob in blobs:
            scanned += 1
            if scanned > max_scanned_blobs:
                truncated = True
                break
            ticker = _extract_ticker_from_blob_name(layer, domain, getattr(blob, "name", ""))
            if ticker:
                tickers.add(ticker)
    except Exception as exc:
        logger.warning("Failed to list blobs for symbol count: container=%s prefix=%s err=%s", client.container_name, prefix, exc)
        return None, False

    return len(tickers), truncated


def _summarize_blob_prefix(
    client: BlobStorageClient,
    *,
    prefix: str,
    max_scanned_blobs: int,
) -> Tuple[Optional[int], Optional[int], bool]:
    files = 0
    total_bytes = 0
    scanned = 0
    truncated = False

    try:
        blobs = client.container_client.list_blobs(name_starts_with=prefix)
        for blob in blobs:
            scanned += 1
            if scanned > max_scanned_blobs:
                truncated = True
                break
            files += 1
            size = getattr(blob, "size", None)
            if isinstance(size, int):
                total_bytes += size
    except Exception as exc:
        logger.warning("Failed to list blobs for prefix summary: container=%s prefix=%s err=%s", client.container_name, prefix, exc)
        return None, None, False

    return files, total_bytes, truncated


def _pick_date_column(rows: List[Dict[str, Any]]) -> Optional[str]:
    if not rows:
        return None
    sample = rows[0]
    candidates: List[str] = []
    for key, value in sample.items():
        if not key.startswith("min."):
            continue
        if isinstance(value, datetime):
            candidates.append(key[len("min.") :])

    if not candidates:
        return None

    # Prefer columns that look like date/time.
    date_like = [c for c in candidates if "date" in c.lower()]
    if date_like:
        date_like.sort(key=lambda c: (0 if c.lower() in {"date", "asofdate"} else 1, c.lower()))
        return date_like[0]
    candidates.sort(key=str.lower)
    return candidates[0]


def collect_delta_table_metadata(container: str, table_path: str) -> Dict[str, Any]:
    uri = delta_core.get_delta_table_uri(container, table_path)
    opts = delta_core.get_delta_storage_options(container)
    dt = DeltaTable(uri, storage_options=opts)

    version = int(dt.version())
    add_actions = dt.get_add_actions(flatten=True).to_struct_array().to_pylist()

    total_rows = 0
    total_bytes = 0
    for action in add_actions:
        num_records = action.get("num_records")
        if isinstance(num_records, int):
            total_rows += num_records
        size_bytes = action.get("size_bytes")
        if isinstance(size_bytes, int):
            total_bytes += size_bytes

    date_column = _pick_date_column(add_actions)
    min_dt: Optional[datetime] = None
    max_dt: Optional[datetime] = None
    if date_column:
        for action in add_actions:
            start = action.get(f"min.{date_column}")
            end = action.get(f"max.{date_column}")
            if isinstance(start, datetime):
                min_dt = start if min_dt is None or start < min_dt else min_dt
            if isinstance(end, datetime):
                max_dt = end if max_dt is None or end > max_dt else max_dt

    def _dt_iso(value: Optional[datetime]) -> Optional[str]:
        if not value:
            return None
        out = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return out.astimezone(timezone.utc).isoformat()

    return {
        "deltaVersion": version,
        "fileCount": len(add_actions),
        "totalBytes": total_bytes,
        "totalRows": total_rows,
        "dateRange": {
            "min": _dt_iso(min_dt),
            "max": _dt_iso(max_dt),
            "column": date_column,
        }
        if date_column
        else None,
    }


def collect_domain_metadata(*, layer: str, domain: str) -> Dict[str, Any]:
    layer_key = _normalize_key(layer)
    domain_key = _normalize_key(domain)

    container = _require_container(_layer_container_env(layer_key))
    computed_at = _utc_now_iso()
    max_scanned_blobs = int(os.environ.get("DOMAIN_METADATA_MAX_SCANNED_BLOBS", "200000"))

    delta_path = _delta_table_path(layer_key, domain_key)
    if delta_path:
        client = BlobStorageClient(container_name=container, ensure_container_exists=False)
        prefix = _ticker_listing_prefix(layer_key, domain_key)
        symbol_count = None
        symbol_truncated = False
        if prefix:
            symbol_count, symbol_truncated = _count_symbols_from_listing(
                client,
                layer=layer_key,
                domain=domain_key,
                prefix=prefix,
                max_scanned_blobs=max_scanned_blobs,
            )

        warnings: List[str] = []
        if symbol_truncated:
            warnings.append(f"Symbol discovery truncated after {max_scanned_blobs} blobs.")

        metrics = collect_delta_table_metadata(container, delta_path)
        return {
            "layer": layer_key,
            "domain": domain_key,
            "container": container,
            "type": "delta",
            "tablePath": delta_path,
            "computedAt": computed_at,
            "symbolCount": symbol_count,
            "warnings": warnings,
            **metrics,
        }

    prefix = _blob_prefix(layer_key, domain_key)
    if prefix:
        client = BlobStorageClient(container_name=container, ensure_container_exists=False)
        files, total_bytes, truncated = _summarize_blob_prefix(
            client, prefix=prefix, max_scanned_blobs=max_scanned_blobs
        )
        warnings: List[str] = []
        if truncated:
            warnings.append(f"Blob listing truncated after {max_scanned_blobs} blobs.")

        whitelist_path = _whitelist_path(domain_key)
        symbol_count = None
        if whitelist_path:
            try:
                symbol_count = _parse_whitelist_symbols(client.download_data(whitelist_path))
            except Exception as exc:
                warnings.append(f"Unable to read whitelist.csv: {exc}")

        return {
            "layer": layer_key,
            "domain": domain_key,
            "container": container,
            "type": "blob",
            "prefix": prefix,
            "computedAt": computed_at,
            "symbolCount": symbol_count,
            "fileCount": files,
            "totalBytes": total_bytes,
            "warnings": warnings,
        }

    raise ValueError(f"Unsupported layer/domain combination: layer={layer_key} domain={domain_key}")
