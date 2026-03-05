from __future__ import annotations

import os
from datetime import datetime, timezone
from io import BytesIO
from typing import Optional

import pandas as pd

from core import core as mdc
from tasks.common import bronze_bucketing


ALPHABET_BUCKETS: tuple[str, ...] = bronze_bucketing.ALPHABET_BUCKETS


def _is_truthy(raw: Optional[str]) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def silver_layout_mode() -> str:
    mode = (os.environ.get("SILVER_LAYOUT_MODE")).strip().lower()
    if mode != "alpha26":
        raise ValueError("SILVER_LAYOUT_MODE must be 'alpha26'.")
    return mode


def is_silver_alpha26_mode() -> bool:
    silver_layout_mode()
    return True


def silver_alpha26_force_rebuild() -> bool:
    raw = os.environ.get("SILVER_ALPHA26_FORCE_REBUILD")
    if raw is None:
        return True
    return _is_truthy(raw)


def gold_layout_mode() -> str:
    mode = (os.environ.get("GOLD_LAYOUT_MODE")).strip().lower()
    if mode != "alpha26":
        raise ValueError("GOLD_LAYOUT_MODE must be 'alpha26'.")
    return mode


def is_gold_alpha26_mode() -> bool:
    gold_layout_mode()
    return True


def gold_alpha26_force_rebuild() -> bool:
    raw = os.environ.get("GOLD_ALPHA26_FORCE_REBUILD")
    if raw is None:
        return True
    return _is_truthy(raw)


def bucket_letter(symbol: str) -> str:
    return bronze_bucketing.bucket_letter(symbol)


def silver_bucket_path(*, domain: str, bucket: str, finance_sub_domain: Optional[str] = None) -> str:
    b = str(bucket or "").strip().upper()
    d = str(domain or "").strip().lower().replace("_", "-")
    if b not in ALPHABET_BUCKETS:
        raise ValueError(f"Invalid bucket {bucket!r}.")
    if d == "market":
        return f"market-data/buckets/{b}"
    if d == "earnings":
        return "earnings-data/buckets/{bucket}".format(bucket=b)
    if d == "price-target":
        return f"price-target-data/buckets/{b}"
    if d == "finance":
        sub = str(finance_sub_domain or "").strip().lower().replace("-", "_")
        if not sub:
            raise ValueError("finance_sub_domain is required for silver finance buckets.")
        return f"finance-data/{sub}/buckets/{b}"
    raise ValueError(f"Unsupported silver bucket domain={domain!r}")


def gold_bucket_path(*, domain: str, bucket: str) -> str:
    b = str(bucket or "").strip().upper()
    d = str(domain or "").strip().lower().replace("_", "-")
    if b not in ALPHABET_BUCKETS:
        raise ValueError(f"Invalid bucket {bucket!r}.")
    if d == "market":
        return f"market/buckets/{b}"
    if d == "earnings":
        return f"earnings/buckets/{b}"
    if d == "price-target":
        return f"targets/buckets/{b}"
    if d == "finance":
        return f"finance/buckets/{b}"
    raise ValueError(f"Unsupported gold bucket domain={domain!r}")


def all_silver_bucket_paths(*, domain: str, finance_sub_domain: Optional[str] = None) -> list[str]:
    return [silver_bucket_path(domain=domain, bucket=b, finance_sub_domain=finance_sub_domain) for b in ALPHABET_BUCKETS]


def all_gold_bucket_paths(*, domain: str) -> list[str]:
    return [gold_bucket_path(domain=domain, bucket=b) for b in ALPHABET_BUCKETS]


def _index_path(*, layer: str, domain: str) -> str:
    clean_layer = str(layer or "").strip().lower()
    clean_domain = str(domain or "").strip().lower().replace("_", "-")
    return f"system/{clean_layer}-index/{clean_domain}/latest.parquet"


def write_layer_symbol_index(
    *,
    layer: str,
    domain: str,
    symbol_to_bucket: dict[str, str],
    sub_domain: Optional[str] = None,
    updated_at: Optional[datetime] = None,
) -> Optional[str]:
    if getattr(mdc, "common_storage_client", None) is None:
        return None
    ts = updated_at or datetime.now(timezone.utc)
    rows: list[dict[str, str]] = []
    clean_sub_domain = str(sub_domain or "").strip().lower().replace("-", "_")
    for symbol, bucket in sorted(symbol_to_bucket.items()):
        row = {
            "symbol": str(symbol).strip().upper(),
            "bucket": str(bucket).strip().upper(),
            "updated_at": ts.isoformat(),
        }
        if clean_sub_domain:
            row["sub_domain"] = clean_sub_domain
        rows.append(row)
    cols = ["symbol", "bucket", "updated_at", "sub_domain"]
    df = pd.DataFrame(rows, columns=cols)
    payload = df.to_parquet(index=False, compression=bronze_bucketing.alpha26_codec())
    path = _index_path(layer=layer, domain=domain)
    mdc.store_raw_bytes(payload, path, client=mdc.common_storage_client)
    return path


def load_layer_symbol_index(*, layer: str, domain: str) -> pd.DataFrame:
    path = _index_path(layer=layer, domain=domain)
    if getattr(mdc, "common_storage_client", None) is None:
        return pd.DataFrame(columns=["symbol", "bucket", "updated_at", "sub_domain"])
    raw = mdc.read_raw_bytes(path, client=mdc.common_storage_client)
    if not raw:
        return pd.DataFrame(columns=["symbol", "bucket", "updated_at", "sub_domain"])
    try:
        df = pd.read_parquet(BytesIO(raw))
    except Exception:
        return pd.DataFrame(columns=["symbol", "bucket", "updated_at", "sub_domain"])
    expected = ["symbol", "bucket", "updated_at", "sub_domain"]
    for col in expected:
        if col not in df.columns:
            df[col] = pd.NA
    return df[expected]


def load_layer_symbol_set(*, layer: str, domain: str, sub_domain: Optional[str] = None) -> set[str]:
    df = load_layer_symbol_index(layer=layer, domain=domain)
    if df.empty:
        return set()
    clean_sub_domain = str(sub_domain or "").strip().lower().replace("-", "_")
    if clean_sub_domain and "sub_domain" in df.columns:
        df = df[df["sub_domain"].astype(str).str.lower() == clean_sub_domain]
    return {
        str(value).strip().upper()
        for value in df["symbol"].dropna().astype(str).tolist()
        if str(value).strip()
    }
