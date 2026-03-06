from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd

from core import delta_core
from tasks.common.silver_contracts import normalize_columns_to_snake_case


@dataclass(frozen=True)
class DeltaWriteDecision:
    action: Literal["write", "skip_empty_no_schema"]
    frame: pd.DataFrame
    reason: str
    existing_schema_columns: tuple[str, ...]


def _normalize_existing_schema_columns(existing_cols: tuple[str, ...]) -> list[str]:
    normalized_existing: list[str] = []
    seen: set[str] = set()
    for col in existing_cols:
        normalized = str(col).strip()
        if not normalized:
            continue
        normalized = normalize_columns_to_snake_case(pd.DataFrame(columns=[normalized])).columns[0]
        if normalized in seen:
            continue
        normalized_existing.append(normalized)
        seen.add(normalized)
    return normalized_existing


def _align_frame_to_existing_schema(df: pd.DataFrame, *, existing_cols: tuple[str, ...]) -> pd.DataFrame:
    out = normalize_columns_to_snake_case(df).reset_index(drop=True)
    normalized_existing = _normalize_existing_schema_columns(existing_cols)
    for col in normalized_existing:
        if col not in out.columns:
            out[col] = pd.NA

    if out.empty:
        ordered_cols = normalized_existing
    else:
        ordered_cols = normalized_existing + [col for col in out.columns if col not in normalized_existing]
    return out[ordered_cols].reset_index(drop=True)


def prepare_delta_write_frame(
    df: pd.DataFrame,
    *,
    container: str,
    path: str,
    skip_empty_without_schema: bool = True,
) -> DeltaWriteDecision:
    existing_schema_columns = tuple(delta_core.get_delta_schema_columns(container, path) or ())
    normalized = normalize_columns_to_snake_case(df).reset_index(drop=True)

    if existing_schema_columns:
        return DeltaWriteDecision(
            action="write",
            frame=_align_frame_to_existing_schema(normalized, existing_cols=existing_schema_columns),
            reason="aligned_to_existing_schema",
            existing_schema_columns=existing_schema_columns,
        )

    if normalized.empty and skip_empty_without_schema:
        return DeltaWriteDecision(
            action="skip_empty_no_schema",
            frame=normalized,
            reason="empty_bucket_no_existing_schema",
            existing_schema_columns=existing_schema_columns,
        )

    return DeltaWriteDecision(
        action="write",
        frame=normalized,
        reason="no_existing_schema",
        existing_schema_columns=existing_schema_columns,
    )
