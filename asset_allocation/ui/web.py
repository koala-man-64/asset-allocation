import io
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

try:
    from scripts.common.blob_storage import BlobStorageClient
    from scripts.common.delta_core import load_delta
except ModuleNotFoundError as exc:
    if exc.name != "scripts":
        raise
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from scripts.common.blob_storage import BlobStorageClient
    from scripts.common.delta_core import load_delta

logger = logging.getLogger("asset_allocation.ui")
logging.basicConfig(level=os.environ.get("UI_LOG_LEVEL", "INFO"))

DEFAULT_MAX_ROWS = 1000
DEFAULT_MAX_CSV_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_PARQUET_BYTES = 32 * 1024 * 1024
ALLOWED_EXTENSIONS = {".csv", ".parquet"}


def _get_int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid int for %s=%s. Using default %s.", name, value, default)
        return default


def _get_container_allowlist() -> List[str]:
    allowlist_env = os.environ.get("UI_CONTAINER_ALLOWLIST", "").strip()
    containers: List[str] = []
    if allowlist_env:
        containers = [c.strip() for c in allowlist_env.split(",") if c.strip()]
    else:
        env_names = [
            "AZURE_CONTAINER_MARKET",
            "AZURE_CONTAINER_FINANCE",
            "AZURE_CONTAINER_EARNINGS",
            "AZURE_CONTAINER_TARGETS",
            "AZURE_CONTAINER_RANKING",
            "AZURE_CONTAINER_COMMON",
        ]
        for name in env_names:
            value = os.environ.get(name)
            if value:
                containers.append(value)
    seen = set()
    ordered = []
    for container in containers:
        if container not in seen:
            ordered.append(container)
            seen.add(container)
    return ordered


def _assert_allowed_container(container: str, allowlist: List[str]) -> None:
    if container not in allowlist:
        raise HTTPException(status_code=404, detail="Container not found.")


@dataclass
class BlobInfo:
    name: str
    last_modified: Optional[datetime]
    size: Optional[int]


@dataclass
class PreviewResult:
    df: pd.DataFrame
    bytes_read: int
    truncated: bool
    file_size: Optional[int]
    last_modified: Optional[datetime]
    file_format: str


def _get_storage_client(container: str) -> BlobStorageClient:
    return BlobStorageClient(container_name=container, ensure_container_exists=False)


def _list_candidate_blobs(client: BlobStorageClient) -> List[BlobInfo]:
    infos = []
    for blob in client.list_blob_infos():
        name = blob.get("name")
        if not name or name.endswith("/"):
            continue
        suffix = Path(name).suffix.lower()
        if suffix in ALLOWED_EXTENSIONS:
            infos.append(
                BlobInfo(
                    name=name,
                    last_modified=blob.get("last_modified"),
                    size=blob.get("size"),
                )
            )
    infos.sort(key=lambda item: item.last_modified or datetime.min, reverse=True)
    return infos


def _read_csv_preview(
    blob_client,
    max_rows: int,
    max_bytes: int,
) -> Tuple[pd.DataFrame, int, bool, Optional[int]]:
    props = blob_client.get_blob_properties()
    size = props.size if props else None
    read_size = size if size is not None else max_bytes
    if size is not None:
        read_size = min(size, max_bytes)
    data = blob_client.download_blob(offset=0, length=read_size).readall()
    bytes_read = len(data)
    try:
        df = pd.read_csv(io.BytesIO(data), nrows=max_rows)
    except Exception as exc:
        logger.warning("CSV preview failed with partial read: %s", exc)
        if size is not None and size > max_bytes:
            raise HTTPException(
                status_code=413,
                detail="CSV too large for preview. Increase UI_MAX_CSV_BYTES.",
            )
        data = blob_client.download_blob().readall()
        bytes_read = len(data)
        df = pd.read_csv(io.BytesIO(data), nrows=max_rows)
    truncated = size is not None and bytes_read < size
    return df, bytes_read, truncated, size


def _read_parquet_preview(
    blob_client,
    max_rows: int,
    max_bytes: int,
) -> Tuple[pd.DataFrame, int, bool, Optional[int]]:
    props = blob_client.get_blob_properties()
    size = props.size if props else None
    if size is not None and size > max_bytes:
        raise HTTPException(
            status_code=413,
            detail="Parquet too large for preview. Increase UI_MAX_PARQUET_BYTES.",
        )
    data = blob_client.download_blob().readall()
    bytes_read = len(data)
    df = pd.read_parquet(io.BytesIO(data))
    truncated = len(df) > max_rows
    return df.head(max_rows), bytes_read, truncated, size


def _load_preview(
    container: str,
    blob_name: str,
    max_rows: int,
    max_csv_bytes: int,
    max_parquet_bytes: int,
) -> PreviewResult:
    client = _get_storage_client(container)
    blob_client = client.container_client.get_blob_client(blob_name)
    props = blob_client.get_blob_properties()
    suffix = Path(blob_name).suffix.lower()
    if suffix == ".csv":
        df, bytes_read, truncated, size = _read_csv_preview(
            blob_client, max_rows=max_rows, max_bytes=max_csv_bytes
        )
        file_format = "csv"
    elif suffix == ".parquet":
        df, bytes_read, truncated, size = _read_parquet_preview(
            blob_client, max_rows=max_rows, max_bytes=max_parquet_bytes
        )
        file_format = "parquet"
    else:
        raise HTTPException(status_code=400, detail="Unsupported file type.")
    return PreviewResult(
        df=df,
        bytes_read=bytes_read,
        truncated=truncated,
        file_size=size if size is not None else (props.size if props else None),
        last_modified=props.last_modified if props else None,
        file_format=file_format,
    )


def _infer_column_metadata(df: pd.DataFrame) -> List[dict]:
    metadata = []
    for column in df.columns:
        dtype = df[column].dtype
        kind = getattr(dtype, "kind", "")
        if kind in {"i", "u", "f"}:
            column_type = "numeric"
        elif kind == "M":
            column_type = "datetime"
        else:
            column_type = "string"
        metadata.append({"name": str(column), "type": column_type})
    return metadata


app = FastAPI(title="Asset Allocation Data Preview", version="1.0.0")
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/rankings", response_class=HTMLResponse)
def rankings_explorer(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("rankings.html", {"request": request})


@app.get("/healthz")
def healthz() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/readyz")
def readyz() -> JSONResponse:
    containers = _get_container_allowlist()
    if not containers:
        raise HTTPException(status_code=503, detail="No containers configured.")
    return JSONResponse({"status": "ok", "containers": len(containers)})


@app.get("/api/containers")
def list_containers() -> JSONResponse:
    containers = _get_container_allowlist()
    return JSONResponse({"containers": containers})


def _get_ranking_container() -> str:
    container = os.environ.get("AZURE_CONTAINER_RANKING", "").strip()
    if not container:
        raise HTTPException(status_code=503, detail="Missing AZURE_CONTAINER_RANKING.")
    return container


def _get_strategy_names(year_month: str) -> List[str]:
    container = _get_ranking_container()
    strategies_df = load_delta(
        container,
        "gold/ranking_signals",
        columns=["strategy"],
        filters=[("year_month", "=", year_month)],
    )
    if strategies_df is None or strategies_df.empty or "strategy" not in strategies_df.columns:
        return []
    return sorted(strategies_df["strategy"].dropna().astype(str).unique().tolist())


@app.get("/api/rankings/strategies")
def rankings_strategies(date: Optional[str] = None) -> JSONResponse:
    target = datetime.utcnow()
    if date:
        try:
            target = pd.to_datetime(date).to_pydatetime()
        except Exception as exc:
            raise HTTPException(
                status_code=400, detail=f"Invalid date '{date}'. Expected YYYY-MM-DD."
            ) from exc
    year_month = target.strftime("%Y-%m")
    return JSONResponse({"year_month": year_month, "strategies": _get_strategy_names(year_month)})


@app.get("/api/rankings/snapshot")
def rankings_snapshot(
    date: str,
    top_n: int = Query(50, ge=1, le=500),
    strategies: Optional[List[str]] = Query(None),
) -> JSONResponse:
    """
    Returns top-N symbols for a given date with composite and per-strategy ranks.
    Reads from Delta tables in the ranking container:
    - gold/composite_signals
    - gold/ranking_signals
    """

    try:
        target_day = pd.to_datetime(date).normalize()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date '{date}'. Expected YYYY-MM-DD.") from exc

    year_month = target_day.strftime("%Y-%m")
    day_start = target_day
    day_end = target_day + pd.Timedelta(days=1)

    selected_strategies = [s for s in (strategies or []) if s]

    container = _get_ranking_container()

    composite = load_delta(
        container,
        "gold/composite_signals",
        filters=[
            ("year_month", "=", year_month),
            ("date", ">=", day_start.to_pydatetime()),
            ("date", "<", day_end.to_pydatetime()),
        ],
    )
    if composite is None or composite.empty:
        raise HTTPException(status_code=404, detail=f"No composite rankings found for {target_day.date()}.")

    composite = composite.sort_values("composite_rank", ascending=True)
    composite = composite.head(top_n).copy()
    symbols = composite["symbol"].astype(str).tolist()

    signals = load_delta(
        container,
        "gold/ranking_signals",
        filters=[
            ("year_month", "=", year_month),
            ("date", ">=", day_start.to_pydatetime()),
            ("date", "<", day_end.to_pydatetime()),
        ],
        columns=["symbol", "strategy", "rank"],
    )
    if signals is None:
        signals = pd.DataFrame()

    if not signals.empty:
        signals["symbol"] = signals["symbol"].astype(str)
        signals["strategy"] = signals["strategy"].astype(str)
        signals["rank"] = pd.to_numeric(signals["rank"], errors="coerce")
        signals = signals.dropna(subset=["rank"])
        signals["rank"] = signals["rank"].astype(int)
        if not selected_strategies:
            selected_strategies = sorted(signals["strategy"].dropna().unique().tolist())
        signals = signals[signals["symbol"].isin(symbols) & signals["strategy"].isin(selected_strategies)]
    else:
        if not selected_strategies:
            selected_strategies = _get_strategy_names(year_month)

    pivot = (
        signals.pivot(index="symbol", columns="strategy", values="rank")
        if not signals.empty
        else pd.DataFrame(index=pd.Index(symbols, name="symbol"))
    )

    rows = []
    for _, row in composite.iterrows():
        symbol = str(row["symbol"])
        ranks = {strategy: None for strategy in selected_strategies}
        if symbol in pivot.index:
            for strategy in selected_strategies:
                value = pivot.at[symbol, strategy] if strategy in pivot.columns else None
                ranks[strategy] = None if pd.isna(value) else int(value)

        rows.append(
            {
                "symbol": symbol,
                "composite_rank": int(row["composite_rank"]),
                "composite_percentile": float(row["composite_percentile"]),
                "strategies_present": int(row.get("strategies_present", 0)),
                "strategies_hit": int(row.get("strategies_hit", 0)),
                "ranks": ranks,
            }
        )

    return JSONResponse(
        {
            "date": target_day.date().isoformat(),
            "year_month": year_month,
            "top_n": top_n,
            "strategies": selected_strategies,
            "rows": rows,
        }
    )


@app.get("/api/files")
def list_files(container: str) -> JSONResponse:
    allowlist = _get_container_allowlist()
    _assert_allowed_container(container, allowlist)
    client = _get_storage_client(container)
    blobs = _list_candidate_blobs(client)
    latest = blobs[0].name if blobs else None
    return JSONResponse({"files": [b.name for b in blobs], "latest": latest})


@app.get("/api/preview")
def preview(
    container: str,
    blob: Optional[str] = None,
    max_rows: int = Query(DEFAULT_MAX_ROWS, ge=1, le=DEFAULT_MAX_ROWS),
) -> JSONResponse:
    allowlist = _get_container_allowlist()
    _assert_allowed_container(container, allowlist)
    client = _get_storage_client(container)
    blobs = _list_candidate_blobs(client)
    if not blobs:
        raise HTTPException(status_code=404, detail="No supported files found.")

    blob_names = {b.name for b in blobs}
    selected_blob = blob or blobs[0].name
    if selected_blob not in blob_names:
        raise HTTPException(status_code=404, detail="Blob not found.")

    max_csv_bytes = _get_int_env("UI_MAX_CSV_BYTES", DEFAULT_MAX_CSV_BYTES)
    max_parquet_bytes = _get_int_env("UI_MAX_PARQUET_BYTES", DEFAULT_MAX_PARQUET_BYTES)
    result = _load_preview(
        container=container,
        blob_name=selected_blob,
        max_rows=max_rows,
        max_csv_bytes=max_csv_bytes,
        max_parquet_bytes=max_parquet_bytes,
    )

    column_metadata = _infer_column_metadata(result.df)

    preview_df = result.df.head(max_rows)
    preview_df = preview_df.fillna("")
    rows = preview_df.astype(str).values.tolist()
    columns = [str(col) for col in preview_df.columns.tolist()]

    return JSONResponse(
        {
            "container": container,
            "blob": selected_blob,
            "columns": columns,
            "rows": rows,
            "preview_rows": len(rows),
        "bytes_read": result.bytes_read,
        "truncated": result.truncated,
        "file_size": result.file_size,
        "last_modified": result.last_modified.isoformat()
        if result.last_modified
        else None,
        "format": result.file_format,
        "column_metadata": column_metadata,
    }
    )
