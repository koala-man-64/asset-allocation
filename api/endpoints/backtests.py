import io
import json
import logging
import math
from dataclasses import replace
from typing import Any, Dict, Optional

import pandas as pd
import yaml
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from backtest.config import BacktestConfig, generate_run_id, validate_config_dict_strict
from api.service.adls_run_store import AdlsRunStore
from api.service.artifacts import (
    download_remote_artifact,
    list_local_artifacts,
    list_remote_artifacts,
)
from api.service.dependencies import (
    get_manager,
    get_settings,
    get_store,
    validate_auth,
)
from api.service.schemas import (
    ArtifactInfoResponse,
    ArtifactListResponse,
    BacktestSubmitRequest,
    BacktestSubmitResponse,
    RollingMetricsResponse,
    RunListResponse,
    RunRecordResponse,
    TimeseriesResponse,
    TradeListResponse,
)
from api.service.security import (
    assert_allowed_container,
    assert_path_under_allowlist,
    parse_container_and_path,
    resolve_under_base,
    validate_run_id,
)
from api.service.settings import ServiceSettings

logger = logging.getLogger("backtest.api.backtests")

router = APIRouter()


def _load_config_from_request(payload: BacktestSubmitRequest) -> Dict[str, Any]:
    if payload.config is not None:
        return dict(payload.config)
    raw = payload.config_yaml or ""
    data = yaml.safe_load(raw) or {}
    if not isinstance(data, dict):
        raise ValueError("config_yaml must parse to a YAML object.")
    return data


def _validate_config_for_service(
    cfg: BacktestConfig, cfg_dict: Dict[str, Any], settings: ServiceSettings
) -> None:
    if cfg.data is None:
        raise ValueError("data section is required.")

    # Enforce secure defaults for local paths in service mode.
    if cfg.data.price_source == "local":
        if not settings.allow_local_data:
            raise ValueError(
                "data.price_source=local is disabled in the service (set BACKTEST_ALLOW_LOCAL_DATA=true)."
            )
        if cfg.data.price_path:
            assert_path_under_allowlist(cfg.data.price_path, settings.allowed_local_data_dirs)
        if cfg.data.signal_path:
            assert_path_under_allowlist(cfg.data.signal_path, settings.allowed_local_data_dirs)

    # ADLS container allowlist (prices/signals).
    if cfg.data.price_source == "ADLS":
        if cfg.data.price_path:
            container, _ = parse_container_and_path(cfg.data.price_path)
            assert_allowed_container(container, settings.adls_container_allowlist)
        if cfg.data.signal_path:
            container, _ = parse_container_and_path(cfg.data.signal_path)
            assert_allowed_container(container, settings.adls_container_allowlist)

    # ADLS container allowlist (artifact uploads).
    if cfg.output.adls_dir:
        container, _ = parse_container_and_path(cfg.output.adls_dir)
        assert_allowed_container(container, settings.adls_container_allowlist)


def _prefer_adls_reads(request: Request) -> bool:
    settings = get_settings(request)
    return settings.run_store_mode == "adls"


def _get_run_record(request: Request, run_id: str):
    store = get_store(request)
    try:
        return store.get_run(run_id)
    except KeyError:
        return None


def _download_run_file(record, name: str) -> Optional[bytes]:
    if not record or not record.adls_container or not record.adls_prefix:
        return None
    remote_path = f"{record.adls_prefix.rstrip('/')}/{name}"
    return download_remote_artifact(
        container=record.adls_container, remote_path=remote_path
    )


@router.post("", response_model=BacktestSubmitResponse)
def submit_backtest(
    payload: BacktestSubmitRequest,
    request: Request,
) -> BacktestSubmitResponse:
    settings = get_settings(request)
    validate_auth(request)

    try:
        config_dict = _load_config_from_request(payload)
        if payload.strict:
            validate_config_dict_strict(config_dict)
        cfg = BacktestConfig.from_dict(config_dict)
        _validate_config_for_service(cfg, config_dict, settings)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    resolved_run_id = validate_run_id(payload.run_id) if payload.run_id else generate_run_id()
    resolved_run_id = validate_run_id(resolved_run_id)

    resolved_adls_dir = cfg.output.adls_dir or settings.adls_runs_dir
    resolved_adls_container: Optional[str] = None
    resolved_adls_prefix: Optional[str] = None
    if resolved_adls_dir:
        container, prefix = parse_container_and_path(str(resolved_adls_dir))
        assert_allowed_container(container, settings.adls_container_allowlist)
        resolved_adls_container = container
        resolved_adls_prefix = f"{prefix.rstrip('/')}/{resolved_run_id}".strip("/")

    effective_cfg = replace(
        cfg, output=replace(cfg.output, local_dir=str(settings.output_base_dir))
    )
    output_dir = str((settings.output_base_dir / resolved_run_id).resolve(strict=False))
    store = get_store(request)
    manager = get_manager(request)

    try:
        store.create_run(
            run_id=resolved_run_id,
            status="queued",
            run_name=cfg.run_name,
            start_date=cfg.start_date.isoformat(),
            end_date=cfg.end_date.isoformat(),
            output_dir=output_dir,
            config_json=json.dumps(config_dict, sort_keys=True),
            effective_config_json=effective_cfg.canonical_json(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    if resolved_adls_container and resolved_adls_prefix:
        try:
            store.update_run(
                resolved_run_id,
                adls_container=resolved_adls_container,
                adls_prefix=resolved_adls_prefix,
            )
        except Exception:
            logger.exception(
                "Failed to persist ADLS run location: run_id=%s", resolved_run_id
            )

    manager.submit(run_id=resolved_run_id, config=cfg)
    return BacktestSubmitResponse(run_id=resolved_run_id, status="queued")


@router.get("", response_model=RunListResponse)
def list_backtests(
    request: Request,
    status: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> RunListResponse:
    validate_auth(request)
    store = get_store(request)

    if status is not None and status not in {"queued", "running", "completed", "failed"}:
        raise HTTPException(status_code=400, detail="Invalid status filter.")
    status_value = status
    runs = store.list_runs(limit=limit, offset=offset, status=status_value, query=q)
    return RunListResponse(
        runs=[RunRecordResponse(**r.to_public_dict()) for r in runs],
        limit=limit,
        offset=offset,
    )


@router.get("/{run_id}/status", response_model=RunRecordResponse)
def get_status(
    run_id: str,
    request: Request,
) -> RunRecordResponse:
    validate_auth(request)
    run_id = validate_run_id(run_id)

    store = get_store(request)
    try:
        record = store.get_run(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Run not found.") from exc
    return RunRecordResponse(**record.to_public_dict())


@router.get("/{run_id}/summary")
def get_summary(
    run_id: str,
    request: Request,
    source: str = Query("auto"),
) -> JSONResponse:
    settings = get_settings(request)
    validate_auth(request)
    run_id = validate_run_id(run_id)

    if source not in {"auto", "local", "adls"}:
        raise HTTPException(status_code=400, detail="Invalid source (use auto|local|adls).")

    order = ["local", "adls"]
    if _prefer_adls_reads(request):
        order = ["adls", "local"]
    if source != "auto":
        order = [source]

    record = _get_run_record(request, run_id) if "adls" in order else None

    errors: list[str] = []
    for attempt in order:
        if attempt == "local":
            run_dir = settings.output_base_dir / run_id
            summary_path = resolve_under_base(run_dir, run_dir / "summary.json")
            if not summary_path.exists():
                errors.append("summary.json not found locally")
                continue
            try:
                data = json.loads(summary_path.read_text(encoding="utf-8"))
            except Exception as exc:
                raise HTTPException(
                    status_code=500, detail="Failed to read local summary."
                ) from exc
            return JSONResponse(data)

        remote = _download_run_file(record, "summary.json")
        if remote is None:
            errors.append("summary.json not found in ADLS")
            continue
        try:
            data = json.loads(remote.decode("utf-8"))
        except Exception as exc:
            raise HTTPException(
                status_code=500, detail="Failed to read ADLS summary."
            ) from exc
        return JSONResponse(data)

    raise HTTPException(status_code=404, detail="summary.json not found.")


@router.get("/{run_id}/metrics/timeseries", response_model=TimeseriesResponse)
def get_timeseries(
    run_id: str,
    request: Request,
    source: str = Query("auto"),
    max_points: int = Query(5000, ge=50, le=200000),
) -> TimeseriesResponse:
    settings = get_settings(request)
    validate_auth(request)
    run_id = validate_run_id(run_id)

    if source not in {"auto", "local", "adls"}:
        raise HTTPException(status_code=400, detail="Invalid source (use auto|local|adls).")

    order = ["local", "adls"]
    if _prefer_adls_reads(request):
        order = ["adls", "local"]
    if source != "auto":
        order = [source]

    record = _get_run_record(request, run_id) if "adls" in order else None

    df: Optional[pd.DataFrame] = None
    for attempt in order:
        if attempt == "local":
            run_dir = settings.output_base_dir / run_id
            path = resolve_under_base(run_dir, run_dir / "daily_metrics.csv")
            if not path.exists():
                continue
            df = pd.read_csv(path)
            break

        remote = _download_run_file(record, "daily_metrics.csv")
        if remote is None:
            continue
        df = pd.read_csv(io.BytesIO(remote))
        break

    if df is None or df.empty:
        raise HTTPException(status_code=404, detail="daily_metrics.csv not found.")

    for col in [
        "portfolio_value",
        "drawdown",
        "daily_return",
        "cumulative_return",
        "cash",
        "gross_exposure",
        "net_exposure",
        "turnover",
        "commission",
        "slippage_cost",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    original = df
    total_points = int(len(original))
    truncated = False
    if total_points > max_points:
        step = max(1, int(math.ceil(total_points / max_points)))
        df = original.iloc[::step].copy()
        last = original.tail(1)
        if not df.empty and not last.empty:
            if str(df.iloc[-1].get("date") or "") != str(last.iloc[-1].get("date") or ""):
                df = pd.concat([df, last], ignore_index=True)
        truncated = True
    else:
        df = original

    points = []
    for row in df.to_dict(orient="records"):
        portfolio_value = row.get("portfolio_value")
        if portfolio_value is None or pd.isna(portfolio_value):
            portfolio_value = 0.0
        drawdown = row.get("drawdown")
        if drawdown is None or pd.isna(drawdown):
            drawdown = 0.0
        points.append(
            {
                "date": str(row.get("date") or ""),
                "portfolio_value": float(portfolio_value),
                "drawdown": float(drawdown),
                "daily_return": None
                if pd.isna(row.get("daily_return"))
                else float(row.get("daily_return")),
                "cumulative_return": None
                if pd.isna(row.get("cumulative_return"))
                else float(row.get("cumulative_return")),
                "cash": None if pd.isna(row.get("cash")) else float(row.get("cash")),
                "gross_exposure": None
                if pd.isna(row.get("gross_exposure"))
                else float(row.get("gross_exposure")),
                "net_exposure": None
                if pd.isna(row.get("net_exposure"))
                else float(row.get("net_exposure")),
                "turnover": None
                if pd.isna(row.get("turnover"))
                else float(row.get("turnover")),
                "commission": None
                if pd.isna(row.get("commission"))
                else float(row.get("commission")),
                "slippage_cost": None
                if pd.isna(row.get("slippage_cost"))
                else float(row.get("slippage_cost")),
            }
        )

    return TimeseriesResponse(
        points=points, total_points=total_points, truncated=truncated
    )


@router.get("/{run_id}/metrics/rolling", response_model=RollingMetricsResponse)
def get_rolling_metrics(
    run_id: str,
    request: Request,
    window_days: int = Query(63, ge=2, le=2000),
    source: str = Query("auto"),
    max_points: int = Query(5000, ge=50, le=200000),
) -> RollingMetricsResponse:
    settings = get_settings(request)
    validate_auth(request)
    run_id = validate_run_id(run_id)

    if source not in {"auto", "local", "adls"}:
        raise HTTPException(status_code=400, detail="Invalid source (use auto|local|adls).")

    order = ["local", "adls"]
    if _prefer_adls_reads(request):
        order = ["adls", "local"]
    if source != "auto":
        order = [source]

    record = _get_run_record(request, run_id) if "adls" in order else None

    df: Optional[pd.DataFrame] = None
    for attempt in order:
        if attempt == "local":
            run_dir = settings.output_base_dir / run_id
            path = resolve_under_base(run_dir, run_dir / "metrics_rolling.parquet")
            if not path.exists():
                continue
            df = pd.read_parquet(path)
            break

        remote = _download_run_file(record, "metrics_rolling.parquet")
        if remote is None:
            continue
        df = pd.read_parquet(io.BytesIO(remote))
        break

    if df is None or df.empty:
        raise HTTPException(status_code=404, detail="metrics_rolling.parquet not found.")

    if "window_days" in df.columns:
        df = df[pd.to_numeric(df["window_days"], errors="coerce") == window_days]

    total_points = int(len(df))
    truncated = False
    if total_points > max_points:
        step = max(1, int(math.ceil(total_points / max_points)))
        df = df.iloc[::step].copy()
        truncated = True

    points = []
    for row in df.to_dict(orient="records"):
        points.append(
            {
                "date": str(row.get("date") or ""),
                "window_days": int(row.get("window_days") or 0),
                "rolling_return": None
                if pd.isna(row.get("rolling_return"))
                else float(row.get("rolling_return")),
                "rolling_volatility": None
                if pd.isna(row.get("rolling_volatility"))
                else float(row.get("rolling_volatility")),
                "rolling_sharpe": None
                if pd.isna(row.get("rolling_sharpe"))
                else float(row.get("rolling_sharpe")),
                "rolling_max_drawdown": None
                if pd.isna(row.get("rolling_max_drawdown"))
                else float(row.get("rolling_max_drawdown")),
                "turnover_sum": None
                if pd.isna(row.get("turnover_sum"))
                else float(row.get("turnover_sum")),
                "commission_sum": None
                if pd.isna(row.get("commission_sum"))
                else float(row.get("commission_sum")),
                "slippage_cost_sum": None
                if pd.isna(row.get("slippage_cost_sum"))
                else float(row.get("slippage_cost_sum")),
                "n_trades_sum": None
                if pd.isna(row.get("n_trades_sum"))
                else float(row.get("n_trades_sum")),
                "gross_exposure_avg": None
                if pd.isna(row.get("gross_exposure_avg"))
                else float(row.get("gross_exposure_avg")),
                "net_exposure_avg": None
                if pd.isna(row.get("net_exposure_avg"))
                else float(row.get("net_exposure_avg")),
            }
        )

    return RollingMetricsResponse(
        points=points, total_points=total_points, truncated=truncated
    )


@router.get("/{run_id}/trades", response_model=TradeListResponse)
def get_trades(
    run_id: str,
    request: Request,
    source: str = Query("auto"),
    limit: int = Query(5000, ge=1, le=200000),
    offset: int = Query(0, ge=0),
) -> TradeListResponse:
    settings = get_settings(request)
    validate_auth(request)
    run_id = validate_run_id(run_id)

    if source not in {"auto", "local", "adls"}:
        raise HTTPException(status_code=400, detail="Invalid source (use auto|local|adls).")

    order = ["local", "adls"]
    if _prefer_adls_reads(request):
        order = ["adls", "local"]
    if source != "auto":
        order = [source]

    record = _get_run_record(request, run_id) if "adls" in order else None

    df: Optional[pd.DataFrame] = None
    for attempt in order:
        if attempt == "local":
            run_dir = settings.output_base_dir / run_id
            path = resolve_under_base(run_dir, run_dir / "trades.csv")
            if not path.exists():
                continue
            df = pd.read_csv(path)
            break

        remote = _download_run_file(record, "trades.csv")
        if remote is None:
            continue
        df = pd.read_csv(io.BytesIO(remote))
        break

    if df is None:
        raise HTTPException(status_code=404, detail="trades.csv not found.")

    total = int(len(df))
    sliced = df.iloc[int(offset) : int(offset) + int(limit)].copy()

    trades = []
    for row in sliced.to_dict(orient="records"):

        def _as_float(value, default: float = 0.0) -> float:
            if value is None or pd.isna(value):
                return float(default)
            return float(value)

        trades.append(
            {
                "execution_date": str(row.get("execution_date") or row.get("date") or ""),
                "symbol": str(row.get("symbol") or ""),
                "quantity": _as_float(row.get("quantity")),
                "price": _as_float(row.get("price")),
                "notional": _as_float(row.get("notional")),
                "commission": _as_float(row.get("commission")),
                "slippage_cost": _as_float(row.get("slippage_cost")),
                "cash_after": _as_float(row.get("cash_after")),
            }
        )

    return TradeListResponse(
        trades=trades, total=total, limit=int(limit), offset=int(offset)
    )


@router.get("/{run_id}/artifacts", response_model=ArtifactListResponse)
def list_artifacts(
    run_id: str,
    request: Request,
    remote: bool = Query(False),
) -> ArtifactListResponse:
    settings = get_settings(request)
    validate_auth(request)
    run_id = validate_run_id(run_id)
    run_dir = settings.output_base_dir / run_id

    local_infos = [
        ArtifactInfoResponse(**info.__dict__) for info in list_local_artifacts(run_dir)
    ]

    if not remote:
        return ArtifactListResponse(local=local_infos)

    store = get_store(request)
    try:
        record = store.get_run(run_id)
    except KeyError:
        return ArtifactListResponse(
            local=local_infos, remote=[], remote_error="Run not found in store."
        )

    if not record.adls_container or not record.adls_prefix:
        return ArtifactListResponse(local=local_infos, remote=[])

    try:
        remote_infos = [
            ArtifactInfoResponse(**info.__dict__)
            for info in list_remote_artifacts(
                container=record.adls_container, prefix=record.adls_prefix
            )
        ]
        return ArtifactListResponse(local=local_infos, remote=remote_infos)
    except Exception as exc:
        logger.exception("Failed to list remote artifacts for run_id=%s", run_id)
        return ArtifactListResponse(
            local=local_infos, remote=[], remote_error=str(exc)
        )


@router.get("/{run_id}/artifacts/{name:path}")
def get_artifact_content(
    run_id: str,
    name: str,
    request: Request,
    source: str = Query("auto"),
) -> Response:
    settings = get_settings(request)
    validate_auth(request)
    run_id = validate_run_id(run_id)

    # Security check: name cannot contain "..", start with /, etc.
    # While FastAPI path param handles some, explicit check is safer for file access.
    if ".." in name or name.startswith("/") or "\\" in name:
        raise HTTPException(status_code=400, detail="Invalid artifact name.")

    if source not in {"auto", "local", "adls"}:
        raise HTTPException(status_code=400, detail="Invalid source (use auto|local|adls).")

    order = ["local", "adls"]
    if _prefer_adls_reads(request):
        order = ["adls", "local"]
    if source != "auto":
        order = [source]

    record = _get_run_record(request, run_id) if "adls" in order else None

    for attempt in order:
        if attempt == "local":
            run_dir = settings.output_base_dir / run_id
            path = resolve_under_base(run_dir, run_dir / name)
            if not path.exists() or not path.is_file():
                continue
            
            # Simple content type guess or default
            media_type = "application/octet-stream"
            if name.endswith(".json"):
                media_type = "application/json"
            elif name.endswith(".pdf"):
                media_type = "application/pdf"
            elif name.endswith(".html"):
                media_type = "text/html"
            elif name.endswith(".csv"):
                media_type = "text/csv"
            elif name.endswith(".png"):
                media_type = "image/png"
            
            try:
                content = path.read_bytes()
                return Response(content=content, media_type=media_type)
            except Exception as exc:
                logger.error("Failed to read local artifact %s: %s", name, exc)
                # If read fails but file exists, might be permissions. 
                # We continue to next source if possible? Usually hard fail 500 or continue.
                # Let's simple raise 500 if we found it but couldn't read.
                raise HTTPException(status_code=500, detail=f"Failed to read local artifact: {exc}")

        # ADLS
        remote = _download_run_file(record, name)
        if remote is not None:
            media_type = "application/octet-stream"
            if name.endswith(".json"):
                media_type = "application/json"
            elif name.endswith(".pdf"):
                media_type = "application/pdf"
            elif name.endswith(".html"):
                media_type = "text/html"
            elif name.endswith(".csv"):
                media_type = "text/csv"
            elif name.endswith(".png"):
                media_type = "image/png"

            return Response(content=remote, media_type=media_type)

    raise HTTPException(status_code=404, detail="Artifact not found.")
