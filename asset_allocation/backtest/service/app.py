from __future__ import annotations

import io
import json
import logging
import math
import os
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import yaml
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles

from asset_allocation.backtest.config import BacktestConfig, generate_run_id, validate_config_dict_strict
from asset_allocation.backtest.service.artifacts import download_remote_artifact, list_local_artifacts, list_remote_artifacts
from asset_allocation.backtest.service.adls_run_store import AdlsRunStore
from asset_allocation.backtest.service.auth import AuthError, AuthManager
from asset_allocation.backtest.service.job_manager import JobManager
from asset_allocation.backtest.service.postgres_run_store import PostgresRunStore
from asset_allocation.backtest.service.run_store import RunStore
from asset_allocation.backtest.service.schemas import (
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
from asset_allocation.backtest.service.security import (
    assert_allowed_container,
    assert_path_under_allowlist,
    parse_container_and_path,
    resolve_under_base,
    validate_artifact_name,
    validate_run_id,
)
from asset_allocation.backtest.service.settings import ServiceSettings
from asset_allocation.monitoring.system_health import collect_system_health_snapshot
from asset_allocation.monitoring.ttl_cache import TtlCache


logger = logging.getLogger("asset_allocation.backtest.api")


def _load_config_from_request(payload: BacktestSubmitRequest) -> Dict[str, Any]:
    if payload.config is not None:
        return dict(payload.config)
    raw = payload.config_yaml or ""
    data = yaml.safe_load(raw) or {}
    if not isinstance(data, dict):
        raise ValueError("config_yaml must parse to a YAML object.")
    return data


def _validate_config_for_service(cfg: BacktestConfig, cfg_dict: Dict[str, Any], settings: ServiceSettings) -> None:
    if cfg.data is None:
        raise ValueError("data section is required.")

    # Enforce secure defaults for local paths in service mode.
    if cfg.data.price_source == "local":
        if not settings.allow_local_data:
            raise ValueError("data.price_source=local is disabled in the service (set BACKTEST_ALLOW_LOCAL_DATA=true).")
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


def _get_settings(app: FastAPI) -> ServiceSettings:
    return app.state.settings


def _get_store(app: FastAPI) -> RunStore:
    return app.state.store


def _get_manager(app: FastAPI) -> JobManager:
    return app.state.manager


def _get_auth(app: FastAPI) -> AuthManager:
    return app.state.auth


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings = ServiceSettings.from_env()
        if settings.run_store_mode == "adls":
            store = AdlsRunStore(settings.adls_runs_dir or "")
        elif settings.run_store_mode == "postgres":
            store = PostgresRunStore(settings.postgres_dsn or "")
        else:
            store = RunStore(settings.db_path)
        store.init_db()
        reconciled = store.reconcile_incomplete_runs()
        if reconciled:
            logger.warning("Reconciled %d incomplete runs on startup.", reconciled)

        manager = JobManager(
            store=store,
            output_base_dir=settings.output_base_dir,
            max_workers=settings.max_concurrent_runs,
            default_adls_dir=settings.adls_runs_dir,
        )
        auth = AuthManager(settings)

        app.state.settings = settings
        app.state.store = store
        app.state.manager = manager
        app.state.auth = auth
        try:
            yield
        finally:
            manager.shutdown()

    app = FastAPI(title="Backtest Service", version="0.1.0", lifespan=lifespan)

    def _system_health_ttl_seconds() -> float:
        raw = os.environ.get("SYSTEM_HEALTH_TTL_SECONDS", "").strip()
        if not raw:
            return 30.0
        try:
            ttl = float(raw)
        except ValueError:
            return 30.0
        return ttl if ttl > 0 else 30.0

    app.state.system_health_cache = TtlCache(ttl_seconds=_system_health_ttl_seconds())

    def _prefer_adls_reads() -> bool:
        settings = _get_settings(app)
        return settings.run_store_mode == "adls"

    def _require_auth(request: Request) -> None:
        settings = _get_settings(app)
        auth = _get_auth(app)
        if settings.auth_mode == "none":
            return
        try:
            auth.authenticate_headers(dict(request.headers))
        except AuthError as exc:
            headers: Dict[str, str] = {}
            if exc.www_authenticate:
                headers["WWW-Authenticate"] = exc.www_authenticate
            raise HTTPException(status_code=exc.status_code, detail=exc.detail, headers=headers) from exc

    @app.middleware("http")
    async def _http_middleware(request: Request, call_next):
        path = request.url.path or ""
        if path.startswith("/backtests") and path.endswith("/"):
            url = request.url.replace(path=path.rstrip("/"))
            return RedirectResponse(url=str(url), status_code=307)

        response = await call_next(request)

        if path.startswith("/assets/") and response.status_code == 200:
            response.headers.setdefault("Cache-Control", "public, max-age=31536000, immutable")

        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")

        csp = os.environ.get("BACKTEST_CSP", "").strip()
        if not csp:
            csp = (
                "default-src 'self'; "
                "base-uri 'none'; "
                "frame-ancestors 'none'; "
                "object-src 'none'; "
                "img-src 'self' data: https:; "
                "script-src 'self'; "
                "style-src 'self' 'unsafe-inline'; "
                "font-src 'self' data: https:; "
                "connect-src 'self' https:"
            )
        response.headers.setdefault("Content-Security-Policy", csp.strip())

        return response

    def _get_run_record(run_id: str):
        store = _get_store(app)
        try:
            return store.get_run(run_id)
        except KeyError:
            return None

    def _download_run_file(record, name: str) -> Optional[bytes]:
        if not record or not record.adls_container or not record.adls_prefix:
            return None
        remote_path = f"{record.adls_prefix.rstrip('/')}/{name}"
        return download_remote_artifact(container=record.adls_container, remote_path=remote_path)

    @app.get("/healthz")
    def healthz() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @app.get("/readyz")
    def readyz() -> JSONResponse:
        settings = _get_settings(app)
        store = _get_store(app)
        try:
            store.init_db()
            if hasattr(store, "ping"):
                store.ping()
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"DB not ready: {exc}") from exc
        if not settings.output_base_dir.exists():
            raise HTTPException(status_code=503, detail="Output dir not ready.")
        return JSONResponse({"status": "ok"})

    @app.get("/system/health")
    def system_health(request: Request, refresh: bool = Query(False)) -> JSONResponse:
        logger.info(f"Accessing /system/health endpoint (refresh={refresh})")
        _require_auth(request)
        settings = _get_settings(app)

        include_ids = False
        if settings.auth_mode != "none":
            raw = os.environ.get("SYSTEM_HEALTH_VERBOSE_IDS", "").strip().lower()
            include_ids = raw in {"1", "true", "t", "yes", "y", "on"}

        cache: TtlCache[Dict[str, Any]] = app.state.system_health_cache

        def _refresh() -> Dict[str, Any]:
            return collect_system_health_snapshot(include_resource_ids=include_ids)

        try:
            result = cache.get(_refresh, force_refresh=bool(refresh))
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"System health unavailable: {exc}") from exc

        headers: Dict[str, str] = {
            "Cache-Control": "no-store",
            "X-System-Health-Cache": "hit" if result.cache_hit else "miss",
        }
        if result.refresh_error:
            headers["X-System-Health-Stale"] = "1"
        return JSONResponse(result.value, headers=headers)

    @app.post("/backtests", response_model=BacktestSubmitResponse)
    def submit_backtest(
        payload: BacktestSubmitRequest,
        request: Request,
    ) -> BacktestSubmitResponse:
        settings = _get_settings(app)
        _require_auth(request)

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

        effective_cfg = replace(cfg, output=replace(cfg.output, local_dir=str(settings.output_base_dir)))
        output_dir = str((settings.output_base_dir / resolved_run_id).resolve(strict=False))
        store = _get_store(app)
        manager = _get_manager(app)

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
                logger.exception("Failed to persist ADLS run location: run_id=%s", resolved_run_id)

        manager.submit(run_id=resolved_run_id, config=cfg)
        return BacktestSubmitResponse(run_id=resolved_run_id, status="queued")

    @app.get("/backtests", response_model=RunListResponse)
    def list_backtests(
        request: Request,
        status: Optional[str] = Query(default=None),
        q: Optional[str] = Query(default=None),
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
    ) -> RunListResponse:
        _require_auth(request)
        store = _get_store(app)

        if status is not None and status not in {"queued", "running", "completed", "failed"}:
            raise HTTPException(status_code=400, detail="Invalid status filter.")
        status_value = status
        runs = store.list_runs(limit=limit, offset=offset, status=status_value, query=q)
        return RunListResponse(
            runs=[RunRecordResponse(**r.to_public_dict()) for r in runs],
            limit=limit,
            offset=offset,
        )

    @app.get("/backtests/{run_id}/status", response_model=RunRecordResponse)
    def get_status(
        run_id: str,
        request: Request,
    ) -> RunRecordResponse:
        _require_auth(request)
        run_id = validate_run_id(run_id)

        store = _get_store(app)
        try:
            record = store.get_run(run_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Run not found.") from exc
        return RunRecordResponse(**record.to_public_dict())

    @app.get("/backtests/{run_id}/summary")
    def get_summary(
        run_id: str,
        request: Request,
        source: str = Query("auto"),
    ) -> JSONResponse:
        settings = _get_settings(app)
        _require_auth(request)
        run_id = validate_run_id(run_id)

        if source not in {"auto", "local", "adls"}:
            raise HTTPException(status_code=400, detail="Invalid source (use auto|local|adls).")

        order = ["local", "adls"]
        if _prefer_adls_reads():
            order = ["adls", "local"]
        if source != "auto":
            order = [source]

        record = _get_run_record(run_id) if "adls" in order else None

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
                    raise HTTPException(status_code=500, detail="Failed to read local summary.") from exc
                return JSONResponse(data)

            remote = _download_run_file(record, "summary.json")
            if remote is None:
                errors.append("summary.json not found in ADLS")
                continue
            try:
                data = json.loads(remote.decode("utf-8"))
            except Exception as exc:
                raise HTTPException(status_code=500, detail="Failed to read ADLS summary.") from exc
            return JSONResponse(data)

        raise HTTPException(status_code=404, detail="summary.json not found.")

    @app.get("/backtests/{run_id}/metrics/timeseries", response_model=TimeseriesResponse)
    def get_timeseries(
        run_id: str,
        request: Request,
        source: str = Query("auto"),
        max_points: int = Query(5000, ge=50, le=200000),
    ) -> TimeseriesResponse:
        settings = _get_settings(app)
        _require_auth(request)
        run_id = validate_run_id(run_id)

        if source not in {"auto", "local", "adls"}:
            raise HTTPException(status_code=400, detail="Invalid source (use auto|local|adls).")

        order = ["local", "adls"]
        if _prefer_adls_reads():
            order = ["adls", "local"]
        if source != "auto":
            order = [source]

        record = _get_run_record(run_id) if "adls" in order else None

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
                    "daily_return": None if pd.isna(row.get("daily_return")) else float(row.get("daily_return")),
                    "cumulative_return": None
                    if pd.isna(row.get("cumulative_return"))
                    else float(row.get("cumulative_return")),
                    "cash": None if pd.isna(row.get("cash")) else float(row.get("cash")),
                    "gross_exposure": None if pd.isna(row.get("gross_exposure")) else float(row.get("gross_exposure")),
                    "net_exposure": None if pd.isna(row.get("net_exposure")) else float(row.get("net_exposure")),
                    "turnover": None if pd.isna(row.get("turnover")) else float(row.get("turnover")),
                    "commission": None if pd.isna(row.get("commission")) else float(row.get("commission")),
                    "slippage_cost": None if pd.isna(row.get("slippage_cost")) else float(row.get("slippage_cost")),
                }
            )

        return TimeseriesResponse(points=points, total_points=total_points, truncated=truncated)

    @app.get("/backtests/{run_id}/metrics/rolling", response_model=RollingMetricsResponse)
    def get_rolling_metrics(
        run_id: str,
        request: Request,
        window_days: int = Query(63, ge=2, le=2000),
        source: str = Query("auto"),
        max_points: int = Query(5000, ge=50, le=200000),
    ) -> RollingMetricsResponse:
        settings = _get_settings(app)
        _require_auth(request)
        run_id = validate_run_id(run_id)

        if source not in {"auto", "local", "adls"}:
            raise HTTPException(status_code=400, detail="Invalid source (use auto|local|adls).")

        order = ["local", "adls"]
        if _prefer_adls_reads():
            order = ["adls", "local"]
        if source != "auto":
            order = [source]

        record = _get_run_record(run_id) if "adls" in order else None

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
                    "rolling_return": None if pd.isna(row.get("rolling_return")) else float(row.get("rolling_return")),
                    "rolling_volatility": None
                    if pd.isna(row.get("rolling_volatility"))
                    else float(row.get("rolling_volatility")),
                    "rolling_sharpe": None if pd.isna(row.get("rolling_sharpe")) else float(row.get("rolling_sharpe")),
                    "rolling_max_drawdown": None
                    if pd.isna(row.get("rolling_max_drawdown"))
                    else float(row.get("rolling_max_drawdown")),
                    "turnover_sum": None if pd.isna(row.get("turnover_sum")) else float(row.get("turnover_sum")),
                    "commission_sum": None
                    if pd.isna(row.get("commission_sum"))
                    else float(row.get("commission_sum")),
                    "slippage_cost_sum": None
                    if pd.isna(row.get("slippage_cost_sum"))
                    else float(row.get("slippage_cost_sum")),
                    "n_trades_sum": None if pd.isna(row.get("n_trades_sum")) else float(row.get("n_trades_sum")),
                    "gross_exposure_avg": None
                    if pd.isna(row.get("gross_exposure_avg"))
                    else float(row.get("gross_exposure_avg")),
                    "net_exposure_avg": None
                    if pd.isna(row.get("net_exposure_avg"))
                    else float(row.get("net_exposure_avg")),
                }
            )

        return RollingMetricsResponse(points=points, total_points=total_points, truncated=truncated)

    @app.get("/backtests/{run_id}/trades", response_model=TradeListResponse)
    def get_trades(
        run_id: str,
        request: Request,
        source: str = Query("auto"),
        limit: int = Query(5000, ge=1, le=200000),
        offset: int = Query(0, ge=0),
    ) -> TradeListResponse:
        settings = _get_settings(app)
        _require_auth(request)
        run_id = validate_run_id(run_id)

        if source not in {"auto", "local", "adls"}:
            raise HTTPException(status_code=400, detail="Invalid source (use auto|local|adls).")

        order = ["local", "adls"]
        if _prefer_adls_reads():
            order = ["adls", "local"]
        if source != "auto":
            order = [source]

        record = _get_run_record(run_id) if "adls" in order else None

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

        return TradeListResponse(trades=trades, total=total, limit=int(limit), offset=int(offset))

    @app.get("/backtests/{run_id}/artifacts", response_model=ArtifactListResponse)
    def list_artifacts(
        run_id: str,
        request: Request,
        remote: bool = Query(False),
    ) -> ArtifactListResponse:
        settings = _get_settings(app)
        _require_auth(request)
        run_id = validate_run_id(run_id)
        run_dir = settings.output_base_dir / run_id

        local_infos = [ArtifactInfoResponse(**info.__dict__) for info in list_local_artifacts(run_dir)]

        if not remote:
            return ArtifactListResponse(local=local_infos)

        store = _get_store(app)
        try:
            record = store.get_run(run_id)
        except KeyError:
            return ArtifactListResponse(local=local_infos, remote=[], remote_error="Run not found in store.")

        if not record.adls_container or not record.adls_prefix:
            return ArtifactListResponse(local=local_infos, remote=[])

        try:
            remote_infos = list_remote_artifacts(container=record.adls_container, prefix=record.adls_prefix)
            return ArtifactListResponse(local=local_infos, remote=[ArtifactInfoResponse(**i.__dict__) for i in remote_infos])
        except Exception as exc:
            return ArtifactListResponse(local=local_infos, remote=[], remote_error=str(exc))

    @app.get("/backtests/{run_id}/artifacts/{name}")
    def get_artifact(
        run_id: str,
        name: str,
        request: Request,
        source: str = Query("auto"),
    ) -> Response:
        settings = _get_settings(app)
        _require_auth(request)
        run_id = validate_run_id(run_id)
        name = validate_artifact_name(name)

        run_dir = settings.output_base_dir / run_id
        local_path = resolve_under_base(run_dir, run_dir / name)

        if source not in {"auto", "local", "adls"}:
            raise HTTPException(status_code=400, detail="Invalid source (use auto|local|adls).")

        if source == "local":
            if not local_path.exists():
                raise HTTPException(status_code=404, detail="Artifact not found.")
            return FileResponse(path=str(local_path), filename=name)

        prefer_adls = _prefer_adls_reads()
        attempts = ["local", "adls"]
        if prefer_adls:
            attempts = ["adls", "local"]
        if source != "auto":
            attempts = [source]

        record = _get_run_record(run_id) if "adls" in attempts else None
        for attempt in attempts:
            if attempt == "local":
                if local_path.exists():
                    return FileResponse(path=str(local_path), filename=name)
                continue

            if not record:
                continue
            if not record.adls_container or not record.adls_prefix:
                continue
            remote_path = f"{record.adls_prefix.rstrip('/')}/{name}"
            try:
                content = download_remote_artifact(container=record.adls_container, remote_path=remote_path)
            except Exception as exc:
                raise HTTPException(status_code=502, detail=str(exc)) from exc
            if content is not None:
                return Response(content=content, media_type="application/octet-stream")

        raise HTTPException(status_code=404, detail="Artifact not found.")

    def _parse_ui_scopes(raw: str) -> list[str]:
        normalized = str(raw or "").replace(",", " ").strip()
        return [s for s in normalized.split() if s]

    def _default_ui_auth_mode(settings: ServiceSettings) -> str:
        return "oidc" if settings.auth_mode in {"oidc", "api_key_or_oidc"} else "none"

    def _normalize_ui_auth_mode(raw: str, settings: ServiceSettings) -> str:
        mode = (raw or "").strip().lower()
        if not mode:
            return _default_ui_auth_mode(settings)
        if mode in {"none", "noauth", "disabled"}:
            return "none"
        if mode in {"oidc", "jwt", "bearer"}:
            return "oidc"
        if mode in {"api_key", "apikey", "key"}:
            return "api_key"
        return _default_ui_auth_mode(settings)

    @app.get("/config.js", include_in_schema=False)
    def ui_runtime_config() -> Response:
        settings = _get_settings(app)

        backtest_api_base_url = os.environ.get("BACKTEST_UI_API_BASE_URL", "").strip()
        ui_mode = _normalize_ui_auth_mode(os.environ.get("BACKTEST_UI_AUTH_MODE", ""), settings)

        oidc_client_id = os.environ.get("BACKTEST_UI_OIDC_CLIENT_ID", "").strip()
        oidc_authority = os.environ.get("BACKTEST_UI_OIDC_AUTHORITY", "").strip()
        if not oidc_authority:
            issuer = (settings.oidc_issuer or "").strip()
            oidc_authority = issuer.removesuffix("/v2.0") if issuer else ""

        oidc_scopes = _parse_ui_scopes(os.environ.get("BACKTEST_UI_OIDC_SCOPES", ""))

        if ui_mode == "oidc" and not (oidc_client_id and oidc_authority):
            ui_mode = "none"

        payload: Dict[str, Any] = {
            "backtestApiBaseUrl": backtest_api_base_url,
            "authMode": ui_mode,
        }
        if ui_mode == "oidc":
            payload.update(
                {
                    "oidcClientId": oidc_client_id,
                    "oidcAuthority": oidc_authority,
                    "oidcScopes": oidc_scopes,
                }
            )

        js = f"window.__BACKTEST_UI_CONFIG__ = {json.dumps(payload)};\n"
        return Response(
            content=js,
            media_type="application/javascript",
            headers={"Cache-Control": "no-store"},
        )

    def _find_ui_dist_dir() -> Optional[Path]:
        raw = os.environ.get("BACKTEST_UI_DIST_DIR", "").strip()
        candidates: list[Path] = []
        if raw:
            candidates.append(Path(raw).expanduser())

        # Dockerfile.backtest_api copies UI dist here by default.
        candidates.append((Path.cwd() / "ui-dist").resolve(strict=False))

        # Local dev: allow serving a locally built UI.
        candidates.append((Path(__file__).resolve().parents[2] / "ui2.0" / "dist").resolve(strict=False))

        for candidate in candidates:
            try:
                index_path = candidate / "index.html"
                assets_dir = candidate / "assets"
                if candidate.is_dir() and index_path.is_file() and assets_dir.is_dir():
                    return candidate
            except Exception:
                logger.exception("Failed to validate UI dist dir: %s", candidate)
        return None

    ui_dist_dir = _find_ui_dist_dir()
    if ui_dist_dir:
        index_path = ui_dist_dir / "index.html"
        assets_dir = ui_dist_dir / "assets"

        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="ui-assets")
        logger.info("UI enabled: dist_dir=%s", ui_dist_dir)

        @app.get("/", include_in_schema=False)
        def ui_index() -> Response:
            return FileResponse(
                path=str(index_path),
                media_type="text/html",
                headers={"Cache-Control": "no-store"},
            )

        @app.get("/{path:path}", include_in_schema=False)
        def ui_fallback(path: str) -> Response:
            # Never shadow API routes.
            if path.startswith("backtests"):
                raise HTTPException(status_code=404, detail="Not found.")
            return FileResponse(
                path=str(index_path),
                media_type="text/html",
                headers={"Cache-Control": "no-store"},
            )
    else:
        logger.info("UI disabled: dist directory not found (set BACKTEST_UI_DIST_DIR or build UI into the image).")

    return app


app = create_app()
