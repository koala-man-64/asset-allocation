from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any, Dict, Optional

import yaml
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse, Response

from asset_allocation.backtest.config import BacktestConfig, generate_run_id, validate_config_dict_strict
from asset_allocation.backtest.service.artifacts import download_remote_artifact, list_local_artifacts, list_remote_artifacts
from asset_allocation.backtest.service.job_manager import JobManager
from asset_allocation.backtest.service.run_store import RunStore
from asset_allocation.backtest.service.schemas import (
    ArtifactInfoResponse,
    ArtifactListResponse,
    BacktestSubmitRequest,
    BacktestSubmitResponse,
    RunListResponse,
    RunRecordResponse,
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


def _require_api_key(
    settings: ServiceSettings,
    api_key: Optional[str],
) -> None:
    if not settings.api_key:
        return
    if not api_key or api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Unauthorized.")


def _get_settings(app: FastAPI) -> ServiceSettings:
    return app.state.settings


def _get_store(app: FastAPI) -> RunStore:
    return app.state.store


def _get_manager(app: FastAPI) -> JobManager:
    return app.state.manager


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings = ServiceSettings.from_env()
        store = RunStore(settings.db_path)
        store.init_db()
        reconciled = store.reconcile_incomplete_runs()
        if reconciled:
            logger.warning("Reconciled %d incomplete runs on startup.", reconciled)

        manager = JobManager(store=store, output_base_dir=settings.output_base_dir, max_workers=settings.max_concurrent_runs)

        app.state.settings = settings
        app.state.store = store
        app.state.manager = manager
        try:
            yield
        finally:
            manager.shutdown()

    app = FastAPI(title="Backtest Service", version="0.1.0", lifespan=lifespan)

    @app.get("/healthz")
    def healthz() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @app.get("/readyz")
    def readyz() -> JSONResponse:
        settings = _get_settings(app)
        store = _get_store(app)
        try:
            store.init_db()
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"DB not ready: {exc}") from exc
        if not settings.output_base_dir.exists():
            raise HTTPException(status_code=503, detail="Output dir not ready.")
        return JSONResponse({"status": "ok"})

    @app.post("/backtests", response_model=BacktestSubmitResponse)
    def submit_backtest(
        payload: BacktestSubmitRequest,
        api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    ) -> BacktestSubmitResponse:
        settings = _get_settings(app)
        _require_api_key(settings, api_key)

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

        manager.submit(run_id=resolved_run_id, config=cfg)
        return BacktestSubmitResponse(run_id=resolved_run_id, status="queued")

    @app.get("/backtests", response_model=RunListResponse)
    def list_backtests(
        status: Optional[str] = Query(default=None),
        q: Optional[str] = Query(default=None),
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
        api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    ) -> RunListResponse:
        settings = _get_settings(app)
        _require_api_key(settings, api_key)
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
        api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    ) -> RunRecordResponse:
        settings = _get_settings(app)
        _require_api_key(settings, api_key)
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
        api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    ) -> JSONResponse:
        settings = _get_settings(app)
        _require_api_key(settings, api_key)
        run_id = validate_run_id(run_id)
        run_dir = settings.output_base_dir / run_id
        summary_path = run_dir / "summary.json"
        summary_path = resolve_under_base(run_dir, summary_path)
        if not summary_path.exists():
            raise HTTPException(status_code=404, detail="summary.json not found.")
        try:
            data = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise HTTPException(status_code=500, detail="Failed to read summary.") from exc
        return JSONResponse(data)

    @app.get("/backtests/{run_id}/artifacts", response_model=ArtifactListResponse)
    def list_artifacts(
        run_id: str,
        remote: bool = Query(False),
        api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    ) -> ArtifactListResponse:
        settings = _get_settings(app)
        _require_api_key(settings, api_key)
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
        source: str = Query("auto"),
        api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    ) -> Response:
        settings = _get_settings(app)
        _require_api_key(settings, api_key)
        run_id = validate_run_id(run_id)
        name = validate_artifact_name(name)

        run_dir = settings.output_base_dir / run_id
        local_path = resolve_under_base(run_dir, run_dir / name)
        if source in {"auto", "local"} and local_path.exists():
            return FileResponse(path=str(local_path), filename=name)

        if source not in {"auto", "adls"}:
            raise HTTPException(status_code=400, detail="Invalid source (use auto|local|adls).")

        store = _get_store(app)
        try:
            record = store.get_run(run_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Run not found.") from exc

        if not record.adls_container or not record.adls_prefix:
            raise HTTPException(status_code=404, detail="Remote artifacts not configured for this run.")

        remote_path = f"{record.adls_prefix.rstrip('/')}/{name}"
        try:
            content = download_remote_artifact(container=record.adls_container, remote_path=remote_path)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        if content is None:
            raise HTTPException(status_code=404, detail="Artifact not found.")
        return Response(content=content, media_type="application/octet-stream")

    return app


app = create_app()
