from types import ModuleType
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse


def _runtime_attr(runtime: ModuleType, name: str) -> Any:
    return getattr(runtime, name)


def build_router(
    *,
    runtime: ModuleType,
    domain_columns_response_model: Any,
    domain_columns_refresh_request_model: Any,
) -> tuple[APIRouter, dict[str, Any]]:
    router = APIRouter()

    @router.get("/domain-columns", response_model=domain_columns_response_model)
    def get_domain_columns(
        request: Request,
        layer: str = Query(..., description="Medallion layer key (bronze|silver|gold)"),
        domain: str = Query(..., description="Domain key (market|finance|earnings|price-target)"),
    ) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        normalize_layer = _runtime_attr(runtime, "_normalize_layer")
        normalize_domain = _runtime_attr(runtime, "_normalize_domain")
        read_domain_columns_from_artifact = _runtime_attr(runtime, "_read_domain_columns_from_artifact")
        domain_artifacts = _runtime_attr(runtime, "domain_artifacts")
        require_common_storage_for_domain_columns = _runtime_attr(
            runtime,
            "_require_common_storage_for_domain_columns",
        )
        domain_columns_read_timeout_seconds = _runtime_attr(runtime, "_domain_columns_read_timeout_seconds")
        run_with_timeout = _runtime_attr(runtime, "_run_with_timeout")
        read_cached_domain_columns = _runtime_attr(runtime, "_read_cached_domain_columns")
        domain_columns_cache_path = _runtime_attr(runtime, "_domain_columns_cache_path")
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        normalized_layer = normalize_layer(layer)
        normalized_domain = normalize_domain(domain)
        if not normalized_layer:
            raise HTTPException(status_code=400, detail="layer is required.")
        if not normalized_domain:
            raise HTTPException(status_code=400, detail="domain is required.")

        try:
            artifact_columns, artifact_updated_at, artifact_found, artifact_path = read_domain_columns_from_artifact(
                normalized_layer,
                normalized_domain,
            )
        except Exception as exc:
            logger.warning(
                "Domain columns artifact read failed: layer=%s domain=%s err=%s",
                normalized_layer,
                normalized_domain,
                exc,
            )
            artifact_columns, artifact_updated_at, artifact_found, artifact_path = [], None, False, None

        if artifact_found:
            return JSONResponse(
                {
                    "layer": normalized_layer,
                    "domain": normalized_domain,
                    "columns": artifact_columns,
                    "found": True,
                    "promptRetrieve": False,
                    "source": "artifact",
                    "cachePath": artifact_path
                    or domain_artifacts.domain_artifact_path(layer=normalized_layer, domain=normalized_domain),
                    "updatedAt": artifact_updated_at,
                },
                headers={"Cache-Control": "no-store"},
            )

        require_common_storage_for_domain_columns()

        read_timeout = domain_columns_read_timeout_seconds()
        try:
            columns, updated_at, found = run_with_timeout(
                lambda: read_cached_domain_columns(normalized_layer, normalized_domain),
                timeout_seconds=read_timeout,
                timeout_message=(
                    f"Domain columns cache read timed out after {read_timeout:.1f}s for "
                    f"{normalized_layer}/{normalized_domain}."
                ),
            )
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception(
                "Domain columns cache read failed: layer=%s domain=%s",
                normalized_layer,
                normalized_domain,
            )
            raise HTTPException(status_code=503, detail=f"Domain columns cache unavailable: {exc}") from exc

        return JSONResponse(
            {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "columns": columns,
                "found": found,
                "promptRetrieve": not found,
                "source": "common-file",
                "cachePath": domain_columns_cache_path(),
                "updatedAt": updated_at,
            },
            headers={"Cache-Control": "no-store"},
        )

    @router.post("/domain-columns/refresh", response_model=domain_columns_response_model)
    def refresh_domain_columns(payload: domain_columns_refresh_request_model, request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        normalize_layer = _runtime_attr(runtime, "_normalize_layer")
        normalize_domain = _runtime_attr(runtime, "_normalize_domain")
        read_domain_columns_from_artifact = _runtime_attr(runtime, "_read_domain_columns_from_artifact")
        domain_artifacts = _runtime_attr(runtime, "domain_artifacts")
        require_common_storage_for_domain_columns = _runtime_attr(
            runtime,
            "_require_common_storage_for_domain_columns",
        )
        domain_columns_refresh_timeout_seconds = _runtime_attr(
            runtime,
            "_domain_columns_refresh_timeout_seconds",
        )
        run_with_timeout = _runtime_attr(runtime, "_run_with_timeout")
        retrieve_domain_columns = _runtime_attr(runtime, "_retrieve_domain_columns")
        write_cached_domain_columns = _runtime_attr(runtime, "_write_cached_domain_columns")
        domain_columns_cache_path = _runtime_attr(runtime, "_domain_columns_cache_path")
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        normalized_layer = normalize_layer(payload.layer)
        normalized_domain = normalize_domain(payload.domain)
        if not normalized_layer:
            raise HTTPException(status_code=400, detail="layer is required.")
        if not normalized_domain:
            raise HTTPException(status_code=400, detail="domain is required.")

        try:
            artifact_columns, artifact_updated_at, artifact_found, artifact_path = read_domain_columns_from_artifact(
                normalized_layer,
                normalized_domain,
            )
        except Exception as exc:
            logger.warning(
                "Domain columns artifact refresh read failed: layer=%s domain=%s err=%s",
                normalized_layer,
                normalized_domain,
                exc,
            )
            artifact_columns, artifact_updated_at, artifact_found, artifact_path = [], None, False, None

        if artifact_found:
            return JSONResponse(
                {
                    "layer": normalized_layer,
                    "domain": normalized_domain,
                    "columns": artifact_columns,
                    "found": True,
                    "promptRetrieve": False,
                    "source": "artifact",
                    "cachePath": artifact_path
                    or domain_artifacts.domain_artifact_path(layer=normalized_layer, domain=normalized_domain),
                    "updatedAt": artifact_updated_at,
                },
                headers={"Cache-Control": "no-store"},
            )

        require_common_storage_for_domain_columns()

        refresh_timeout = domain_columns_refresh_timeout_seconds()
        try:
            columns = run_with_timeout(
                lambda: retrieve_domain_columns(normalized_layer, normalized_domain, int(payload.sample_limit)),
                timeout_seconds=refresh_timeout,
                timeout_message=(
                    f"Domain columns retrieval timed out after {refresh_timeout:.1f}s for "
                    f"{normalized_layer}/{normalized_domain}."
                ),
            )
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception(
                "Domain columns refresh retrieval failed: layer=%s domain=%s",
                normalized_layer,
                normalized_domain,
            )
            raise HTTPException(status_code=503, detail=f"Domain columns retrieval unavailable: {exc}") from exc

        if not columns:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No columns discovered for {normalized_layer}/{normalized_domain}. "
                    "Verify data exists and retry refresh."
                ),
            )

        try:
            cached_columns, updated_at = run_with_timeout(
                lambda: write_cached_domain_columns(normalized_layer, normalized_domain, columns),
                timeout_seconds=refresh_timeout,
                timeout_message=(
                    f"Domain columns cache write timed out after {refresh_timeout:.1f}s for "
                    f"{normalized_layer}/{normalized_domain}."
                ),
            )
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Common storage is unavailable for column cache updates: {exc}",
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception(
                "Domain columns cache update failed: layer=%s domain=%s",
                normalized_layer,
                normalized_domain,
            )
            raise HTTPException(status_code=500, detail=f"Failed to update domain columns cache: {exc}") from exc

        return JSONResponse(
            {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "columns": cached_columns,
                "found": True,
                "promptRetrieve": False,
                "source": "common-file",
                "cachePath": domain_columns_cache_path(),
                "updatedAt": updated_at,
            },
            headers={"Cache-Control": "no-store"},
        )

    return router, {
        "get_domain_columns": get_domain_columns,
        "refresh_domain_columns": refresh_domain_columns,
    }
