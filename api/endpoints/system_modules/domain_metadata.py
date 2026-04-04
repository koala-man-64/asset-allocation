import hashlib
import json
from types import ModuleType
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response


def _runtime_attr(runtime: ModuleType, name: str) -> Any:
    return getattr(runtime, name)


def build_router(
    *,
    runtime: ModuleType,
    domain_metadata_response_model: Any,
    domain_metadata_snapshot_response_model: Any,
) -> tuple[APIRouter, dict[str, Any]]:
    router = APIRouter()

    @router.get("/domain-metadata", response_model=domain_metadata_response_model)
    def domain_metadata(
        request: Request,
        layer: str = Query(..., description="Medallion layer key (bronze|silver|gold|platinum)"),
        domain: str = Query(..., description="Domain key (market|finance|earnings|price-target|platinum)"),
        refresh: bool = Query(
            default=False,
            description="When true, collect live metadata, persist refreshed snapshot documents, and return the refreshed payload.",
        ),
    ) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        reject_removed_query_params = _runtime_attr(runtime, "_reject_removed_query_params")
        normalize_layer = _runtime_attr(runtime, "_normalize_layer")
        normalize_domain = _runtime_attr(runtime, "_normalize_domain")
        refresh_domain_metadata_snapshot = _runtime_attr(runtime, "_refresh_domain_metadata_snapshot")
        read_cached_domain_metadata_snapshot = _runtime_attr(runtime, "_read_cached_domain_metadata_snapshot")
        build_snapshot_miss_payload = _runtime_attr(runtime, "build_snapshot_miss_payload")
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        reject_removed_query_params(request, "cacheOnly")
        normalized_layer = normalize_layer(layer)
        normalized_domain = normalize_domain(domain)
        if not normalized_layer:
            raise HTTPException(status_code=400, detail="layer is required.")
        if not normalized_domain:
            raise HTTPException(status_code=400, detail="domain is required.")

        if refresh:
            payload = refresh_domain_metadata_snapshot(normalized_layer, normalized_domain)
            headers: Dict[str, str] = {
                "Cache-Control": "no-store",
                "X-Domain-Metadata-Source": "live-refresh",
            }
            cached_at = payload.get("cachedAt")
            if isinstance(cached_at, str) and cached_at.strip():
                headers["X-Domain-Metadata-Cached-At"] = cached_at
            return JSONResponse(payload, headers=headers)

        try:
            payload = read_cached_domain_metadata_snapshot(
                normalized_layer,
                normalized_domain,
                force_refresh=False,
            )
        except Exception as exc:
            logger.warning(
                "Domain metadata snapshot read failed. layer=%s domain=%s err=%s",
                normalized_layer,
                normalized_domain,
                exc,
            )
            payload = None

        if payload is None:
            placeholder_payload = build_snapshot_miss_payload(
                layer=normalized_layer,
                domain=normalized_domain,
            )
            return JSONResponse(
                placeholder_payload,
                headers={
                    "Cache-Control": "no-store",
                    "X-Domain-Metadata-Source": "snapshot-miss",
                    "X-Domain-Metadata-Cache-Miss": "1",
                },
            )

        headers: Dict[str, str] = {
            "Cache-Control": "no-store",
            "X-Domain-Metadata-Source": "snapshot",
        }
        cached_at = payload.get("cachedAt")
        if isinstance(cached_at, str) and cached_at.strip():
            headers["X-Domain-Metadata-Cached-At"] = cached_at
        return JSONResponse(payload, headers=headers)

    @router.get("/domain-metadata/snapshot", response_model=domain_metadata_snapshot_response_model)
    def domain_metadata_snapshot(
        request: Request,
        layers: Optional[str] = Query(
            default=None,
            description="Optional comma-separated layer filter (e.g. bronze,silver,gold).",
        ),
        domains: Optional[str] = Query(
            default=None,
            description="Optional comma-separated domain filter (e.g. market,finance,earnings,price-target).",
        ),
        refresh: bool = Query(
            default=False,
            description="When true, bypass the in-process snapshot document cache before reading persisted metadata.",
        ),
    ) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        reject_removed_query_params = _runtime_attr(runtime, "_reject_removed_query_params")
        build_domain_metadata_snapshot_payload = _runtime_attr(runtime, "_build_domain_metadata_snapshot_payload")

        validate_auth(request)
        reject_removed_query_params(request, "cacheOnly")
        response_payload = build_domain_metadata_snapshot_payload(
            layers=layers,
            domains=domains,
            refresh=bool(refresh),
        )
        headers: Dict[str, str] = {
            "Cache-Control": "no-store",
            "X-Domain-Metadata-Source": "snapshot-batch",
            "X-Domain-Metadata-Entry-Count": str(len(response_payload.get("entries") or {})),
        }
        updated_at = response_payload.get("updatedAt")
        if isinstance(updated_at, str) and updated_at.strip():
            headers["X-Domain-Metadata-Updated-At"] = updated_at
            headers["Last-Modified"] = updated_at
        etag_basis = {
            "updatedAt": response_payload.get("updatedAt"),
            "keys": sorted((response_payload.get("entries") or {}).keys()),
        }
        etag = (
            'W/"'
            + hashlib.sha256(
                json.dumps(etag_basis, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()[:24]
            + '"'
        )
        headers["ETag"] = etag
        if (request.headers.get("if-none-match") or "").strip() == etag:
            return Response(status_code=304, headers=headers)
        return JSONResponse(response_payload, headers=headers)

    @router.get("/domain-metadata/snapshot/cache", response_model=domain_metadata_snapshot_response_model)
    def get_domain_metadata_snapshot_cache(request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        mdc = _runtime_attr(runtime, "mdc")
        domain_metadata_ui_cache_path = _runtime_attr(runtime, "_domain_metadata_ui_cache_path")
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        warnings: List[str] = []
        cache_hit = False
        payload: Dict[str, Any] = {}

        try:
            raw = mdc.get_common_json_content(domain_metadata_ui_cache_path())
        except Exception as exc:
            logger.warning("Failed to read persisted UI domain metadata cache: %s", exc)
            raw = None
            warnings.append(f"Read failed: {exc}")

        if isinstance(raw, dict):
            try:
                parsed = domain_metadata_snapshot_response_model(**raw)
                cache_hit = True
                payload = parsed.model_dump() if hasattr(parsed, "model_dump") else parsed.dict()
            except Exception as exc:
                logger.warning("Persisted UI cache payload was invalid. Returning empty snapshot. err=%s", exc)
                warnings.append(f"Invalid cache payload ignored: {exc}")

        if not payload:
            payload = {
                "version": 1,
                "updatedAt": None,
                "entries": {},
                "warnings": warnings or ["No persisted UI domain metadata snapshot found."],
            }
        elif warnings:
            payload["warnings"] = [*list(payload.get("warnings") or []), *warnings]

        return JSONResponse(
            payload,
            headers={
                "Cache-Control": "no-store",
                "X-Domain-Metadata-UI-Cache": "hit" if cache_hit else "miss",
                "X-Domain-Metadata-Entry-Count": str(len(payload.get("entries") or {})),
            },
        )

    @router.put("/domain-metadata/snapshot/cache", response_model=domain_metadata_snapshot_response_model)
    def put_domain_metadata_snapshot_cache(
        request: Request,
        payload: domain_metadata_snapshot_response_model,
    ) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        utc_timestamp = _runtime_attr(runtime, "_utc_timestamp")
        mdc = _runtime_attr(runtime, "mdc")
        domain_metadata_ui_cache_path = _runtime_attr(runtime, "_domain_metadata_ui_cache_path")
        logger = _runtime_attr(runtime, "logger")
        emit_domain_metadata_snapshot_changed = _runtime_attr(runtime, "_emit_domain_metadata_snapshot_changed")
        extract_domain_metadata_targets_from_entries = _runtime_attr(
            runtime,
            "_extract_domain_metadata_targets_from_entries",
        )

        validate_auth(request)
        payload_out = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        if not str(payload_out.get("updatedAt") or "").strip():
            payload_out["updatedAt"] = utc_timestamp()
        try:
            mdc.save_common_json_content(payload_out, domain_metadata_ui_cache_path())
        except Exception as exc:
            logger.warning("Failed to persist UI domain metadata cache: %s", exc)
            raise HTTPException(status_code=503, detail=f"Failed to persist UI domain metadata cache: {exc}") from exc

        emit_domain_metadata_snapshot_changed(
            "ui-cache-write",
            extract_domain_metadata_targets_from_entries(payload_out.get("entries") or {}),
        )

        return JSONResponse(
            payload_out,
            headers={
                "Cache-Control": "no-store",
                "X-Domain-Metadata-UI-Cache": "written",
                "X-Domain-Metadata-Entry-Count": str(len(payload_out.get("entries") or {})),
            },
        )

    return router, {
        "domain_metadata": domain_metadata,
        "domain_metadata_snapshot": domain_metadata_snapshot,
        "get_domain_metadata_snapshot_cache": get_domain_metadata_snapshot_cache,
        "put_domain_metadata_snapshot_cache": put_domain_metadata_snapshot_cache,
    }
