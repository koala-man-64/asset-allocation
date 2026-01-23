from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from scripts.common.blob_storage import BlobStorageClient

from backtest.service.run_store import RunRecord, RunStatus
from backtest.service.security import parse_container_and_path


logger = logging.getLogger("backtest.service.adls_run_store")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _dt_to_iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value else None


def _iso_to_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value)


class AdlsRunStore:
    """
    ADLS-backed run store using one JSON blob per run record.

    Remote layout:
      <container>/<prefix>/runs/<run_id>.json
    """

    def __init__(self, adls_dir: str, *, ensure_container_exists: bool = True):
        container, prefix = parse_container_and_path(adls_dir)
        self._container = container
        self._base_prefix = prefix.strip().strip("/")
        self._runs_prefix = f"{self._base_prefix}/runs".strip("/")
        self._client = BlobStorageClient(container_name=self._container, ensure_container_exists=ensure_container_exists)

    @property
    def container(self) -> str:
        return self._container

    @property
    def runs_prefix(self) -> str:
        return self._runs_prefix

    def init_db(self) -> None:
        # No-op; kept for interface parity with sqlite RunStore.
        return None

    def ping(self) -> None:
        """
        Best-effort connectivity check for readiness probes.
        """
        prefix = f"{self._runs_prefix.rstrip('/')}/"
        self._client.list_blob_infos(name_starts_with=prefix)

    def reconcile_incomplete_runs(self) -> int:
        now = _utc_now()
        msg = "Service restarted; run was not completed."
        updated = 0
        for record in self._list_all_records():
            if record.status not in {"queued", "running"}:
                continue
            try:
                self.update_run(
                    record.run_id,
                    status="failed",
                    completed_at=now,
                    error=record.error or msg,
                )
                updated += 1
            except Exception:
                logger.exception("Failed to reconcile run: run_id=%s", record.run_id)
        return updated

    def _record_path(self, run_id: str) -> str:
        return f"{self._runs_prefix.rstrip('/')}/{run_id}.json"

    def _serialize_record(self, record: RunRecord) -> bytes:
        payload: Dict[str, Any] = {
            "run_id": record.run_id,
            "status": record.status,
            "submitted_at": record.submitted_at.isoformat(),
            "started_at": _dt_to_iso(record.started_at),
            "completed_at": _dt_to_iso(record.completed_at),
            "run_name": record.run_name,
            "start_date": record.start_date,
            "end_date": record.end_date,
            "output_dir": record.output_dir,
            "adls_container": record.adls_container,
            "adls_prefix": record.adls_prefix,
            "error": record.error,
            "config_json": record.config_json,
            "effective_config_json": record.effective_config_json,
        }
        return json.dumps(payload, sort_keys=True).encode("utf-8")

    def _deserialize_record(self, data: bytes) -> RunRecord:
        raw = json.loads(data.decode("utf-8"))
        return RunRecord(
            run_id=str(raw["run_id"]),
            status=raw["status"],
            submitted_at=_iso_to_dt(raw.get("submitted_at")) or _utc_now(),
            started_at=_iso_to_dt(raw.get("started_at")),
            completed_at=_iso_to_dt(raw.get("completed_at")),
            run_name=raw.get("run_name"),
            start_date=raw.get("start_date"),
            end_date=raw.get("end_date"),
            output_dir=raw.get("output_dir"),
            adls_container=raw.get("adls_container"),
            adls_prefix=raw.get("adls_prefix"),
            error=raw.get("error"),
            config_json=str(raw.get("config_json") or ""),
            effective_config_json=str(raw.get("effective_config_json") or ""),
        )

    def _list_all_records(self) -> List[RunRecord]:
        records: List[RunRecord] = []
        prefix = f"{self._runs_prefix.rstrip('/')}/"
        for blob in self._client.list_blob_infos(name_starts_with=prefix):
            name = blob.get("name")
            if not name or name.endswith("/"):
                continue
            if not name.endswith(".json"):
                continue
            data = self._client.download_data(name)
            if data is None:
                continue
            try:
                records.append(self._deserialize_record(data))
            except Exception:
                logger.exception("Failed to parse run record: blob=%s", name)
        return records

    def create_run(
        self,
        *,
        run_id: str,
        status: RunStatus,
        submitted_at: Optional[datetime] = None,
        run_name: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
        output_dir: Optional[str],
        config_json: str,
        effective_config_json: str,
    ) -> None:
        path = self._record_path(run_id)
        if self._client.file_exists(path):
            raise ValueError(f"run_id already exists: {run_id}")

        record = RunRecord(
            run_id=run_id,
            status=status,
            submitted_at=submitted_at or _utc_now(),
            started_at=None,
            completed_at=None,
            run_name=run_name,
            start_date=start_date,
            end_date=end_date,
            output_dir=output_dir,
            adls_container=None,
            adls_prefix=None,
            error=None,
            config_json=config_json,
            effective_config_json=effective_config_json,
        )
        self._client.upload_data(path, self._serialize_record(record), overwrite=True)

    def update_run(
        self,
        run_id: str,
        *,
        status: Optional[RunStatus] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        output_dir: Optional[str] = None,
        adls_container: Optional[str] = None,
        adls_prefix: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        existing = self.get_run(run_id)
        updated = RunRecord(
            run_id=existing.run_id,
            status=status or existing.status,
            submitted_at=existing.submitted_at,
            started_at=started_at if started_at is not None else existing.started_at,
            completed_at=completed_at if completed_at is not None else existing.completed_at,
            run_name=existing.run_name,
            start_date=existing.start_date,
            end_date=existing.end_date,
            output_dir=output_dir if output_dir is not None else existing.output_dir,
            adls_container=adls_container if adls_container is not None else existing.adls_container,
            adls_prefix=adls_prefix if adls_prefix is not None else existing.adls_prefix,
            error=error if error is not None else existing.error,
            config_json=existing.config_json,
            effective_config_json=existing.effective_config_json,
        )
        self._client.upload_data(self._record_path(run_id), self._serialize_record(updated), overwrite=True)

    def get_run(self, run_id: str) -> RunRecord:
        data = self._client.download_data(self._record_path(run_id))
        if data is None:
            raise KeyError(run_id)
        return self._deserialize_record(data)

    def list_runs(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        status: Optional[RunStatus] = None,
        query: Optional[str] = None,
    ) -> List[RunRecord]:
        records = self._list_all_records()

        if status:
            records = [r for r in records if r.status == status]

        if query:
            q = query.strip()
            if q:
                q_lower = q.lower()
                records = [
                    r
                    for r in records
                    if q_lower in r.run_id.lower() or (r.run_name and q_lower in r.run_name.lower())
                ]

        records.sort(key=lambda r: r.submitted_at, reverse=True)
        start = int(offset)
        end = start + int(limit)
        return records[start:end]
