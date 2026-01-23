from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple


RunStatus = Literal["queued", "running", "completed", "failed"]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _dt_to_iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value else None


def _iso_to_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value)


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    status: RunStatus
    submitted_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    run_name: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    output_dir: Optional[str]
    adls_container: Optional[str]
    adls_prefix: Optional[str]
    error: Optional[str]
    config_json: str
    effective_config_json: str

    def to_public_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "submitted_at": self.submitted_at.isoformat(),
            "started_at": _dt_to_iso(self.started_at),
            "completed_at": _dt_to_iso(self.completed_at),
            "run_name": self.run_name,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "output_dir": self.output_dir,
            "adls_container": self.adls_container,
            "adls_prefix": self.adls_prefix,
            "error": self.error,
        }


class RunStore:
    def __init__(self, db_path: Path):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def db_path(self) -> Path:
        return self._db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path), timeout=30, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    submitted_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    run_name TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    output_dir TEXT,
                    adls_container TEXT,
                    adls_prefix TEXT,
                    error TEXT,
                    config_json TEXT NOT NULL,
                    effective_config_json TEXT NOT NULL
                );
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_submitted_at ON runs(submitted_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);")

    def reconcile_incomplete_runs(self) -> int:
        """
        Marks queued/running runs as failed on service startup to avoid stale state.
        Returns the number of runs updated.
        """
        now = _utc_now().isoformat()
        msg = "Service restarted; run was not completed."
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE runs
                SET status='failed',
                    completed_at=COALESCE(completed_at, ?),
                    error=COALESCE(error, ?)
                WHERE status IN ('queued', 'running');
                """,
                (now, msg),
            )
            return int(cur.rowcount or 0)

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
        submitted = submitted_at or _utc_now()
        with self._connect() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO runs (
                        run_id, status, submitted_at, started_at, completed_at,
                        run_name, start_date, end_date,
                        output_dir, adls_container, adls_prefix,
                        error, config_json, effective_config_json
                    ) VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?);
                    """,
                    (
                        run_id,
                        status,
                        submitted.isoformat(),
                        run_name,
                        start_date,
                        end_date,
                        output_dir,
                        config_json,
                        effective_config_json,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError(f"run_id already exists: {run_id}") from exc

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
        fields: List[Tuple[str, Any]] = []
        if status is not None:
            fields.append(("status", status))
        if started_at is not None:
            fields.append(("started_at", started_at.isoformat()))
        if completed_at is not None:
            fields.append(("completed_at", completed_at.isoformat()))
        if output_dir is not None:
            fields.append(("output_dir", output_dir))
        if adls_container is not None:
            fields.append(("adls_container", adls_container))
        if adls_prefix is not None:
            fields.append(("adls_prefix", adls_prefix))
        if error is not None:
            fields.append(("error", error))

        if not fields:
            return

        set_clause = ", ".join([f"{name}=?" for name, _ in fields])
        values = [value for _, value in fields] + [run_id]

        with self._connect() as conn:
            cur = conn.execute(f"UPDATE runs SET {set_clause} WHERE run_id=?;", values)
            if (cur.rowcount or 0) == 0:
                raise KeyError(run_id)

    def get_run(self, run_id: str) -> RunRecord:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id=?;", (run_id,)).fetchone()
        if row is None:
            raise KeyError(run_id)
        return _row_to_record(row)

    def list_runs(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        status: Optional[RunStatus] = None,
        query: Optional[str] = None,
    ) -> List[RunRecord]:
        where = []
        params: List[Any] = []

        if status:
            where.append("status=?")
            params.append(status)

        if query:
            q = f"%{query.strip()}%"
            where.append("(run_id LIKE ? OR run_name LIKE ?)")
            params.extend([q, q])

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        params.extend([int(limit), int(offset)])

        sql = f"SELECT * FROM runs {where_sql} ORDER BY submitted_at DESC LIMIT ? OFFSET ?;"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_row_to_record(r) for r in rows]


def _row_to_record(row: sqlite3.Row) -> RunRecord:
    return RunRecord(
        run_id=str(row["run_id"]),
        status=row["status"],
        submitted_at=_iso_to_dt(row["submitted_at"]) or _utc_now(),
        started_at=_iso_to_dt(row["started_at"]),
        completed_at=_iso_to_dt(row["completed_at"]),
        run_name=row["run_name"],
        start_date=row["start_date"],
        end_date=row["end_date"],
        output_dir=row["output_dir"],
        adls_container=row["adls_container"],
        adls_prefix=row["adls_prefix"],
        error=row["error"],
        config_json=str(row["config_json"]),
        effective_config_json=str(row["effective_config_json"]),
    )
