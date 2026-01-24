from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple

from core.postgres import PostgresError, connect

from api.service.run_store import RunRecord, RunStatus


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


RUN_COLUMNS = [
    "run_id",
    "status",
    "submitted_at",
    "started_at",
    "completed_at",
    "run_name",
    "start_date",
    "end_date",
    "output_dir",
    "adls_container",
    "adls_prefix",
    "error",
    "config_json",
    "effective_config_json",
]
RUN_COLUMNS_SQL = ", ".join(RUN_COLUMNS)


class PostgresRunStore:
    def __init__(self, dsn: str):
        value = (dsn or "").strip()
        if not value:
            raise ValueError("dsn is required")
        self._dsn = value

    def _connect(self):
        return connect(self._dsn)

    def init_db(self) -> None:
        """
        Validates connectivity and that repo-owned migrations have created required tables.
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('backtest.runs')")
                value = cur.fetchone()[0]
                if value is None:
                    raise PostgresError(
                        "Missing required table backtest.runs. Apply migrations via "
                        "deploy/apply_postgres_migrations.ps1."
                    )

    def ping(self) -> None:
        """
        Best-effort connectivity check for readiness probes.
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")

    def reconcile_incomplete_runs(self) -> int:
        """
        Marks queued/running runs as failed on service startup to avoid stale state.
        Returns the number of runs updated.
        """
        msg = "Service restarted; run was not completed."
        now = _utc_now()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE backtest.runs
                    SET status='failed',
                        completed_at=COALESCE(completed_at, %s),
                        error=COALESCE(error, %s)
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
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        INSERT INTO backtest.runs (
                            run_id, status, submitted_at, started_at, completed_at,
                            run_name, start_date, end_date,
                            output_dir, adls_container, adls_prefix,
                            error, config_json, effective_config_json
                        ) VALUES (%s, %s, %s, NULL, NULL, %s, %s, %s, %s, NULL, NULL, NULL, %s, %s);
                        """,
                        (
                            run_id,
                            status,
                            submitted,
                            run_name,
                            start_date,
                            end_date,
                            output_dir,
                            config_json,
                            effective_config_json,
                        ),
                    )
                except Exception as exc:
                    if _is_unique_violation(exc):
                        raise ValueError(f"run_id already exists: {run_id}") from exc
                    raise

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
            fields.append(("started_at", started_at))
        if completed_at is not None:
            fields.append(("completed_at", completed_at))
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

        set_clause = ", ".join([f"{name}=%s" for name, _ in fields])
        values = [value for _, value in fields] + [run_id]

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE backtest.runs SET {set_clause} WHERE run_id=%s;", values)
                if (cur.rowcount or 0) == 0:
                    raise KeyError(run_id)

    def get_run(self, run_id: str) -> RunRecord:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {RUN_COLUMNS_SQL} FROM backtest.runs WHERE run_id=%s;",
                    (run_id,),
                )
                row = cur.fetchone()
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
            where.append("status=%s")
            params.append(status)

        if query:
            q = query.strip()
            if q:
                where.append("(run_id ILIKE %s OR run_name ILIKE %s)")
                wildcard = f"%{q}%"
                params.extend([wildcard, wildcard])

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        params.extend([int(limit), int(offset)])

        sql = (
            f"SELECT {RUN_COLUMNS_SQL} FROM backtest.runs {where_sql} "
            "ORDER BY submitted_at DESC LIMIT %s OFFSET %s;"
        )
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        return [_row_to_record(row) for row in rows]


def _is_unique_violation(exc: Exception) -> bool:
    code = getattr(getattr(exc, "diag", None), "sqlstate", None)
    if code == "23505":
        return True
    return exc.__class__.__name__ in {"UniqueViolation", "IntegrityError"}


def _row_to_record(row: Tuple[Any, ...]) -> RunRecord:
    return RunRecord(
        run_id=str(row[0]),
        status=row[1],
        submitted_at=row[2] or _utc_now(),
        started_at=row[3],
        completed_at=row[4],
        run_name=row[5],
        start_date=row[6],
        end_date=row[7],
        output_dir=row[8],
        adls_container=row[9],
        adls_prefix=row[10],
        error=row[11],
        config_json=str(row[12]),
        effective_config_json=str(row[13]),
    )
