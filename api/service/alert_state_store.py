from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional

from core.postgres import PostgresError, connect


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class AlertState:
    alert_id: str
    acknowledged_at: Optional[datetime]
    acknowledged_by: Optional[str]
    snoozed_until: Optional[datetime]
    resolved_at: Optional[datetime]
    resolved_by: Optional[str]


class PostgresAlertStateStore:
    def __init__(self, dsn: str):
        value = (dsn or "").strip()
        if not value:
            raise ValueError("dsn is required")
        self._dsn = value

    def _connect(self):
        return connect(self._dsn)

    def init_db(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('monitoring.alert_state')")
                value = cur.fetchone()[0]
                if value is None:
                    raise PostgresError(
                        "Missing required table monitoring.alert_state. Apply migrations via "
                        "deploy/apply_postgres_migrations.ps1."
                    )

    def get_states(self, alert_ids: Iterable[str]) -> Dict[str, AlertState]:
        ids = [str(alert_id).strip() for alert_id in alert_ids if str(alert_id).strip()]
        if not ids:
            return {}
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT alert_id, acknowledged_at, acknowledged_by, snoozed_until, resolved_at, resolved_by
                    FROM monitoring.alert_state
                    WHERE alert_id = ANY(%s)
                    """,
                    (ids,),
                )
                rows = cur.fetchall()

        out: Dict[str, AlertState] = {}
        for row in rows:
            out[str(row[0])] = AlertState(
                alert_id=str(row[0]),
                acknowledged_at=row[1],
                acknowledged_by=row[2],
                snoozed_until=row[3],
                resolved_at=row[4],
                resolved_by=row[5],
            )
        return out

    def acknowledge(self, alert_id: str, *, actor: Optional[str]) -> AlertState:
        resolved = str(alert_id).strip()
        if not resolved:
            raise ValueError("alert_id is required")
        now = _utc_now()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO monitoring.alert_state(alert_id, acknowledged_at, acknowledged_by)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (alert_id) DO UPDATE
                    SET acknowledged_at = EXCLUDED.acknowledged_at,
                        acknowledged_by = EXCLUDED.acknowledged_by
                    """,
                    (resolved, now, actor),
                )
        return self.get_states([resolved]).get(resolved) or AlertState(
            alert_id=resolved,
            acknowledged_at=now,
            acknowledged_by=actor,
            snoozed_until=None,
            resolved_at=None,
            resolved_by=None,
        )

    def snooze(self, alert_id: str, *, until: datetime, actor: Optional[str]) -> AlertState:
        resolved = str(alert_id).strip()
        if not resolved:
            raise ValueError("alert_id is required")
        if until.tzinfo is None:
            until = until.replace(tzinfo=timezone.utc)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO monitoring.alert_state(alert_id, snoozed_until, acknowledged_by)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (alert_id) DO UPDATE
                    SET snoozed_until = EXCLUDED.snoozed_until,
                        acknowledged_by = COALESCE(EXCLUDED.acknowledged_by, monitoring.alert_state.acknowledged_by)
                    """,
                    (resolved, until, actor),
                )
        return self.get_states([resolved]).get(resolved) or AlertState(
            alert_id=resolved,
            acknowledged_at=None,
            acknowledged_by=actor,
            snoozed_until=until,
            resolved_at=None,
            resolved_by=None,
        )

    def resolve(self, alert_id: str, *, actor: Optional[str]) -> AlertState:
        resolved = str(alert_id).strip()
        if not resolved:
            raise ValueError("alert_id is required")
        now = _utc_now()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO monitoring.alert_state(alert_id, resolved_at, resolved_by)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (alert_id) DO UPDATE
                    SET resolved_at = EXCLUDED.resolved_at,
                        resolved_by = EXCLUDED.resolved_by
                    """,
                    (resolved, now, actor),
                )
        return self.get_states([resolved]).get(resolved) or AlertState(
            alert_id=resolved,
            acknowledged_at=None,
            acknowledged_by=None,
            snoozed_until=None,
            resolved_at=now,
            resolved_by=actor,
        )

