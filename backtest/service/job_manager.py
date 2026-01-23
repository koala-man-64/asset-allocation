from __future__ import annotations

import logging
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from backtest.config import BacktestConfig
from backtest.runner import run_backtest
from backtest.service.adls_uploader import upload_run_artifacts
from backtest.service.run_store import RunStore


logger = logging.getLogger("backtest.service")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class JobManager:
    def __init__(
        self,
        *,
        store: RunStore,
        output_base_dir: Path,
        max_workers: int,
        default_adls_dir: Optional[str] = None,
    ):
        self._store = store
        self._output_base_dir = Path(output_base_dir)
        self._default_adls_dir = default_adls_dir
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="backtest")
        self._futures: Dict[str, Future[None]] = {}

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=False)

    def submit(self, *, run_id: str, config: BacktestConfig) -> None:
        """
        Submits a run to the internal executor.
        """
        if run_id in self._futures:
            return

        def _run() -> None:
            started_at = _utc_now()
            try:
                self._store.update_run(run_id, status="running", started_at=started_at)

                # Ensure artifacts reflect the effective output base dir in config.yaml.
                requested_adls_dir = config.output.adls_dir
                resolved_adls_dir = requested_adls_dir or self._default_adls_dir
                effective = replace(
                    config,
                    output=replace(
                        config.output,
                        local_dir=str(self._output_base_dir),
                        adls_dir=None,
                    ),
                )

                result = run_backtest(
                    effective,
                    run_id=run_id,
                    output_base_dir=self._output_base_dir,
                )

                # Optional artifact upload.
                if resolved_adls_dir:
                    upload = upload_run_artifacts(
                        run_id=run_id,
                        run_dir=result.output_dir,
                        adls_dir=resolved_adls_dir,
                    )
                    self._store.update_run(
                        run_id,
                        adls_container=upload.container,
                        adls_prefix=upload.prefix,
                    )

                completed_at = _utc_now()
                self._store.update_run(
                    run_id,
                    status="completed",
                    completed_at=completed_at,
                    output_dir=str(result.output_dir),
                )
            except Exception as exc:
                completed_at = _utc_now()
                logger.exception("Backtest run failed: run_id=%s", run_id)
                try:
                    self._store.update_run(
                        run_id,
                        status="failed",
                        completed_at=completed_at,
                        error=str(exc),
                    )
                except Exception:
                    logger.exception("Failed to persist run failure: run_id=%s", run_id)

        future = self._executor.submit(_run)
        self._futures[run_id] = future

        def _cleanup(_: Future[None]) -> None:
            self._futures.pop(run_id, None)

        future.add_done_callback(_cleanup)
