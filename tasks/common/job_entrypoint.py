from __future__ import annotations

import time
import traceback
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from typing import Any

from core import core as mdc


_LogFn = Callable[[str], None]
_RunFn = Callable[[], int | None]
_CallbackFn = Callable[[], Any]


def _default_info(message: str) -> None:
    mdc.write_line(message)


def _default_warning(message: str) -> None:
    mdc.write_warning(message)


def _default_error(message: str) -> None:
    mdc.write_error(message)


def run_logged_job(
    *,
    job_name: str,
    run: _RunFn,
    on_success: Sequence[_CallbackFn] = (),
    log_info: _LogFn | None = None,
    log_warning: _LogFn | None = None,
    log_error: _LogFn | None = None,
    log_exception: _LogFn | None = None,
) -> int:
    info = log_info or _default_info
    warning = log_warning or _default_warning
    error = log_error or _default_error

    started_at = datetime.now(timezone.utc)
    started = time.perf_counter()
    info(f"Job started: job={job_name} started_at={started_at.isoformat()}")

    try:
        raw_exit_code = run()
        exit_code = 0 if raw_exit_code is None else int(raw_exit_code)
        finished_at = datetime.now(timezone.utc)
        elapsed = time.perf_counter() - started
        if exit_code == 0:
            for callback in on_success:
                callback()
            info(
                "Job completed successfully: "
                f"job={job_name} exit_code={exit_code} finished_at={finished_at.isoformat()} "
                f"elapsed_sec={elapsed:.2f}"
            )
            return exit_code

        warning(
            "Job completed with failures: "
            f"job={job_name} exit_code={exit_code} finished_at={finished_at.isoformat()} "
            f"elapsed_sec={elapsed:.2f}"
        )
        return exit_code
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        elapsed = time.perf_counter() - started
        message = (
            "Job failed with exception: "
            f"job={job_name} error={type(exc).__name__}: {exc} "
            f"finished_at={finished_at.isoformat()} elapsed_sec={elapsed:.2f}"
        )
        if log_exception is not None:
            log_exception(message)
        else:
            error(f"{message}\n{traceback.format_exc()}")
        raise
