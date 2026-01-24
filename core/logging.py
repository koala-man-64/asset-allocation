from __future__ import annotations

import logging
import os
from typing import Optional

from core.logging_config import JsonFormatter, configure_logging


def setup_logging(level: str = "INFO", json_format: bool = False, log_file: Optional[str] = None) -> None:
    """
    Backward-compatible wrapper around `core.logging_config.configure_logging()`.

    `LOG_FORMAT`/`LOG_LEVEL` remain the source of truth; this helper only sets
    defaults when they are not already provided by the environment.
    """
    os.environ.setdefault("LOG_LEVEL", level)
    os.environ.setdefault("LOG_FORMAT", "JSON" if json_format else "TEXT")

    logger = configure_logging()

    if not log_file:
        return

    existing = {
        getattr(handler, "baseFilename", None)
        for handler in logger.handlers
        if isinstance(handler, logging.FileHandler)
    }
    if log_file in existing:
        return

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logger.level)
    if (os.environ.get("LOG_FORMAT") or "").strip().upper() == "JSON":
        file_handler.setFormatter(JsonFormatter())
    else:
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(module)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
    logger.addHandler(file_handler)
