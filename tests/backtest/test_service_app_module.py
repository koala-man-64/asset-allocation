from __future__ import annotations

from fastapi import FastAPI

import backtest.service.app as app_module


def test_backtest_service_module_exports_app() -> None:
    assert isinstance(app_module.app, FastAPI)

