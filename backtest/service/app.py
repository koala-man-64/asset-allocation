"""
Deprecated: this module used to host the Backtest API FastAPI app.

The service entrypoint now lives at `services.backtest_api.app` so the deployed API
is not rooted under the `backtest` package.
"""

from services.backtest_api.app import app, create_app  # noqa: F401
