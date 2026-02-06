from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx
from fastapi import FastAPI


@asynccontextmanager
async def get_test_client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    """
    Async test client for FastAPI/Starlette apps.

    We intentionally avoid starlette.testclient.TestClient because it relies on
    thread + socket wakeups (anyio.from_thread), which are blocked in this
    sandboxed environment.
    """

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client

