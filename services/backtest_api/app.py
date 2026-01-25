"""
Deprecated compatibility shim.

The canonical FastAPI entrypoint lives at `api.service.app:app` (see tests and
Dockerfile). Keep this module to avoid breaking older imports.
"""

from api.service.app import app, create_app  # noqa: F401
