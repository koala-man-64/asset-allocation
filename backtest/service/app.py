"""
Compatibility shim for legacy entrypoints. 
Re-exports the application instance from the new consolidated api/ location.
"""
from api.service.app import app

__all__ = ["app"]
