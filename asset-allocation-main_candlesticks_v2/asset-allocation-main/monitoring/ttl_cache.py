from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable, Generic, Optional, TypeVar


T = TypeVar("T")


@dataclass(frozen=True)
class CacheGetResult(Generic[T]):
    value: T
    cache_hit: bool
    refresh_error: Optional[Exception]


class TtlCache(Generic[T]):
    """
    Small in-memory TTL cache with refresh coalescing and stale-on-error behavior.

    - If the cached value is fresh, returns it immediately.
    - If refresh fails and a cached value exists, returns the cached value and surfaces the error.
    - If refresh fails and no cached value exists, re-raises the refresh error.
    """

    def __init__(self, ttl_seconds: float, *, time_fn: Callable[[], float] = time.monotonic) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        self._ttl_seconds = float(ttl_seconds)
        self._time_fn = time_fn
        self._value: Optional[T] = None
        self._expires_at: float = 0.0
        self._refreshing = False
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)

    @property
    def ttl_seconds(self) -> float:
        return float(self._ttl_seconds)

    def set_ttl_seconds(self, ttl_seconds: float) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")

        with self._lock:
            self._ttl_seconds = float(ttl_seconds)
            # Ensure an existing cached value cannot outlive the new TTL.
            now = self._time_fn()
            if self._value is not None:
                self._expires_at = min(self._expires_at, now + self._ttl_seconds)

    def get(self, refresh_fn: Callable[[], T], *, force_refresh: bool = False) -> CacheGetResult[T]:
        now = self._time_fn()

        with self._lock:
            if not force_refresh and self._value is not None and now < self._expires_at:
                return CacheGetResult(value=self._value, cache_hit=True, refresh_error=None)

            if self._refreshing:
                # Another request is refreshing. Wait briefly and then re-check cache.
                self._cond.wait(timeout=5.0)
                now = self._time_fn()
                if not force_refresh and self._value is not None and now < self._expires_at:
                    return CacheGetResult(value=self._value, cache_hit=True, refresh_error=None)

            self._refreshing = True

        try:
            value = refresh_fn()
        except Exception as exc:
            with self._lock:
                self._refreshing = False
                self._cond.notify_all()
                if self._value is not None:
                    return CacheGetResult(value=self._value, cache_hit=True, refresh_error=exc)
            raise

        with self._lock:
            self._value = value
            self._expires_at = self._time_fn() + self._ttl_seconds
            self._refreshing = False
            self._cond.notify_all()
            return CacheGetResult(value=value, cache_hit=False, refresh_error=None)

