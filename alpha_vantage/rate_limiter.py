"""
Simple thread‑safe rate limiter.

Alpha Vantage enforces per‑minute request quotas on API keys.  To
ensure that client code does not exceed those quotas, this module
implements a lightweight token bucket that serializes outgoing
requests such that no more than ``rate_per_minute`` calls are made
within any sixty second window.  The limiter uses a mutex to
coordinate concurrent threads and sleeps only when necessary.

Note that this limiter does not account for second‑level burst limits
(e.g. a maximum number of calls per second) – it simply spaces calls
evenly over a minute.  If your subscription tier specifies more
complex rules you may wish to implement a more sophisticated
mechanism.
"""

import threading
import time
from typing import Optional


class RateLimiter:
    """A basic token bucket rate limiter.

    Parameters
    ----------
    rate_per_minute : int
        The number of requests permitted per minute.  A value of
        ``1`` means the client will wait one minute between calls,
        while ``60`` allows one call per second.  Values less than
        one are coerced to one.

    """

    def __init__(self, rate_per_minute: Optional[int] = None) -> None:
        rate = rate_per_minute or 1
        self.rate = max(1, rate)
        self.interval = 60.0 / float(self.rate)
        self._lock = threading.Lock()
        self._last_call = 0.0

    def wait(self) -> None:
        """Block until the next request is permitted.

        This method should be called immediately before making an
        outbound request.  It calculates how much time has elapsed
        since the previous request and sleeps just long enough to
        maintain the configured call rate.  Because it holds a lock
        while sleeping, concurrent threads will queue up and each call
        will respect the overall rate limit.
        """
        with self._lock:
            now = time.time()
            elapsed = now - self._last_call
            wait_time = self.interval - elapsed
            if wait_time > 0:
                time.sleep(wait_time)
            # update the timestamp to the current time (after waiting)
            self._last_call = time.time()