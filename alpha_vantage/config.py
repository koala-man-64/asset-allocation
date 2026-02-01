"""
Alpha Vantage configuration support.

This module defines the :class:`AlphaVantageConfig` dataclass which
encapsulates the essential configuration parameters required to
interact with the Alpha Vantage API.  These parameters include the
API key, base URL, request timeout, maximum number of concurrent
workers and the allowed number of calls per minute.  The default
values reflect conservative settings that work well with the free
tier of Alpha Vantage.  You should increase ``rate_limit_per_min``
and ``max_workers`` to match your subscription tier and hardware
capabilities.

Example
-------

    >>> from alpha_vantage_client.config import AlphaVantageConfig
    >>> cfg = AlphaVantageConfig(api_key="demo", rate_limit_per_min=60, max_workers=5)
    >>> cfg.get_query_url()
    'https://www.alphavantage.co/query'
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class AlphaVantageConfig:
    """Configuration container for the Alpha Vantage client.

    Attributes
    ----------
    api_key:
        The API key issued by Alpha Vantage.  You can obtain one for
        free at https://www.alphavantage.co.  This key is required for
        every request.

    base_url:
        The base URL of the Alpha Vantage service.  If you run a
        proxy or mirror you can override this value, otherwise the
        default ``https://www.alphavantage.co`` should be used.

    rate_limit_per_min:
        Maximum number of requests permitted per minute.  The free
        Alpha Vantage tier allows only a small number of calls each
        day【7†L49-L53】, while premium tiers support higher limits【6†L2153-L2156】.
        Set this value to match your subscription to avoid throttling.

    max_workers:
        Number of concurrent workers used when fetching multiple
        endpoints in parallel.  Increasing this value can reduce
        overall runtime when pulling many symbols, but it should not
        exceed your rate limit.

    timeout:
        Timeout in seconds for individual HTTP requests.  Requests
        taking longer than this will raise an exception.
    """

    api_key: str
    base_url: str = "https://www.alphavantage.co"
    rate_limit_per_min: int = 5
    max_workers: int = 1
    timeout: float = 10.0

    def get_query_url(self) -> str:
        """Return the full query endpoint for the API.

        The Alpha Vantage API is accessed via a single endpoint
        ``/query``; all parameters are passed as query string
        arguments.  This method composes the base URL and the
        endpoint path.

        Returns
        -------
        str
            Full URL to the query endpoint.
        """
        return f"{self.base_url.rstrip('/')}/query"