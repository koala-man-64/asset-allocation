"""
High‑level Alpha Vantage API client.

This module defines the :class:`AlphaVantageClient` class which
exposes methods corresponding to the various functions described in
the official Alpha Vantage documentation【23†L141-L149】【34†L374-L382】.  The
client takes care of appending your API key to each request, waiting
between calls to respect rate limits and optionally fetching multiple
symbols in parallel.

Typical usage looks like this::

    from alpha_vantage_client import AlphaVantageClient, AlphaVantageConfig

    cfg = AlphaVantageConfig(api_key="YOUR_KEY", rate_limit_per_min=60, max_workers=5)
    av = AlphaVantageClient(cfg)
    # Fetch daily data for a single symbol
    data = av.get_daily_time_series("AAPL", outputsize="full")
    # Convert to DataFrame
    df = av.parse_time_series(data)
    # Fetch multiple symbols concurrently
    requests = [
        {"function": "TIME_SERIES_DAILY", "symbol": "MSFT", "outputsize": "compact"},
        {"function": "TIME_SERIES_DAILY", "symbol": "TSLA", "outputsize": "compact"},
    ]
    results = av.fetch_many(requests)

See the ``api_keys`` page on Alpha Vantage for rate limits and
subscription tiers【7†L49-L53】【6†L2153-L2156】.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional, Union

import httpx

from .config import AlphaVantageConfig
from .rate_limiter import RateLimiter
from .utils import parse_time_series, parse_financial_reports


logger = logging.getLogger(__name__)


class AlphaVantageClient:
    """Client for interacting with the Alpha Vantage REST API.

    Parameters
    ----------
    config : AlphaVantageConfig
        Configuration object holding your API key, rate limit and
        connection settings.
    """

    def __init__(self, config: AlphaVantageConfig) -> None:
        self.config = config
        self._rate_limiter = RateLimiter(config.rate_limit_per_min)
        # httpx will attempt to use proxy settings from the environment by
        # default.  In containerized deployments this can result in an
        # ImportError when the optional ``socksio`` dependency is not
        # installed.  Setting ``trust_env=False`` prevents httpx from
        # reading proxy configuration from the environment and avoids
        # that error.
        self._client = httpx.Client(timeout=config.timeout, trust_env=False)
        self._query_url = config.get_query_url()

    def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        self._client.close()

    def _request(self, params: Dict[str, Any], raw: bool = False) -> Union[Dict[str, Any], str]:
        """Perform a GET request to the Alpha Vantage API.

        Parameters
        ----------
        params : dict
            Dictionary of query parameters.  Must contain at least a
            ``"function"`` entry.
        raw : bool, optional
            If ``True``, return the raw response text instead of
            attempting to parse JSON.  Use this for CSV endpoints.

        Returns
        -------
        dict or str
            Parsed JSON object or raw text depending on ``raw``.

        Raises
        ------
        httpx.HTTPStatusError
            If the response indicates an HTTP error.  Alpha Vantage
            returns ``200 OK`` for most errors, in which case the
            message will be contained in the JSON payload.
        """
        self._rate_limiter.wait()
        # Copy the parameters to avoid side effects
        query_params = dict(params)
        # Always append the API key
        query_params["apikey"] = self.config.api_key
        response = self._client.get(self._query_url, params=query_params)
        response.raise_for_status()
        if raw:
            return response.text
        try:
            return response.json()
        except ValueError:
            # If JSON parsing fails, return the raw text
            return response.text

    # ------------------------------------------------------------------
    # Generic request helpers
    # ------------------------------------------------------------------
    def fetch(self, function: str, symbol: Optional[str] = None, **params: Any) -> Dict[str, Any]:
        """Fetch a JSON response from any Alpha Vantage endpoint.

        Parameters
        ----------
        function : str
            The API function name (e.g. ``'TIME_SERIES_DAILY'``).
        symbol : str, optional
            The primary symbol for the request.  Many endpoints
            require this argument; for functions that do not take a
            symbol (e.g. macroeconomic indicators) set this to
            ``None``.
        **params : dict, optional
            Additional query parameters as documented by Alpha
            Vantage (e.g. ``interval``, ``outputsize``, ``datatype``).

        Returns
        -------
        dict
            The parsed JSON response.
        """
        query_params: Dict[str, Any] = {"function": function}
        if symbol:
            query_params["symbol"] = symbol
        # Merge additional parameters
        query_params.update(params)
        return self._request(query_params, raw=False)

    def fetch_csv(self, function: str, symbol: Optional[str] = None, **params: Any) -> str:
        """Fetch a CSV response from any Alpha Vantage endpoint.

        Alpha Vantage supports a ``datatype=csv`` parameter for many
        functions.  When this helper is used the raw CSV text is
        returned instead of JSON.

        Parameters
        ----------
        function : str
            API function name.
        symbol : str, optional
            Primary symbol for the request.
        **params : dict
            Additional query parameters.

        Returns
        -------
        str
            Raw CSV data as returned by the API.
        """
        query_params: Dict[str, Any] = {"function": function, "datatype": "csv"}
        if symbol:
            query_params["symbol"] = symbol
        query_params.update(params)
        return self._request(query_params, raw=True)

    def fetch_many(self, request_params: Iterable[Dict[str, Any]]) -> List[Union[Dict[str, Any], str]]:
        """Fetch multiple endpoints concurrently.

        This method accepts an iterable of parameter dictionaries.  Each
        dictionary must contain at least a ``"function"`` key and may
        optionally contain a ``"symbol"`` entry and any additional
        parameters supported by the API.  The calls will be executed
        concurrently using a thread pool limited by
        ``config.max_workers``.  Results are returned in the same order
        as the input sequence.

        Because all workers share the same rate limiter, the overall
        throughput will never exceed the configured calls per minute.

        Parameters
        ----------
        request_params : iterable of dict
            Each dict describes one API call with keys ``"function"``,
            ``"symbol"`` and other parameters.

        Returns
        -------
        list of dict or str
            List of parsed JSON objects or raw CSV strings in the same
            order as provided.
        """
        results: List[Union[Dict[str, Any], str]] = [None] * len(list(request_params))
        # Convert to list so we can index into it
        reqs = list(request_params)

        def worker(index: int, params: Dict[str, Any]) -> Union[Dict[str, Any], str]:
            # Unpack function and symbol from the dict; copy so we don't mutate the caller's data
            params_copy = dict(params)
            func = params_copy.pop("function")
            symbol = params_copy.pop("symbol", None)
            # Determine if CSV is requested based on explicit datatype
            datatype = params_copy.get("datatype")
            if datatype and str(datatype).lower() == "csv":
                return self.fetch_csv(func, symbol, **params_copy)
            return self.fetch(func, symbol, **params_copy)

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            future_to_index = {}
            for idx, params in enumerate(reqs):
                future_to_index[executor.submit(worker, idx, params)] = idx
            for future in as_completed(future_to_index):
                idx = future_to_index[future]
                try:
                    results[idx] = future.result()
                except Exception as e:
                    results[idx] = {"error": str(e)}
        return results

    # ------------------------------------------------------------------
    # High‑level convenience methods
    # ------------------------------------------------------------------
    def get_daily_time_series(
        self,
        symbol: str,
        outputsize: str = "compact",
        adjusted: bool = False,
        datatype: str = "json",
    ) -> Union[Dict[str, Any], str]:
        """Retrieve daily open/high/low/close/volume data for a symbol.

        Parameters
        ----------
        symbol : str
            Ticker symbol to query (e.g. ``"AAPL"``).
        outputsize : {'compact', 'full'}, optional
            ``'compact'`` returns only the latest 100 data points,
            whereas ``'full'`` returns the entire available history【34†L395-L402】.
        adjusted : bool, optional
            If ``True``, return adjusted closing prices (dividend and
            split adjusted) using the ``TIME_SERIES_DAILY_ADJUSTED``
            function.
        datatype : {'json', 'csv'}, optional
            Format of the response.  When ``'csv'``, the raw CSV text
            is returned.  Otherwise a JSON object is returned.

        Returns
        -------
        dict or str
            Parsed JSON response or raw CSV text.
        """
        function = "TIME_SERIES_DAILY_ADJUSTED" if adjusted else "TIME_SERIES_DAILY"
        params = {"symbol": symbol, "outputsize": outputsize}
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol, **params)
        return self.fetch(function, symbol, **params)

    def get_weekly_time_series(
        self, symbol: str, adjusted: bool = False, datatype: str = "json"
    ) -> Union[Dict[str, Any], str]:
        """Retrieve weekly OHLCV data for a symbol.

        Parameters
        ----------
        symbol : str
            Ticker symbol.
        adjusted : bool, optional
            Whether to request the adjusted time series.
        datatype : {'json', 'csv'}, optional
            Response format.

        Returns
        -------
        dict or str
            Parsed JSON or raw CSV.
        """
        function = "TIME_SERIES_WEEKLY_ADJUSTED" if adjusted else "TIME_SERIES_WEEKLY"
        params = {"symbol": symbol}
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol, **params)
        return self.fetch(function, symbol, **params)

    def get_monthly_time_series(
        self, symbol: str, adjusted: bool = False, datatype: str = "json"
    ) -> Union[Dict[str, Any], str]:
        """Retrieve monthly OHLCV data for a symbol."""
        function = "TIME_SERIES_MONTHLY_ADJUSTED" if adjusted else "TIME_SERIES_MONTHLY"
        params = {"symbol": symbol}
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol, **params)
        return self.fetch(function, symbol, **params)

    def get_intraday_time_series(
        self,
        symbol: str,
        interval: str = "5min",
        outputsize: str = "compact",
        month: Optional[str] = None,
        datatype: str = "json",
    ) -> Union[Dict[str, Any], str]:
        """Retrieve intraday price series for a symbol.

        Alpha Vantage supports various intervals (e.g. 1min, 5min,
        15min, 30min, 60min).  The ``outputsize`` parameter for
        intraday data defaults to the last 30 days; specifying
        ``month`` allows retrieving a particular historical month up to
        20 years back for premium plans【23†L193-L202】.

        Parameters
        ----------
        symbol : str
            Ticker symbol.
        interval : str, optional
            Time step between points ("1min", "5min", etc.).
        outputsize : {'compact', 'full'}, optional
            Data volume to return.  ``'full'`` is only available for
            premium keys for intraday data.
        month : str, optional
            A specific month in ``YYYY-MM`` format to fetch historical
            data.  Requires premium subscription.
        datatype : {'json', 'csv'}, optional
            Response format.

        Returns
        -------
        dict or str
            Parsed JSON or raw CSV.
        """
        function = "TIME_SERIES_INTRADAY"
        params: Dict[str, Any] = {"symbol": symbol, "interval": interval, "outputsize": outputsize}
        if month:
            params["month"] = month
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol, **params)
        return self.fetch(function, symbol, **params)

    def get_fx_time_series(
        self,
        from_symbol: str,
        to_symbol: str,
        interval: str = "daily",
        outputsize: str = "compact",
        datatype: str = "json",
    ) -> Union[Dict[str, Any], str]:
        """Retrieve FX exchange rate series between two currencies.

        Valid intervals are ``'daily'``, ``'weekly'`` and ``'monthly'``;
        intraday FX series are premium only and are not exposed here.

        Parameters
        ----------
        from_symbol : str
            Base currency (e.g. ``"EUR"``).
        to_symbol : str
            Quote currency (e.g. ``"USD"``).
        interval : {'daily', 'weekly', 'monthly'}, optional
            Frequency of the data.
        outputsize : {'compact', 'full'}, optional
            Number of points to return.
        datatype : {'json', 'csv'}, optional
            Response format.

        Returns
        -------
        dict or str
            Parsed JSON or raw CSV.
        """
        function_map = {
            "daily": "FX_DAILY",
            "weekly": "FX_WEEKLY",
            "monthly": "FX_MONTHLY",
        }
        function = function_map.get(interval.lower()) or "FX_DAILY"
        params: Dict[str, Any] = {
            "from_symbol": from_symbol,
            "to_symbol": to_symbol,
            "outputsize": outputsize,
        }
        # For FX functions, the "symbol" parameter is not used
        if datatype.lower() == "csv":
            return self.fetch_csv(function, None, **params)
        return self.fetch(function, None, **params)

    def get_crypto_time_series(
        self,
        symbol: str,
        market: str = "USD",
        interval: str = "daily",
        datatype: str = "json",
    ) -> Union[Dict[str, Any], str]:
        """Retrieve cryptocurrency price series for a given market.

        Supported intervals are ``'daily'``, ``'weekly'`` and ``'monthly'``.

        Parameters
        ----------
        symbol : str
            Cryptocurrency ticker (e.g. ``"BTC"``).
        market : str, optional
            Quoted currency (e.g. ``"USD"``, ``"EUR"``).
        interval : {'daily', 'weekly', 'monthly'}, optional
            Frequency of the data.
        datatype : {'json', 'csv'}, optional
            Response format.

        Returns
        -------
        dict or str
            Parsed JSON or raw CSV.
        """
        function_map = {
            "daily": "DIGITAL_CURRENCY_DAILY",
            "weekly": "DIGITAL_CURRENCY_WEEKLY",
            "monthly": "DIGITAL_CURRENCY_MONTHLY",
        }
        function = function_map.get(interval.lower()) or "DIGITAL_CURRENCY_DAILY"
        params: Dict[str, Any] = {"symbol": symbol, "market": market}
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol, **params)
        return self.fetch(function, symbol, **params)

    def get_technical_indicator(
        self,
        indicator: str,
        symbol: str,
        interval: str,
        series_type: str = "close",
        time_period: Optional[int] = None,
        datatype: str = "json",
        **kwargs: Any,
    ) -> Union[Dict[str, Any], str]:
        """Retrieve a technical indicator series.

        Alpha Vantage supports dozens of technical analysis functions
        (e.g. SMA, EMA, RSI, MACD).  The generic API uses the
        indicator name as the function parameter.  In addition to the
        standard arguments documented here, many indicators accept
        extra parameters (e.g. ``series_type``, ``time_period``,
        ``slow_period``, ``fast_period``).  Any additional
        keyword arguments passed to this method will be forwarded
        directly to the API.

        Parameters
        ----------
        indicator : str
            The indicator function name (e.g. ``"SMA"``, ``"EMA"``).
        symbol : str
            The symbol to calculate the indicator for.
        interval : str
            The time frame ("1min", "5min", "daily", etc.).
        series_type : {'open', 'high', 'low', 'close'}, optional
            Which price field to use.  Not all indicators require this.
        time_period : int, optional
            The number of points used in the lookback window.  Not
            applicable for all indicators.
        datatype : {'json', 'csv'}, optional
            Response format.
        **kwargs : dict
            Extra query parameters accepted by the chosen indicator.

        Returns
        -------
        dict or str
            Parsed JSON or raw CSV.
        """
        params: Dict[str, Any] = {"symbol": symbol, "interval": interval, "series_type": series_type}
        if time_period is not None:
            params["time_period"] = time_period
        # Merge additional parameters
        params.update(kwargs)
        if datatype.lower() == "csv":
            return self.fetch_csv(indicator, symbol, **params)
        return self.fetch(indicator, symbol, **params)

    def get_company_overview(self, symbol: str, datatype: str = "json") -> Union[Dict[str, Any], str]:
        """Retrieve a company overview (metadata and summary metrics)."""
        function = "OVERVIEW"
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol)
        return self.fetch(function, symbol)

    def get_income_statement(self, symbol: str, datatype: str = "json") -> Union[Dict[str, Any], str]:
        """Retrieve the income statement for a company."""
        function = "INCOME_STATEMENT"
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol)
        return self.fetch(function, symbol)

    def get_balance_sheet(self, symbol: str, datatype: str = "json") -> Union[Dict[str, Any], str]:
        """Retrieve the balance sheet for a company."""
        function = "BALANCE_SHEET"
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol)
        return self.fetch(function, symbol)

    def get_cash_flow(self, symbol: str, datatype: str = "json") -> Union[Dict[str, Any], str]:
        """Retrieve the cash flow statement for a company."""
        function = "CASH_FLOW"
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol)
        return self.fetch(function, symbol)

    def get_earnings(self, symbol: str, datatype: str = "json") -> Union[Dict[str, Any], str]:
        """Retrieve historical earnings (EPS) for a company."""
        function = "EARNINGS"
        if datatype.lower() == "csv":
            return self.fetch_csv(function, symbol)
        return self.fetch(function, symbol)

    # ------------------------------------------------------------------
    # Parsing helpers (delegated to utils)
    # ------------------------------------------------------------------
    @staticmethod
    def parse_time_series(response_json: Dict[str, Any]) -> Any:
        """Convert a time series JSON into a pandas DataFrame.

        This is a thin wrapper around :func:`utils.parse_time_series` for
        convenience.  See that function for details.
        """
        return parse_time_series(response_json)

    @staticmethod
    def parse_financial_reports(response_json: Dict[str, Any], report_type: str = "annualReports") -> Any:
        """Convert a financial statement JSON into a pandas DataFrame.

        This wraps :func:`utils.parse_financial_reports`.
        """
        return parse_financial_reports(response_json, report_type=report_type)