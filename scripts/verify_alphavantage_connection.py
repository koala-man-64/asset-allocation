#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from typing import Any

import requests
from dotenv import load_dotenv


class AlphaVantageRequestError(RuntimeError):
    """Raised when Alpha Vantage returns an unusable response."""


def _parse_timeout(default: float = 15.0) -> float:
    raw = os.environ.get("ALPHA_VANTAGE_TIMEOUT_SECONDS")
    if raw is None:
        return default
    try:
        return float(raw)
    except Exception:
        return default


def _request_json(
    *,
    base_url: str,
    api_key: str,
    timeout_seconds: float,
    function: str,
    symbol: str | None = None,
    extra_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"function": function, "apikey": api_key}
    if symbol:
        params["symbol"] = symbol
    if extra_params:
        params.update(extra_params)

    url = f"{base_url.rstrip('/')}/query"

    try:
        response = requests.get(url, params=params, timeout=timeout_seconds)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise AlphaVantageRequestError(f"{function}: HTTP request failed: {exc}") from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise AlphaVantageRequestError(f"{function}: response was not valid JSON.") from exc

    if not isinstance(payload, dict):
        raise AlphaVantageRequestError(f"{function}: expected JSON object, got {type(payload).__name__}.")

    for key in ("Error Message", "Information", "Note"):
        message = payload.get(key)
        if isinstance(message, str) and message.strip():
            raise AlphaVantageRequestError(f"{function}: {message.strip()}")

    return payload


def _get_latest_daily_bar(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    series_key = next((key for key in payload if key.lower().startswith("time series")), None)
    if not series_key:
        raise AlphaVantageRequestError("TIME_SERIES_DAILY: missing time-series block in response.")

    series = payload.get(series_key)
    if not isinstance(series, dict) or not series:
        raise AlphaVantageRequestError("TIME_SERIES_DAILY: time-series block is empty.")

    latest_date = max(series.keys())
    latest_bar = series.get(latest_date)
    if not isinstance(latest_bar, dict):
        raise AlphaVantageRequestError("TIME_SERIES_DAILY: latest bar was not a JSON object.")

    return latest_date, latest_bar


def _first_report(records: Any) -> dict[str, Any] | None:
    if isinstance(records, list) and records and isinstance(records[0], dict):
        return records[0]
    return None


def _print_header(title: str) -> None:
    print()
    print(f"=== {title} ===")


def _print_key_values(data: dict[str, Any], keys: list[str]) -> None:
    for key in keys:
        value = data.get(key, "N/A")
        if value in (None, ""):
            value = "N/A"
        print(f"{key}: {value}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify Alpha Vantage connectivity and fetch market, financial, and earnings data."
    )
    parser.add_argument("--symbol", default="AAPL", help="Ticker symbol to query (default: AAPL).")
    parser.add_argument(
        "--api-key",
        default=None,
        help="Alpha Vantage API key. Defaults to ALPHA_VANTAGE_API_KEY from environment/.env.",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("ALPHA_VANTAGE_BASE_URL", "https://www.alphavantage.co"),
        help="Base URL for Alpha Vantage (default: ALPHA_VANTAGE_BASE_URL or https://www.alphavantage.co).",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=_parse_timeout(),
        help="HTTP timeout in seconds (default: ALPHA_VANTAGE_TIMEOUT_SECONDS or 15).",
    )
    parser.add_argument(
        "--outputsize",
        choices=("compact", "full"),
        default="compact",
        help="Market data output size for TIME_SERIES_DAILY (default: compact).",
    )

    args = parser.parse_args()

    load_dotenv(override=False)
    api_key = (args.api_key or os.environ.get("ALPHA_VANTAGE_API_KEY") or "").strip()
    if not api_key:
        print("Error: ALPHA_VANTAGE_API_KEY is not set. Provide --api-key or set it in your environment/.env.")
        return 1

    symbol = args.symbol.strip().upper()
    if not symbol:
        print("Error: --symbol cannot be empty.")
        return 1

    started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print("Alpha Vantage verification run")
    print(f"Timestamp (UTC): {started_at}")
    print(f"Base URL: {args.base_url.rstrip('/')}")
    print(f"Symbol: {symbol}")
    print(f"Timeout: {args.timeout_seconds}s")

    try:
        market_payload = _request_json(
            base_url=args.base_url,
            api_key=api_key,
            timeout_seconds=args.timeout_seconds,
            function="TIME_SERIES_DAILY",
            symbol=symbol,
            extra_params={"outputsize": args.outputsize},
        )
        overview_payload = _request_json(
            base_url=args.base_url,
            api_key=api_key,
            timeout_seconds=args.timeout_seconds,
            function="OVERVIEW",
            symbol=symbol,
        )
        income_payload = _request_json(
            base_url=args.base_url,
            api_key=api_key,
            timeout_seconds=args.timeout_seconds,
            function="INCOME_STATEMENT",
            symbol=symbol,
        )
        earnings_payload = _request_json(
            base_url=args.base_url,
            api_key=api_key,
            timeout_seconds=args.timeout_seconds,
            function="EARNINGS",
            symbol=symbol,
        )
    except AlphaVantageRequestError as exc:
        print()
        print(f"Verification failed: {exc}")
        return 2

    _print_header("Market Data (TIME_SERIES_DAILY)")
    latest_date, latest_bar = _get_latest_daily_bar(market_payload)
    print(f"Latest trading day: {latest_date}")
    _print_key_values(latest_bar, ["1. open", "2. high", "3. low", "4. close", "5. volume"])

    _print_header("Financial Overview (OVERVIEW)")
    _print_key_values(
        overview_payload,
        [
            "Name",
            "Symbol",
            "Exchange",
            "Currency",
            "Sector",
            "Industry",
            "MarketCapitalization",
            "PERatio",
            "EPS",
            "DividendYield",
        ],
    )

    _print_header("Financial Statements (INCOME_STATEMENT)")
    annual_report = _first_report(income_payload.get("annualReports"))
    quarterly_report = _first_report(income_payload.get("quarterlyReports"))
    annual_count = len(income_payload.get("annualReports", [])) if isinstance(income_payload.get("annualReports"), list) else 0
    quarterly_count = (
        len(income_payload.get("quarterlyReports", []))
        if isinstance(income_payload.get("quarterlyReports"), list)
        else 0
    )
    print(f"Annual reports returned: {annual_count}")
    print(f"Quarterly reports returned: {quarterly_count}")
    if annual_report:
        print("Latest annual report:")
        _print_key_values(annual_report, ["fiscalDateEnding", "totalRevenue", "grossProfit", "netIncome"])
    else:
        print("Latest annual report: N/A")
    if quarterly_report:
        print("Latest quarterly report:")
        _print_key_values(quarterly_report, ["fiscalDateEnding", "totalRevenue", "grossProfit", "netIncome"])
    else:
        print("Latest quarterly report: N/A")

    _print_header("Earnings (EARNINGS)")
    annual_earnings = earnings_payload.get("annualEarnings")
    quarterly_earnings = earnings_payload.get("quarterlyEarnings")
    annual_earnings_count = len(annual_earnings) if isinstance(annual_earnings, list) else 0
    quarterly_earnings_count = len(quarterly_earnings) if isinstance(quarterly_earnings, list) else 0
    print(f"Annual earnings rows returned: {annual_earnings_count}")
    print(f"Quarterly earnings rows returned: {quarterly_earnings_count}")

    latest_quarter = _first_report(quarterly_earnings)
    latest_annual = _first_report(annual_earnings)
    if latest_quarter:
        print("Latest quarterly earnings:")
        _print_key_values(
            latest_quarter,
            ["fiscalDateEnding", "reportedDate", "reportedEPS", "estimatedEPS", "surprise", "surprisePercentage"],
        )
    else:
        print("Latest quarterly earnings: N/A")

    if latest_annual:
        print("Latest annual earnings:")
        _print_key_values(latest_annual, ["fiscalDateEnding", "reportedEPS"])
    else:
        print("Latest annual earnings: N/A")

    print()
    print("Verification completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
