from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _is_truthy(raw: Optional[str]) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


if not _is_truthy(os.environ.get("DISABLE_DOTENV")):
    load_dotenv(override=False)


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise ValueError(f"{name} is required.")
    return value.strip()


def require_env_bool(name: str) -> bool:
    raw = require_env(name).strip().lower()
    if raw in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value for {name}: {raw!r}")


class AppSettings(BaseSettings):
    # NOTE: We disable env value decoding so list-like settings (e.g. DEBUG_SYMBOLS)
    # can be provided as a simple comma-separated string (AAPL,MSFT) without requiring
    # JSON array syntax. This also avoids failures when the env var is present but empty.
    model_config = SettingsConfigDict(extra="ignore", enable_decoding=False)

    # Basic runtime metadata.
    domain: str = "asset_allocation"
    env: str = "dev"

    # Logging (used by core.logging_config).
    log_level: str = "INFO"

    # Azure Storage auth (at least one required).
    AZURE_STORAGE_ACCOUNT_NAME: Optional[str] = None
    AZURE_STORAGE_CONNECTION_STRING: Optional[str] = None

    # Azure container routing.
    #
    # Defaults match the deployment manifests. Override via env vars when running
    # against a different storage account/container topology.
    AZURE_FOLDER_MARKET: str = "market-data"
    AZURE_FOLDER_FINANCE: str = "finance-data"
    AZURE_FOLDER_EARNINGS: str = "earnings-data"
    AZURE_FOLDER_TARGETS: str = "price-target-data"
    AZURE_CONTAINER_COMMON: str = "common"
    AZURE_CONTAINER_BRONZE: str = "bronze"
    AZURE_CONTAINER_SILVER: str = "silver"
    AZURE_CONTAINER_GOLD: str = "gold"
    AZURE_CONTAINER_PLATINUM: Optional[str] = "platinum"

    # API Configuration
    API_PORT: int = 8000

    # Optional data source credentials (varies by workflow).
    YAHOO_USERNAME: Optional[str] = None
    YAHOO_PASSWORD: Optional[str] = None

    # Playwright configuration.
    HEADLESS_MODE: bool = True
    DOWNLOADS_PATH: Optional[Path] = None
    USER_DATA_DIR: Optional[Path] = Field(default=None, validation_alias="PLAYWRIGHT_USER_DATA_DIR")

    USER_AGENT: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    # Yahoo download URL parameters.
    YAHOO_MAX_PERIOD: int = 99999999999

    # Comma-separated list for debug runs (e.g., \"AAPL,MSFT\"). Empty disables filtering.
    DEBUG_SYMBOLS: list[str] = Field(default_factory=list)

    @field_validator("DEBUG_SYMBOLS", mode="before")
    @classmethod
    def _parse_debug_symbols(cls, value):
        if value is None:
            return []
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return []

            # Accept either JSON array syntax or a comma-separated string.
            if raw.startswith("["):
                try:
                    decoded = json.loads(raw)
                except Exception:
                    decoded = None
                else:
                    if isinstance(decoded, list):
                        return [str(item).strip() for item in decoded if str(item).strip()]

            return [item.strip() for item in raw.split(",") if item.strip()]
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return value

    @model_validator(mode="after")
    def _validate_storage_auth(self):
        # Storage credentials are only required for workflows that actually read/write to Azure.
        # Allow FastAPI/local tooling to start without Azure configured.
        if _is_truthy(os.environ.get("ASSET_ALLOCATION_REQUIRE_AZURE_STORAGE")):
            if not self.AZURE_STORAGE_ACCOUNT_NAME and not self.AZURE_STORAGE_CONNECTION_STRING:
                raise ValueError("AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_CONNECTION_STRING is required.")
        return self


settings = AppSettings()

AZURE_STORAGE_ACCOUNT_NAME = settings.AZURE_STORAGE_ACCOUNT_NAME
AZURE_STORAGE_CONNECTION_STRING = settings.AZURE_STORAGE_CONNECTION_STRING

AZURE_FOLDER_MARKET = settings.AZURE_FOLDER_MARKET
AZURE_FOLDER_FINANCE = settings.AZURE_FOLDER_FINANCE
AZURE_FOLDER_EARNINGS = settings.AZURE_FOLDER_EARNINGS
AZURE_FOLDER_TARGETS = settings.AZURE_FOLDER_TARGETS
AZURE_CONTAINER_COMMON = settings.AZURE_CONTAINER_COMMON
AZURE_CONTAINER_BRONZE = settings.AZURE_CONTAINER_BRONZE
AZURE_CONTAINER_SILVER = settings.AZURE_CONTAINER_SILVER
AZURE_CONTAINER_GOLD = settings.AZURE_CONTAINER_GOLD
AZURE_CONTAINER_PLATINUM = settings.AZURE_CONTAINER_PLATINUM

EARNINGS_DATA_PREFIX: str = "earnings-data"

YAHOO_USERNAME = settings.YAHOO_USERNAME
YAHOO_PASSWORD = settings.YAHOO_PASSWORD

HEADLESS_MODE = settings.HEADLESS_MODE
DOWNLOADS_PATH = settings.DOWNLOADS_PATH
USER_DATA_DIR = settings.USER_DATA_DIR
USER_AGENT = settings.USER_AGENT
YAHOO_MAX_PERIOD = settings.YAHOO_MAX_PERIOD
DEBUG_SYMBOLS = settings.DEBUG_SYMBOLS

TICKERS_TO_ADD = [
    {
        "Symbol": "SPY",
        "Description": "S&P 500 Index ETF",
        "Sector": "Market Analysis",
        "Industry": "Index",
    },
    {
        "Symbol": "DIA",
        "Description": "Dow Jones Index ETF",
        "Sector": "Market Analysis",
        "Industry": "Index",
    },
    {
        "Symbol": "QQQ",
        "Description": "Nasdaq Index ETF",
        "Sector": "Market Analysis",
        "Industry": "Index",
    },
    {
        "Symbol": "^VIX",
        "Description": "Volatility Index ETF",
        "Sector": "Market Analysis",
        "Industry": "Index",
    },
    {
        "Symbol": "UST",
        "Description": "US Treasury ETF",
        "Sector": "Market Analysis",
        "Industry": "Index",
    },
    {
        "Symbol": "IWC",
        "Description": "Micro Cap ETF",
        "Sector": "Market Analysis",
        "Industry": "Market Cap",
    },
    {
        "Symbol": "VB",
        "Description": "Small Cap ETF",
        "Sector": "Market Analysis",
        "Industry": "Market Cap",
    },
]
