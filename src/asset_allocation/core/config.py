from typing import Literal, Optional
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
import os

class AlpacaConfig(BaseModel):
    env: Literal["paper", "live"] = "paper"
    api_key_env: str = "ALPACA_API_KEY"
    api_secret_env: str = "ALPACA_SECRET_KEY"
    trading_base_url: str = "https://paper-api.alpaca.markets"
    trading_ws_url: str = "wss://paper-api.alpaca.markets/stream"
    marketdata_feed: str = "iex"
    marketdata_ws_url: str = "wss://stream.data.alpaca.markets/v2/iex"
    
    # Nested configs
    http_timeout_s: float = Field(default=30.0, alias="http.timeout_s")
    http_max_retries: int = Field(default=3, alias="http.max_retries")

class ExecutionConfig(BaseModel):
    allow_fractional_shares: bool = True
    lot_size: int = 100
    rounding_mode: Literal["down", "nearest"] = "down"
    min_trade_notional: float = 1.0
    
class AppSettings(BaseSettings):
    """
    Global application settings loaded from .env or environment variables.
    """
    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="__",
        extra="ignore"
    )

    domain: str = "asset_allocation"
    env: str = "dev"
    log_level: str = "INFO"
    
    # Azure Containers (Legacy constants)
    AZURE_CONTAINER_MARKET: str = Field(default="market")
    AZURE_CONTAINER_FINANCE: str = Field(default="finance")
    AZURE_CONTAINER_EARNINGS: str = Field(default="earnings")
    AZURE_CONTAINER_TARGETS: str = Field(default="targets")
    AZURE_CONTAINER_RANKING: str = Field(default="ranking")
    AZURE_CONTAINER_BRONZE: str = Field(default="bronze")
    AZURE_CONTAINER_SILVER: str = Field(default="silver")
    AZURE_CONTAINER_GOLD: str = Field(default="gold")
    AZURE_CONTAINER_COMMON: str = Field(default="common")

    # Sections
    alpaca: AlpacaConfig = Field(default_factory=AlpacaConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)

settings = AppSettings()

# Expose constants at module level for backward compatibility
AZURE_CONTAINER_MARKET = settings.AZURE_CONTAINER_MARKET
AZURE_CONTAINER_FINANCE = settings.AZURE_CONTAINER_FINANCE
AZURE_CONTAINER_EARNINGS = settings.AZURE_CONTAINER_EARNINGS
AZURE_CONTAINER_TARGETS = settings.AZURE_CONTAINER_TARGETS
AZURE_CONTAINER_RANKING = settings.AZURE_CONTAINER_RANKING
AZURE_CONTAINER_BRONZE = settings.AZURE_CONTAINER_BRONZE
AZURE_CONTAINER_SILVER = settings.AZURE_CONTAINER_SILVER
AZURE_CONTAINER_GOLD = settings.AZURE_CONTAINER_GOLD
AZURE_CONTAINER_COMMON = settings.AZURE_CONTAINER_COMMON
