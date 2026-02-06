import os
import json
import logging
from typing import Optional, Dict

from core.postgres import connect

logger = logging.getLogger(__name__)

class StrategyRepository:
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or os.environ.get("POSTGRES_DSN")
        if not self.dsn:
            logger.warning("POSTGRES_DSN not set. StrategyRepository will not function.")

    def get_strategy_config(self, name: str) -> Optional[Dict]:
        """
        Retrieves a strategy configuration by name.
        Returns the 'config' JSONB content as a dictionary.
        """
        if not self.dsn:
            return None
            
        try:
            with connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT config FROM strategies WHERE name = %s", 
                        (name,)
                    )
                    row = cur.fetchone()
                    if row:
                        return row[0]
                    return None
        except Exception as e:
            logger.error(f"Failed to fetch strategy '{name}': {e}")
            raise

    def save_strategy(self, name: str, config: Dict, strategy_type: str = "configured", description: str = "") -> None:
        """
        Upserts a strategy configuration.
        """
        if not self.dsn:
            raise ValueError("Database connection not configured")
            
        try:
            with connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO strategies (name, config, type, description, updated_at)
                        VALUES (%s, %s, %s, %s, NOW())
                        ON CONFLICT (name) 
                        DO UPDATE SET 
                            config = EXCLUDED.config,
                            type = EXCLUDED.type,
                            description = EXCLUDED.description,
                            updated_at = NOW()
                        """,
                        (name, json.dumps(config), strategy_type, description)
                    )
        except Exception as e:
            logger.error(f"Failed to save strategy '{name}': {e}")
            raise

    def list_strategies(self) -> list[Dict]:
        """
        Returns a list of all strategies metadata.
        """
        if not self.dsn:
            return []
            
        try:
            with connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT name, type, description, updated_at FROM strategies ORDER BY name")
                    columns = ["name", "type", "description", "updated_at"]
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Failed to list strategies: {e}")
            raise
