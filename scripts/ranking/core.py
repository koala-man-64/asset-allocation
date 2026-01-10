"""
Core data models and storage logic for the Ranking Framework.
"""
from dataclasses import dataclass, asdict
from datetime import date, datetime
from typing import List, Optional, Dict, Any, Union
import pandas as pd
import json

from scripts.common import config as cfg
from scripts.common.delta_core import store_delta, load_delta
from scripts.common.core import write_line

@dataclass
class RankingResult:
    """
    Represents a single symbol's ranking within a strategy for a specific date.
    """
    date: date
    strategy: str
    symbol: str
    rank: int
    score: float
    meta: Optional[Dict[str, Any]] = None

    def to_dict(self):
        d = asdict(self)
        if isinstance(d['date'], (date, datetime)):
            d['date'] = d['date'].isoformat()
        if d['meta'] is None:
            del d['meta']
        elif isinstance(d['meta'], dict):
             d['meta'] = json.dumps(d['meta'])
        return d

def save_rankings(rankings: List[RankingResult], container: str = None):
    """
    Saves a list of RankingResult objects to the Delta table.
    """
    if not rankings:
        write_line("No rankings to save.")
        return

    container = container or cfg.AZURE_CONTAINER_RANKING
    if not container:
         container = "ranking"

    # Convert to DataFrame
    data = [r.to_dict() for r in rankings]
    df = pd.DataFrame(data)

    # Ensure correct types
    df['date'] = pd.to_datetime(df['date'])
    df['rank'] = df['rank'].astype(int)
    df['score'] = df['score'].astype(float)
    
    # Path for the delta table
    table_path = "gold/rankings" 
    
    write_line(f"Saving {len(df)} rankings to {container}/{table_path}...")
    
    store_delta(
        df, 
        container=container, 
        path=table_path, 
        mode='append', 
        partition_by=['strategy', 'date'], 
        merge_schema=True
    )
    write_line("Rankings saved successfully.")

def get_rankings(strategy: str, date_val: Optional[date] = None, container: str = None) -> pd.DataFrame:
    """
    Retrieves rankings for a specific strategy and optionally a date.
    """
    container = container or cfg.AZURE_CONTAINER_RANKING or "ranking"
    table_path = "gold/rankings"
    
    df = load_delta(container, table_path)
    if df is None or df.empty:
        return pd.DataFrame()
    
    # Filter
    mask = (df['strategy'] == strategy)
    if date_val:
        mask &= (df['date'].dt.date == date_val)
        
    return df[mask].sort_values('rank')
