
import asyncio
import logging
from typing import List, Callable, Optional, Set, Any
from pathlib import Path

from scripts.common import core as mdc

logger = logging.getLogger(__name__)

class DataPaths:
    """
    Centralized configuration for data storage paths (Bronze Layer).
    """
    BRONZE_ROOT = "bronze"

    @staticmethod
    def get_market_data_path(ticker: str) -> str:
        return f"{DataPaths.BRONZE_ROOT}/price_data/{ticker.replace('.', '-')}"

    @staticmethod
    def get_price_target_path(ticker: str) -> str:
        return f"{DataPaths.BRONZE_ROOT}/price_targets/{ticker}"

    @staticmethod
    def get_earnings_path(ticker: str) -> str:
        return f"{DataPaths.BRONZE_ROOT}/earnings/{ticker}"

    @staticmethod
    def get_finance_path(folder: str, ticker: str, file_suffix: str) -> str:
        """
        folder: e.g. 'Balance Sheet' -> 'balance_sheet'
        """
        clean_folder = folder.lower().replace(' ', '_')
        return f"{DataPaths.BRONZE_ROOT}/{clean_folder}/{ticker}_{file_suffix}"


class ListManager:
    """
    Manages Whitelist and Blacklist for a specific scraper context.
    """
    def __init__(self, client, context_prefix: str):
        self.client = client
        self.whitelist_file = f"{context_prefix}_whitelist.csv"
        self.blacklist_file = f"{context_prefix}_blacklist.csv"
        
        self.whitelist: Set[str] = set()
        self.blacklist: Set[str] = set()
        self._loaded = False

    def load(self):
        """Loads lists from Azure Storage."""
        if not self.client:
             mdc.write_warning("ListManager has no client. Lists will be empty.")
             return

        w_list = mdc.load_ticker_list(self.whitelist_file, client=self.client)
        b_list = mdc.load_ticker_list(self.blacklist_file, client=self.client)
        
        self.whitelist = set(w_list)
        self.blacklist = set(b_list)
        self._loaded = True
        mdc.write_line(f"ListManager loaded: {len(self.whitelist)} whitelisted, {len(self.blacklist)} blacklisted.")

    def is_blacklisted(self, ticker: str) -> bool:
        if not self._loaded: self.load()
        return ticker in self.blacklist

    def is_whitelisted(self, ticker: str) -> bool:
        if not self._loaded: self.load()
        return ticker in self.whitelist

    def add_to_whitelist(self, ticker: str):
        if ticker not in self.whitelist:
            self.whitelist.add(ticker)
            mdc.update_csv_set(self.whitelist_file, ticker, client=self.client)
            # If it was in blacklist, maybe remove it? Policy decision: Keep it simple for now.

    def add_to_blacklist(self, ticker: str):
        if ticker not in self.blacklist:
            self.blacklist.add(ticker)
            mdc.update_csv_set(self.blacklist_file, ticker, client=self.client)


class ScraperRunner:
    """
    Generic orchestrator for running async scraping tasks with concurrency control.
    """
    def __init__(self, concurrency: int = 3):
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)

    async def run(
        self, 
        symbols: List[str], 
        process_func: Callable[[str], Any], 
        list_manager: Optional[ListManager] = None
    ):
        """
        symbols: List of ticker strings.
        process_func: Async function that takes a ticker and returns a result (or None).
        list_manager: Optional manager to filter symbols before processing.
        """
        
        # 1. Filter
        if list_manager:
            list_manager.load()
            filtered_symbols = [
                s for s in symbols 
                if not list_manager.is_blacklisted(s)
            ]
            if len(filtered_symbols) < len(symbols):
                 mdc.write_line(f"Filtered {len(symbols) - len(filtered_symbols)} blacklisted symbols.")
            symbols = filtered_symbols

        mdc.write_line(f"ScraperRunner starting for {len(symbols)} symbols with concurrency {self.concurrency}...")

        # 2. task wrapper
        async def worker(ticker):
            async with self.semaphore:
                try:
                    # Whitelist check could happen here if we wanted to skip validation logic
                    # relying on the process_func to handle specific logic
                    await process_func(ticker)
                except Exception as e:
                    mdc.write_error(f"Error processing {ticker}: {e}")

        # 3. Execution
        tasks = [worker(sym) for sym in symbols]
        if tasks:
            await asyncio.gather(*tasks)
        
        mdc.write_line("ScraperRunner completed.")
