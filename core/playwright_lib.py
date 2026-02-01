import asyncio
import datetime
import glob
import hashlib
import json
import os
import random
import re
import sys
import threading
import time
import warnings
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from pathlib import Path
from typing import Any, Coroutine, Dict, List, Literal, Optional, Tuple, Union
from urllib.parse import urlparse

import pandas as pd
import pytz


def _is_truthy(raw: Optional[str]) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


_IS_TEST_ENVIRONMENT = "PYTEST_CURRENT_TEST" in os.environ or _is_truthy(os.environ.get("TEST_MODE"))


def _is_interactive_session() -> bool:
    try:
        return sys.stdin is not None and sys.stdin.isatty()
    except Exception:
        return False

try:
    from bs4 import BeautifulSoup
except ModuleNotFoundError:
    if not _IS_TEST_ENVIRONMENT:
        raise
    BeautifulSoup = None  # type: ignore[assignment]

try:
    from filelock import FileLock
except ModuleNotFoundError:
    if not _IS_TEST_ENVIRONMENT:
        raise
    FileLock = None  # type: ignore[assignment]

_PLAYWRIGHT_AVAILABLE = True
try:
    # sync API/types
    from playwright.sync_api import (
        Browser,
        BrowserContext,
        Download,
        Error,
        Page,
        Playwright,
        TimeoutError as PlaywrightTimeout,
        TimeoutError,
        sync_playwright,
    )
    from playwright.sync_api import (
        Playwright as SyncPlaywright,
        Browser as SyncBrowser,
        BrowserContext as SyncBrowserContext,
        Page as SyncPage,
    )

    # async API/types
    from playwright.async_api import (
        Playwright as AsyncPlaywright,
        Browser as AsyncBrowser,
        BrowserContext as AsyncBrowserContext,
        Page as AsyncPage,
        Download as AsyncDownload,
        TimeoutError as PlaywrightTimeoutError,
        async_playwright,
    )
except ModuleNotFoundError:
    _PLAYWRIGHT_AVAILABLE = False

    if not _IS_TEST_ENVIRONMENT:
        raise

    # Lightweight fallbacks for test environments that patch Playwright calls.
    class Error(Exception):
        pass

    class TimeoutError(Exception):
        pass

    class PlaywrightTimeout(TimeoutError):
        pass

    class PlaywrightTimeoutError(TimeoutError):
        pass

    Playwright = Browser = BrowserContext = Page = Download = Any  # type: ignore[assignment]
    SyncPlaywright = SyncBrowser = SyncBrowserContext = SyncPage = Any  # type: ignore[assignment]
    AsyncPlaywright = AsyncBrowser = AsyncBrowserContext = AsyncPage = AsyncDownload = Any  # type: ignore[assignment]

    def sync_playwright(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("Playwright is not installed. Install 'playwright' to use browser automation.")

    async def async_playwright(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("Playwright is not installed. Install 'playwright' to use browser automation.")

from core import config as cfg

warnings.filterwarnings("ignore")

from core import core as mdc


def _parse_int_env(name: str, *, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _parse_float_env(name: str, *, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip()
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _is_yahoo_related_url(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return False

    if not host:
        return False

    return host.endswith("yahoo.com") or host.endswith("yimg.com")


@dataclass(frozen=True)
class YahooGuardEvent:
    reason: str  # "rate_limited" | "unauthorized"
    status: Optional[int]
    url: str
    source: str  # "response" | "console" | "exception"
    message: str


class YahooGuardTriggered(RuntimeError):
    def __init__(self, event: YahooGuardEvent):
        super().__init__(f"Yahoo guard triggered: {event.reason} (status={event.status}) url={event.url}")
        self.event = event


class YahooInteractionGuard:
    """
    Watches Playwright network + console signals for Yahoo throttling/auth failures.

    Trigger conditions (Yahoo-only):
      - HTTP 429 (rate limited)
      - HTTP 401/403 (unauthorized)
      - Console messages containing "Too Many Requests" / "Unauthorized" / code=429

    On trigger, schedules closing the browser context to abort in-flight work.
    """

    def __init__(self) -> None:
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._context: Optional[AsyncBrowserContext] = None
        self._browser: Optional[AsyncBrowser] = None
        self._event: Optional[YahooGuardEvent] = None
        self._closed = False

    @property
    def triggered(self) -> bool:
        return self._event is not None

    @property
    def event(self) -> Optional[YahooGuardEvent]:
        return self._event

    def attach(
        self,
        *,
        context: AsyncBrowserContext,
        browser: AsyncBrowser,
        page: Optional[AsyncPage] = None,
    ) -> None:
        self._loop = asyncio.get_running_loop()
        self._context = context
        self._browser = browser

        try:
            context.on("response", self._on_response)
        except Exception:
            pass
        try:
            context.on("page", self._on_page)
        except Exception:
            pass

        if page is not None:
            self._attach_page(page)

    def _on_page(self, page: AsyncPage) -> None:
        self._attach_page(page)

    def _attach_page(self, page: AsyncPage) -> None:
        try:
            page.on("console", self._on_console)
        except Exception:
            pass
        try:
            page.on("pageerror", self._on_page_error)
        except Exception:
            pass

    def _on_page_error(self, exc: Exception) -> None:
        # Page errors are noisy; only react if they look like 429/unauthorized symptoms.
        text = str(exc)
        if self.triggered:
            return
        lowered = text.lower()
        if "too many requests" in lowered or " code=429" in lowered or "unauthorized" in lowered:
            self.trigger(
                YahooGuardEvent(
                    reason="rate_limited" if "429" in lowered or "too many requests" in lowered else "unauthorized",
                    status=429 if "429" in lowered else None,
                    url="",
                    source="exception",
                    message=text,
                )
            )

    def _on_console(self, message: Any) -> None:
        if self.triggered:
            return

        text = ""
        try:
            text = str(getattr(message, "text", "") or "")
            if callable(getattr(message, "text", None)):
                text = str(message.text())
        except Exception:
            text = ""

        lowered = text.lower()
        if not lowered:
            return

        if "too many requests" in lowered or '"code":429' in lowered or "code\":429" in lowered:
            self.trigger(
                YahooGuardEvent(
                    reason="rate_limited",
                    status=429,
                    url="",
                    source="console",
                    message=text,
                )
            )
            return

        if "unauthorized" in lowered or "code\":401" in lowered or '"code":401' in lowered:
            self.trigger(
                YahooGuardEvent(
                    reason="unauthorized",
                    status=401,
                    url="",
                    source="console",
                    message=text,
                )
            )

    def _on_response(self, response: Any) -> None:
        if self.triggered:
            return

        try:
            url = str(getattr(response, "url", "") or "")
            status = int(getattr(response, "status", 0) or 0)
        except Exception:
            return

        if not url or not _is_yahoo_related_url(url):
            return

        if status == 429:
            self.trigger(
                YahooGuardEvent(
                    reason="rate_limited",
                    status=status,
                    url=url,
                    source="response",
                    message="Yahoo responded with HTTP 429",
                )
            )
            return

        if status in {401, 403}:
            self.trigger(
                YahooGuardEvent(
                    reason="unauthorized",
                    status=status,
                    url=url,
                    source="response",
                    message=f"Yahoo responded with HTTP {status}",
                )
            )

    def trigger(self, event: YahooGuardEvent) -> None:
        if self._event is not None:
            return

        self._event = event
        try:
            mdc.write_error(
                f"YAHOO_GUARD_TRIGGERED reason={event.reason} status={event.status} source={event.source} url={event.url}"
            )
        except Exception:
            pass

        loop = self._loop
        if loop is None:
            return

        # Best-effort: close context/browser quickly to stop in-flight work.
        if not self._closed:
            self._closed = True
            loop.call_soon_threadsafe(lambda: loop.create_task(self._close_resources()))

    async def _close_resources(self) -> None:
        context = self._context
        browser = self._browser

        if context is not None:
            try:
                await context.close()
            except Exception:
                pass

        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass


async def run_with_yahoo_backoff(
    run_once: Any,
    *,
    headless: Optional[bool] = None,
    slow_mo: Optional[int] = None,
) -> Any:
    """
    Run a Yahoo-backed Playwright workload with automatic recovery on 429/unauthorized signals.

    Behavior:
      - Creates a fresh persistent context (via get_playwright_browser).
      - Attaches YahooInteractionGuard to watch for throttling/auth failures.
      - If triggered, closes the browser, sleeps with jitter, and retries.

    Env knobs (optional):
      - YAHOO_GUARD_MAX_ATTEMPTS (default 3)
      - YAHOO_GUARD_BACKOFF_MIN_SECONDS (default 60)
      - YAHOO_GUARD_BACKOFF_MAX_SECONDS (default 240)
    """

    max_attempts = max(1, _parse_int_env("YAHOO_GUARD_MAX_ATTEMPTS", default=3))
    backoff_min = max(0.0, _parse_float_env("YAHOO_GUARD_BACKOFF_MIN_SECONDS", default=60.0))
    backoff_max = max(backoff_min, _parse_float_env("YAHOO_GUARD_BACKOFF_MAX_SECONDS", default=240.0))

    last_event: Optional[YahooGuardEvent] = None

    for attempt in range(1, max_attempts + 1):
        playwright: Optional[AsyncPlaywright] = None
        browser: Optional[AsyncBrowser] = None
        context: Optional[AsyncBrowserContext] = None
        page: Optional[AsyncPage] = None
        guard = YahooInteractionGuard()

        try:
            playwright, browser, context, page = await get_playwright_browser(
                headless=headless,
                slow_mo=slow_mo,
                use_async=True,
            )
            guard.attach(context=context, browser=browser, page=page)

            result = await run_once(playwright, browser, context, page, guard)

            if guard.triggered and guard.event is not None:
                raise YahooGuardTriggered(guard.event)

            return result

        except YahooGuardTriggered as exc:
            last_event = exc.event
            if attempt >= max_attempts:
                raise

            sleep_seconds = random.uniform(backoff_min, backoff_max)
            mdc.write_line(
                "YAHOO_GUARD_BACKOFF attempt={attempt}/{max_attempts} reason={reason} status={status} sleep_seconds={sleep:.0f}".format(
                    attempt=attempt,
                    max_attempts=max_attempts,
                    reason=last_event.reason,
                    status=last_event.status,
                    sleep=sleep_seconds,
                )
            )
            await asyncio.sleep(sleep_seconds)

        except Exception as exc:
            # If the guard triggered and the context was force-closed, treat as a recoverable Yahoo event.
            if guard.triggered and guard.event is not None:
                last_event = guard.event
                if attempt >= max_attempts:
                    raise YahooGuardTriggered(last_event) from exc

                sleep_seconds = random.uniform(backoff_min, backoff_max)
                mdc.write_line(
                    "YAHOO_GUARD_BACKOFF attempt={attempt}/{max_attempts} reason={reason} status={status} sleep_seconds={sleep:.0f}".format(
                        attempt=attempt,
                        max_attempts=max_attempts,
                        reason=last_event.reason,
                        status=last_event.status,
                        sleep=sleep_seconds,
                    )
                )
                await asyncio.sleep(sleep_seconds)
            else:
                raise

        finally:
            # Best-effort cleanup. Guard may already have closed context/browser.
            if context is not None:
                try:
                    await context.close()
                except Exception:
                    pass
            if browser is not None:
                try:
                    await browser.close()
                except Exception:
                    pass
            if playwright is not None:
                try:
                    await playwright.stop()
                except Exception:
                    pass

    # Should not happen; loop returns/raises above.
    if last_event is not None:
        raise YahooGuardTriggered(last_event)
    raise RuntimeError("Yahoo backoff runner exhausted attempts without an explicit guard event.")


def _require_path(value: Optional[Path], env_name: str) -> Path:
    if value is None:
        raise RuntimeError(f"{env_name} must be set for Playwright execution.")
    return value


def _get_downloads_path() -> Path:
    return _require_path(cfg.DOWNLOADS_PATH, "DOWNLOADS_PATH")

def _get_user_data_dir() -> Path:
    return _require_path(cfg.USER_DATA_DIR, "PLAYWRIGHT_USER_DATA_DIR")

COMMON_DIR = Path(__file__).parent.resolve()

def write_line(msg: str):
    """
    Print a line to the console w/ a timestamp
    Parameters:
        str:
    """
    ct = datetime.datetime.now()
    print("{}: {}".format(ct, msg))

def extract_quarter_date(s):
    quarter_match = re.search(r'Q([1-4]) (\d{4})', s, re.IGNORECASE)
    if quarter_match:
        quarter, year = int(quarter_match.group(1)), int(quarter_match.group(2))
        
        quarter_months = {
            1: "March 31",
            2: "June 30",
            3: "September 30",
            4: "December 31"
        }
        
        dt_naive = datetime.strptime(f"{quarter_months[quarter]}, {year}", "%B %d, %Y")
        return dt_naive
    
    return None

async def download_yahoo_price_data_async(
    page: AsyncPage,
    url: str,
    timeout: int = 10000,          # ms
    max_attempts: int = 3,
    delay_between_attempts: float = 1.0,
) -> Path:
    """
    Async version: Navigate to a Yahoo Finance download URL, capture the file, and
    save it to *downloads_dir* (defaults to ~/Downloads).

    Returns
    -------
    Path
        Location of the saved file.

    Raises
    ------
    RuntimeError
        If the download never starts within all attempts.
    """
    dl_root = _get_downloads_path()
    dl_root.mkdir(parents=True, exist_ok=True)
    write_line(f"Downloading to: {dl_root}")

    for attempt in range(1, max_attempts + 1):
        try:
            # Kick off download
            async with page.expect_download(timeout=timeout) as dl_info:
                try:
                    await page.goto(url, wait_until="commit", timeout=timeout)
                except Exception as e:
                    # Stock has no price data
                    if 'ERR_ABORTED' in str(e) and 'query1' in str(e):
                        pass
                    else:                        
                        # Avoid blocking the event loop in async contexts.
                        sleep_time = random.randint(1, 1)
                        write_line(f"Sleeping for {sleep_time} seconds...")
                        await asyncio.sleep(sleep_time)
                        if not("Download is starting" in str(e)):
                            raise RuntimeError(f"Failed to download after {max_attempts} attempts: {e}") from e

            dl = await dl_info.value  # playwright.async_api.Download
            target = dl_root / dl.suggested_filename
            await dl.save_as(str(target))
            # delete_newer_duplicates(str(target))
            return target

        except PlaywrightTimeoutError as pe:
            # Download didn’t start in time—retry
            if attempt < max_attempts:
                await asyncio.sleep(delay_between_attempts)
                continue
            raise RuntimeError(f"Timed out waiting for download from {url!r}")

        except Exception as e:
            # Other transient issues
            if 'ERR_ABORTED' in str(e) and 'query1' in str(e):
                raise RuntimeError(f"No data available for {url!r}") from e
            if attempt < max_attempts:
                await asyncio.sleep(delay_between_attempts)
                continue
            raise RuntimeError(f"Failed to download after {max_attempts} attempts: {e}") from e



def convert_earnings_date(s):
    try:
        s = str.lower(s)
        # Remove 'at' and extra spaces
        s = s.replace(" at", "").strip()

        # Remove timezone abbreviations (e.g., EST, EDT)
        s = re.sub(r'\s(est|edt)$', '', s)

        # Check for "half year" in the text
        half_year_match = re.search(r'half year (\d{4})', s, re.IGNORECASE)
        if half_year_match:
            year = half_year_match.group(1)
            dt_naive = datetime.datetime.strptime(f"June 30, {year}", "%B %d, %Y")
        
        # Handle 'Full Year' format
        elif re.match(r'full year \d{4}', s, re.IGNORECASE):
            year = re.search(r'\d{4}', s).group()
            dt_naive = datetime.datetime.strptime(f"December 31, {year}", "%B %d, %Y")
        else:
            try:
                # Try parsing the expected format
                dt_naive = datetime.datetime.strptime(s, '%B %d, %Y %I %p')
            except ValueError:
                # If standard parsing fails, check for a quarter-based date
                dt_naive = extract_quarter_date(s)
                if not dt_naive:
                    write_line(f"WARNING: Unable to extract a valid date from '{s}', skipping.")
                    return ""

        # Get the timezone info (Eastern Time)
        timezone = pytz.timezone('US/Eastern')

        # Localize the naive datetime object to the specific timezone
        dt_aware = timezone.localize(dt_naive)

        return dt_aware
    except Exception as e:
        write_line(f"ERROR: Failed to convert date '{s}' - {e}")
        return ""
    


    
async def get_yahoo_earnings_data(
    page: AsyncPage,
    symbol: str,
    timeout,          # ms
    max_attempts: int = 3,
    delay_between_attempts: float = 1.0,
) -> pd.DataFrame:
    """
    
    Navigate to a Yahoo Finance download URL, capture the file, and
    save it to *downloads_dir* (defaults to ~/Downloads).

    Returns
    -------
    Path
        Location of the saved file.

    Raises
    ------
    RuntimeError
        If the download never starts within all attempts.
    """
    dl_root = _get_downloads_path()
    dl_root.mkdir(parents=True, exist_ok=True)

    dl_root.mkdir(parents=True, exist_ok=True)

    columns = ['Date', 'Symbol', 'Reported EPS', 'EPS Estimate', 'Surprise']
    df_symbol_earnings = pd.DataFrame()
    for attempt in range(1, max_attempts + 1):
        try:
            write_line(f"Processing earnings for {symbol}")
            offset = 0
            same_counter = 0

            # loop while offsetting the url continues to return results
            while True:
                try:
                    # build target url with offset: 
                    target_base_url = f"https://finance.yahoo.com/calendar/earnings?symbol={symbol}"
                    temp_url = f"{target_base_url}&offset={offset}&size=100"

                    # load target url
                    await page.goto(temp_url, wait_until="load")
                    
                    # grab html table with earnings results
                    html_content = await page.content()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    cal_table = soup.find('table')  # grab first instance of <table> element

                    # If there is no table, or error indications, break out of the loop                

                    
                    if cal_table is not None:
                        # Loop through each row in the table and extract data
                        rows = cal_table.find_all('tr')
                        old_count = len(df_symbol_earnings)
                        for i, row_elem in enumerate(rows):
                            cells = row_elem.find_all('td')
                            if len(cells) >= 5:  # Adjust if your table structure requires
                                # Extract and convert the values from the <td> elements
                                date_raw = cells[2].get_text(strip=True)
                                date_converted = convert_earnings_date(date_raw)
                                
                                
                                # Check if this date falls in the upcoming window
                                try:
                                    date_obj = date_converted.date()
                                except Exception:
                                    continue  # Skip if date conversion fails
                                # if date_obj not in upcoming_dates:
                                #     continue  # Skip rows that are not in the upcoming window

                                sym = cells[0].get_text(strip=True)
                                reported_eps = cells[4].get_text(strip=True)
                                eps_estimate = cells[3].get_text(strip=True)
                                surprise = cells[5].get_text(strip=True)

                                # Create a new row DataFrame
                                new_row_df = pd.DataFrame([[date_converted, sym, reported_eps, eps_estimate, surprise]], columns=columns)
                                new_row_df['Date'] = pd.to_datetime(new_row_df['Date']).dt.tz_localize(None)
                                new_row_df['Reported EPS'] = pd.to_numeric(new_row_df['Reported EPS'], errors='coerce')
                                new_row_df['Surprise'] = new_row_df['Surprise'].str.replace('+', '', regex=False)
                                new_row_df['Surprise'] = pd.to_numeric(new_row_df['Surprise'], errors='coerce') / 100                            
                                new_row_df['Date_parsed'] = pd.to_datetime(new_row_df['Date'], errors='coerce', utc=True).dt.date
                                # Concatenate the new row to the existing DataFrame
                                df_symbol_earnings = pd.concat([df_symbol_earnings, new_row_df], ignore_index=True)
                                df_symbol_earnings = df_symbol_earnings.drop_duplicates(subset=['Date_parsed'], keep='last')
                                
                        new_count = len(df_symbol_earnings)
                        # If no new rows were added, increment same_counter; otherwise, reset and increase offset
                        write_line(f'Retrieved {(new_count - old_count)} earnings rows for {symbol}')
                        if old_count == new_count or (new_count - old_count) < 90:
                            # cl.write_line(f'Retrieved {(new_count - old_count)} earnings rows for {symbol}')
                            break#same_counter += 1
                        else:#if (new_count-old_count) > 90:
                            same_counter = 0
                            offset += 100
                    elif '<h1>500</h1>' in html_content or 'We are experiencing some temporary issues.' in html_content:
                        try:
                            await page.reload()
                            # Avoid blocking the event loop in async contexts.
                            await asyncio.sleep(random.randint(5, 10))
                        except Exception as e:
                            raise e
                    elif ("We couldn't find any results." in html_content and len(df_symbol_earnings) == 0):
                        write_line(f"No results found for {symbol}. Signaling for blacklist.")
                        raise ValueError("Symbol not found")
                    elif cal_table is None or "We couldn't find any results." in html_content:
                        break
                except Exception as ex:
                    write_line(f"ERROR: Failed processing {symbol} for offset {offset}: {ex}")
                    raise ex
            return df_symbol_earnings

        except PlaywrightTimeout:
            # Download didn’t start fast enough – try again.
            if attempt < max_attempts:
                # Avoid blocking the event loop in async contexts.
                await asyncio.sleep(delay_between_attempts)
                continue
            raise RuntimeError(f"Timed out fetching Yahoo earnings page for {symbol!r}")

        except Exception as e:
            # Other transient issues (net::ERR_ABORTED, etc.).
            if 'ERR_ABORTED' in str(e) and 'query1' in str(e): #means this stock has no price data
                raise RuntimeError(f"Failed to download after {max_attempts} attempts: {e}") from e
            if attempt < max_attempts:
                # Avoid blocking the event loop in async contexts.
                await asyncio.sleep(delay_between_attempts)
                continue
            else:
                raise RuntimeError(f"Failed to download after {max_attempts} attempts: {e}") from e

    return df_symbol_earnings


def pw_download_after_click_by_selectors(
    page: Page,
    selectors: List[Dict[str, Any]],
    downloads_dir: str,
    timeout: int = 10_000,
    max_attempts: int = 3,
    delay_between_attempts: float = 0.5
) -> Path:
    """
    Click the first element matching one of your selector definitions,
    capture the resulting download, and save it into your Downloads folder
    (or a custom path given as a string).

    Args:
      page:          Playwright Page instance (context must have accept_downloads=True).
      selectors:     A list of dicts, each with:
                       - property_name:  attribute name (e.g. "id", "data-testid")
                       - property_value: attribute value to match
                       - property_type:  (optional) tag name (e.g. "button", "a")
      downloads_dir: Where to save the file; defaults to '~/Downloads'. Pass a string path.
      timeout:       How long (ms) to wait for each locator & download.
      max_attempts:  How many times to retry clicking all selectors.
      delay_between_attempts: Seconds to wait before retrying.

    Returns:
      A pathlib.Path to the saved download.

    Raises:
      RuntimeError if no selector yields a download within all attempts.
    """
    # Resolve downloads_dir (string) into a Path
    download_path = Path(downloads_dir) if downloads_dir else _get_downloads_path()
    download_path.mkdir(parents=True, exist_ok=True)
    write_line(f"Downloading to: {download_path}")

    for attempt in range(1, max_attempts + 1):
        for sel in selectors:
            # build CSS selector
            tag = sel.get("property_type", "").strip()
            name = sel["property_name"]
            value = sel["property_value"]
            if name.lower() == "id":
                css = f"#{value}"
            else:
                css = f'[{name}="{value}"]'
            if tag:
                css = f"{tag}{css}"

            locator = page.locator(css)
            try:
                if locator.count() > 0:
                    # trigger download and wait for it
                    with page.expect_download(timeout=timeout) as dl_info:
                        locator.first.click(timeout=timeout)
                        # time.sleep(1)
                    download: Download = dl_info.value
                    target = download_path / download.suggested_filename
                    download.save_as(str(target))
                    # delete_newer_duplicates(str(target))
                    # go_to_sleep(1,1)
                    return target
            except TimeoutError as te:
                # locator didn't appear or download didn't start in time
                continue
            except Exception as e:
                # click failed, try next selector
                continue

        if attempt < max_attempts:
            time.sleep(delay_between_attempts)

    raise RuntimeError(f"Download never started after clicking selectors: {selectors!r}")

async def pw_download_after_click_by_selectors_async(
    page: Page,
    selectors: List[Dict[str, Any]],
    downloads_dir: Optional[str],
    timeout: int = 10_000,
    max_attempts: int = 3,
    delay_between_attempts: float = 0.5
) -> Path:
    """
    Click the first element matching one of your selector definitions,
    capture the resulting download, and save it into `downloads_dir`
    (or ~/Downloads if None/empty).

    Note: The page's context should allow downloads (accept_downloads=True).

    Args:
      page:          Playwright Page (async).
      selectors:     A list of dicts, each with:
                       - property_name:  attribute name (e.g. "id", "data-testid")
                       - property_value: attribute value to match
                       - property_type:  (optional) tag name (e.g. "button", "a")
      downloads_dir: Where to save the file; pass a string path or None to use ~/Downloads.
      timeout:       How long (ms) to wait for each locator & download.
      max_attempts:  How many times to retry clicking all selectors.
      delay_between_attempts: Seconds to wait before retrying.

    Returns:
      Path to the saved download.

    Raises:
      RuntimeError if no selector yields a download within all attempts.
    """
    # Resolve downloads_dir into a Path
    download_path = Path(downloads_dir).expanduser() if downloads_dir else _get_downloads_path()
    download_path.mkdir(parents=True, exist_ok=True)
    write_line(f"Downloading to: {download_path}")

    for attempt in range(1, max_attempts + 1):
        for sel in selectors:
            # Build CSS selector
            tag = (sel.get("property_type") or "").strip()
            name = sel["property_name"]
            value = sel["property_value"]

            if name.lower() == "id":
                css = f"#{value}"
            else:
                css = f'[{name}="{value}"]'
            if tag:
                css = f"{tag}{css}"

            locator = page.locator(css)
            try:
                if await locator.count() > 0:
                    # Trigger download and wait for it
                    async with page.expect_download(timeout=timeout) as dl_info:
                        await locator.first.click(timeout=timeout)
                    download: Download = await dl_info.value

                    target = download_path / download.suggested_filename
                    await download.save_as(str(target))

                    # # Optional helper in your codebase; ignore if undefined.
                    # try:
                    #     delete_newer_duplicates(str(target))  # type: ignore[name-defined]
                    # except Exception:
                    #     pass

                    return target

            except PlaywrightTimeoutError:
                # Locator didn't appear or download didn't start in time
                continue
            except Exception:
                # Click failed, try next selector
                continue

        if attempt < max_attempts:
            await asyncio.sleep(delay_between_attempts)

    raise RuntimeError(f"Download never started after clicking selectors: {selectors!r}")

async def get_playwright_browser(
    headless: Optional[bool] = None,
    slow_mo: Optional[int] = None,
    use_async: bool = False,
) -> Tuple[AsyncPlaywright, AsyncBrowser, AsyncBrowserContext, AsyncPage]:
    """
    Launch a fresh Playwright browser (Async).
    If use_async=False, raises ValueError as sync support is removed.
    
    Returns (playwright, browser, context, page).
    """
    if headless is None:
        headless = cfg.HEADLESS_MODE

    if not use_async:
        raise ValueError("Synchronous Playwright execution is no longer supported. Please use use_async=True.")

    return await _get_playwright_browser_async(headless, slow_mo)


async def _get_playwright_browser_async(
    headless: bool,
    slow_mo: Optional[int],
) -> Tuple[AsyncPlaywright, AsyncBrowser, AsyncBrowserContext, AsyncPage]:
    write_line("Starting _get_playwright_browser_async...")
    # 1. Start the async controller
    write_line("Initializing async_playwright()...")
    playwright = await async_playwright().start()
    write_line("async_playwright started.")

    # 2. Persistent context (this launches Chromium under the hood)
    write_line("Launching persistent context...")
    context = await playwright.chromium.launch_persistent_context(
        user_data_dir=str(_get_user_data_dir()),
        headless=headless,
        slow_mo=slow_mo or 0,
        accept_downloads=True,
        downloads_path=str(_get_downloads_path()),
        user_agent=cfg.USER_AGENT,
        viewport={"width": 1920, "height": 1080},
        args=["--disable-blink-features=AutomationControlled", "--disable-infobars", "--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
    )
    write_line("Persistent context launched.")

    browser = context.browser
    if browser is None:
        raise RuntimeError("Persistent context did not expose a browser instance (unexpected).")

    # Stealth Init Script
    write_line("Adding stealth init script2...")
    stealth_js = """
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined
    });
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en']
    });
    Object.defineProperty(navigator, 'plugins', {
        get: () => [1, 2, 3, 4, 5]
    });
    """
    await context.add_init_script(stealth_js)
    write_line("Stealth script added.")

    # 4. Open a new tab
    write_line("Opening new page...")
    page = await context.new_page()
    write_line("New page opened. Returning Playwright objects.")

    return playwright, browser, context, page




async def load_url_async(
    page: Page,
    url: str,
    wait_until: Literal["load", "domcontentloaded", "networkidle", "commit"] = "load",
    timeout: int = 30_000
) -> Optional[Page]:
    """
    Async navigate the given Playwright Page to the specified URL.

    Returns:
      The same Page instance on success; None if navigation fails/times out.
    """
    try:
        await page.goto(url, wait_until=wait_until, timeout=timeout)
        return page
    except PlaywrightTimeoutError:
        try:
            write_line(f"TIMEOUT: Loading {url} (wait_until={wait_until}, timeout={timeout} ms)")
        except NameError:
            print(f"TIMEOUT: Loading {url} (wait_until={wait_until}, timeout={timeout} ms)")
        return None
    except Exception as e:
        try:
            write_line(f"ERROR: Failed loading url {url}: {e}")
        except NameError:
            print(f"ERROR: Failed loading url {url}: {e}")
        return None
    


async def pw_save_cookies_async(
    context: AsyncBrowserContext,
    cookies_path: str
) -> None:
    """
    Async version: Save all cookies from the given Playwright BrowserContext to a JSON file.
    Ensures parent directory exists and forces a flush+fsync.
    """
    # 1) Grab cookies
    cookies = await context.cookies()

    # 2) Make sure directory exists
    parent = os.path.dirname(cookies_path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

    # 3) Write + flush + fsync
    with open(cookies_path, "w", encoding="utf-8") as f:
        json.dump(cookies, f, indent=2)
        f.flush()
        os.fsync(f.fileno())

async def pw_load_cookies_async(
    context: AsyncBrowserContext,
    cookies_path: str
) -> None:
    """
    Async version: Load cookies from a JSON file into an async Playwright BrowserContext.

    Args:
      context:      An async BrowserContext returned by your async get_playwright_browser().
      cookies_path: Path to a JSON file containing a list of cookie dicts.
    """
    if os.path.exists(cookies_path):
        # 1. Read cookie list from disk (blocking I/O – OK for small files)
        with open(cookies_path, "r", encoding="utf-8") as f:
            cookies = json.load(f)

        # 2. Add them into the context’s cookie store
        await context.add_cookies(cookies)


 
 
    
async def pw_fill_by_selectors_async(
    page: AsyncPage,
    selectors: List[Dict[str, Any]],
    text: str,
    timeout: int = 10_000
) -> bool:
    """
    Async version: Try each selector to locate an element and fill it with the provided text.

    Args:
      page:       The Playwright AsyncPage instance.
      selectors:  A list of dicts, each with:
                    - property_name: the attribute name (e.g. "id", "name", "data-test")
                    - property_value: the expected attribute value.
      text:       The text to type into the first matching element.
      timeout:    How long (ms) to wait for each locator to appear.

    Returns:
      True if a fill succeeded.

    Raises:
      RuntimeError: if none of the selectors match an element.
    """
    for sel in selectors:
        name  = sel["property_name"]
        value = sel["property_value"]

        # Build a CSS selector: #id or [attr="value"]
        if name.lower() == "id":
            css = f"#{value}"
        else:
            css = f'[{name}="{value}"]'

        locator = page.locator(css)
        try:
            # Wait for the element to be attached and visible
            await locator.wait_for(state="attached", timeout=timeout)
            await locator.first.wait_for(state="visible", timeout=timeout)

            # Fill the field
            await locator.first.fill(text, timeout=timeout)
            return True

        except TimeoutError:
            # This selector didn’t appear in time—try the next one
            continue

    # No selectors matched
    # raise RuntimeError(f"No element found for selectors: {selectors!r}")    
    return False
    
async def pw_login_to_yahoo_async(
    page: AsyncPage,
    context: AsyncBrowserContext
) -> None:
    # 1) Load Yahoo
    write_line("Loading Yahoo Finance…")
    await page.goto("https://yahoo.com/finance", wait_until="domcontentloaded")
    write_line("Yahoo Finance loaded")

    # 2) Check if already logged in
    if await is_yahoo_logged_in_async(page):
        write_line("Already logged in via saved cookies.")
    else:
        # 2a) If “select account” flow
        if await page_has_text_async(page, "Select an account to sign in"):
            selectors = [
                {"property_name": "name", "property_value": "username"}
            ]
            await pw_click_by_selectors_async(page, selectors)

        # 2b) Standard username→password flow
        else:
            # — enter username —
            write_line("Entering username…")
            selectors = [
                {"property_name": "id", "property_value": "login-username"},
                {"property_name": "name", "property_value": "username"},
            ]
            if await pw_fill_by_selectors_async(page, selectors, cfg.YAHOO_USERNAME):
                write_line("Username entered")
            else:
                if await is_yahoo_logged_in_async(page):
                    pass
                else:
                    message = "Username entry failed; interactive login is required."
                    write_line(message)
                    if _is_interactive_session():
                        input("Press Enter once done…")
                    else:
                        raise RuntimeError(f"{message} (non-interactive session; cannot prompt for input).")

            # — submit username —
            write_line("Submitting username…")
            selectors = [
                {"property_name": "id", "property_value": "login-signin", "property_type": "input"},
                {"property_name": "id", "property_value": "tpa-google-button", "property_type": "button"}
            ]
            if await pw_click_by_selectors_async(page, selectors):
                write_line("Username submitted")
                await asyncio.sleep(2)
            else:
                write_line("Submit failed; proceed manually.")

        # — enter password —
        write_line("Entering password…")
        selectors = [
            {"property_name": "name", "property_value": "password"},
            {"property_name": "id", "property_value": "login-passwd"}
        ]
        if await pw_fill_by_selectors_async(page, selectors, cfg.YAHOO_PASSWORD):
            write_line("Password entered")
        else:
            message = "Password entry failed; interactive login is required."
            write_line(message)
            if _is_interactive_session():
                input("Press Enter once done…")
            else:
                raise RuntimeError(f"{message} (non-interactive session; cannot prompt for input).")

        # — submit password —
        write_line("Submitting password…")
        selectors = [
            {"property_name": "id", "property_value": "login-signin", "property_type": "button"},
            {"property_name": "id", "property_value": "tpa-google-button", "property_type": "button"}
        ]
        if await pw_click_by_selectors_async(page, selectors):
            write_line("Password submitted")
            await asyncio.sleep(10)
        else:
            write_line("Submit failed; proceed manually.")

    # 3) Dismiss the theme picker
    selectors = [
        {"property_name": "aria-label", "property_value": "OK", "property_type": "button"}
    ]
    if await pw_click_by_selectors_async(page, selectors):
        write_line("Dismissed theme dialog")
        await asyncio.sleep(5)
    else:
        write_line("No theme dialog, continuing.")

async def authenticate_yahoo_async(page: AsyncPage, context: AsyncBrowserContext) -> None:
    """
    Centralized authentication logic for Yahoo Finance.
    Loads cookies, logs in (if needed), and saves fresh cookies.
    """
    write_line("Attempting to load cookies for authentication...")
    cookies_path = "pw_cookies.json"
    cookies_data = mdc.get_common_json_content(cookies_path)
    
    loaded_count = 0
    if cookies_data:
        loaded_count = len(cookies_data)
        await context.add_cookies(cookies_data)
        write_line(f"Loaded {loaded_count} cookies from {cookies_path} (Common Container).")
    else:
        write_line(f"No existing cookies found at {cookies_path} in Common Container.")
        
    # Reload to apply cookies
    await page.reload()
    
    # Perform login check / execution
    await pw_login_to_yahoo_async(page, context)
    
    # Save fresh cookies
    new_cookies = await context.cookies()
    saved_count = len(new_cookies)
    mdc.save_common_json_content(new_cookies, cookies_path)
    write_line(f"Authentication complete. Saved {saved_count} cookies to {cookies_path} (Common Container).")    
    

   
async def is_yahoo_logged_in_async(
    page: AsyncPage,
    timeout: int = 5_000
) -> bool:
    """
    Async version: Determine if the user is logged into Yahoo by checking for the
    subscriptions badge link that only appears when authenticated.

    Args:
      page:     The Playwright AsyncPage instance.
      timeout:  Maximum time in milliseconds to wait for the element.

    Returns:
      True if the <a> with href="/subscriptions" containing the text "gold" is present.
    """
    locator = page.locator(
        "a[href='/subscriptions']",
        has_text="gold"
    )
    try:
        # Wait until at least one matching element is visible
        await locator.wait_for(timeout=timeout)
        return True
    except TimeoutError:
        return False    
    


async def pw_click_by_selectors_async(
    page: AsyncPage,
    selectors: List[Dict[str, Any]],
    max_attempts: int = 3,
    timeout: int = 2_000,
    delay_between_attempts: float = 0.5,
    wait_until: str = "load"
) -> bool:
    """
    Async version: Try each selector definition up to max_attempts times to locate an element,
    click it, and then wait for navigation or the load event.

    Args:
      page:                 The Playwright AsyncPage instance.
      selectors:            A list of dicts, each with:
                               - property_name:  attribute name (e.g. "id", "name")
                               - property_value: attribute value to match
                               - property_type:  (optional) tag name (e.g. "button", "input")
      max_attempts:         How many times to retry the selector list.
      timeout:              How long (ms) to wait for locators and navigation.
      delay_between_attempts: Seconds to wait before each retry cycle.
      wait_until:           When to consider navigation “done”: "load", 
                            "domcontentloaded", "networkidle", or "commit"

    Returns:
      True if a click succeeded (and any navigation/load completed), False otherwise.
    """
    for attempt in range(1, max_attempts + 1):
        for sel in selectors:
            name  = sel["property_name"]
            value = sel["property_value"]
            tag   = sel.get("property_type", "").strip()

            # Build CSS selector
            if name.lower() == "id":
                css = f"#{value}"
            else:
                css = f'[{name}="{value}"]'
            if tag:
                css = f"{tag}{css}"

            locator = page.locator(css)
            try:
                count = await locator.count()
                if count == 0:
                    continue

                # Attempt click, waiting for any navigation
                try:
                    async with page.expect_navigation(wait_until=wait_until, timeout=timeout):
                        await locator.first.click(timeout=timeout)
                except TimeoutError:
                    # No navigation happened within timeout; proceed anyway
                    pass

                return True

            except Exception:
                # locator.count() or click failed—try next selector
                continue

        # nothing clicked this round—pause before retrying
        if attempt < max_attempts:
            await asyncio.sleep(delay_between_attempts)

    # all attempts exhausted
    return False



async def page_has_text_async(
    page: AsyncPage,
    search_text: str,
    case_sensitive: bool = True
) -> bool:
    """
    Async version: Returns True if `search_text` is found anywhere in the page DOM.

    - Uses document.body.textContent, so it catches text even if split across tags.
    - By default it’s case-sensitive; pass case_sensitive=False for a case-insensitive check.
    """
    # 1) Pull all text from the DOM
    full_text = await page.evaluate("() => document.body.textContent")

    if not full_text:
        return False

    # 2) Optionally normalize case
    if not case_sensitive:
        full_text = full_text.lower()
        search_text = search_text.lower()

    # 3) Check for a simple substring match
    return search_text in full_text

def delete_time_window_duplicates(filename: str, window_seconds: int = 60) -> List[str]:
    """
    Compute a SHA-256 hash of the given file to uniquely identify it,
    then delete any files in the same directory whose creation time
    falls within ±window_seconds of the reference file's creation time
    and whose contents match (same hash).

    Args:
        filename:        Path to the reference file.
        window_seconds:  Number of seconds before/after the ref file's
                         creation time to include.

    Returns:
        A list of file paths that were deleted.
    """
    ref_path = Path(filename)
    if not ref_path.is_file():
        raise FileNotFoundError(f"{filename!r} does not exist or is not a file.")
    
    # Helper to compute SHA-256 hash of a file
    def compute_hash(path: Path) -> str:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    
    ref_hash = compute_hash(ref_path)
    ref_ctime = ref_path.stat().st_ctime
    min_ctime = ref_ctime - window_seconds
    max_ctime = ref_ctime + window_seconds

    deleted_files: List[str] = []
    for candidate in ref_path.parent.iterdir():
        if not candidate.is_file() or candidate == ref_path:
            continue
        ctime = candidate.stat().st_ctime
        # Check if creation time is within the ±window
        if min_ctime <= ctime <= max_ctime:
            if compute_hash(candidate) == ref_hash:
                candidate.unlink()
                deleted_files.append(str(candidate))

    return deleted_files

def merge_and_dedup_csv(
    input_files: List[str],
    output_path: str,
    dedup_subset: Optional[List[str]],
    keep: str,
    symbol: str
) -> None:
    """
    Merge multiple CSV files into one (including any existing output file),
    drop duplicate rows, and write to disk. Creates any missing folders in
    the output_path hierarchy.

    Args:
      input_files:    List of paths to CSV files to merge.
      output_path:    Path where the merged, deduplicated CSV will be saved.
      dedup_subset:   List of column names to consider when identifying duplicates.
                      If None, all columns are used.
      keep:           Which duplicate to keep: "first" or "last".

    Raises:
      ValueError:     If input_files is empty.
    """
    if not input_files:
        return
        #raise ValueError("No input files provided to merge_and_dedup_csv().")

    output_file = Path(output_path)

    # Include the existing output file in the merge if it already exists
    files_to_merge = input_files.copy()
    if output_file.is_file() and str(output_file) not in input_files:
        files_to_merge.append(str(output_file))

    # Read each CSV into a DataFrame
    dfs = [pd.read_csv(fp) for fp in files_to_merge]

    # Concatenate into one DataFrame
    merged = pd.concat(dfs, ignore_index=True)

    # Drop duplicates
    merged = merged.drop_duplicates(subset=dedup_subset, keep=keep)

    # Ensure the output directory exists (mkdir -p behavior)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Write out the result
    
    merged.to_csv(output_path, index=False)
     
def file_contains_all_keywords(file_name: str, keywords: list[str]) -> bool:
    """
    Returns True if the filename contains all keywords (case-insensitive).
    """
    lower_name = file_name.lower()
    return all(kw.lower() in lower_name for kw in keywords)


def go_to_sleep(range_low = 5, range_high = 20):
    # sleep for certain amount of time
    sleep_time = random.randint(range_low, range_high)
    write_line(f'Sleeping for {sleep_time} seconds...')
    time.sleep(random.randint(range_low, range_high))
def find_file_with_keywords(directory: str, keywords: list[str]) -> bool:
    """
    Walks through directory (and subdirectories), returning True as soon as it finds
    a file whose contents contain all of the specified keywords.
    """
    for root, _, files in os.walk(directory):
        for fname in files:
            full_path = os.path.join(root, fname)
            if file_contains_all_keywords(full_path, keywords):
                return True
    return False

def click_download_button(page):
    selectors = [
        {
            "property_type": "button",
            "property_name": "data-testid",
            "property_value": "download-link"
        },
        # Fallback: match by data-rapid_p attribute
        {
            "property_type": "button",
            "property_name": "data-rapid_p",
            "property_value": "21"
        }
    ]

    # Usage:
    clicked = pw_click_by_selectors(page, selectors, max_attempts=5)
    if not clicked:
        raise RuntimeError("Failed to click the Download button")

def find_latest_files(folder_path, search_string, extensions, new_extension):
    # Ensure extensions is a list, even if a single extension is provided
    if 'CRESY' in search_string:
        d = ''
    if isinstance(extensions, str):
        extensions = [extensions]
    
    counter = 0
    times_to_check = 3
    files = []

    while counter < times_to_check:
        files = []
        # Collect files matching any of the extensions
        for ext in extensions:
            search_pattern = os.path.join(folder_path, f"*.{ext}")
            files.extend(glob.glob(search_pattern))
        
        # Filter files to match only those containing the search string as a word
        filtered_files = [file for file in files if re.search(search_string, os.path.basename(file))]
        
        if not filtered_files:
            counter += 1
            time.sleep(random.randint(1, 3))
        else:
            files = filtered_files
            break
        
    if not files:
        print(f"No files found matching pattern: '{search_string}' as a word and extensions '{extensions}'")
        return None

def find_latest_file(folder_path, search_string, extensions, new_extension):
    # Ensure extensions is a list, even if a single extension is provided
    if 'CRESY' in search_string:
        d = ''
    if isinstance(extensions, str):
        extensions = [extensions]
    
    counter = 0
    times_to_check = 3
    files = []

    while counter < times_to_check:
        files = []
        # Collect files matching any of the extensions
        for ext in extensions:
            search_pattern = os.path.join(folder_path, f"*.{ext}")
            files.extend(glob.glob(search_pattern))
        
        # Filter files to match only those containing the search string as a word
        filtered_files = [file for file in files if re.search(search_string, os.path.basename(file))]
        
        if not filtered_files:
            counter += 1
            time.sleep(random.randint(1, 3))
        else:
            files = filtered_files
            break
        
    if not files:
        print(f"No files found matching pattern: '{search_string}' as a word and extensions '{extensions}'")


async def is_aria_selected_true_async(
    page: Page,
    selector: str,
    timeout: int = 5_000
) -> bool:
    """
    Async check whether the element matching `selector` has aria-selected="true".
    Returns False on timeout or if the attribute is missing/!= "true".
    """
    locator = page.locator(selector)
    try:
        await locator.wait_for(state="attached", timeout=timeout)
    except PlaywrightTimeoutError:
        return False

    attr = await locator.get_attribute("aria-selected")
    return attr == "true"


async def element_exists_async(
    page: Page,
    selector: str,
    timeout: int = 5_000
) -> bool:
    """
    Async check whether an element matching `selector` exists in the DOM.
    Returns False on timeout.
    """
    locator = page.locator(selector)
    try:
        await locator.wait_for(state="attached", timeout=timeout)
        return True
    except PlaywrightTimeoutError:
        return False
    except Exception as e:
        if "Timeout" in str(e):
            return False
        raise
    
