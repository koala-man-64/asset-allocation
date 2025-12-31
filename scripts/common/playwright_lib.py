from playwright.sync_api import sync_playwright
from typing import Tuple, Optional
import asyncio
from playwright.sync_api import Playwright, Browser, BrowserContext, Page, Download, TimeoutError as PlaywrightTimeout
from typing import (
    Optional,
    Tuple,
    Union,
    Coroutine,
    Literal,
    Any,
)
from playwright.async_api import (
    Page,
    Download,
    TimeoutError as PlaywrightTimeoutError,
)

from pathlib import Path

# sync types
from playwright.sync_api import (
    Playwright as SyncPlaywright,
    Browser as SyncBrowser,
    BrowserContext as SyncBrowserContext,
    Page as SyncPage,
    sync_playwright,
    TimeoutError
)
# async types
from playwright.async_api import (
    Playwright as AsyncPlaywright,
    Browser as AsyncBrowser,
    BrowserContext as AsyncBrowserContext,
    Page as AsyncPage,
    async_playwright,
    TimeoutError as PlaywrightTimeoutError
)
import json
import warnings
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import re
import pytz
import datetime
from datetime import timedelta
import hashlib
import os
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from filelock import FileLock
import pandas as pd
import glob
import random
from playwright.sync_api import Error          # generic base class
from playwright.sync_api import TimeoutError   # subclass for time-outs
from scripts.common import config as cfg
warnings.filterwarnings('ignore')


DOWNLOADS_PATH = Path.home() / "Downloads"
USER_DATA_DIR  = Path.home() / ".playwright_userdata"
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
    dl_root = Path(DOWNLOADS_PATH or Path.home() / "Downloads")
    dl_root.mkdir(parents=True, exist_ok=True)

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
                        go_to_sleep(1,1)
                        if not("Download is starting" in str(e)):
                            raise RuntimeError(f"Failed to download after {max_attempts} attempts: {e}") from e

            dl = await dl_info.value  # playwright.async_api.Download
            target = dl_root / dl.suggested_filename
            await dl.save_as(str(target))
            delete_newer_duplicates(str(target))
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

def download_yahoo_price_data(
    page: str,
    url: str,
    timeout: int = 30000,          # ms
    max_attempts: int = 3,
    delay_between_attempts: float = 1.0,
) -> Path:
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
    dl_root = Path(DOWNLOADS_PATH or Path.home() / "Downloads")
    dl_root.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, max_attempts + 1):
        try:
            # Navigation that ends in “download” instead of a page render.
            with page.expect_download(timeout=timeout) as dl_info:
                try:
                    page.goto(url, wait_until="commit")   # skip full load
                except Exception as e:
                    if 'ERR_ABORTED' in str(e) and 'query1' in str(e): #means this stock has no price data
                        pass
                    else:
                        raise RuntimeError(f"Failed to download after {max_attempts} attempts: {e}") from e
            dl = dl_info.value                       # playwright.download.Download
            target = dl_root / dl.suggested_filename
            dl.save_as(target)
            delete_newer_duplicates(str(target))
            return target

        except PlaywrightTimeout:
            # Download didn’t start fast enough – try again.
            if attempt < max_attempts:
                time.sleep(delay_between_attempts)
                continue
            raise RuntimeError(f"Timed out waiting for download from {url!r}")

        except Exception as e:
            # Other transient issues (net::ERR_ABORTED, etc.).
            if 'ERR_ABORTED' in str(e) and 'query1' in str(e): #means this stock has no price data
                raise RuntimeError(f"Failed to download after {max_attempts} attempts: {e}") from e
            if attempt < max_attempts:
                time.sleep(delay_between_attempts)
                continue
            else:
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
    
def get_yahoo_price_data(
    page,
    symbol: str,
    max_attempts: int = 3,
    delay_between_attempts: float = 1.0,
) -> Path:
  
    blacklist_file = str(COMMON_DIR / 'blacklist.csv')
    columns = ['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']
    df_price_data = pd.DataFrame()
    for attempt in range(1, max_attempts + 1):
        try:
            write_line(f"Downloading price data for {symbol}")

            # loop while offsetting the url continues to return results
            while True:
                try:
                    # Load price data url
                    target_base_url = f"https://finance.yahoo.com/quote/{symbol}/history/"
                    page = load_url(page, target_base_url)
                    
                    # Check dom for errors
                    html_content = page.content()             
                    if '<h1>500</h1>' in html_content or 'We are experiencing some temporary issues.' in html_content:
                        try:
                            page.reload()
                            go_to_sleep(5, 10)
                        except Exception as e:
                            raise e
                    elif ("We couldn't find any results." in html_content and len(df_price_data) == 0):
                        write_line(f"Blacklisting {symbol}.")
                        new_row = {"Ticker": symbol}
                        append_to_csv(blacklist_file, pd.DataFrame([new_row]))
                        break                    
                    else:
                        selectors = [
                            {
                                "property_name": "data-testid",        
                                "property_value": "download-link"
                            }
                        ]
                        result = pw_download_after_click_by_selectors(page, selectors, DOWNLOADS_PATH)
                        return result
                    
                except Exception as ex:
                    write_line(f"ERROR: Failed processing {symbol}: {ex}")               
                    raise ex

        except PlaywrightTimeout:
            # Download didn’t start fast enough – try again.
            if attempt < max_attempts:
                time.sleep(delay_between_attempts)
                continue
            raise RuntimeError(f"Timed out waiting for download from {url!r}")

        except Exception as e:
            # Other transient issues (net::ERR_ABORTED, etc.).
            if 'ERR_ABORTED' in str(e) and 'query1' in str(e): #means this stock has no price data
                raise RuntimeError(f"Failed to download after {max_attempts} attempts: {e}") from e
            if attempt < max_attempts:
                time.sleep(delay_between_attempts)
                continue
            else:
                raise RuntimeError(f"Failed to download after {max_attempts} attempts: {e}") from e

    return df_symbol_earnings

    
async def get_yahoo_earnings_data(
    page: AsyncPage,
    symbol: str,
    timeout,          # ms
    max_attempts: int = 3,
    delay_between_attempts: float = 1.0,
) -> Path:
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
    dl_root = Path(DOWNLOADS_PATH or Path.home() / "Downloads")
    dl_root.mkdir(parents=True, exist_ok=True)

    blacklist_file = str(COMMON_DIR / 'blacklist_earnings.csv')
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
                            page.reload()
                            go_to_sleep(5, 10)
                        except Exception as e:
                            raise e
                    elif ("We couldn't find any results." in html_content and len(df_symbol_earnings) == 0):
                        write_line(f"Blacklisting {symbol}.")
                        new_row = {"Ticker": symbol}
                        append_to_csv(blacklist_file, pd.DataFrame([new_row]))
                        break
                    elif cal_table is None or "We couldn't find any results." in html_content:
                        break
                except Exception as ex:
                    write_line(f"ERROR: Failed processing {symbol} for offset {offset}: {ex}")
                    # try:
                    #     driver.login_to_yahoo()
                    #     driver.refresh()
                    #     go_to_sleep(5, 10)
                    #     break
                    # except:
                    #     driver.quit()
                    #     driver = cl.get_driver(True)
                    #     driver.login_to_yahoo()                  
                    raise ex
            return df_symbol_earnings

        except PlaywrightTimeout:
            # Download didn’t start fast enough – try again.
            if attempt < max_attempts:
                time.sleep(delay_between_attempts)
                continue
            raise RuntimeError(f"Timed out waiting for download from {url!r}")

        except Exception as e:
            # Other transient issues (net::ERR_ABORTED, etc.).
            if 'ERR_ABORTED' in str(e) and 'query1' in str(e): #means this stock has no price data
                raise RuntimeError(f"Failed to download after {max_attempts} attempts: {e}") from e
            if attempt < max_attempts:
                time.sleep(delay_between_attempts)
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
    download_path = Path(downloads_dir) if downloads_dir else (Path.home() / "Downloads")
    download_path.mkdir(parents=True, exist_ok=True)

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
                    delete_newer_duplicates(str(target))
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
    download_path = Path(downloads_dir).expanduser() if downloads_dir else (Path.home() / "Downloads")
    download_path.mkdir(parents=True, exist_ok=True)

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

                    # Optional helper in your codebase; ignore if undefined.
                    try:
                        delete_newer_duplicates(str(target))  # type: ignore[name-defined]
                    except Exception:
                        pass

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

def get_playwright_browser(
    headless: Optional[bool] = None,
    slow_mo: Optional[int] = None,
    use_async: bool = False,
) -> Union[
    Tuple[SyncPlaywright, SyncBrowser, SyncBrowserContext, SyncPage],
    Coroutine[Any, Any, Tuple[AsyncPlaywright, AsyncBrowser, AsyncBrowserContext, AsyncPage]],
]:
    """
    Launch a fresh Playwright browser.  
    If use_async=False (default), returns a 4-tuple of sync objects.
    If use_async=True, returns an async coroutine that yields async objects when awaited.

    Returns (playwright, browser, context, page).
    """
    if headless is None:
        headless = cfg.HEADLESS_MODE

    if use_async:
        return _get_playwright_browser_async(headless, slow_mo)
    else:
        return _get_playwright_browser_sync(headless, slow_mo)


def _get_playwright_browser_sync(
    headless: bool,
    slow_mo: Optional[int],
) -> Tuple[SyncPlaywright, SyncBrowser, SyncBrowserContext, SyncPage]:
    # 1. Start the sync controller
    playwright = sync_playwright().start()

    # 2. Launch Chromium
    browser = playwright.chromium.launch(
        headless=headless,
        slow_mo=slow_mo or 0,
        args=["--disable-blink-features=AutomationControlled", "--disable-infobars"]
    )

    # 3. Persistent context (incognito-like with user data)
    context = playwright.chromium.launch_persistent_context(
        user_data_dir=str(USER_DATA_DIR),
        headless=headless,
        slow_mo=slow_mo or 0,
        accept_downloads=True,
        downloads_path=str(DOWNLOADS_PATH),
        user_agent=cfg.USER_AGENT,
        viewport={"width": 1920, "height": 1080},
        args=["--disable-blink-features=AutomationControlled", "--disable-infobars"]
    )
    
    # Stealth Init Script
    stealth_js = """
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
    """
    context.add_init_script(stealth_js)

    # 4. Open a new tab
    page = context.new_page()

    return playwright, browser, context, page


async def _get_playwright_browser_async(
    headless: bool,
    slow_mo: Optional[int],
) -> Tuple[AsyncPlaywright, AsyncBrowser, AsyncBrowserContext, AsyncPage]:
    # 1. Start the async controller
    playwright = await async_playwright().start()

    # 2. Launch Chromium
    browser = await playwright.chromium.launch(
        headless=headless,
        slow_mo=slow_mo or 0,
        args=["--disable-blink-features=AutomationControlled", "--disable-infobars"]
    )

    # 3. Persistent context
    context = await playwright.chromium.launch_persistent_context(
        user_data_dir=str(USER_DATA_DIR),
        headless=headless,
        slow_mo=slow_mo or 0,
        accept_downloads=True,
        downloads_path=str(DOWNLOADS_PATH),
        user_agent=cfg.USER_AGENT,
        viewport={"width": 1920, "height": 1080},
        args=["--disable-blink-features=AutomationControlled", "--disable-infobars"]
    )

    # Stealth Init Script
    stealth_js = """
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
    """
    await context.add_init_script(stealth_js)

    # 4. Open a new tab
    page = await context.new_page()

    return playwright, browser, context, page

def load_url(
    page: Page,
    url: str,
    wait_until: str = "load",
    timeout: int = 30_000
) -> Page:
    """
    Navigate the given Playwright Page to the specified URL.

    Args:
      page:       The Page object (tab) returned by get_playwright_browser().
      url:        The target URL to load.
      wait_until: When to consider navigation “done”:
                    - "load"            : wait for the load event
                    - "domcontentloaded": wait for DOMContentLoaded
                    - "networkidle"     : wait until there are no network connections for at least 500 ms
                    - "commit"          : wait until navigation is committed
      timeout:    Maximum time in milliseconds to wait before timing out.

    Returns:
      The same Page instance, now pointed at the new URL.
    """
    try:
        # 1. Use the Page (tab) from your context:
        #    This is where all your interactions happen—navigation, clicks, fills, etc.
        page.goto(url, wait_until=wait_until, timeout=timeout)

        # 2. At this point the page has fully loaded per the wait_until strategy.
        #    You can now interact with page.locator(), take screenshots, intercept network, etc.

        return page
    except Exception as e:
        write_line(f"ERROR: Failed loading url {url}")


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
    
def pw_save_cookies(
    context: BrowserContext,
    cookies_path: str
) -> None:
    """
    Save all cookies from the given Playwright BrowserContext to a JSON file.

    This lets you persist login/session cookies after you authenticate, so you
    can reload them on subsequent browser launches and skip the login step.

    Args:
      context:      A BrowserContext (from get_playwright_browser or similar).
      cookies_path: Path where to write the JSON list of cookie dicts.
    """
    # 1. Extract all cookies currently stored in this context
    cookies = context.cookies()

    # 2. Write them out to disk in JSON format
    with open(cookies_path, "w", encoding="utf-8") as f:
        json.dump(cookies, f, indent=2)

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

def pw_load_cookies(
    context: BrowserContext,
    cookies_path: str
) -> None:
    """
    Load cookies from a JSON file into the given Playwright BrowserContext.

    This lets you reuse login/session cookies across browser launches so you
    don’t have to re-authenticate every time.

    Args:
      context:       A BrowserContext returned by get_playwright_browser().
      cookies_path:  Path to a JSON file containing a list of cookie dicts,
                     e.g. [{"name": "...", "value": "...", "domain": "...", ...}, ...]
    """
    if os.path.exists(cookies_path):
        # 1. Read the cookie data from disk
        with open(cookies_path, "r", encoding="utf-8") as f:
            cookies = json.load(f)

        # 2. Add each cookie to the context’s cookie store
        #    Playwright will automatically scope them by domain/path.
        context.add_cookies(cookies)
 
def pw_fill_by_selectors(
    page: Page,
    selectors: list[dict],
    text: str,
    timeout: int = 10_000
) -> None:
    """
    Try each selector definition to locate an element on the page
    and fill it with the provided text.

    Args:
      page:       The Playwright Page instance.
      selectors:  A list of dicts, each with:
                    - property_name: the attribute name (e.g. "id", "name", "data-test")
                    - property_value: the expected attribute value.
      text:       The text to type into the first matching element.
      timeout:    How long (ms) to wait for each locator to appear.

    Raises:
      RuntimeError: if none of the selectors match an element.
    """
    for sel in selectors:
        name = sel["property_name"]
        value = sel["property_value"]

        # Build a CSS selector. If it's an id, use the # shortcut, otherwise use [attr="value"]
        if name.lower() == "id":
            css = f"#{value}"
        else:
            css = f'[{name}="{value}"]'

        locator = page.locator(css)
        # count() waits up to `timeout` for elements to appear
        # if locator.count(timeout=timeout) > 0:
        if locator.count() > 0:
            # fill() automatically waits for visibility & clears before typing
            locator.first.fill(text, timeout=timeout)
            return True

    raise RuntimeError(f"No element found for selectors: {selectors!r}") 
    
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
    await page.goto("https://yahoo.com/finance", wait_until="load")
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
                {"property_name": "id", "property_value": "login-username"}
            ]
            if await pw_fill_by_selectors_async(page, selectors, "rdprokes@gmail.com"):
                write_line("Username entered")
            else:
                if await is_yahoo_logged_in_async(page):
                    pass
                else:
                    write_line("Username entry failed; please enter manually.")
                    input("Press Enter once done…")

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
        if await pw_fill_by_selectors_async(page, selectors, "IRoll24Deep#1988"):
            write_line("Password entered")
        else:
            write_line("Password entry failed; please enter manually.")
            input("Press Enter once done…")

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
    
def pw_login_to_yahoo(page, context):
    
    # Load Yahoo login URL    
    write_line("Loading yahoo login url...")
    yahoo_login_url = (
        "https://yahoo.com/finance"
    )
    load_url(page, yahoo_login_url)
    write_line("Loaded yahoo login url")
    
    if is_yahoo_logged_in(page):
        write_line("Already logged into Yahoo from saved cookies.")
    else:
        
        # Select username from list
        if page_has_text(page, "Select an account to sign in"):
            selectors = [
                {"property_name": "name", "property_value": 'username'}
            ]
            pw_click_by_selectors(page, selectors)
            
        # Enter username
        else:
            write_line("Entering username...")
            selectors = [
                {"property_name": "id", "property_value": 'login-username'}
            ]
            result = pw_fill_by_selectors(page, selectors, 'rdprokes@gmail.com')
            if result: # text entered successfully
                write_line("Username entered")
            else:
                write_line("Username entry failed after retries. Must manually enter manually and proceed...")
                input("Press enter to continue...")
        
            # Click submit button
            write_line("Clicking submit button...")
            selectors = [
                {"property_name": "id", "property_value": 'login-signin', "property_type": "input"},
                {"property_name": "id", "property_value": 'tpa-google-button', "property_type": "button"}
            ]
            result = pw_click_by_selectors(page, selectors)
            if result: # text entered successfully
                write_line("Clicked submit button")
                time.sleep(2)
            else:
                write_line("Clicking submit failed. Must perform manually and proceed...")

        
        # Enter the password
        write_line("Entering password...")
        selectors = [
            {"property_name": 'name', "property_value": 'password'},
            {"property_name": 'id', "property_value": 'login-passwd'}
        ]
        result = pw_fill_by_selectors(page, selectors, 'IRoll24Deep#1988')
        if result: # text entered successfully
            write_line("Password entered")
        else:
            write_line("Password entry failed. Must manually enter and proceed...")
            input("Press enter to continue...")
        
        # Click submit button
        write_line("Clicking submit button...")
        selectors = [
            {"property_name": "id", "property_value": 'login-signin', "property_type": "button"},
            {"property_name": "id", "property_value": 'tpa-google-button', "property_type": "button"}
        ]
        result = pw_click_by_selectors(page, selectors)
        if result: # text entered successfully
            write_line("Clicked submit button")
            time.sleep(10)
        else:
            write_line("Clicking submit failed. Must perform manually and proceed...")
           
    # Click ok button for theme selector 
    selectors = [
        {"property_name": "aria-label", "property_value": 'OK', "property_type": "button"}
    ]
    result = pw_click_by_selectors(page, selectors)
    if result: # text entered successfully
        write_line("Clicked OK button")
        time.sleep(10)
    else:
        write_line("Clicking OK failed. Continuing.")

async def pw_login_to_yahoo_async(page: AsyncPage, context: AsyncBrowserContext):
    
    # Load Yahoo login URL    
    write_line("Loading yahoo login url...")
    yahoo_login_url = (
        "https://yahoo.com/finance"
    )
    await load_url_async(page, yahoo_login_url)
    write_line("Loaded yahoo login url")
    
    if await is_yahoo_logged_in_async(page):
        write_line("Already logged into Yahoo from saved cookies.")
    else:
        
        # Select username from list
        if await page_has_text_async(page, "Select an account to sign in"):
            selectors = [
                {"property_name": "name", "property_value": 'username'}
            ]
            await pw_click_by_selectors_async(page, selectors)
            
        # Enter username
        else:
            write_line("Entering username...")
            selectors = [
                {"property_name": "id", "property_value": 'login-username'}
            ]
            result = await pw_fill_by_selectors_async(page, selectors, 'rdprokes@gmail.com')
            if result: # text entered successfully
                write_line("Username entered")
            else:
                write_line("Username entry failed after retries. Must manually enter manually and proceed...")
                # input("Press enter to continue...") # Cannot input in async headless usually, rely on timeouts or logs
        
            # Click submit button
            write_line("Clicking submit button...")
            selectors = [
                {"property_name": "id", "property_value": 'login-signin', "property_type": "input"},
                {"property_name": "id", "property_value": 'tpa-google-button', "property_type": "button"}
            ]
            result = await pw_click_by_selectors_async(page, selectors)
            if result: # text entered successfully
                write_line("Clicked submit button")
                await asyncio.sleep(2)
            else:
                write_line("Clicking submit failed. Must perform manually and proceed...")

        
        # Enter the password
        write_line("Entering password...")
        selectors = [
            {"property_name": 'name', "property_value": 'password'},
            {"property_name": 'id', "property_value": 'login-passwd'}
        ]
        result = await pw_fill_by_selectors_async(page, selectors, 'IRoll24Deep#1988')
        if result: # text entered successfully
            write_line("Password entered")
        else:
            write_line("Password entry failed. Must manually enter and proceed...")
            # input("Press enter to continue...")
        
        # Click submit button
        write_line("Clicking submit button...")
        selectors = [
            {"property_name": "id", "property_value": 'login-signin', "property_type": "button"},
            {"property_name": "id", "property_value": 'tpa-google-button', "property_type": "button"}
        ]
        result = await pw_click_by_selectors_async(page, selectors)
        if result: # text entered successfully
            write_line("Clicked submit button")
            await asyncio.sleep(10)
        else:
            write_line("Clicking submit failed. Must perform manually and proceed...")
           
    # Click ok button for theme selector 
    selectors = [
        {"property_name": "aria-label", "property_value": 'OK', "property_type": "button"}
    ]
    result = await pw_click_by_selectors_async(page, selectors)
    if result: # text entered successfully
        write_line("Clicked OK button")
        await asyncio.sleep(10)
    else:
        write_line("Clicking OK failed. Continuing.")
        
def is_yahoo_logged_in(page: Page, timeout: int = 5_000) -> bool:
    """
    Determine if the user is logged into Yahoo by checking for the
    subscriptions badge link that only appears when authenticated.

    This version matches on both the href and the inner text "gold".

    Args:
      page:     The Playwright Page instance.
      timeout:  Maximum time in milliseconds to wait for the element.

    Returns:
      True if the <a> with href="/subscriptions" containing the text "gold" is present.
    """
    # Locate an <a> tag with the correct href and inner span text:
    locator = page.locator(
        "a[href='/subscriptions']",
        has_text="gold"
    )
    return locator.count() > 0
   
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
    
def pw_click_by_selectors(
    page: Page,
    selectors: List[Dict[str, Any]],
    max_attempts: int = 3,
    timeout: int = 2_000,
    delay_between_attempts: float = 0.5,
    wait_until: str = "load"
) -> bool:
    """
    Try each selector definition up to max_attempts times to locate an element,
    click it, and then wait for navigation or the load event.

    Args:
      page:                 The Playwright Page instance.
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
            name = sel["property_name"]
            value = sel["property_value"]
            tag  = sel.get("property_type", "").strip()

            # Build CSS selector
            if name.lower() == "id":
                css = f"#{value}"
            else:
                css = f'[{name}="{value}"]'
            if tag:
                css = f"{tag}{css}"

            locator = page.locator(css)
            try:
                # wait up to `timeout` for the element
                if locator.count() > 0:
                    try:
                        # If click triggers a navigation, this will wait for it.
                        with page.expect_navigation(wait_until=wait_until, timeout=timeout):
                            locator.first.click(timeout=timeout)
                            # time.sleep(1)
                    except Exception as e:
                        if 'Timeout' in str(e):
                            # No navigation occurred within timeout – likely page loaded instantly
                            pass
                    return True
            except Exception as e:
                
                # element not found or click failed; try next selector
                continue

        # nothing clicked this round—pause before retrying
        if attempt < max_attempts:
            time.sleep(delay_between_attempts)

    # all attempts exhausted
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

def page_has_text(page, search_text: str, case_sensitive: bool = True) -> bool:
    """
    Returns True if `search_text` is found anywhere in the page DOM.

    - Uses document.body.textContent, so it catches text even if split across tags.
    - By default it’s case-sensitive; pass case_sensitive=False for a case-insensitive check.
    """
    # 1) Pull all text from the DOM
    full_text = page.evaluate("() => document.body.textContent")

    # 2) Optionally normalize case
    if not case_sensitive:
        full_text = full_text.lower()
        search_text = search_text.lower()

    # 3) Check for a simple substring match
    return search_text in full_text

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
        return None
    
    # Determine the latest file by modification time
    latest_file = max(files, key=os.path.getmtime)
    if search_string not in latest_file:
        return None
        d =''
        find_latest_file(folder_path, search_string, extensions, new_extension)
    return latest_file


def scan_ticker_files(
    folder_path: str,
    ticker: str,
    substring: str,
    extensions: List[str]
) -> Tuple[List[str], List[str], List[str]]:
    """
    Scan the given folder for files whose names contain the ticker and
    one of the given extensions, then classify them into quarterly,
    monthly, and annual lists based on filename.

    Args:
      folder_path: Path to the directory to scan (non-recursive).
      ticker:      Stock ticker to match in filenames (case-insensitive).
      extensions:  List of file extensions to include (e.g. ["csv", "xlsx"]).

    Returns:
      A tuple of three lists of file paths (strings):
        - quarterly_files: filenames containing passed in ticker
    """
    base = Path(folder_path)
    ticker_lower = ticker.lower()
    ext_set = {ext.lower().lstrip('.') for ext in extensions}

    files: List[str] = []
    for file_path in base.iterdir():
        if not file_path.is_file():
            continue

        name_lower = file_path.name.lower()
        # Must contain ticker and one of the extensions
        if ticker_lower not in name_lower:
            continue
        if file_path.suffix.lstrip('.').lower() not in ext_set:
            continue

        # Classify based on the keywords in the filename
        if substring in name_lower:
            files.append(str(file_path))

    return files

def delete_newer_duplicates(filename: str) -> List[str]:
    """
    Compute a SHA-256 hash of the given file to uniquely identify it,
    then delete any files in the same directory that were created on or
    after this file and have the same hash.

    Args:
        filename: Path to the reference file.

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

    deleted_files: List[str] = []
    for candidate in ref_path.parent.iterdir():
        try:
            if not candidate.is_file() or candidate == ref_path:
                continue
            # check creation time
            window_seconds = 10 * 60  # 5 minutes
            for candidate in ref_path.parent.iterdir():
                if not candidate.is_file() or candidate == ref_path:
                    continue

                candidate_ctime = candidate.stat().st_ctime
                # check if created within 5 minutes before the ref file or any time after it
                if candidate_ctime >= ref_ctime - window_seconds:
                    if compute_hash(candidate) == ref_hash:
                        candidate.unlink()
                        deleted_files.append(str(candidate))
                        return deleted_files
        except Exception as e:
            continue
    return deleted_files

def append_to_csv(path, data):
    """
    Appends rows from `data` to CSV at `path`, creating the file (with header)
    if it doesn’t exist or is empty, and locking via a .lock file to avoid races.
    """
    lock_file = path + ".lock"
    lock = FileLock(lock_file)
    df = pd.DataFrame(data)

    with lock:
        # Treat non-existent OR zero‐byte files as “need header”
        file_empty = (not os.path.exists(path)) or (os.path.getsize(path) == 0)

        df.to_csv(
            path,
            mode='a', 
            header=file_empty,
            index=False
        )

def delete_files_with_string(folder_path, search_string, extensions):
    """
    Deletes files in the specified folder that contain the given search string 
    as a whole word and have one of the specified extensions.
    
    Args:
        folder_path (str): Path to the folder containing the files.
        search_string (str): String to search for as a whole word in file names.
        extensions (list or str): List of file extensions to filter by, or a single extension.
    """
    search_string = str(search_string)
    # if np.isnan(search_string):
    #     return
    
    # Ensure extensions is a list, even if a single extension is provided
    if isinstance(extensions, str):
        extensions = [extensions]
    
    matching_files = []
    
    # Iterate over each extension to collect matching files
    for ext in extensions:
        search_pattern = os.path.join(folder_path, f"*.{ext}")
        files = glob.glob(search_pattern)
        # Filter files where the search string matches as a whole word in the filename (excluding extension)
        matching_files.extend([
            file for file in files
            if search_string in os.path.splitext(os.path.basename(file))[0]
        ])
    
    if not matching_files:
        # print(f"No files found matching the search string '{search_string}' as a whole word with extensions {extensions}.")
        pass
    else:
        # Iterate through the list of matching files and delete each one
        for file in matching_files:
            try:
                os.remove(file)
                print(f"Deleted file: {file}")
            except OSError as e:
                print(f"Error deleting file {file}: {e}")

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

def is_aria_selected_true(
    page: Page,
    selector: str,
    timeout: int = 5_000
) -> bool:
    """
    Check whether the element matching `selector` has aria-selected="true".

    Args:
      page:      Playwright Page instance.
      selector:  CSS selector for the target element.
      timeout:   How long (ms) to wait for the element to appear.

    Returns:
      True if aria-selected is exactly "true", False otherwise.
    """
    locator = page.locator(selector)
    try:
        # wait for the element to be in the DOM
        locator.wait_for(state="attached", timeout=timeout)
    except TimeoutError:
        return False

    # get_attribute returns a string or None
    return locator.get_attribute("aria-selected") == "true"

def element_exists(
    page: Page,
    selector: str,
    timeout: int = 5_000
) -> bool:
    """
    Check whether an element matching `selector` exists in the DOM.

    Args:
      page:      Playwright Page instance.
      selector:  CSS selector for the target element.
      timeout:   How long (ms) to wait for the element to appear.

    Returns:
      True if the element is found, False otherwise.
    """
    locator = page.locator(selector)
    try:
        locator.wait_for(state="attached", timeout=timeout)
        return True
    except TimeoutError as te:
        return False
    except Exception as e:
        if 'Timeout' in str(e):
            return False
        else:
            raise e
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
    
def get_all_reports_to_refresh(ticker: str):
    # Define the configuration for each report type.
    # Note: Adjust paths and xpaths as needed.
    reports = [
        {
            "name": "Quarterly Balance Sheet",
            "black_path": str(COMMON_DIR / 'blacklist_financial.csv'),
            "file_path": str(COMMON_DIR / 'Yahoo' / 'Balance Sheet' / f'{ticker}_quarterly_balance-sheet.csv'),
            "file_suffix": "quarterly_balance-sheet",
            "url": f'https://finance.yahoo.com/quote/{ticker}/balance-sheet?p={ticker}',
            "ticker": ticker,
            "period": "quarterly",
            "error_condition": lambda current_url: (
                '404' in current_url 
            ),
            "blacklist_condition": lambda ticker, content:(
                f"No results for '{ticker}'" in content or f"Symbols similar to '{ticker}'" in content
            )
        },
        {
            "name": "Quarterly Valuations",
            "black_path": str(COMMON_DIR / 'blacklist_financial.csv'),
            "file_path": str(COMMON_DIR / 'Yahoo' / 'Valuation' / f'{ticker}_quarterly_valuation_measures.csv'),
            "file_suffix": "quarterly_valuation_measures",
            "url": f'https://finance.yahoo.com/quote/{ticker}/key-statistics?p={ticker}',
            "ticker": ticker,
            "period": "quarterly",
            "error_condition": lambda current_url,: (
                '404' in current_url 
            ),
            "blacklist_condition": lambda ticker, content:(
                f"No results for '{ticker}'" in content or f"Symbols similar to '{ticker}'" in content
            )
        },
        {
            "name": "Quarterly Cash Flow",
            "black_path": str(COMMON_DIR / 'blacklist_financial.csv'),
            "file_path": str(COMMON_DIR / 'Yahoo' / 'Cash Flow' / f'{ticker}_quarterly_cash-flow.csv'),
            "file_suffix": "quarterly_cash-flow",
            "url": f'https://finance.yahoo.com/quote/{ticker}/cash-flow?p={ticker}',
            "ticker": ticker,
            "period": "quarterly",
            "error_condition": lambda current_url,: (
                '404' in current_url 
            ),
            "blacklist_condition": lambda ticker, content:(
                f"No results for '{ticker}'" in content or f"Symbols similar to '{ticker}'" in content
            )
        },
        {
            "name": "Quarterly Income Statement",
            "black_path": str(COMMON_DIR / 'blacklist_financial.csv'),
            "file_path": str(COMMON_DIR / 'Yahoo' / 'Income Statement' / f'{ticker}_quarterly_financials.csv'),
            "file_suffix": "quarterly_financials",
            "url": f'https://finance.yahoo.com/quote/{ticker}/financials?p={ticker}',
            "ticker": ticker,
            "period": "quarterly",
            "error_condition": lambda current_url: (
                '404' in current_url 
            ),
            "blacklist_condition": lambda ticker, content:(
                f"No results for '{ticker}'" in content or f"Symbols similar to '{ticker}'" in content
            )
        }
    ]
    try:
        reports_to_refresh = []
        for report in reports:
            # file_paths = [report["annual_file_path"], report["quarterly_file_path"],  report["monthly_file_path"]]
            # for file_path in file_paths:
            file_path = report['file_path']
            if os.path.exists(file_path):
                file_mtime = os.path.getmtime(file_path)
                file_modification_date = datetime.datetime.fromtimestamp(file_mtime)
                threshold_date = datetime.datetime.now() - timedelta(days=28)
                if file_modification_date > threshold_date:
                    # write_line(f'{file_path} updated within the past 28 days so skipping.')
                    continue
            
            reports_to_refresh.append(report)
        write_line(f"{len(reports_to_refresh)} report(s) added for {ticker}")
    except Exception as e:
        pass
    return reports_to_refresh

def transpose_yahoo_dataframe(filepath: str, ticker, **read_csv_kwargs) -> None:
    """
    Read a CSV export from Yahoo Finance, transpose it, compute percent changes,
    and overwrite the original file with the transformed data.

    Parameters
    ----------
    filepath : str
        Path to the CSV file containing the Yahoo Finance table.
    **read_csv_kwargs :
        Additional kwargs to pass to pd.read_csv (e.g. sep, header, encoding).

    Returns
    -------
    None
    """
    print(f"▶ Reading file: {filepath}")
    df = pd.read_csv(filepath, **read_csv_kwargs)

    # Set the 'name' column as the index
    df.set_index("name", inplace=True)

    # Drop rows where all elements are NaN
    df.dropna(how='all', inplace=True)

    # Drop 'ttm' column if present
    if 'ttm' in df.columns:
    #     df.drop(columns=['ttm'], inplace=True)
        df = df.rename(columns={"ttm": datetime.date.today().strftime("%m/%d/%Y")})
    
    # Remove commas and cast to float
    df = df.replace(',', '', regex=True).astype(float)

    # Replace any remaining NA/NaN with 0
    df.fillna(0, inplace=True)

    # Transpose so dates become the index
    df_transposed = df.T

    # Convert the index to datetime
    df_transposed.index = pd.to_datetime(df_transposed.index)

    # Compute percent change
    # df_transposed = df_transposed.pct_change() * 100
    df_transposed['Symbol'] = ticker
    # Write back to the same file, labeling the index as "Date"
    print("▶ Transformation complete — writing back to file")
    
    df_transposed.to_csv(filepath, index=True, index_label='Date')
    print(f"✔ File updated: {filepath}")

def process_single_report(report, df_blacklist, DOWNLOADS_PATH, page_factory):
    """
    Encapsulate the per-report logic you had in your loop.
    `page_factory()` should return a fresh Playwright `page` object for each thread.
    """
    ticker = report['ticker']
    if ticker in df_blacklist['Ticker'].tolist():
        write_line(f"Skipping blacklisted {ticker}")
        return

    write_line(f"Getting {report['name']} for {ticker}")
    retry_counter = 0
    max_retries = 3

    page = page_factory()  # new browser/page for this thread

    while retry_counter < max_retries:
        retry_counter += 1
        try:
            # … put all your existing download-and-click logic here …
            # Instead of spawning a thread for the pipeline, just call it directly:
            files = scan_ticker_files(DOWNLOADS_PATH, f"{ticker}_", report['file_suffix'], ['csv','crdownload'])
            if files:
                merge_and_dedup_csv(
                    input_files=files,
                    output_path=report['file_path'],
                    dedup_subset=None,
                    keep="first",
                    symbol=ticker
                )
                transpose_yahoo_dataframe(report['file_path'], ticker)
                delete_files_with_string(DOWNLOADS_PATH, f"{ticker}_", ['csv','crdownload'])
                write_line(f"✅ Completed pipeline for {ticker}")
                break
        except Exception as e:
            write_line(f"ERROR in {report['name']} for {ticker}: {e}")
    else:
        write_line(f"❌ Failed after {max_retries} retries: {ticker}")

def run_reports_in_parallel(reports, df_blacklist, DOWNLOADS_PATH, page_factory, max_workers=4):
    """
    Kick off up to `max_workers` threads to process reports in parallel.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        # Submit all reports
        futures = {
            exe.submit(process_single_report, rpt, df_blacklist, DOWNLOADS_PATH, page_factory): rpt
            for rpt in reports
        }

        # Optional: wait for them and log any exceptions
        for future in as_completed(futures):
            rpt = futures[future]
            try:
                future.result()
            except Exception as e:
                write_line(f"Unhandled exception for {rpt['ticker']}: {e}")

# --- Usage ---
# Define a page factory that returns a new Playwright page each call:
def make_new_page():
    playwright, browser, context, page = get_playwright_browser()
    return page


def _process_ticker(files, report_path, ticker, DOWNLOADS_PATH):
    # 1) merge & dedup
    merge_and_dedup_csv(
        input_files=files,
        output_path=report_path,
        dedup_subset=None,
        keep="first",
        symbol=ticker
    )
    # 2) transpose in‑place
    transpose_yahoo_dataframe(report_path, ticker)
    # 3) clean up downloads
    delete_files_with_string(DOWNLOADS_PATH, ticker + '_', ['csv', 'crdownload'])

def run_report_pipeline(files, report, ticker, DOWNLOADS_PATH):
    """
    Kick off the merge→transpose→cleanup sequence on a background thread.
    Returns the Thread object in case you want to .join() or inspect .is_alive().
    """
    t = threading.Thread(
        target=_process_ticker,
        args=(files, report['file_path'], ticker, DOWNLOADS_PATH),
        daemon=True
    )
    t.start()
    return t

def get_all_reports_list(params: Tuple[Playwright, Browser, BrowserContext, Page], reports: list, df_blacklist: pd.DataFrame):
    playwright = params[0] 
    browser = params[1]
    context = params[2]
    page = params[3]
    report_counter = 0
    for report in reports:
        report_counter += 1
        ticker = report['ticker']
        if ticker in list(df_blacklist['Ticker']) or pd.isna(ticker):
            continue
        
        # Begin retry loop for the report download        
        write_line(f"Getting {report['name']} for {ticker} - {report_counter} / {len(reports)}")
        max_retries = 3
        retry_counter = 0
        while retry_counter < max_retries:
            retry_counter += 1
            try:           
                max_attempts = 3
                for attempt in range(1, max_attempts + 1):
                    load_url(page, report["url"])  
                    selector = f'button#tab-{report["period"]}[role="tab"]'
                    exists = element_exists(page, selector)  
                    if not exists:
                        write_line(f"Skipping because {report['period']} {report['name']} doesn't exist for {ticker}")    
                        # if report["blacklist_condition"](ticker, page.content()):                        
                        new_row = {"Ticker": ticker}
                        df_blacklist.loc[len(df_blacklist)] = new_row
                        append_to_csv(report['black_path'], pd.DataFrame([new_row]))
                        
                        retry_counter = 4
                        break      
                    else:
                        try:                      
                            # Click period button
                            selectors = [
                                {
                                    "property_type": "button",
                                    "property_name": "id",
                                    "property_value": f"tab-{report['period']}"
                                },
                                {
                                    "property_type": "button",
                                    "property_name": "title",
                                    "property_value": f"{report['period'].capitalize()}"
                                }
                            ]
                            
                            result = pw_click_by_selectors(page, selectors)
                            while not is_aria_selected_true(page, f'button#tab-{report["period"]}[role="tab"]'):
                                result = pw_click_by_selectors(page, selectors)
                            if result:
                                write_line(f"Successfully clicked {report['period']} tab for {ticker} {report['name']}")
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
                                
                                exists = element_exists(page, 'button[data-testid="download-link"]')
                                if exists:
                                    result = pw_download_after_click_by_selectors(page, selectors, DOWNLOADS_PATH)
                                    if result:
                                        write_line(f"Successfully downloaded {report['period']} data for {ticker} {report['name']} on attempt {attempt}")
                                        files = scan_ticker_files(DOWNLOADS_PATH, f"{ticker}_", report['file_suffix'], ['csv', 'crdownload'] )
                                        if len(files) > 0:
                                            thread = run_report_pipeline(files, report, ticker, DOWNLOADS_PATH)
                                            retry_counter = 4
                                            break
                                        else:
                                            write_line(f"Failed to download {report['period']} data for {ticker} {report['name']}")

                                    else:
                                        write_line(f"Attempt {attempt} failed downloading {report['period']} data for {ticker} {report['name']}.")
                                        if attempt < max_attempts:
                                            time.sleep(1)  # brief pause before retrying
                                        else:
                                            write_line(f"Failed downloading {report['period']} data for {ticker} {report['name']} after {max_attempts} attempts.")
                                else:
                                    new_row = {"Ticker": ticker}
                                    df_blacklist.loc[len(df_blacklist)] = new_row
                                    append_to_csv(report['black_path'], pd.DataFrame([new_row]))
                                    retry_counter = 4
                                    break
                            else:
                                write_line(f"Failed clicking {report['period']} tab for {ticker} {report['name']}.")        
                        except Exception as e:
                            write_line(f"ERROR: Failed downloading {report['name']} for {ticker} - {str(e)}")
            except Exception as e:
                write_line(f'ERROR: {ticker} in {report["name"]} - {e}')       
                # If the error condition is met, update the blacklist and break.
                content = page.content()
                if report["blacklist_condition"](ticker, content) or retry_counter >= max_retries:                        
                    new_row = {"Ticker": ticker}
                    df_blacklist.loc[len(df_blacklist)] = new_row
                    append_to_csv(report['black_path'], pd.DataFrame([new_row]))
                    retry_counter = 4
                    break
                elif report["error_condition"](page.url):
                    write_line(f'No {report["name"]} found for {ticker}')    
                    # break
                pass
                # else:
                #     playwright, browser, context, page = get_playwright_browser()
                    
                
    return (playwright, browser, context, page)
     
     
async def get_all_reports_list_async(params: Tuple[Playwright, Browser, BrowserContext, Page], reports: list):
    # Extract playwright parameters
    playwright = params[0] 
    browser = params[1]
    context = params[2]
    page = params[3]
    
    # Iterate through list of reports
    report_counter = 0
    for report in reports:
        report_counter += 1
        ticker = report['ticker']
        
        # Begin retry loop for the report download        
        write_line(f"Getting {report['name']} for {ticker} - {report_counter} / {len(reports)}")
        max_retries = 3
        retry_counter = 0
        while retry_counter < max_retries:
            
            # Increment retry counter
            retry_counter += 1
            try:           
                max_attempts = 3
                
                # Try max number of times
                for attempt in range(1, max_attempts + 1):
                    
                    # Load url for report
                    await load_url_async(page, report["url"])  
                    
                    # Check if period tab exists
                    selector = f'button#tab-{report["period"]}[role="tab"]'
                    exists = await element_exists_async(page, selector)  
                    if not exists:
                        
                        # If it doesn't exist add it to the blacklist
                        write_line(f"Skipping because {report['period']} {report['name']} doesn't exist for {ticker}")    
                        new_row = {"Ticker": ticker}
                        append_to_csv(report['black_path'], pd.DataFrame([new_row]))
                        
                        # Break out of the retry loop
                        retry_counter = 4
                        break  
                       
                    else:
                        try:                      
                            # Setup selectors for period button
                            selectors = [
                                {
                                    "property_type": "button",
                                    "property_name": "id",
                                    "property_value": f"tab-{report['period']}"
                                },
                                {
                                    "property_type": "button",
                                    "property_name": "title",
                                    "property_value": f"{report['period'].capitalize()}"
                                }
                            ]
                            
                            
                            result = await pw_click_by_selectors_async(page, selectors)
                            if result:
                                write_line(f"Successfully clicked {report['period']} tab for {ticker} {report['name']}")
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
                                
                                # Check if download link exists
                                exists = await element_exists_async(page, 'button[data-testid="download-link"]')
                                if exists:
                                    
                                    # Click download button
                                    result = await pw_download_after_click_by_selectors_async(page, selectors, DOWNLOADS_PATH)
                                    if result:
                                        write_line(f"Successfully downloaded {report['period']} data for {ticker} {report['name']} on attempt {attempt}")
                                        files = scan_ticker_files(DOWNLOADS_PATH, f"{ticker}_", report['file_suffix'], ['csv', 'crdownload'] )
                                        if len(files) == 0:
                                            write_line(f"Failed to download {report['period']} data for {ticker} {report['name']}")
                                            continue
                                        thread = run_report_pipeline(files, report, ticker, DOWNLOADS_PATH)
                                        retry_counter = 4
                                        break
                                    else:
                                        write_line(f"Attempt {attempt} failed downloading {report['period']} data for {ticker} {report['name']}.")
                                        if attempt < max_attempts:
                                            time.sleep(1)  # brief pause before retrying
                                        else:
                                            write_line(f"Failed downloading {report['period']} data for {ticker} {report['name']} after {max_attempts} attempts.")
                                else:
                                    new_row = {"Ticker": ticker}
                                    append_to_csv(report['black_path'], pd.DataFrame([new_row]))
                                    retry_counter = 4
                                    break
                            else:
                                write_line(f"Failed clicking {report['period']} tab for {ticker} {report['name']}.")        
                        except Exception as e:
                            write_line(f"ERROR: Failed downloading {report['name']} for {ticker} - {str(e)}")
            except Exception as e:
                write_line(f'ERROR: {ticker} in {report["name"]} - {e}')       
                # If the error condition is met, update the blacklist and break.
                content = await page.content()
                if report["blacklist_condition"](ticker, content) or retry_counter >= max_retries:                        
                    new_row = {"Ticker": ticker}
                    append_to_csv(report['black_path'], pd.DataFrame([new_row]))
                    retry_counter = 4
                    break
                elif report["error_condition"](page.url):
                    write_line(f'No {report["name"]} found for {ticker}')    
                    # breaks                    
                
    return (playwright, browser, context, page)
     