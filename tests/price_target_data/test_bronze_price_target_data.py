import pytest
import pandas as pd
import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

from tasks.price_target_data import bronze_price_target_data as bronze
from core import config as cfg
from core import core as mdc

# --- Helpers ---

@pytest.fixture
def unique_ticker():
    return f"TEST_INT_{uuid.uuid4().hex[:8].upper()}"

@pytest.fixture
def storage_cleanup(unique_ticker):
    container = cfg.AZURE_CONTAINER_BRONZE
    mdc.get_storage_client(container) 
    yield unique_ticker

# --- Integration Tests ---


@patch('tasks.price_target_data.bronze_price_target_data.nasdaqdatalink')
@patch('tasks.price_target_data.bronze_price_target_data.bronze_client')
@patch('tasks.price_target_data.bronze_price_target_data.list_manager')
def test_process_batch_bronze(mock_list_manager, mock_client, mock_nasdaq, unique_ticker, storage_cleanup):
    symbol = unique_ticker
    
    # 1. Mock Blob checks (return False -> Stale -> Fetch)
    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = False
    mock_client.get_blob_client.return_value = mock_blob_client
    
    # 2. Mock API return
    mock_api_df = pd.DataFrame({
        'ticker': [symbol],
        'obs_date': [pd.Timestamp('2023-01-01')],
        'tp_mean_est': [50.0]
    })
    mock_nasdaq.get_table.return_value = mock_api_df
    
    # 3. Execute
    semaphore = asyncio.Semaphore(1)
    
    async def run_test():
        # We patch store_raw_bytes to verify write
        with patch('core.core.store_raw_bytes') as mock_store:
            await bronze.process_batch_bronze([symbol], semaphore)
            
            # 4. Verify
            # Check API called
            mock_nasdaq.get_table.assert_called()
            
            # Check Store Raw
            mock_store.assert_called_once()
            args, kwargs = mock_store.call_args
            # args[1] should be path
            assert args[1] == f"price-target-data/{symbol}.parquet"
            
            # Check Whitelist updated
            mock_list_manager.add_to_whitelist.assert_called_with(symbol)

    asyncio.run(run_test())


@patch('tasks.price_target_data.bronze_price_target_data.nasdaqdatalink')
@patch('tasks.price_target_data.bronze_price_target_data.bronze_client')
@patch('tasks.price_target_data.bronze_price_target_data.list_manager')
def test_process_batch_bronze_skips_blacklist_for_filtered_missing(
    mock_list_manager,
    mock_client,
    mock_nasdaq,
):
    symbol_with_data = "AAA"
    symbol_missing = "BBB"

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = False
    mock_client.get_blob_client.return_value = mock_blob_client

    mock_api_df = pd.DataFrame({
        'ticker': [symbol_with_data],
        'obs_date': [pd.Timestamp('2024-03-01')],
        'tp_mean_est': [55.0]
    })
    mock_nasdaq.get_table.return_value = mock_api_df

    semaphore = asyncio.Semaphore(1)

    async def run_test():
        with patch('core.core.store_raw_bytes') as mock_store:
            summary = await bronze.process_batch_bronze(
                [symbol_with_data, symbol_missing],
                semaphore,
                backfill_start=pd.Timestamp('2024-01-01').date(),
            )
            assert summary["blacklisted"] == 0
            assert summary["filtered_missing"] == 1
            assert summary["deleted"] == 1
            mock_list_manager.add_to_blacklist.assert_not_called()
            assert mock_store.call_count == 1
            mock_client.delete_file.assert_called_once_with(f"price-target-data/{symbol_missing}.parquet")

    asyncio.run(run_test())


@patch('tasks.price_target_data.bronze_price_target_data.nasdaqdatalink')
@patch('tasks.price_target_data.bronze_price_target_data.bronze_client')
@patch('tasks.price_target_data.bronze_price_target_data.list_manager')
def test_process_batch_bronze_deletes_stale_when_cutoff_and_empty_response(
    mock_list_manager,
    mock_client,
    mock_nasdaq,
):
    symbols = ["AAA", "BBB"]

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = False
    mock_client.get_blob_client.return_value = mock_blob_client
    mock_nasdaq.get_table.return_value = pd.DataFrame()

    semaphore = asyncio.Semaphore(1)

    async def run_test():
        with patch('core.core.store_raw_bytes') as mock_store:
            summary = await bronze.process_batch_bronze(
                symbols,
                semaphore,
                backfill_start=pd.Timestamp('2024-01-01').date(),
            )
            assert summary["blacklisted"] == 0
            assert summary["filtered_missing"] == 2
            assert summary["deleted"] == 2
            assert summary["save_failed"] == 0
            mock_store.assert_not_called()
            assert mock_client.delete_file.call_count == 2
            mock_client.delete_file.assert_any_call("price-target-data/AAA.parquet")
            mock_client.delete_file.assert_any_call("price-target-data/BBB.parquet")

    asyncio.run(run_test())


@patch('tasks.price_target_data.bronze_price_target_data.nasdaqdatalink')
@patch('tasks.price_target_data.bronze_price_target_data.bronze_client')
@patch('tasks.price_target_data.bronze_price_target_data.list_manager')
def test_process_batch_bronze_uses_watermark_and_appends_existing(
    mock_list_manager,
    mock_client,
    mock_nasdaq,
):
    symbol = "AAA"
    existing_df = pd.DataFrame(
        {
            "ticker": [symbol],
            "obs_date": [pd.Timestamp("2024-03-01")],
            "tp_mean_est": [50.0],
        }
    )
    existing_parquet = existing_df.to_parquet(index=False)

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = True
    mock_blob_client.get_blob_properties.return_value = MagicMock(
        last_modified=datetime.now(timezone.utc) - timedelta(days=2)
    )
    mock_client.get_blob_client.return_value = mock_blob_client

    mock_nasdaq.get_table.return_value = pd.DataFrame(
        {
            "ticker": [symbol],
            "obs_date": [pd.Timestamp("2024-03-02")],
            "tp_mean_est": [55.0],
        }
    )
    semaphore = asyncio.Semaphore(1)

    async def run_test():
        with patch("core.core.read_raw_bytes", return_value=existing_parquet), patch(
            "core.core.store_raw_bytes"
        ) as mock_store:
            summary = await bronze.process_batch_bronze([symbol], semaphore)
            assert summary["saved"] == 1
            assert summary["blacklisted"] == 0

            _, call_kwargs = mock_nasdaq.get_table.call_args
            assert call_kwargs["obs_date"]["gte"] == "2024-03-02"

            args, _ = mock_store.call_args
            written_df = pd.read_parquet(BytesIO(args[0]))
            assert set(pd.to_datetime(written_df["obs_date"]).dt.date.astype(str).tolist()) == {
                "2024-03-01",
                "2024-03-02",
            }

    asyncio.run(run_test())


@patch('tasks.price_target_data.bronze_price_target_data.nasdaqdatalink')
@patch('tasks.price_target_data.bronze_price_target_data.bronze_client')
@patch('tasks.price_target_data.bronze_price_target_data.list_manager')
def test_process_batch_bronze_missing_after_watermark_keeps_existing(
    mock_list_manager,
    mock_client,
    mock_nasdaq,
):
    symbol = "AAA"
    existing_df = pd.DataFrame(
        {
            "ticker": [symbol],
            "obs_date": [pd.Timestamp("2024-03-01")],
            "tp_mean_est": [50.0],
        }
    )
    existing_parquet = existing_df.to_parquet(index=False)

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = True
    mock_blob_client.get_blob_properties.return_value = MagicMock(
        last_modified=datetime.now(timezone.utc) - timedelta(days=2)
    )
    mock_client.get_blob_client.return_value = mock_blob_client
    mock_nasdaq.get_table.return_value = pd.DataFrame()
    semaphore = asyncio.Semaphore(1)

    async def run_test():
        with patch("core.core.read_raw_bytes", return_value=existing_parquet), patch(
            "core.core.store_raw_bytes"
        ) as mock_store:
            summary = await bronze.process_batch_bronze([symbol], semaphore)
            assert summary["saved"] == 0
            assert summary["blacklisted"] == 0
            assert summary["filtered_missing"] == 1
            mock_store.assert_not_called()
            mock_client.delete_file.assert_not_called()
            mock_list_manager.add_to_blacklist.assert_not_called()
            mock_list_manager.add_to_whitelist.assert_called_with(symbol)

    asyncio.run(run_test())
