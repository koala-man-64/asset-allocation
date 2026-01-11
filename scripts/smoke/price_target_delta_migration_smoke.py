import argparse
import logging
import os
import sys
import time
import uuid
from typing import List

import pandas as pd

# Add project root to sys.path to ensure absolute imports work
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
if project_root not in sys.path:
    sys.path.append(project_root)


def _delete_prefix_blobs(container_name: str, prefix: str) -> List[str]:
    from scripts.common.blob_storage import BlobStorageClient

    client = BlobStorageClient(container_name=container_name, ensure_container_exists=False)
    blobs = client.list_files(name_starts_with=prefix)
    blobs.sort(key=len, reverse=True)
    for blob in blobs:
        client.delete_file(blob)
    return blobs


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Writes a small Delta table using legacy `ticker`, then overwrites it with `symbol` to validate schema migration."
        )
    )
    parser.add_argument("--confirm", required=True, help="Type SMOKE to confirm this will write test data.")
    parser.add_argument(
        "--container",
        default=os.environ.get("AZURE_CONTAINER_TARGETS") or "",
        help="Azure blob container for price targets (defaults to AZURE_CONTAINER_TARGETS).",
    )
    parser.add_argument(
        "--path-prefix",
        default="gold/_smoke/price_target_feature_migration",
        help="Delta table base prefix (a unique run id will be appended).",
    )
    parser.add_argument(
        "--cleanup",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Delete blobs under the test prefix after completion.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.confirm != "SMOKE":
        logging.error("Refusing to run without --confirm SMOKE.")
        return 2

    if not args.container:
        logging.error("Missing required --container (or AZURE_CONTAINER_TARGETS).")
        return 2

    _require_env("AZURE_STORAGE_ACCOUNT_NAME")
    _require_env("AZURE_STORAGE_CONNECTION_STRING")

    from scripts.common import delta_core

    run_id = f"{time.strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
    table_path = f"{args.path_prefix.strip('/')}/{run_id}"

    logging.info("Smoke path: %s/%s", args.container, table_path)

    df_old = pd.DataFrame(
        {
            "ticker": ["AAPL", "AAPL"],
            "obs_date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
            "tp_mean_est": [100.0, 101.0],
        }
    )
    df_new = df_old.rename(columns={"ticker": "symbol"})

    try:
        logging.info("Writing legacy schema (ticker) ...")
        delta_core.store_delta(df_old, args.container, table_path, mode="overwrite")

        logging.info("Overwriting with new schema (symbol) ...")
        delta_core.store_delta(df_new, args.container, table_path, mode="overwrite", schema_mode="overwrite")

        loaded = delta_core.load_delta(args.container, table_path)
        if loaded is None or loaded.empty:
            logging.error("Failed to load written Delta table back from %s/%s.", args.container, table_path)
            return 1

        cols = set(map(str, loaded.columns.tolist()))
        if "symbol" not in cols:
            logging.error("Expected migrated table to contain 'symbol' column. cols=%s", sorted(cols))
            return 1
        if "ticker" in cols:
            logging.error("Expected migrated table to NOT contain legacy 'ticker' column. cols=%s", sorted(cols))
            return 1

        logging.info("OK: migrated schema contains 'symbol' and not 'ticker'.")
        return 0
    finally:
        if args.cleanup:
            try:
                deleted = _delete_prefix_blobs(args.container, table_path)
                logging.info("Cleanup complete: deleted %d blob(s) under %s/%s.", len(deleted), args.container, table_path)
            except Exception as exc:
                logging.warning("Cleanup failed for %s/%s: %s", args.container, table_path, exc)


if __name__ == "__main__":
    raise SystemExit(main())
