from __future__ import annotations

from datetime import datetime, timezone

from tasks.common import run_manifests


def test_create_bronze_finance_manifest_writes_manifest_and_latest(monkeypatch):
    saved: dict[str, dict] = {}

    monkeypatch.setattr(run_manifests.mdc, "common_storage_client", object())
    monkeypatch.setattr(
        run_manifests.mdc,
        "save_common_json_content",
        lambda payload, path: saved.setdefault(path, payload),
    )

    out = run_manifests.create_bronze_finance_manifest(
        producer_job_name="bronze-finance-job",
        listed_blobs=[
            {
                "name": "finance-data/Valuation/AAPL_quarterly_valuation_measures.json",
                "etag": "etag-a",
                "last_modified": datetime(2026, 2, 26, 16, 0, tzinfo=timezone.utc),
                "size": 42,
            }
        ],
        metadata={"processed": 1},
    )

    assert out is not None
    run_id = str(out["runId"])
    manifest_path = f"system/run-manifests/bronze_finance/{run_id}.json"
    latest_path = "system/run-manifests/bronze_finance/latest.json"
    assert manifest_path in saved
    assert latest_path in saved
    assert saved[manifest_path]["blobCount"] == 1
    assert saved[latest_path]["runId"] == run_id


def test_load_latest_bronze_finance_manifest_resolves_pointer(monkeypatch):
    monkeypatch.setattr(run_manifests.mdc, "common_storage_client", object())

    def _fake_get(path: str):
        if path.endswith("/latest.json"):
            return {
                "runId": "bronze-finance-20260226T000000000000Z-abcd1234",
                "manifestPath": "system/run-manifests/bronze_finance/bronze-finance-20260226T000000000000Z-abcd1234.json",
            }
        if path.endswith("abcd1234.json"):
            return {"runId": "bronze-finance-20260226T000000000000Z-abcd1234", "blobs": []}
        return None

    monkeypatch.setattr(run_manifests.mdc, "get_common_json_content", _fake_get)
    manifest = run_manifests.load_latest_bronze_finance_manifest()
    assert manifest is not None
    assert manifest["runId"].endswith("abcd1234")
    assert manifest["manifestPath"].endswith("abcd1234.json")


def test_write_and_read_silver_manifest_ack(monkeypatch):
    saved: dict[str, dict] = {}

    monkeypatch.setattr(run_manifests.mdc, "common_storage_client", object())
    monkeypatch.setattr(
        run_manifests.mdc,
        "save_common_json_content",
        lambda payload, path: saved.setdefault(path, payload),
    )
    monkeypatch.setattr(
        run_manifests.mdc,
        "get_common_json_content",
        lambda path: saved.get(path),
    )

    ack_path = run_manifests.write_silver_finance_ack(
        run_id="bronze-finance-20260226T000000000000Z-abcd1234",
        manifest_path="system/run-manifests/bronze_finance/bronze-finance-20260226T000000000000Z-abcd1234.json",
        status="succeeded",
        metadata={"processed": 10},
    )
    assert ack_path is not None
    assert run_manifests.silver_finance_ack_exists("bronze-finance-20260226T000000000000Z-abcd1234") is True


def test_manifest_blobs_normalizes_and_sorts():
    manifest = {
        "blobs": [
            {"name": "finance-data/Balance Sheet/B_quarterly_balance-sheet.json"},
            {"name": "finance-data/Balance Sheet/A_quarterly_balance-sheet.json"},
        ]
    }
    out = run_manifests.manifest_blobs(manifest)
    assert [item["name"] for item in out] == [
        "finance-data/Balance Sheet/A_quarterly_balance-sheet.json",
        "finance-data/Balance Sheet/B_quarterly_balance-sheet.json",
    ]
