from __future__ import annotations

from typing import Any, Dict, Optional

from monitoring.control_plane import collect_jobs_and_executions


class FakeArmClient:
    def __init__(self, *, responses: Dict[str, Dict[str, Any]]) -> None:
        self._responses = responses

    def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
        return f"https://example.test/{provider}/{resource_type}/{name}"

    def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        return self._responses[url]


def test_collect_jobs_and_executions_maps_status_sorts_and_limits() -> None:
    arm = FakeArmClient(
        responses={
            "https://example.test/Microsoft.App/jobs/my-backtest-job": {
                "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/my-backtest-job",
                "properties": {"provisioningState": "Succeeded"},
            },
            "https://example.test/Microsoft.App/jobs/my-backtest-job/executions": {
                "value": [
                    {
                        "properties": {
                            "status": "Failed",
                            "startTime": "2024-01-01T00:00:00Z",
                            "endTime": "2024-01-01T00:01:00Z",
                        }
                    },
                    {
                        "properties": {
                            "status": "Succeeded",
                            "startTime": "2024-01-02T00:00:00Z",
                            "endTime": "2024-01-02T00:02:00Z",
                        }
                    },
                    {
                        "properties": {
                            "status": "Running",
                            "startTime": "2024-01-03T00:00:00Z",
                        }
                    },
                    {
                        "properties": {
                            "status": "Unknown",
                            "startTime": "2024-01-04T00:00:00Z",
                        }
                    },
                ]
            },
        }
    )

    resources, runs = collect_jobs_and_executions(
        arm,
        job_names=["my-backtest-job"],
        last_checked_iso="2024-01-10T00:00:00+00:00",
        include_ids=False,
        max_executions_per_job=3,
    )

    assert len(resources) == 1
    assert resources[0].name == "my-backtest-job"
    assert resources[0].status == "healthy"

    # Limit applies before sorting.
    assert len(runs) == 3
    assert [r["status"] for r in runs] == ["running", "success", "failed"]
    assert [r["startTime"] for r in runs] == [
        "2024-01-03T00:00:00+00:00",
        "2024-01-02T00:00:00+00:00",
        "2024-01-01T00:00:00+00:00",
    ]
    assert [r["duration"] for r in runs] == [None, 120, 60]
    assert all(r["jobType"] == "backtest" for r in runs)
    assert all(r["triggeredBy"] == "azure" for r in runs)

