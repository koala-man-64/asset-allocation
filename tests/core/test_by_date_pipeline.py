from core.by_date_pipeline import run_partner_then_by_date


def test_run_partner_then_by_date_uses_year_months_provider(monkeypatch):
    calls = []

    def partner_main():
        return 0

    def by_date_main(argv=None):
        calls.append(argv)
        return 0

    def provider():
        return ["2024-12", "2025-01"]

    monkeypatch.delenv("MATERIALIZE_YEAR_MONTH", raising=False)

    result = run_partner_then_by_date(
        job_name="test-job",
        partner_main=partner_main,
        by_date_main=by_date_main,
        year_months_provider=provider,
    )

    assert result == 0
    assert calls == [["--year-month", "2024-12"], ["--year-month", "2025-01"]]
