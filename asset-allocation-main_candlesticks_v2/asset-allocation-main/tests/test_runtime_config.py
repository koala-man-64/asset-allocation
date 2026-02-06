from core.runtime_config import normalize_env_override


def test_normalize_env_override_bool_truthy():
    assert normalize_env_override("SILVER_LATEST_ONLY", "true") == "true"
    assert normalize_env_override("SILVER_LATEST_ONLY", "1") == "true"
    assert normalize_env_override("SILVER_LATEST_ONLY", "Yes") == "true"


def test_normalize_env_override_bool_falsey():
    assert normalize_env_override("SILVER_LATEST_ONLY", "false") == "false"
    assert normalize_env_override("SILVER_LATEST_ONLY", "0") == "false"
    assert normalize_env_override("SILVER_LATEST_ONLY", "off") == "false"


def test_normalize_env_override_required_nonempty_rejects_blank():
    try:
        normalize_env_override("SYSTEM_HEALTH_TTL_SECONDS", "")
    except ValueError as exc:
        assert "cannot be empty" in str(exc)
    else:
        raise AssertionError("Expected ValueError for empty SYSTEM_HEALTH_TTL_SECONDS")


def test_normalize_env_override_date():
    assert normalize_env_override("BACKFILL_START_DATE", "2024-01-02") == "2024-01-02"


def test_normalize_env_override_date_rejects_invalid():
    try:
        normalize_env_override("BACKFILL_START_DATE", "not-a-date")
    except ValueError as exc:
        assert "YYYY-MM-DD" in str(exc)
    else:
        raise AssertionError("Expected ValueError for invalid date")


def test_normalize_env_override_year_month():
    assert normalize_env_override("MATERIALIZE_YEAR_MONTH", "2026-01") == "2026-01"


def test_normalize_env_override_year_month_rejects_invalid():
    for value in ("2026-1", "2026-13", "abcd-ef"):
        try:
            normalize_env_override("MATERIALIZE_YEAR_MONTH", value)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Expected ValueError for {value!r}")


def test_normalize_env_override_utc_hour_allows_empty():
    assert normalize_env_override("MATERIALIZE_BY_DATE_RUN_AT_UTC_HOUR", "") == ""


def test_normalize_env_override_utc_hour_rejects_invalid():
    for value in ("abc", "-1", "24"):
        try:
            normalize_env_override("MATERIALIZE_BY_DATE_RUN_AT_UTC_HOUR", value)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Expected ValueError for {value!r}")
