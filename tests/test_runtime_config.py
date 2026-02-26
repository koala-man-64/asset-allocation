from core.runtime_config import normalize_env_override


def test_normalize_env_override_bool_truthy():
    assert normalize_env_override("SILVER_LATEST_ONLY", "true") == "true"
    assert normalize_env_override("SILVER_LATEST_ONLY", "1") == "true"
    assert normalize_env_override("SILVER_LATEST_ONLY", "Yes") == "true"


def test_normalize_env_override_bool_falsey():
    assert normalize_env_override("SILVER_LATEST_ONLY", "false") == "false"
    assert normalize_env_override("SILVER_LATEST_ONLY", "0") == "false"
    assert normalize_env_override("SILVER_LATEST_ONLY", "off") == "false"


def test_normalize_env_override_gold_market_by_date_bool():
    assert normalize_env_override("GOLD_MARKET_BY_DATE_ENABLED", "true") == "true"
    assert normalize_env_override("GOLD_MARKET_BY_DATE_ENABLED", "0") == "false"


def test_normalize_env_override_gold_by_date_domain_passthrough():
    assert normalize_env_override("GOLD_BY_DATE_DOMAIN", "finance") == "finance"


def test_normalize_env_override_alpha_vantage_rate_wait_timeout_float():
    assert normalize_env_override("ALPHA_VANTAGE_RATE_WAIT_TIMEOUT_SECONDS", "120.5") == "120.5"


def test_normalize_env_override_alpha_vantage_throttle_cooldown_float():
    assert normalize_env_override("ALPHA_VANTAGE_THROTTLE_COOLDOWN_SECONDS", "60.5") == "60.5"


def test_normalize_env_override_required_nonempty_rejects_blank():
    try:
        normalize_env_override("SYSTEM_HEALTH_TTL_SECONDS", "")
    except ValueError as exc:
        assert "cannot be empty" in str(exc)
    else:
        raise AssertionError("Expected ValueError for empty SYSTEM_HEALTH_TTL_SECONDS")


def test_normalize_env_override_unknown_key_passthrough():
    assert normalize_env_override("UNMANAGED_KEY", "  any-value  ") == "any-value"


