from datetime import datetime, timezone

from core import debug_symbols
from core.runtime_config import RuntimeConfigItem


def _item(
    *,
    value: str,
    enabled: bool = True,
    scope: str = "global",
    updated_by: str | None = "tester",
) -> RuntimeConfigItem:
    return RuntimeConfigItem(
        scope=scope,
        key="DEBUG_SYMBOLS",
        enabled=enabled,
        value=value,
        description="desc",
        updated_at=datetime.now(timezone.utc),
        updated_by=updated_by,
    )


def test_read_debug_symbols_state_reads_runtime_config_row(monkeypatch):
    monkeypatch.setattr(
        debug_symbols,
        "list_runtime_config",
        lambda dsn, scopes, keys: [_item(value="aapl, msft", enabled=False)],
    )

    state = debug_symbols.read_debug_symbols_state("postgresql://user:pass@localhost/db")

    assert state.enabled is False
    assert state.symbols_raw == "aapl, msft"
    assert state.symbols == ["AAPL", "MSFT"]
    assert state.updated_by == "tester"


def test_update_debug_symbols_state_upserts_runtime_config(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_upsert(**kwargs):
        captured.update(kwargs)
        return _item(value=str(kwargs["value"]), enabled=bool(kwargs["enabled"]))

    monkeypatch.setattr(debug_symbols, "upsert_runtime_config", _fake_upsert)
    monkeypatch.setattr(
        debug_symbols,
        "read_debug_symbols_state",
        lambda dsn=None: debug_symbols.DebugSymbolsState(
            enabled=True,
            symbols_raw="AAPL,MSFT",
            symbols=["AAPL", "MSFT"],
            updated_at=None,
            updated_by="tester",
        ),
    )

    state = debug_symbols.update_debug_symbols_state(
        dsn="postgresql://user:pass@localhost/db",
        enabled=True,
        symbols='["aapl", "msft"]',
        actor="tester",
    )

    assert captured["scope"] == "global"
    assert captured["key"] == "DEBUG_SYMBOLS"
    assert captured["enabled"] is True
    assert captured["value"] == "AAPL,MSFT"
    assert captured["actor"] == "tester"
    assert state.symbols == ["AAPL", "MSFT"]


def test_refresh_debug_symbols_from_db_uses_effective_runtime_config(monkeypatch):
    applied: list[list[str]] = []

    monkeypatch.setattr(
        debug_symbols,
        "default_scopes_by_precedence",
        lambda: ["job:bronze-market-job", "global"],
    )
    monkeypatch.setattr(
        debug_symbols,
        "get_effective_runtime_config",
        lambda dsn, scopes_by_precedence, keys: {
            "DEBUG_SYMBOLS": _item(
                value='["aapl", "msft"]',
                enabled=True,
                scope="job:bronze-market-job",
            )
        },
    )
    monkeypatch.setattr(
        debug_symbols,
        "_apply_debug_symbols_to_config",
        lambda symbols: applied.append(list(symbols)),
    )

    symbols = debug_symbols.refresh_debug_symbols_from_db("postgresql://user:pass@localhost/db")

    assert symbols == ["AAPL", "MSFT"]
    assert applied == [["AAPL", "MSFT"]]
