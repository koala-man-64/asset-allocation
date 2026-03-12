from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_move_public_tables_to_core_handles_prior_public_symbols_shape() -> None:
    repo_root = _repo_root()
    migration = (
        repo_root
        / "deploy"
        / "sql"
        / "postgres"
        / "migrations"
        / "0016_move_public_tables_to_core.sql"
    )
    text = migration.read_text(encoding="utf-8")

    assert "IF to_regclass('public.symbols') IS NOT NULL THEN" in text, (
        "0016 must only move public.symbols when the prior table still exists"
    )
    assert "information_schema.columns" in text, (
        "0016 must inspect prior public.symbols columns before referencing them"
    )
    assert "column_name = 'source_alpha_vantage'" in text, (
        "0016 must detect the prior source_alpha_vantage column"
    )
    assert "column_name = 'source_alphavantage'" in text, (
        "0016 must tolerate environments where source_alphavantage exists instead"
    )
    assert "EXECUTE format($symbols_move$" in text, (
        "0016 must build the public.symbols move dynamically to avoid invalid column references"
    )
    assert "COALESCE(source_alpha_vantage, source_alphavantage, FALSE)" not in text, (
        "0016 must not statically reference both previous source columns in the SELECT list"
    )


def test_apply_postgres_migrations_streams_file_inputs_to_docker_psql() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "apply_postgres_migrations.ps1"
    text = script.read_text(encoding="utf-8")

    assert '$dockerArgs += "-f"' in text, (
        "apply_postgres_migrations must preserve -f when rewriting Docker psql args"
    )
    assert '$dockerArgs += "-"' in text, (
        "apply_postgres_migrations must rewrite Docker file inputs to stdin"
    )
    assert 'Get-Content -Path $dockerStdinPath -Raw -Encoding UTF8 | & docker @cmd' in text, (
        "apply_postgres_migrations must stream migration SQL into dockerized psql"
    )


def test_gold_sync_migration_rebuilds_incompatible_gold_tables_without_backup_renames() -> None:
    repo_root = _repo_root()
    migration = (
        repo_root
        / "deploy"
        / "sql"
        / "postgres"
        / "migrations"
        / "0019_gold_postgres_sync.sql"
    )
    text = migration.read_text(encoding="utf-8")

    assert "DROP TABLE gold.market_data;" in text
    assert "DROP TABLE gold.finance_data;" in text
    assert "DROP TABLE gold.earnings_data;" in text
    assert "DROP TABLE gold.price_target_data;" in text
    assert "ALTER TABLE gold.market_data RENAME TO" not in text
    assert "_0006" not in text


def test_cleanup_migration_drops_noncanonical_gold_tables() -> None:
    repo_root = _repo_root()
    migration = (
        repo_root
        / "deploy"
        / "sql"
        / "postgres"
        / "migrations"
        / "0029_drop_noncanonical_gold_tables.sql"
    )
    text = migration.read_text(encoding="utf-8")

    assert "FROM pg_tables" in text
    assert "schemaname = 'gold'" in text
    assert "tablename NOT IN (" in text
    assert "DROP TABLE IF EXISTS gold.%I" in text
