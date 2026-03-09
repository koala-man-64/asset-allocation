from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_move_public_tables_to_core_handles_legacy_public_symbols_shape() -> None:
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
        "0016 must only move public.symbols when the legacy table still exists"
    )
    assert "information_schema.columns" in text, (
        "0016 must inspect legacy public.symbols columns before referencing them"
    )
    assert "column_name = 'source_alpha_vantage'" in text, (
        "0016 must detect the legacy source_alpha_vantage column"
    )
    assert "column_name = 'source_alphavantage'" in text, (
        "0016 must tolerate environments where source_alphavantage exists instead"
    )
    assert "EXECUTE format($symbols_move$" in text, (
        "0016 must build the public.symbols move dynamically to avoid invalid column references"
    )
    assert "COALESCE(source_alpha_vantage, source_alphavantage, FALSE)" not in text, (
        "0016 must not statically reference both legacy source columns in the SELECT list"
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
