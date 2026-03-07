BEGIN;

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS gold;

ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS industry_2 TEXT;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS optionable TEXT;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS exchange TEXT;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS asset_type TEXT;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS ipo_date TEXT;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS delisting_date TEXT;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS status TEXT;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS source_nasdaq BOOLEAN;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS source_massive BOOLEAN;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS source_alpha_vantage BOOLEAN;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS source_alphavantage BOOLEAN;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS is_optionable BOOLEAN;
ALTER TABLE core.symbols ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

UPDATE core.symbols
SET optionable = CASE
      WHEN is_optionable IS TRUE THEN 'Y'
      WHEN is_optionable IS FALSE THEN 'N'
      ELSE optionable
    END
WHERE optionable IS NULL AND is_optionable IS NOT NULL;

UPDATE core.symbols
SET is_optionable = CASE
      WHEN upper(trim(optionable)) IN ('Y', 'YES', 'TRUE', 'T', '1') THEN TRUE
      WHEN upper(trim(optionable)) IN ('N', 'NO', 'FALSE', 'F', '0') THEN FALSE
      ELSE is_optionable
    END
WHERE optionable IS NOT NULL AND is_optionable IS NULL;

CREATE TABLE IF NOT EXISTS core.symbol_sync_state (
  id SMALLINT PRIMARY KEY,
  last_refreshed_at TIMESTAMPTZ,
  last_refreshed_sources JSONB,
  last_refresh_error TEXT
);

DO $$
BEGIN
  IF to_regclass('public.symbols') IS NOT NULL THEN
    INSERT INTO core.symbols AS s (
      symbol,
      name,
      description,
      sector,
      industry,
      industry_2,
      optionable,
      is_optionable,
      country,
      exchange,
      asset_type,
      ipo_date,
      delisting_date,
      status,
      source_nasdaq,
      source_massive,
      source_alpha_vantage,
      source_alphavantage,
      updated_at
    )
    SELECT
      symbol,
      name,
      description,
      sector,
      industry,
      industry_2,
      optionable,
      CASE
        WHEN upper(trim(COALESCE(optionable, ''))) IN ('Y', 'YES', 'TRUE', 'T', '1') THEN TRUE
        WHEN upper(trim(COALESCE(optionable, ''))) IN ('N', 'NO', 'FALSE', 'F', '0') THEN FALSE
        ELSE NULL
      END AS is_optionable,
      country,
      exchange,
      asset_type,
      ipo_date,
      delisting_date,
      status,
      source_nasdaq,
      source_massive,
      COALESCE(source_alpha_vantage, source_alphavantage, FALSE),
      COALESCE(source_alphavantage, source_alpha_vantage, FALSE),
      updated_at
    FROM public.symbols
    ON CONFLICT (symbol) DO UPDATE
    SET name = COALESCE(EXCLUDED.name, s.name),
        description = COALESCE(EXCLUDED.description, s.description),
        sector = COALESCE(EXCLUDED.sector, s.sector),
        industry = COALESCE(EXCLUDED.industry, s.industry),
        industry_2 = COALESCE(EXCLUDED.industry_2, s.industry_2),
        optionable = COALESCE(EXCLUDED.optionable, s.optionable),
        is_optionable = COALESCE(EXCLUDED.is_optionable, s.is_optionable),
        country = COALESCE(EXCLUDED.country, s.country),
        exchange = COALESCE(EXCLUDED.exchange, s.exchange),
        asset_type = COALESCE(EXCLUDED.asset_type, s.asset_type),
        ipo_date = COALESCE(EXCLUDED.ipo_date, s.ipo_date),
        delisting_date = COALESCE(EXCLUDED.delisting_date, s.delisting_date),
        status = COALESCE(EXCLUDED.status, s.status),
        source_nasdaq = COALESCE(EXCLUDED.source_nasdaq, s.source_nasdaq),
        source_massive = COALESCE(EXCLUDED.source_massive, s.source_massive),
        source_alpha_vantage = COALESCE(EXCLUDED.source_alpha_vantage, s.source_alpha_vantage),
        source_alphavantage = COALESCE(EXCLUDED.source_alphavantage, s.source_alphavantage),
        updated_at = GREATEST(s.updated_at, EXCLUDED.updated_at);

    DROP TABLE public.symbols;
  END IF;

  IF to_regclass('public.symbol_sync_state') IS NOT NULL THEN
    INSERT INTO core.symbol_sync_state AS s (
      id,
      last_refreshed_at,
      last_refreshed_sources,
      last_refresh_error
    )
    SELECT
      id,
      last_refreshed_at,
      last_refreshed_sources,
      last_refresh_error
    FROM public.symbol_sync_state
    ON CONFLICT (id) DO UPDATE
    SET last_refreshed_at = COALESCE(EXCLUDED.last_refreshed_at, s.last_refreshed_at),
        last_refreshed_sources = COALESCE(EXCLUDED.last_refreshed_sources, s.last_refreshed_sources),
        last_refresh_error = COALESCE(EXCLUDED.last_refresh_error, s.last_refresh_error);

    DROP TABLE public.symbol_sync_state;
  END IF;
END $$;

INSERT INTO core.symbol_sync_state(id) VALUES (1) ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_core_symbols_sector ON core.symbols(sector);
CREATE INDEX IF NOT EXISTS idx_core_symbols_industry ON core.symbols(industry);
CREATE INDEX IF NOT EXISTS idx_core_symbols_status ON core.symbols(status);
CREATE INDEX IF NOT EXISTS idx_core_symbols_exchange ON core.symbols(exchange);
CREATE INDEX IF NOT EXISTS idx_core_symbols_updated_at ON core.symbols(updated_at DESC);

COMMIT;
