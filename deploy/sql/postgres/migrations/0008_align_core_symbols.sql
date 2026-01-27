BEGIN;

-- Ensure additional columns exist on core.symbols.
ALTER TABLE IF EXISTS core.symbols
  ADD COLUMN IF NOT EXISTS description TEXT,
  ADD COLUMN IF NOT EXISTS industry_2 TEXT,
  ADD COLUMN IF NOT EXISTS is_optionable BOOLEAN;

-- If a legacy public.symbols table exists, backfill core.symbols from it.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'symbols'
  ) THEN
    INSERT INTO core.symbols (
      symbol,
      name,
      description,
      sector,
      industry,
      industry_2,
      country,
      is_optionable
    )
    SELECT
      s.symbol,
      s.name,
      s.description,
      s.sector,
      s.industry,
      s.industry_2,
      s.country,
      COALESCE(s.is_optionable, s.optionable)
    FROM public.symbols s
    ON CONFLICT (symbol) DO UPDATE
    SET name = EXCLUDED.name,
        description = EXCLUDED.description,
        sector = EXCLUDED.sector,
        industry = EXCLUDED.industry,
        industry_2 = EXCLUDED.industry_2,
        country = EXCLUDED.country,
        is_optionable = EXCLUDED.is_optionable;
  END IF;
END $$;

COMMIT;
