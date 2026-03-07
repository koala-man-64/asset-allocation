BEGIN;

CREATE SCHEMA IF NOT EXISTS platinum;

DO $$
BEGIN
  IF to_regclass('platinum.strategies') IS NULL THEN
    IF to_regclass('public.strategies') IS NOT NULL THEN
      ALTER TABLE public.strategies SET SCHEMA platinum;
    ELSE
      CREATE TABLE platinum.strategies (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        description TEXT,
        type TEXT NOT NULL DEFAULT 'configured',
        config JSONB NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    END IF;
  ELSIF to_regclass('public.strategies') IS NOT NULL THEN
    INSERT INTO platinum.strategies (name, description, type, config, created_at, updated_at)
    SELECT name, description, type, config, created_at, updated_at
    FROM public.strategies
    ON CONFLICT (name) DO UPDATE
    SET description = EXCLUDED.description,
        type = EXCLUDED.type,
        config = EXCLUDED.config,
        created_at = LEAST(platinum.strategies.created_at, EXCLUDED.created_at),
        updated_at = GREATEST(platinum.strategies.updated_at, EXCLUDED.updated_at)
    WHERE platinum.strategies.updated_at IS NULL
       OR EXCLUDED.updated_at >= platinum.strategies.updated_at;

    DROP TABLE public.strategies;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS platinum.idx_strategies_type
  ON platinum.strategies(type);
CREATE INDEX IF NOT EXISTS platinum.idx_strategies_updated_at
  ON platinum.strategies(updated_at DESC);

COMMIT;
