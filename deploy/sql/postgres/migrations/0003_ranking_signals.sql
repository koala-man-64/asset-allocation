BEGIN;

CREATE TABLE IF NOT EXISTS ranking.ranking_signal (
  date DATE NOT NULL,
  year_month TEXT NOT NULL,
  symbol TEXT NOT NULL,
  strategy TEXT NOT NULL,
  rank INTEGER NOT NULL,
  rank_percentile DOUBLE PRECISION NOT NULL,
  n_symbols INTEGER NOT NULL,
  score DOUBLE PRECISION NULL,
  PRIMARY KEY (date, symbol, strategy)
);

CREATE TABLE IF NOT EXISTS ranking.composite_signal_daily (
  date DATE NOT NULL,
  year_month TEXT NOT NULL,
  symbol TEXT NOT NULL,
  composite_percentile DOUBLE PRECISION NOT NULL,
  composite_rank INTEGER NOT NULL,
  strategies_present INTEGER NOT NULL,
  strategies_hit INTEGER NOT NULL,
  PRIMARY KEY (date, symbol)
);

CREATE TABLE IF NOT EXISTS ranking.signal_sync_state (
  year_month TEXT PRIMARY KEY,
  synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  signals_rows INTEGER NOT NULL,
  composite_rows INTEGER NOT NULL,
  status TEXT NOT NULL,
  error TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_ranking_signal_year_month
  ON ranking.ranking_signal(year_month);

CREATE INDEX IF NOT EXISTS idx_ranking_signal_symbol_date
  ON ranking.ranking_signal(symbol, date);

CREATE INDEX IF NOT EXISTS idx_composite_signal_daily_year_month
  ON ranking.composite_signal_daily(year_month);

CREATE INDEX IF NOT EXISTS idx_composite_signal_daily_symbol_date
  ON ranking.composite_signal_daily(symbol, date);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ranking_writer') THEN
    GRANT USAGE ON SCHEMA ranking TO ranking_writer;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ranking.ranking_signal TO ranking_writer;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ranking.composite_signal_daily TO ranking_writer;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ranking.signal_sync_state TO ranking_writer;
  END IF;
END $$;

COMMIT;

