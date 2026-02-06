BEGIN;

CREATE TABLE IF NOT EXISTS backtest.runs (
  run_id TEXT PRIMARY KEY,
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed')),
  submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ NULL,
  completed_at TIMESTAMPTZ NULL,
  run_name TEXT NULL,
  start_date TEXT NULL,
  end_date TEXT NULL,
  output_dir TEXT NULL,
  adls_container TEXT NULL,
  adls_prefix TEXT NULL,
  error TEXT NULL,
  config_json TEXT NOT NULL,
  effective_config_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_submitted_at
  ON backtest.runs(submitted_at);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_status
  ON backtest.runs(status);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA backtest TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE backtest.runs TO backtest_service;
  END IF;
END $$;

COMMIT;

