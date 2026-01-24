BEGIN;

CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.alert_state (
  alert_id TEXT PRIMARY KEY,
  acknowledged_at TIMESTAMPTZ NULL,
  acknowledged_by TEXT NULL,
  snoozed_until TIMESTAMPTZ NULL,
  resolved_at TIMESTAMPTZ NULL,
  resolved_by TEXT NULL,
  note TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION monitoring.set_updated_at() RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_alert_state_updated_at ON monitoring.alert_state;
CREATE TRIGGER trg_alert_state_updated_at
BEFORE UPDATE ON monitoring.alert_state
FOR EACH ROW
EXECUTE FUNCTION monitoring.set_updated_at();

CREATE INDEX IF NOT EXISTS idx_alert_state_snoozed_until
  ON monitoring.alert_state(snoozed_until);

CREATE INDEX IF NOT EXISTS idx_alert_state_resolved_at
  ON monitoring.alert_state(resolved_at);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ranking_writer') THEN
    GRANT USAGE ON SCHEMA monitoring TO ranking_writer;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE monitoring.alert_state TO ranking_writer;
  END IF;
END $$;

COMMIT;

