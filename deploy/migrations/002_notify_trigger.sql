CREATE OR REPLACE FUNCTION notify_run_update() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify(
    'run_updates',
    json_build_object(
      'run_id', NEW.run_id,
      'status', NEW.status,
      'event', TG_OP
    )::text
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS run_update_trigger ON backtest.runs;

CREATE TRIGGER run_update_trigger
AFTER INSERT OR UPDATE ON backtest.runs
FOR EACH ROW EXECUTE FUNCTION notify_run_update();
