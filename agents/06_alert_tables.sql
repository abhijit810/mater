-- ===========================================================================
-- 06_alert_tables.sql
-- ===========================================================================
-- Output tables (= Kafka topics) for the enriched agent alerts. The agent
-- returns its JSON verdict as a single STRING column (`alert_json`); we keep
-- the vehicle id + signal time alongside for easy correlation downstream.
--
-- Skip / DROP first if you already pre-created these topics in the UI and Flink
-- inferred a different schema.
-- ===========================================================================

CREATE TABLE crash_alerts (
  vehicle_id  STRING,
  signal_time TIMESTAMP_LTZ(3),
  alert_json  STRING
) WITH (
  'changelog.mode' = 'append',
  'value.format'   = 'json'  -- Changed from 'json-registry'
);

CREATE TABLE engine_alerts (
  vehicle_id STRING,
  window_end TIMESTAMP_LTZ(3),
  alert_json STRING
) WITH (
  'changelog.mode' = 'append',
  'value.format'   = 'json'  -- Changed from 'json-registry'
);
