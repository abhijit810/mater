-- ===========================================================================
-- 02_signals_tables.sql
-- ===========================================================================
-- Output tables (= Kafka topics) for the Flink detection layer. These hold the
-- "summary events" that the Streaming Agents consume.
--
-- If you pre-created the topics `crash_signals` / `engine_signals` in the UI,
-- Flink will have already inferred tables for them and these CREATE statements
-- may conflict. In that case, skip this file (the inferred schema works) or run
-- `DROP TABLE` first. Creating them here lets us pin an explicit schema.
-- ===========================================================================

CREATE TABLE crash_signals (
  vehicle_id  STRING,
  signal_time TIMESTAMP_LTZ(3),
  speed_kmph  DOUBLE,
  prev_speed  DOUBLE,
  speed_delta DOUBLE,
  accel_mag   DOUBLE,
  reason      STRING
) WITH (
  'changelog.mode' = 'append',
  'value.format'   = 'json'  -- Changed from 'json-registry'
);

CREATE TABLE engine_signals (
  vehicle_id   STRING,
  window_start TIMESTAMP_LTZ(3),
  window_end   TIMESTAMP_LTZ(3),
  avg_load     DOUBLE,
  stddev_load  DOUBLE,
  load_range   DOUBLE,
  reason       STRING
) WITH (
  'changelog.mode' = 'append',
  'value.format'   = 'json'  -- Changed from 'json-registry'
);
