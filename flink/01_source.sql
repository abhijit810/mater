-- ===========================================================================
-- 01_source.sql  (OPTIONAL)
-- ===========================================================================
-- In Confluent Cloud, the `vehicle_telemetry` Kafka topic is ALREADY a Flink
-- table with an implicit `$rowtime` column (the Kafka record timestamp) and a
-- default per-partition watermark. The detection queries below default to
-- `$rowtime`, so you do NOT need to run this file for the MVP.
--
-- Run this ONLY if your producer back-dates/batches events and you want Flink to
-- order/window by the business `event_time` field with an explicit watermark.
-- If you run it, the topic must not already be mapped, or drop/recreate the
-- inferred table first.
--
-- Note: the JSON payload nests `accel` and `gps`; declare them as ROW<...>.
-- ===========================================================================

CREATE TABLE vehicle_telemetry (
  vehicle_id        STRING,
  event_id          STRING,
  event_time        TIMESTAMP_LTZ(3),
  speed_kmph        DOUBLE,
  engine_rpm        INT,
  engine_load_pct   DOUBLE,
  throttle_pct      DOUBLE,
  coolant_temp_c    DOUBLE,
  intake_air_temp_c DOUBLE,
  maf_gps           DOUBLE,
  fuel_level_pct    DOUBLE,
  battery_voltage   DOUBLE,
  odometer_km       DOUBLE,
  dtc_codes         ARRAY<STRING>,
  accel             ROW<x DOUBLE, y DOUBLE, z DOUBLE>,
  gps               ROW<lat DOUBLE, lon DOUBLE, heading DOUBLE, altitude_m DOUBLE>,
  WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
) WITH (
  'changelog.mode' = 'append',
  'value.format'   = 'json'  -- Changed from 'json-registry'
);
