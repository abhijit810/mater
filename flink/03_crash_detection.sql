-- ===========================================================================
-- 03_crash_detection.sql   ->  crash_signals
-- ===========================================================================
-- Per-vehicle drastic-deceleration / impact detection using LAG over an
-- event-time OVER window (verified against Confluent's "compare current and
-- previous values" how-to). Emits a signal only when, between two CONSECUTIVE
-- events within 2 seconds, either:
--   * speed drops more than 40 km/h, or
--   * the accelerometer magnitude spikes above 25 (impact).
--
-- This is a long-running statement; it stays RUNNING and continuously sinks
-- into crash_signals. Adjust thresholds in the WHERE clause to tune sensitivity.
--
-- If you ran 01_source.sql, replace `$rowtime` with `event_time`.
-- ===========================================================================

INSERT INTO crash_signals
SELECT
  vehicle_id,
  ts AS signal_time,
  speed_kmph,
  prev_speed,
  speed_delta,
  accel_mag,
  CASE
    WHEN speed_delta <= -40 THEN 'HARD_DECEL'
    WHEN accel_mag   >=  25 THEN 'IMPACT_SPIKE'
  END AS reason
FROM (
  SELECT
    vehicle_id,
    `$rowtime` AS ts,
    speed_kmph,
    LAG(speed_kmph) OVER w AS prev_speed,
    speed_kmph - LAG(speed_kmph) OVER w AS speed_delta,
    SQRT(accel.x * accel.x + accel.y * accel.y + accel.z * accel.z) AS accel_mag,
    LAG(`$rowtime`) OVER w AS prev_time
  FROM vehicle_telemetry
  WINDOW w AS (PARTITION BY vehicle_id ORDER BY `$rowtime`)
)
WHERE prev_time IS NOT NULL
  AND (ts - prev_time) <= INTERVAL '2' SECOND
  AND (speed_delta <= -40 OR accel_mag >= 25);
