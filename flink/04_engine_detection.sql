-- ===========================================================================
-- 04_engine_detection.sql   ->  engine_signals
-- ===========================================================================
-- Per-vehicle engine-load instability detection over a 10-second tumbling
-- window (window TVF). Emits a summary row only when the window shows either
-- high variability (stddev > 15) or a large swing (max-min range > 40) in
-- engine_load_pct -- both indicative of an impending engine/car fault.
--
-- Long-running statement; stays RUNNING and sinks into engine_signals.
--
-- If you ran 01_source.sql, replace `$rowtime` with `event_time` in DESCRIPTOR.
-- ===========================================================================

INSERT INTO engine_signals
SELECT
  vehicle_id,
  window_start,
  window_end,
  AVG(engine_load_pct)                          AS avg_load,
  STDDEV_SAMP(engine_load_pct)                  AS stddev_load,
  MAX(engine_load_pct) - MIN(engine_load_pct)   AS load_range,
  'ENGINE_LOAD_INSTABILITY'                     AS reason
FROM TABLE(
  TUMBLE(TABLE vehicle_telemetry, DESCRIPTOR(`$rowtime`), INTERVAL '10' SECONDS)
)
GROUP BY vehicle_id, window_start, window_end
HAVING STDDEV_SAMP(engine_load_pct) > 15
    OR (MAX(engine_load_pct) - MIN(engine_load_pct)) > 40;
