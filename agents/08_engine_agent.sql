-- ===========================================================================
-- 08_engine_agent.sql   engine_signals -> engine_fault_agent ("Mater") -> engine_alerts
-- ===========================================================================
-- The engine-diagnostics agent ("Mater") reasons over the windowed engine-load
-- statistics produced by Flink and emits a JSON verdict into engine_alerts.
--
-- Same Open-Preview caveats as 07_crash_agent.sql apply.
-- ===========================================================================

CREATE AGENT engine_fault_agent
USING MODEL alert_llm
USING PROMPT 'You are an engine-diagnostics analyst ("Mater") for a connected-car
platform. You receive windowed engine-load statistics for one vehicle:
avg_load (%), stddev_load, load_range (max-min %), and reason. High variability or
a large load swing can precede engine or drivetrain failure. Output ONLY a compact
JSON object, no prose, with exactly these keys:
  "severity": one of "LOW","MEDIUM","HIGH","CRITICAL",
  "explanation": one short sentence,
  "recommended_action": one short sentence.'
COMMENT 'Enriches engine_signals into human-readable engine_alerts'
WITH ('max_iterations' = '3');

INSERT INTO engine_alerts
SELECT
  e.vehicle_id,
  e.window_end,
  agent_output AS alert_json
FROM engine_signals AS e,
  LATERAL TABLE(
    AI_RUN_AGENT(
      `engine_fault_agent`,
      CONCAT(
        'vehicle_id=', e.vehicle_id,
        ' avg_load=',    CAST(e.avg_load AS STRING),
        ' stddev_load=', CAST(e.stddev_load AS STRING),
        ' load_range=',  CAST(e.load_range AS STRING),
        ' reason=',      e.reason
      ),
      e.vehicle_id
    )
  );
