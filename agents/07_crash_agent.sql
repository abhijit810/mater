-- ===========================================================================
-- 07_crash_agent.sql   crash_signals  ->  crash_detection_agent  ->  crash_alerts
-- ===========================================================================
-- A basic alerting agent needs only a model + prompt (no TOOL required).
-- AI_RUN_AGENT is invoked via LATERAL TABLE and the result is sunk into
-- crash_alerts. The prompt forces JSON-only output so downstream consumers can
-- parse it directly.
--
-- VERIFY at build time (Open Preview): CREATE AGENT / AI_RUN_AGENT grammar and
-- that the `agent_output` column name matches your tenant. Smoke-test the
-- INSERT-INTO wrapper with one row first.
-- Docs: https://docs.confluent.io/cloud/current/ai/streaming-agents/create-and-run-streaming-agents.html
-- ===========================================================================

CREATE AGENT crash_detection_agent
USING MODEL alert_llm
USING PROMPT 'You are a vehicle-safety analyst for a connected-car platform.
You receive a single crash signal with: vehicle_id, speed_delta (km/h, negative
means deceleration), accel_mag (g-like magnitude), and reason. Decide how serious
the event is and what should happen next. Output ONLY a compact JSON object, no
prose, with exactly these keys:
  "severity": one of "LOW","MEDIUM","HIGH","CRITICAL",
  "explanation": one short sentence,
  "recommended_action": one short sentence.'
COMMENT 'Enriches crash_signals into human-readable crash_alerts'
WITH ('max_iterations' = '3');

INSERT INTO crash_alerts
SELECT
  s.vehicle_id,
  s.signal_time,
  agent_output AS alert_json
FROM crash_signals AS s,
  LATERAL TABLE(
    AI_RUN_AGENT(
      `crash_detection_agent`,
      CONCAT(
        'vehicle_id=', s.vehicle_id,
        ' speed_delta=', CAST(s.speed_delta AS STRING),
        ' accel_mag=',   CAST(s.accel_mag AS STRING),
        ' reason=',      s.reason
      ),
      s.vehicle_id
    )
  );
