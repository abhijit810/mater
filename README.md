# Real-Time Vehicle Crash & Engine-Fault Detection on Confluent Cloud

Hackathon MVP that ingests Jio OBD vehicle telemetry (mocked), streams it through
Confluent Cloud Kafka, runs stateful detection in **managed Flink**, and reasons
over the detected signals with two **Confluent Streaming Agents** — producing
enriched, human-readable alerts in seconds.

```
Mock Generator ──HTTP──> Proxy (FastAPI) ──produce──> vehicle_telemetry (Kafka)
                                                              │
                                          Confluent Cloud managed Flink (detection)
                                                              │
                            ┌─────────────────────────────────┴─────────────────────────────────┐
                      crash_signals                                                        engine_signals
                            │                                                                    │
              Streaming Agent: crash_detection_agent                       Streaming Agent: engine_fault_agent ("Mater")
                            │                                                                    │
                      crash_alerts                                                          engine_alerts
                            └─────────────────────────────────┬─────────────────────────────────┘
                                                  Alerts Consumer (pretty-print)
```

## Repository layout

| Path | What |
|------|------|
| `data/sample.json` | Canonical OBD-II event payload (schema reference) |
| `proxy-app/` | FastAPI REST endpoint → confluent-kafka producer |
| `mock-generator/` | Realtime event generator with `crash` / `engine_fault` injection |
| `flink/*.sql` | Managed-Flink detection: raw telemetry → `crash_signals` / `engine_signals` |
| `agents/*.sql` | Streaming Agents: signals → enriched `crash_alerts` / `engine_alerts` |
| `consumer/` | Consumes alert topics and prints them |
| `scripts/run_demo.sh` | Starts proxy + consumer + generator locally |

## Prerequisites

- A Confluent Cloud account with a **Standard Kafka cluster** in a **Flink-supported region**.
- An LLM provider for the agents: **AWS Bedrock** (Claude) *or* **Azure OpenAI**.
- Python 3.10+.

## 1. Provision Confluent Cloud (one-time, via UI)

1. Create/choose an environment and a **Standard Kafka cluster** in a Flink-supported region.
2. Create these **topics** (keep them all in the same cluster):
   `vehicle_telemetry`, `crash_signals`, `engine_signals`, `crash_alerts`, `engine_alerts`.
3. Create a **Flink compute pool** in the same environment (required for the detection SQL *and* the agents).
4. Create a **Kafka API key/secret** (used by the proxy + consumer).
5. Get LLM provider credentials ready (Bedrock model access, or an Azure OpenAI deployment).

## 2. Configure local env

```bash
cp .env.example .env
# edit .env: CC_BOOTSTRAP_SERVERS, CC_API_KEY, CC_API_SECRET
```

## 3. Install Python deps

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r proxy-app/requirements.txt -r mock-generator/requirements.txt -r consumer/requirements.txt
```

## 4. Deploy Flink detection + agents (Confluent Cloud Flink SQL workspace)

Open the **Flink SQL workspace** for your compute pool and run the files **in order**.
Each `INSERT INTO ... SELECT` is a long-running statement that stays RUNNING.

```
flink/02_signals_tables.sql     # create crash_signals / engine_signals
flink/03_crash_detection.sql    # vehicle_telemetry -> crash_signals
flink/04_engine_detection.sql   # vehicle_telemetry -> engine_signals

agents/05_connection_model.sql  # CREATE CONNECTION + CREATE MODEL (pick a provider, fill placeholders)
agents/06_alert_tables.sql      # create crash_alerts / engine_alerts
agents/07_crash_agent.sql       # CREATE AGENT + AI_RUN_AGENT -> crash_alerts
agents/08_engine_agent.sql      # CREATE AGENT ("Mater") + AI_RUN_AGENT -> engine_alerts
```

> `flink/01_source.sql` is **optional** — only needed if you want Flink to order/window by
> the business `event_time` instead of the default Kafka `$rowtime`.

## 5. Run the local apps

Easiest — one script (proxy + consumer in background, generator in foreground):

```bash
./scripts/run_demo.sh
```

Or run each in its own terminal:

```bash
# terminal 1 — proxy
cd proxy-app && uvicorn main:app --port 8000

# terminal 2 — alerts consumer
cd consumer && python alerts_consumer.py

# terminal 3 — mock generator
cd mock-generator && python generator.py
```

## 6. Demo

In the generator prompt:

```
crash 0           # inject a hard-deceleration crash on vehicle index 0
engine_fault 0    # inject engine-load instability on vehicle index 0
list              # show vehicles
quit
```

Within a few seconds the consumer prints the enriched alert, e.g.:

```
🔴 [CRASH] JIO-OBD-0001  severity=CRITICAL
    why:    Speed dropped 55 km/h in under a second with a strong impact spike.
    action: Dispatch emergency services and contact the driver immediately.
```

## Verification

| Step | Expected |
|------|----------|
| Producer path | POST `data/sample.json` to `http://localhost:8000/events` → message appears in `vehicle_telemetry` (Cloud Message viewer) |
| Crash path | `crash 0` → row in `crash_signals` → alert in `crash_alerts` / consumer |
| Engine path | `engine_fault 0` → row in `engine_signals` → alert in `engine_alerts` / consumer |
| Negative control | `normal` traffic → telemetry flows but **no** signals/alerts (thresholds not crossed) |

Use the Flink SQL workspace to `SELECT * FROM crash_signals;` etc. to debug stage-by-stage.

## Tuning

- **Crash sensitivity:** thresholds in `flink/03_crash_detection.sql` (`speed_delta <= -40`, `accel_mag >= 25`).
- **Engine sensitivity:** thresholds in `flink/04_engine_detection.sql` (`STDDEV_SAMP > 15`, range `> 40`) and window size.
- **Scenario intensity:** `mock-generator/scenarios.py`.

## Notes / caveats

- **Confluent Streaming Agents is Open Preview** — re-verify `CREATE AGENT` / `AI_RUN_AGENT`
  grammar and the `CREATE MODEL` provider property keys against the docs on demo day.
- No Schema Registry is used for the raw topic (plain JSON) to keep the table definitions simple.
- The agent prompt forces JSON-only output; the consumer parses `alert_json` and degrades gracefully if the model adds stray text.
