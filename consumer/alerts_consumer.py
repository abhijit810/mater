"""Consume enriched alerts from crash_alerts + engine_alerts and pretty-print them.

A lightweight stand-in for a dashboard -- enough to show the end-to-end result
during the demo.

Run:
    cd consumer
    python alerts_consumer.py
"""
from __future__ import annotations

import json
import os
import sys

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("CC_BOOTSTRAP_SERVERS", "")
API_KEY = os.getenv("CC_API_KEY", "")
API_SECRET = os.getenv("CC_API_SECRET", "")
GROUP = os.getenv("CONSUMER_GROUP", "obd-alerts-demo")
CRASH_TOPIC = os.getenv("CRASH_ALERTS_TOPIC", "crash_alerts")
ENGINE_TOPIC = os.getenv("ENGINE_ALERTS_TOPIC", "engine_alerts")

SEV_ICON = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🟠", "CRITICAL": "🔴"}


def _build_consumer() -> Consumer:
    if not (BOOTSTRAP and API_KEY and API_SECRET):
        sys.exit("Missing CC_BOOTSTRAP_SERVERS / CC_API_KEY / CC_API_SECRET. See .env.example.")
    return Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": API_KEY,
            "sasl.password": API_SECRET,
            "group.id": GROUP,
            "auto.offset.reset": "latest",
        }
    )


def _render(topic: str, value: bytes) -> None:
    kind = "CRASH" if topic == CRASH_TOPIC else "ENGINE"
    try:
        row = json.loads(value)
    except json.JSONDecodeError:
        print(f"[{kind}] (unparseable) {value!r}")
        return

    vehicle = row.get("vehicle_id", "?")
    raw_alert = row.get("alert_json", "")
    # alert_json is itself a JSON string emitted by the LLM; parse if possible.
    alert = raw_alert
    if isinstance(raw_alert, str):
        try:
            alert = json.loads(raw_alert)
        except json.JSONDecodeError:
            alert = {"explanation": raw_alert}

    severity = (alert.get("severity") or "?").upper() if isinstance(alert, dict) else "?"
    icon = SEV_ICON.get(severity, "⚪")
    explanation = alert.get("explanation", "") if isinstance(alert, dict) else ""
    action = alert.get("recommended_action", "") if isinstance(alert, dict) else ""

    print(f"\n{icon} [{kind}] {vehicle}  severity={severity}")
    if explanation:
        print(f"    why:    {explanation}")
    if action:
        print(f"    action: {action}")


def main() -> None:
    consumer = _build_consumer()
    consumer.subscribe([CRASH_TOPIC, ENGINE_TOPIC])
    print(f"Listening for alerts on '{CRASH_TOPIC}' and '{ENGINE_TOPIC}' ... (Ctrl-C to stop)")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[consumer error] {msg.error()}", file=sys.stderr)
                continue
            _render(msg.topic(), msg.value())
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("\nstopped.")


if __name__ == "__main__":
    main()
