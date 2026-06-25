"""Thin wrapper around confluent-kafka Producer configured for Confluent Cloud."""
import json
import logging
from typing import Any

from confluent_kafka import Producer

from config import config

log = logging.getLogger("proxy.producer")


def _build_producer() -> Producer:
    config.validate()
    return Producer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": config.API_KEY,
            "sasl.password": config.API_SECRET,
            # Sensible client-side defaults for a demo producer.
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
            "client.id": "obd-proxy-producer",
        }
    )


class TelemetryProducer:
    def __init__(self) -> None:
        self._producer = _build_producer()
        self._topic = config.TELEMETRY_TOPIC

    def _delivery_report(self, err, msg) -> None:
        if err is not None:
            log.error("Delivery failed for key=%s: %s", msg.key(), err)
        else:
            log.debug(
                "Delivered to %s [%s] @ %s", msg.topic(), msg.partition(), msg.offset()
            )

    def produce(self, event: dict[str, Any]) -> None:
        """Produce a single telemetry event keyed by vehicle_id."""
        key = str(event.get("vehicle_id", ""))
        self._producer.produce(
            topic=self._topic,
            key=key.encode("utf-8") if key else None,
            value=json.dumps(event).encode("utf-8"),
            on_delivery=self._delivery_report,
        )
        # Serve delivery callbacks without blocking the request path.
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Block until all outstanding messages are delivered. Returns # still queued."""
        return self._producer.flush(timeout)
