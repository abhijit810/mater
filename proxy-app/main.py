"""FastAPI proxy: accepts OBD telemetry over HTTP and produces it to Kafka.

Run:
    cd proxy-app
    uvicorn main:app --host 0.0.0.0 --port 8000
"""
import logging
from contextlib import asynccontextmanager
from typing import Any, Optional, Union

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from config import config
from producer import TelemetryProducer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("proxy.main")

# Single producer instance reused across requests.
_producer: Optional[TelemetryProducer] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _producer
    _producer = TelemetryProducer()
    log.info("Producer ready -> topic '%s'", config.TELEMETRY_TOPIC)
    yield
    if _producer is not None:
        remaining = _producer.flush()
        log.info("Flushed producer on shutdown (%d still queued)", remaining)


app = FastAPI(title="OBD Telemetry Proxy", version="0.1.0", lifespan=lifespan)


class Accel(BaseModel):
    x: float
    y: float
    z: float


class Gps(BaseModel):
    lat: float
    lon: float
    heading: Optional[float] = None
    altitude_m: Optional[float] = None


class TelemetryEvent(BaseModel):
    """Mirrors data/sample.json. Extra fields are allowed and forwarded as-is."""

    model_config = {"extra": "allow"}

    vehicle_id: str
    event_id: Optional[str] = None
    event_time: Optional[str] = None
    speed_kmph: float
    engine_rpm: Optional[int] = None
    engine_load_pct: float
    throttle_pct: Optional[float] = None
    coolant_temp_c: Optional[float] = None
    intake_air_temp_c: Optional[float] = None
    maf_gps: Optional[float] = None
    fuel_level_pct: Optional[float] = None
    battery_voltage: Optional[float] = None
    odometer_km: Optional[float] = None
    dtc_codes: list[str] = Field(default_factory=list)
    accel: Optional[Accel] = None
    gps: Optional[Gps] = None


@app.get("/healthz")
def healthz() -> dict[str, Any]:
    return {"status": "ok", "topic": config.TELEMETRY_TOPIC}


@app.post("/events")
def post_events(
    payload: Union[TelemetryEvent, list[TelemetryEvent]],
) -> dict[str, Any]:
    """Accept a single event or a batch and produce each to Kafka."""
    if _producer is None:  # pragma: no cover - lifespan guarantees this
        raise HTTPException(status_code=503, detail="Producer not initialized")

    events = payload if isinstance(payload, list) else [payload]
    for ev in events:
        _producer.produce(ev.model_dump(exclude_none=False))

    return {"accepted": len(events), "topic": config.TELEMETRY_TOPIC}
