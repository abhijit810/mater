"""Scenario state machines that mutate a vehicle's telemetry over time.

Each vehicle carries a baseline cruise state. A scenario advances that state one
tick at a time and returns the next telemetry sample. Scenarios are intentionally
deterministic enough to reliably trip the Flink detection thresholds during a demo.

Detection thresholds these scenarios are tuned against (see flink/ SQL):
  - crash:        speed drop > 40 km/h between consecutive events, accel mag > 25
  - engine_fault: 10s window engine_load stddev > 15 OR range > 40
"""
from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


@dataclass
class VehicleState:
    vehicle_id: str
    speed_kmph: float = 80.0
    engine_rpm: int = 3000
    engine_load_pct: float = 55.0
    throttle_pct: float = 40.0
    coolant_temp_c: float = 90.0
    fuel_level_pct: float = 70.0
    odometer_km: float = 48000.0
    lat: float = 19.0760
    lon: float = 72.8777
    heading: float = 145.0
    # Scenario control: when > 0, an injected scenario is "playing out".
    scenario: str = "normal"
    scenario_ticks_left: int = 0
    # Carries the accel produced this tick (impact spike during a crash).
    _accel: tuple[float, float, float] = field(default=(0.0, 0.0, 1.0))

    def trigger(self, scenario: str) -> None:
        """Arm a scenario; it will play out over the next few ticks."""
        self.scenario = scenario
        if scenario == "crash":
            self.scenario_ticks_left = 1  # a single drastic event
        elif scenario == "engine_fault":
            self.scenario_ticks_left = 8  # sustained instability across a window
        else:
            self.scenario_ticks_left = 0


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _advance(state: VehicleState) -> None:
    """Mutate state in place for the next tick according to the active scenario."""
    if state.scenario == "crash" and state.scenario_ticks_left > 0:
        # Drastic deceleration + impact spike on the accelerometer.
        state.speed_kmph = _clamp(state.speed_kmph - 55.0, 0.0, 220.0)
        state.engine_rpm = 900
        state.throttle_pct = 0.0
        state._accel = (
            round(random.uniform(-30, 30), 2),
            round(random.uniform(-30, 30), 2),
            round(random.uniform(20, 35), 2),
        )
        state.scenario_ticks_left -= 1

    elif state.scenario == "engine_fault" and state.scenario_ticks_left > 0:
        # Wild engine-load oscillation -> high stddev / range within the 10s window.
        swing = random.choice([-1, 1]) * random.uniform(35, 50)
        state.engine_load_pct = _clamp(55.0 + swing, 0.0, 100.0)
        state.engine_rpm = int(_clamp(3000 + swing * 40, 800, 7000))
        state.coolant_temp_c = _clamp(state.coolant_temp_c + random.uniform(0, 1.5), 80, 120)
        state._accel = (round(random.uniform(-0.1, 0.1), 2), 0.0, 0.98)
        state.scenario_ticks_left -= 1

    else:
        # Normal cruise: gentle jitter around the baseline, no threshold crossings.
        state.scenario = "normal"
        state.speed_kmph = _clamp(state.speed_kmph + random.uniform(-3, 3), 30, 110)
        state.engine_load_pct = _clamp(state.engine_load_pct + random.uniform(-4, 4), 35, 75)
        state.engine_rpm = int(_clamp(state.engine_rpm + random.uniform(-150, 150), 1200, 4500))
        state.throttle_pct = _clamp(35 + (state.speed_kmph - 80) * 0.5, 5, 90)
        state._accel = (round(random.uniform(-0.05, 0.05), 2), round(random.uniform(-0.05, 0.05), 2), 0.98)

    # Shared physics-ish updates.
    state.odometer_km += state.speed_kmph / 3600.0  # km travelled in ~1s
    state.fuel_level_pct = _clamp(state.fuel_level_pct - 0.002, 0, 100)
    state.heading = (state.heading + random.uniform(-2, 2)) % 360


def next_event(state: VehicleState) -> dict:
    """Advance the vehicle one tick and return an OBD telemetry event dict."""
    _advance(state)
    ax, ay, az = state._accel
    return {
        "vehicle_id": state.vehicle_id,
        "event_id": str(uuid.uuid4()),
        "event_time": _now_iso(),
        "speed_kmph": round(state.speed_kmph, 1),
        "engine_rpm": int(state.engine_rpm),
        "engine_load_pct": round(state.engine_load_pct, 1),
        "throttle_pct": round(state.throttle_pct, 1),
        "coolant_temp_c": round(state.coolant_temp_c, 1),
        "intake_air_temp_c": 38.0,
        "maf_gps": round(state.engine_load_pct * 0.2, 1),
        "fuel_level_pct": round(state.fuel_level_pct, 1),
        "battery_voltage": 13.9,
        "odometer_km": round(state.odometer_km, 2),
        "dtc_codes": ["P0301"] if state.scenario == "engine_fault" else [],
        "accel": {"x": ax, "y": ay, "z": az},
        "gps": {
            "lat": round(state.lat, 5),
            "lon": round(state.lon, 5),
            "heading": round(state.heading, 1),
            "altitude_m": 14,
        },
    }
