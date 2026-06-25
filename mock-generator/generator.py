"""Realtime OBD mock generator.

Emits telemetry for N vehicles every EMIT_INTERVAL_SEC by POSTing to the proxy.
Type scenario commands at the prompt to inject anomalies on a vehicle:

    crash [vehicle_index]          # default vehicle_index = 0
    engine_fault [vehicle_index]
    list                           # show vehicles
    quit

Run:
    cd mock-generator
    python generator.py
"""
from __future__ import annotations

import os
import sys
import threading
import time

import requests
from dotenv import load_dotenv

from scenarios import VehicleState, next_event

load_dotenv()

PROXY_URL = os.getenv("PROXY_URL", "http://localhost:8000/events")
NUM_VEHICLES = int(os.getenv("NUM_VEHICLES", "3"))
EMIT_INTERVAL_SEC = float(os.getenv("EMIT_INTERVAL_SEC", "1.0"))

_stop = threading.Event()
_vehicles: list[VehicleState] = [
    VehicleState(vehicle_id=f"JIO-OBD-{i:04d}") for i in range(1, NUM_VEHICLES + 1)
]


def _emit_loop() -> None:
    session = requests.Session()
    while not _stop.is_set():
        batch = [next_event(v) for v in _vehicles]
        try:
            resp = session.post(PROXY_URL, json=batch, timeout=5)
            resp.raise_for_status()
        except requests.RequestException as exc:
            print(f"  [emit error] {exc}", file=sys.stderr)
        _stop.wait(EMIT_INTERVAL_SEC)


def _trigger(scenario: str, idx: int) -> None:
    if 0 <= idx < len(_vehicles):
        _vehicles[idx].trigger(scenario)
        print(f"  -> {scenario} armed on {_vehicles[idx].vehicle_id}")
    else:
        print(f"  [error] vehicle index {idx} out of range (0..{len(_vehicles)-1})")


def main() -> None:
    print(f"Emitting {NUM_VEHICLES} vehicle(s) every {EMIT_INTERVAL_SEC}s -> {PROXY_URL}")
    for i, v in enumerate(_vehicles):
        print(f"  [{i}] {v.vehicle_id}")
    print("Commands: 'crash [i]', 'engine_fault [i]', 'list', 'quit'\n")

    worker = threading.Thread(target=_emit_loop, daemon=True)
    worker.start()

    try:
        while True:
            line = input("> ").strip()
            if not line:
                continue
            parts = line.split()
            cmd = parts[0].lower()
            idx = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0

            if cmd in ("quit", "exit", "q"):
                break
            elif cmd == "list":
                for i, v in enumerate(_vehicles):
                    print(f"  [{i}] {v.vehicle_id} scenario={v.scenario}")
            elif cmd in ("crash", "engine_fault"):
                _trigger(cmd, idx)
            else:
                print("  unknown command. use: crash [i] | engine_fault [i] | list | quit")
    except (EOFError, KeyboardInterrupt):
        pass
    finally:
        _stop.set()
        worker.join(timeout=2)
        print("\nstopped.")


if __name__ == "__main__":
    main()
