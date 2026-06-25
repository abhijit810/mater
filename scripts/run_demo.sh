#!/usr/bin/env bash
# Start the local pieces of the demo: proxy (background), alerts consumer
# (background), then the interactive mock generator in the foreground.
#
# Prereqs:
#   - .env created from .env.example with valid Confluent Cloud creds
#   - Confluent Cloud topics + Flink statements + agents already running
#   - A Python venv with the requirements installed (see README)
#
# Usage:  ./scripts/run_demo.sh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f .env ]]; then
  echo "ERROR: .env not found. Copy .env.example to .env and fill in your values." >&2
  exit 1
fi

PIDS=()
cleanup() {
  echo ""
  echo "Shutting down background processes..."
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

echo "Starting proxy on :${PROXY_PORT:-8000} ..."
( cd proxy-app && uvicorn main:app --host "${PROXY_HOST:-0.0.0.0}" --port "${PROXY_PORT:-8000}" ) &
PIDS+=($!)

sleep 2

echo "Starting alerts consumer ..."
( cd consumer && python alerts_consumer.py ) &
PIDS+=($!)

sleep 1

echo "Starting mock generator (interactive). Type 'crash 0' or 'engine_fault 0'."
( cd mock-generator && python generator.py )
