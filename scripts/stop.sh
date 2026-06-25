#!/usr/bin/env bash
# Stop the local pieces of the demo started by run_demo.sh: the proxy, the
# alerts consumer, and the mock generator. Useful when they were left running
# in the background or orphaned (e.g. run_demo.sh was killed uncleanly).
#
# Usage:  ./scripts/stop_demo.sh
set -uo pipefail

# Command-line patterns identifying each demo process.
PATTERNS=(
  "uvicorn main:app"        # proxy-app
  "alerts_consumer.py"      # consumer
  "generator.py"            # mock-generator
)

stopped_any=0
for pat in "${PATTERNS[@]}"; do
  # -f matches against the full command line; exclude this script itself.
  pids="$(pgrep -f "$pat" 2>/dev/null | grep -v "^$$\$" || true)"
  if [[ -z "$pids" ]]; then
    echo "No process matching '$pat'."
    continue
  fi
  for pid in $pids; do
    echo "Stopping '$pat' (pid $pid) ..."
    kill "$pid" 2>/dev/null || true
    stopped_any=1
  done
done

if [[ "$stopped_any" -eq 0 ]]; then
  echo "Nothing to stop."
  exit 0
fi

# Give processes a moment to exit, then force-kill any stragglers.
sleep 2
for pat in "${PATTERNS[@]}"; do
  for pid in $(pgrep -f "$pat" 2>/dev/null | grep -v "^$$\$" || true); do
    echo "Force killing '$pat' (pid $pid) ..."
    kill -9 "$pid" 2>/dev/null || true
  done
done

echo "Demo stopped."
