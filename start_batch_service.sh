#!/usr/bin/env bash
# start_batch_service.sh — start the work-tools2-batch microservice
#
# Usage: ./start_batch_service.sh [--host HOST] [--port PORT] [--no-reload]
#
# Defaults: host=0.0.0.0, port=8001, reload enabled

set -euo pipefail

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8001}"
RELOAD="--reload"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)      HOST="$2"; shift 2 ;;
    --port)      PORT="$2"; shift 2 ;;
    --no-reload) RELOAD=""; shift ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load .env if present
if [[ -f "$SCRIPT_DIR/.env" ]]; then
  set -a
  source "$SCRIPT_DIR/.env"
  set +a
fi

echo "Starting batch service on http://${HOST}:${PORT} ..."
echo "  VZ_MODELS_DIR=${VZ_MODELS_DIR:-<not set, using default>}"
exec poetry run uvicorn batch_service.main:app --host "$HOST" --port "$PORT" $RELOAD
