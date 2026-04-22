#!/usr/bin/env bash
# start_api_server.sh — start the FastAPI backend with uvicorn
#
# Usage: ./start_api_server.sh [--host HOST] [--port PORT] [--reload]
#
# Defaults: host=0.0.0.0, port=8000, reload enabled

set -euo pipefail

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"
RELOAD="--reload"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)   HOST="$2"; shift 2 ;;
    --port)   PORT="$2"; shift 2 ;;
    --no-reload) RELOAD=""; shift ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load .env if present (export each non-comment line as an env var)
if [[ -f "$SCRIPT_DIR/.env" ]]; then
  set -a
  source "$SCRIPT_DIR/.env"
  set +a
fi

echo "Starting API server on http://${HOST}:${PORT} ..."
echo "  VZ_MODELS_DIR=${VZ_MODELS_DIR:-<not set, using default>}"
exec poetry run uvicorn api.main:app --host "$HOST" --port "$PORT" $RELOAD
