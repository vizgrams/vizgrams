#!/usr/bin/env bash
# start_ui_server.sh — start the Vite dev server for ui
#
# Usage: ./start_ui_server.sh [--port PORT] [--api-url URL]
#
# Defaults: port=5173, proxies /api to http://localhost:8000

set -euo pipefail

PORT="${PORT:-5173}"
API_URL="${VITE_API_URL:-http://localhost:8000}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)    PORT="$2";    shift 2 ;;
    --api-url) API_URL="$2"; shift 2 ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UI_DIR="$SCRIPT_DIR/ui"

if [[ ! -d "$UI_DIR/node_modules" ]]; then
  echo "node_modules not found — running npm install..."
  npm install --prefix "$UI_DIR"
fi

echo "Starting UI dev server on http://localhost:${PORT} (API proxy → ${API_URL}) ..."
exec env VITE_API_URL="$API_URL" npm run dev --prefix "$UI_DIR" -- --port "$PORT"
