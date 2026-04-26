#!/usr/bin/env bash
#
# Start the FastAPI execution backend that powers the v2 dashboard.
#
# Usage:
#   scripts/start_dashboard_backend.sh                # 127.0.0.1:8090
#   PORT=8190 scripts/start_dashboard_backend.sh      # custom port
#   API_KEY=secret scripts/start_dashboard_backend.sh # custom key
#
# The dashboard's ``.env.local`` must set:
#   VITE_USE_MOCK_API=false
#   VITE_EXECUTION_API_KEY=<API_KEY>           # default: local-dev-key
#   VITE_EXECUTION_PROXY_TARGET=http://127.0.0.1:<PORT>
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${PORT:-8090}"
HOST="${HOST:-127.0.0.1}"
API_KEY="${API_KEY:-local-dev-key}"

if [[ ! -x "${REPO_ROOT}/.venv/bin/python" ]]; then
  echo "error: ${REPO_ROOT}/.venv/bin/python not found — run \`uv venv && uv sync\` (or pip install) first" >&2
  exit 1
fi

cd "${REPO_ROOT}"
echo "→ starting execution API on ${HOST}:${PORT} (key=${API_KEY})"
EXECUTION_API_KEY="${API_KEY}" \
AI_TRADING_PROJECT_ROOT="${REPO_ROOT}" \
  exec "${REPO_ROOT}/.venv/bin/python" -m ai_trading_system.ui.execution_api.app \
    --host "${HOST}" --port "${PORT}"
