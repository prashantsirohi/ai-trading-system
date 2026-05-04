#!/usr/bin/env bash
# Weekly stage classification job — wrapper for cron.
#
# Resolves the repo root from this script's location, sources the user's
# profile so `uv` is on PATH, and runs the classifier. Output is appended
# to logs/weekly_stage.log under the repo root.
#
# Cron entry (Friday 16:30 IST after market close):
#   30 16 * * 5  /path/to/repo/scripts/run_weekly_stage.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$REPO_DIR/logs"
mkdir -p "$LOG_DIR"

# Pull PATH additions like ~/.local/bin where uv typically lives.
# shellcheck disable=SC1090
[ -f "$HOME/.profile" ] && . "$HOME/.profile"
# Avoid sourcing ~/.zshrc under bash because zsh-specific commands can abort
# the wrapper before the job starts. PATH is extended explicitly below.
export PATH="$HOME/.local/bin:$PATH"

cd "$REPO_DIR"

RUN_ID="weekly-stage-$(date -u +%Y%m%dT%H%M%SZ)"
echo "=== $RUN_ID start: $(date -u +%FT%TZ) ===" >> "$LOG_DIR/weekly_stage.log"

uv run python -m scripts.run_weekly_stage \
    --exchange NSE \
    --run-id "$RUN_ID" \
    >> "$LOG_DIR/weekly_stage.log" 2>&1

echo "=== $RUN_ID done:  $(date -u +%FT%TZ) ===" >> "$LOG_DIR/weekly_stage.log"
