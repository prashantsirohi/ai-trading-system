# Deployment: Mac mini

- **Purpose:** Run the pipeline reliably on a Mac mini with 16 GB RAM, scheduled around NSE market hours.
- **Audience:** Operator.
- **Last verified:** 2026-05-16
- **Source of truth:** [`docs/operations/installation.md`](../_legacy/archived_2026-05-16/operations_installation.md), [`docs/reference/commands.md`](../reference/commands.md), [`docs/_audit/current_code_truth_map.md`](../_audit/current_code_truth_map.md).

> Honest scope note: the truth map does not document a tested launchd configuration. The cron and launchd snippets below are **starting points**, not verified setups. Adapt and test before relying on them.

---

## Assumptions

- macOS, Apple Silicon or Intel Mac mini.
- 16 GB RAM, SSD with at least 50 GB free for data + backups.
- Python 3.11+.
- Repo cloned with a working `.venv` (see [`docs/operations/installation.md`](../_legacy/archived_2026-05-16/operations_installation.md)).
- Network access to NSE, Dhan (if used), Google, Telegram.

## One-time setup

```bash
cd <repo-root>
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
pip install -e .
python -m ai_trading_system.interfaces.cli.bootstrap_runtime_data
python -m ai_trading_system.domains.ingest.masterdata
```

Required env vars only when the corresponding capability is used — see [`docs/reference/environment_variables.md`](../reference/environment_variables.md). The repo loads the nearest `.env` automatically.

### Sanity-check first run

```bash
python -m ai_trading_system.pipeline.orchestrator \
  --skip-preflight --stages ingest,features,rank,publish --local-publish
```

## 16 GB RAM strategy

The full pipeline — especially `features` over the full NSE universe — is the highest memory consumer. Recommendations:

1. **Stage the pipeline in two halves.** Run ingest + features first, then the rest. This shortens any single Python process lifetime and lets the OS reclaim memory between halves.
2. **Avoid running the React console dev build (`npm run dev`) and a pipeline run simultaneously.** Both compete for memory.
3. **Avoid running `ai-trading-execution-api` and a pipeline on the same time slot.** The API can stay up; just don't trigger a heavy backfill while it serves.
4. **Use canary mode for ad-hoc checks** (`--canary`) — reduces `symbol_limit` and stage scope.
5. **Don't keep multiple browser tabs of the React console open** against this host during a run.

### Staged execution

```bash
# Slot A (memory-heavy)
python -m ai_trading_system.pipeline.orchestrator --stages ingest,features

# Slot B (lighter, downstream)
python -m ai_trading_system.pipeline.orchestrator --stages rank,candidates,events,execute,insight,narrative,publish,perf_tracker
```

Both can share the same logical "day" — the orchestrator records separate run_ids per invocation.

## Scheduling — cron (starting point)

NSE closes around 15:30 IST. A simple daily cron line that runs the full pipeline after market close:

```cron
# Run daily Mon-Fri at 17:00 local time. Adjust TZ to match the host.
0 17 * * 1-5 cd /Users/<you>/repo && /Users/<you>/repo/.venv/bin/ai-trading-pipeline >> /Users/<you>/repo/logs/pipeline.$(date +\%Y\%m\%d).log 2>&1
```

Notes:

- macOS cron may require Full Disk Access for `cron` in System Settings → Privacy & Security if the repo lives in a protected location.
- Always pin the absolute path to the venv binary.
- Redirect stdout+stderr to a dated log so failures are recoverable.

> Current code status: not a tested cron configuration. Confirm timezone and Full Disk Access on your host.

## Scheduling — launchd (starting point)

A minimal `~/Library/LaunchAgents/com.local.ai-trading.daily.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.local.ai-trading.daily</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/you/repo/.venv/bin/ai-trading-pipeline</string>
  </array>
  <key>WorkingDirectory</key><string>/Users/you/repo</string>
  <key>StartCalendarInterval</key>
  <array>
    <dict><key>Weekday</key><integer>1</integer><key>Hour</key><integer>17</integer><key>Minute</key><integer>0</integer></dict>
    <dict><key>Weekday</key><integer>2</integer><key>Hour</key><integer>17</integer><key>Minute</key><integer>0</integer></dict>
    <dict><key>Weekday</key><integer>3</integer><key>Hour</key><integer>17</integer><key>Minute</key><integer>0</integer></dict>
    <dict><key>Weekday</key><integer>4</integer><key>Hour</key><integer>17</integer><key>Minute</key><integer>0</integer></dict>
    <dict><key>Weekday</key><integer>5</integer><key>Hour</key><integer>17</integer><key>Minute</key><integer>0</integer></dict>
  </array>
  <key>StandardOutPath</key><string>/Users/you/repo/logs/pipeline.out</string>
  <key>StandardErrorPath</key><string>/Users/you/repo/logs/pipeline.err</string>
</dict>
</plist>
```

Load with:

```bash
launchctl load ~/Library/LaunchAgents/com.local.ai-trading.daily.plist
```

> Current code status: this plist is illustrative. Verify it on your host; launchd will fail silently if paths or permissions are wrong.

## Avoiding OOM

- Watch the first few full-pipeline runs with Activity Monitor. If RAM pressure shows yellow/red, switch to staged execution above.
- The features stage is the typical culprit. Reducing the universe (via stage params `symbol_limit` / `canary_mode`) lowers memory.
- DuckDB queries against `_catalog` are streaming-friendly, but long-running ad-hoc analytical queries on top of a running pipeline can still spike memory. Avoid concurrent heavy `duckdb` shells during runs.

## Log rotation

There is no in-repo log rotation. A starting-point strategy is to use dated-file logs (as in the cron snippet) and prune weekly:

```bash
find /Users/<you>/repo/logs -name 'pipeline.*.log' -mtime +21 -delete
```

> Current code status: not enforced by code. Add to your weekly maintenance — see [weekly_operations.md](./weekly_operations.md).

## Surfaces in production

- **FastAPI:** `python -m ai_trading_system.ui.execution_api.app --port 8090`. Keep it on localhost; bind a reverse proxy if you need external access.
- **React console:** `web/execution-console-v2/ai-trading-dashboard-starter` — `npm run dev` is for development. For a deployed console, build (`npm run build`) and serve via a static host. The FastAPI app does **not** serve the React build.

## Live trading note

The live Dhan adapter raises `RuntimeError("Live Dhan execution is intentionally disabled...")` unless invoked with `dry_run=True`. Do not treat this Mac mini setup — or any current setup — as production-ready for live execution. Paper execute is the only verified path.
