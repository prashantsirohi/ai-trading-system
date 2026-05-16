# market_intel collector — operations runbook

The trading pipeline reads corporate-action context from a DuckDB store
maintained by the **`market_intel`** package. That package runs as a
separate, always-on process. The pipeline only reads — it never writes — so
the two sides can be deployed and restarted independently.

## What it does

Polls NSE / BSE / SEBI / rating-agency feeds on a schedule and writes
events into `data/market_intel.duckdb`. The trading pipeline's `events`
stage and `ai-trading-healthcheck` CLI both query this DB.

## Prerequisites

- Python 3.10+
- The `market_intel` package installed in the same environment as
  `ai-trading-system` (already wired via `pyproject.toml`).
- Network access to `nseindia.com`, `nsearchives.nseindia.com`,
  `bseindia.com`, `api.bseindia.com`, and the rating-agency sites.

If you've installed the trading system, the dep is already present:

```bash
pip install -e .
python -c "import market_intel; print(market_intel.__file__)"
```

## Where the DB lives

Default: `data/market_intel.duckdb` (relative to the working directory).
Override via the `AI_TRADING_MARKET_INTEL_DB` env var (the trading pipeline
reads the same env). Keep both processes pointed at the same path.

## Starting the collector (foreground)

```bash
mkdir -p data
python -m market_intel.cli run \
  --db-path data/market_intel.duckdb \
  --interval-rss 300 \
  --interval-api 900 \
  --interval-llm 1800
```

- `--interval-rss 300` — poll NSE RSS every 5 minutes
- `--interval-api 900` — poll NSE/BSE/SAST/PIT JSON APIs every 15 minutes
- `--interval-llm 1800` — analyse PDF attachments every 30 minutes

## Starting the collector (launchd, macOS)

Drop this at `~/Library/LaunchAgents/com.local.market-intel.plist` and
`launchctl load` it:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>com.local.market-intel</string>
  <key>ProgramArguments</key>
  <array>
    <string>/path/to/.venv/bin/python</string>
    <string>-m</string><string>market_intel.cli</string>
    <string>run</string>
    <string>--db-path</string>
    <string>/abs/path/to/data/market_intel.duckdb</string>
  </array>
  <key>KeepAlive</key><true/>
  <key>RunAtLoad</key><true/>
  <key>StandardErrorPath</key>
  <string>/abs/path/to/logs/market_intel.err</string>
  <key>StandardOutPath</key>
  <string>/abs/path/to/logs/market_intel.out</string>
  <key>WorkingDirectory</key><string>/abs/path/to</string>
</dict></plist>
```

```bash
launchctl load ~/Library/LaunchAgents/com.local.market-intel.plist
launchctl list | grep market-intel
```

## Starting the collector (systemd, Linux)

`/etc/systemd/system/market-intel.service`:

```ini
[Unit]
Description=market_intel collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=trading
WorkingDirectory=/opt/ai-trading-system
Environment=AI_TRADING_MARKET_INTEL_DB=/opt/ai-trading-system/data/market_intel.duckdb
ExecStart=/opt/ai-trading-system/.venv/bin/python -m market_intel.cli run \
    --db-path /opt/ai-trading-system/data/market_intel.duckdb
Restart=always
RestartSec=15

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now market-intel
sudo systemctl status market-intel
journalctl -u market-intel -f
```

## Verifying it's healthy

The trading repo ships a healthcheck CLI:

```bash
python -m ai_trading_system.interfaces.cli.healthcheck market-intel
# or, if installed:
ai-trading-healthcheck market-intel
```

Output (text mode):

```
✓ market_intel: OK
  reason       : Heartbeat 2.3min old
  db_path      : data/market_intel.duckdb
  last_heartbeat: 2026-05-02T13:04:11+00:00
  last_cycle_at : 2026-05-02T13:04:11+00:00
  heartbeat_age : 2.30 min
  error_count   : 0
```

JSON mode for monitors / cron:

```bash
ai-trading-healthcheck market-intel --json
ai-trading-healthcheck market-intel --max-stale-min 30 --json | jq .status
```

Exit codes:
- `0` — ok
- `1` — degraded (stale heartbeat or recent errors)
- `2` — down (no heartbeat row, or DB file missing)
- `3` — usage error

Wire `1` and `2` to your alerting (PagerDuty / Slack webhook / Telegram).
A degraded collector won't crash the trading pipeline — the events stage
will simply emit empty enrichment for symbols whose data is stale and tag
the publish payload with `data_freshness=stale`.

## Common issues

| Symptom | Likely cause | Fix |
|---|---|---|
| `down` with "DB file not found" | Collector hasn't run yet, or DB path mismatch between collector and trading pipeline | Confirm `AI_TRADING_MARKET_INTEL_DB` matches and the collector has completed at least one cycle |
| `down` with "No heartbeat row" | Collector crashed before writing first heartbeat | Check the collector logs; usually an NSE 403 from a missing warmup |
| `degraded` with stale heartbeat | Collector died silently, or NSE / BSE rate-limited it | Restart the collector; if NSE keeps 403'ing, increase `min_request_gap_sec` |
| `degraded` with `error_count > 0` | Single source (e.g. one rating-agency adapter) is failing | Check `cycle_stats` JSON for which collector raised |
| Pipeline reads zero events for known symbols | Collectors are running but `tracked_entity` table is empty | `python -m market_intel.cli add-entity --symbol RELIANCE --company-name "Reliance Industries"` |

## Schema dump for debugging

```bash
duckdb data/market_intel.duckdb \
  "SELECT count(*) FROM raw_event;
   SELECT count(*) FROM resolved_event;
   SELECT count(*) FROM bulk_deal WHERE trade_date >= current_date - 5;
   SELECT * FROM scheduler_state ORDER BY state_id DESC LIMIT 1;"
```

## Backups

The collector store is regenerable from upstream sources, but losing it
costs a few hours of warmup latency on first restart. A nightly snapshot
is recommended:

```bash
duckdb data/market_intel.duckdb "EXPORT DATABASE 'data/backups/mi_$(date +%Y%m%d)' (FORMAT PARQUET);"
```
