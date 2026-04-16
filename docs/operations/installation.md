# Installation

## Prerequisites

Required:
- Python 3.11 or newer
- local filesystem access to the repo workspace

Optional, only for specific surfaces:
- Node.js and npm for `web/execution-console/`
- Google Sheets credentials for network publish
- Telegram bot credentials for Telegram publish
- Dhan credentials for Dhan-primary collectors, token renewal, and current ingest/features preflight

## Initial setup

Create and activate a virtual environment:
```bash
python3 -m venv .venv
. .venv/bin/activate
```

Install Python dependencies and the local package entrypoints:
```bash
pip install -r requirements.txt
pip install -e .
```

Bootstrap master data:
```bash
python -m collectors.masterdata
```

Notes:
- there is no authoritative `.env.example` in the repo
- env vars are loaded from the nearest repo `.env` when present
- `pip install -e .` installs the console scripts declared in `pyproject.toml`, but Python dependencies still come from `requirements.txt`

## Required environment variables

Current code does not require one global env block for every workflow.

Required only when the associated capability is used:
- Dhan and current ingest/features preflight: `DHAN_API_KEY`, `DHAN_CLIENT_ID`, plus one of `DHAN_ACCESS_TOKEN`, `DHAN_REFRESH_TOKEN`, or `DHAN_TOTP`
- Google Sheets publish: `GOOGLE_SPREADSHEET_ID`, plus a credentials or token file discovered through `GOOGLE_SHEETS_CREDENTIALS`, `GOOGLE_TOKEN_PATH`, `client_secret.json`, or `token.json`
- Telegram publish: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
- FastAPI project-root override: `AI_TRADING_PROJECT_ROOT`

Optional runtime selectors:
- `DATA_DOMAIN`
- `ENV`

## First-run validation

Recommended first local operator run:
```bash
python -m run.orchestrator --skip-preflight --stages ingest,features,rank,publish --local-publish
```

Why this is the safest first validation:
- uses the default orchestrated ingest path
- avoids network publish dependencies
- skips the stricter Dhan preflight gate
- avoids `execute`, which the CLI includes by default

What a healthy first run should create:
- `data/ohlcv.duckdb`
- `data/control_plane.duckdb`
- `data/pipeline_runs/<run_id>/...`
- `data/feature_store/...`
- `data/masterdata.db`

## Local operator startup

NiceGUI operator console:
```bash
python -m ui.execution.app --port 8080
```

FastAPI operator backend:
```bash
python -m ui.execution_api.app --port 8090
```

Research Streamlit UI:
```bash
python -m streamlit run ui/research/app.py
```

ML Streamlit workbench:
```bash
python -m streamlit run ui/ml/app.py
```

React execution console:
```bash
cd web/execution-console
npm install
npm run dev
```

The React console requires the FastAPI backend to be running separately.
