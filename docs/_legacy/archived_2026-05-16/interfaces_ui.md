# UI

## Current UI Surface

### React Execution Console V2

Path:
- `web/execution-console-v2/ai-trading-dashboard-starter/`

Purpose:
- single operator dashboard for runs, task status, rankings, market snapshots, publish retry, ML workbench views, and shadow-monitor review
- talks to the FastAPI execution backend

Start:
```bash
cd web/execution-console-v2/ai-trading-dashboard-starter
npm install
npm run dev
```

Important behavior:
- API base URL defaults to `http://localhost:8090`
- backend and frontend are separate processes
- pipeline-run request defaults to `ingest,features,rank,publish`

### FastAPI Execution Backend

Path:
- `src/ai_trading_system/ui/execution_api/app.py`

Purpose:
- JSON/SSE backend for the V2 React console and direct operator HTTP usage

Start:
```bash
python -m ai_trading_system.ui.execution_api.app --port 8090
```

Important behavior:
- exposes the operator API only
- does not serve the React frontend bundle

## Removed Surfaces

The previous Python dashboard surfaces were removed. Do not add new operator workflows there; put UI work in the V2 React dashboard and backend logic in `src/ai_trading_system/ui/execution_api/services/`.
