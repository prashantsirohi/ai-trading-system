# UI

## Current UI surfaces

### Research Streamlit

Path:
- `ui/research/app.py`

Purpose now:
- analyst and research dashboard
- reads operational and research stores depending on page
- not the primary operator control plane for pipeline execution

Start:
```bash
python -m streamlit run ui/research/app.py
```

### ML Streamlit workbench

Path:
- `ui/ml/app.py`

Purpose now:
- dataset prep, training, deployment review, and ML shadow-monitor workflows

Start:
```bash
python -m streamlit run ui/ml/app.py
```

### NiceGUI execution console

Path:
- `ui/execution/app.py`

Purpose now:
- operator console for runs, tasks, ranking outputs, trust-aware workspace summaries, and shadow summaries
- can launch tracked background actions through shared UI services

Start:
```bash
python -m ui.execution.app --port 8080
```

Important current behavior:
- its “full pipeline” action runs `ingest,features,rank,publish`
- it does not include `execute` by default

### FastAPI execution backend

Path:
- `ui/execution_api/app.py`

Purpose now:
- operator API backend for the React console and any direct operator HTTP usage

Start:
```bash
python -m ui.execution_api.app --port 8090
```

Important current behavior:
- exposes JSON and SSE endpoints only
- does not serve the React frontend bundle

### React execution console

Path:
- `web/execution-console/`

Purpose now:
- standalone frontend workspace over the FastAPI backend

Start:
```bash
cd web/execution-console
npm install
npm run dev
```

Important current behavior:
- API base URL defaults to `http://localhost:8090`
- backend and frontend are separate processes
- its pipeline-run request model defaults to `ingest,features,rank,publish`

## Which UI is primary for what

Operational control:
- NiceGUI and FastAPI/React are the current operator-control surfaces

Research and analysis:
- Streamlit research app

ML workflow:
- Streamlit ML workbench

## NiceGUI vs React/FastAPI status

Current state:
- NiceGUI is still current and runnable
- FastAPI plus React is also current
- React has not fully replaced NiceGUI

Do not describe NiceGUI as retired, and do not describe React/FastAPI as the only operator surface.

## Legacy or compatibility UI layers

`dashboard/` contains wrapper modules that re-export current UI modules. Treat them as compatibility wrappers, not separate products.

## Operator workflow mapping

Use NiceGUI or React/FastAPI for:
- viewing recent runs
- viewing task logs and status
- viewing ranking and market snapshots
- retrying publish
- launching tracked pipeline and research tasks

Use Streamlit research for:
- exploratory analysis
- historical and cross-domain inspection
- research dashboards that are not part of the operator control loop

Use Streamlit ML for:
- dataset generation
- training and review
- deployment and shadow-monitor review
