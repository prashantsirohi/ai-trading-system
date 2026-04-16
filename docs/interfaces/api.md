# API

## Status

`ui/execution_api/app.py` exposes the current operator backend.

This API is:
- internal to this repo
- operator-facing, not public
- JSON and SSE only
- not responsible for serving the React frontend bundle

Default bind:
- host `0.0.0.0`
- port `8090`

Project root resolution:
- defaults to the repo root inferred from the module path
- can be overridden with `AI_TRADING_PROJECT_ROOT`

## Request models

### `POST /api/execution/pipeline/run`

Body:
```json
{
  "label": "Execution API pipeline run",
  "stages": ["ingest", "features", "rank", "publish"],
  "params": {},
  "run_id": null,
  "run_date": null
}
```

Notes:
- API default stages skip `execute`
- response shape is `{ "task": ... }`

### `POST /api/execution/pipeline/publish-retry`

Body:
```json
{
  "local_publish": false
}
```

Behavior:
- finds the latest publishable run
- launches `publish` only
- returns `{ "task": ... }`

### `POST /api/execution/shadow/run`

Body:
```json
{
  "label": "Shadow refresh",
  "backfill_days": 0,
  "prediction_date": null
}
```

Behavior:
- launches the ML shadow-monitor flow
- returns `{ "task": ... }`

### `POST /api/execution/research/launch`

Body:
```json
{
  "port": 8501
}
```

Behavior:
- launches the Streamlit research dashboard as a tracked task
- returns `{ "task": ... }`

## Read endpoints

### `GET /api/execution/summary`

Purpose:
- combined operator summary view

Current payload includes:
- `db_stats`
- `health`
- summary panels built from execution data and latest operator state

### `GET /api/execution/health`

Purpose:
- convenience health payload

Current response:
- the `health` section from `/api/execution/summary`

### `GET /api/execution/ranking?limit=<n>`

Purpose:
- latest top-ranked rows

Current payload includes:
- `top_ranked`
- ranking-derived summary values

### `GET /api/execution/market?limit=<n>`

Purpose:
- current market-facing operator snapshot

Current payload includes current records derived from rank artifacts such as:
- `breakouts`
- `patterns`
- `sectors`
- `stock_scan`

### `GET /api/execution/workspace/pipeline?limit=<n>`

Purpose:
- one payload for the operator workspace

Current payload includes:
- `top_ranked`
- `breakouts`
- `patterns`
- `sectors`
- `stock_scan`
- `counts`
- `ops_health`
- `data_trust`

### `GET /api/execution/shadow`

Purpose:
- current shadow-monitor summary and overlay state

### `GET /api/execution/runs?limit=<n>`

Purpose:
- recent pipeline runs

Current response:
```json
{ "runs": [ ... ] }
```

### `GET /api/execution/runs/{run_id}`

Purpose:
- detailed run view

Current response includes:
- `run`
- stage attempts
- alerts
- publish logs
- artifact metadata

404 behavior:
- returns HTTP 404 when the run id is unknown

### `GET /api/execution/tasks?limit=<n>`

Purpose:
- recent operator tasks

Current response:
```json
{ "tasks": [ ... ] }
```

Task rows may include:
- `task_id`
- `task_type`
- `operator_action_type`
- `status`
- `run_id`
- `current_stage`
- `current_stage_label`
- `stage_statuses`
- publish progress summaries

### `GET /api/execution/tasks/{task_id}`

Purpose:
- detailed operator task view

Current response:
```json
{ "task": { ... } }
```

404 behavior:
- returns HTTP 404 when the task id is unknown

### `GET /api/execution/tasks/{task_id}/logs?after=<cursor>&limit=<n>`

Purpose:
- paged task log rows plus current task snapshot

Current response includes:
- `task`
- `logs`
- cursor-like `log_cursor` values per row

### `GET /api/execution/tasks/{task_id}/events?cursor=<n>`

Purpose:
- SSE stream for live task updates

Current event payload includes:
- `task`
- `logs`
- `cursor`

Terminal task statuses that end the stream:
- `completed`
- `completed_with_publish_errors`
- `failed`
- `terminated`

### `GET /api/execution/processes`

Purpose:
- list tracked background processes

Current response:
```json
{ "processes": [ ... ] }
```

## Action endpoints

### `POST /api/execution/processes/{pid}/terminate`

Purpose:
- terminate a tracked background process

### `POST /api/execution/tasks/{task_id}/terminate`

Purpose:
- terminate a tracked task

404 behavior:
- returns HTTP 404 when the task id is unknown

## Stability guidance

Current stability level by endpoint group:
- summary, workspace, runs, tasks: operator-facing and currently used by the repo UI surfaces
- pipeline run, publish retry, shadow run, research launch: operator actions and task launchers
- process and task terminate: operator control-plane actions

These endpoints should be treated as internal repo interfaces. Any shape change should update this file and the consuming UI in the same change.
