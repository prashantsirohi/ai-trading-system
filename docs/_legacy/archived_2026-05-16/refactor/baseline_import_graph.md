# Baseline Import Graph

This is a high-level dependency snapshot before moving any existing modules into `src/`.

## Top-level flow

```text
run/*
  -> run/stages/*
  -> services/{ingest,features,rank,execute,publish}
  -> analytics/*
  -> execution/*
  -> publishers/*
  -> core/*

services/ingest
  -> collectors/*
  -> analytics.data_trust
  -> core/*

services/features
  -> collectors.daily_update_runner
  -> analytics.data_trust
  -> core/*

services/rank
  -> analytics/*
  -> channel/*
  -> core/*
  -> run/stages/base

services/execute
  -> execution/*
  -> core/*
  -> run/stages/base

services/publish
  -> run/stages/base
  -> core/*

ui/execution_api.app
  -> ui/services/execution_operator
  -> ui/services/control_center
  -> ui/services/readmodels/*
  -> analytics.registry

ui/research/*
  -> analytics/*
  -> execution/*
  -> core/*

collectors/*, analytics/*, research/*, channel/*, publishers/*
  -> mixed imports from core/* and utils/*
```

## Key observations

- `run/orchestrator.py` is the main import root for the staged production pipeline.
- `services/` already provides a useful seam for later src-based moves without changing current stage names.
- `ui/` depends on artifact readers and registry access rather than importing pipeline stages directly.
- `utils/` is still part of the live dependency graph, so this baseline step keeps it untouched.
- The new `src/ai_trading_system` package is only a placeholder and does not yet own runtime imports.
