# Glossary

## `artifact`

A materialized output recorded for a pipeline stage attempt and stored under `data[/research]/pipeline_runs/<run_id>/<stage>/attempt_<n>/`.

## `canary`

A reduced live run mode triggered with `--canary`. On the CLI and daily wrapper, the untouched default stage string is reduced to `ingest,features,rank`, and the symbol universe defaults to a smaller limit.

## `data-domain`

The storage domain selected for a run, currently `operational` or `research`.

## `pipeline run`

One orchestrated execution tracked by `pipeline_run`, identified by a `run_id`, containing zero or more stage attempts.

## `stage attempt`

One execution of a specific stage for a specific `run_id`, tracked in `pipeline_stage_run` and mapped to one `attempt_<n>` directory.

## `trust`

The current assessment of market-data reliability derived from provider lineage, fallback usage, and quarantine state. Current summary states include `trusted`, `degraded`, `blocked`, `legacy`, and `missing`.

## `quarantine`

The per-symbol, per-date exception model stored in `_catalog_quarantine` for unresolved, observed, or resolved trust issues.

## `soft gate`

A non-absolute filter that influences downstream selection without changing upstream artifacts. Current code uses `execution_breakout_linkage=soft_gate` for optional breakout-aware execution filtering.

## `publish dedupe`

The publish-stage replay protection keyed by `run_id + channel + artifact hash`. Successful prior deliveries are recorded as `duplicate` on retry instead of being resent.

## `shadow mode`

Current ML overlay and evaluation behavior where ML scores are generated, recorded, and reviewed without replacing the baseline operational rank pipeline as the system of record.

## `artifact hash`

The file hash recorded for a stage artifact in `pipeline_artifact` and reused by publish dedupe.

## `delivery required`

An ingest-stage option that turns delivery-collection failure from a recorded degraded result into a blocking stage error.

## `publish-of-record`

A publish channel that current code treats as an official outward delivery target rather than a diagnostic or informational side channel.
