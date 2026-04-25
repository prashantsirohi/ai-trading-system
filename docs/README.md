# Documentation

This repository runs a staged NSE trading workflow with four distinct areas:
- operational data ingest, feature computation, ranking, paper execution, and publish delivery
- control-plane tracking, DQ, trust, quarantine, and operator tasks
- research and ML recipe workflows under the research data domain
- multiple UI surfaces for operators, analysts, and ML work

Code is the source of truth. These docs describe current behavior only.

## Start here

Operators:
- [operations/runbook.md](operations/runbook.md)
- [operations/troubleshooting.md](operations/troubleshooting.md)

Developers new to the repo:
- [architecture/system-overview.md](architecture/system-overview.md)
- [architecture/module-map.md](architecture/module-map.md)
- [architecture/pipeline.md](architecture/pipeline.md)

UI and API work:
- [interfaces/ui.md](interfaces/ui.md)
- [interfaces/api.md](interfaces/api.md)

## Doc layout

Architecture:
- [architecture/system-overview.md](architecture/system-overview.md): repo purpose, runtime split, storage, UI status
- [architecture/pipeline.md](architecture/pipeline.md): stage order, stage contracts, block conditions, retries, trust and DQ gates
- [architecture/pattern-scan.md](architecture/pattern-scan.md): end-to-end operational pattern scan flow, seed universe, lifecycle, artifacts, and operator commands
- [architecture/data-model.md](architecture/data-model.md): DuckDB and SQLite stores, key tables, artifact layout, lineage model
- [architecture/module-map.md](architecture/module-map.md): directory ownership and operational vs research vs legacy scope

Operations:
- [operations/installation.md](operations/installation.md): prerequisites, setup, first-run bootstrap, local startup
- [operations/configuration.md](operations/configuration.md): env vars, flags, mode selection, execution and publish controls
- [operations/runbook.md](operations/runbook.md): common operator workflows and recovery flows
- [operations/troubleshooting.md](operations/troubleshooting.md): issue-driven checks and exact recovery steps

Interfaces:
- [interfaces/api.md](interfaces/api.md): current FastAPI operator endpoints
- [interfaces/ui.md](interfaces/ui.md): current Streamlit, NiceGUI, and React/FastAPI surfaces

Reference:
- [reference/commands.md](reference/commands.md): authoritative runnable commands
- [reference/artifacts.md](reference/artifacts.md): per-stage artifacts and reports
- [reference/glossary.md](reference/glossary.md): project-specific terms

Governance:
- [DOCS_STANDARD.md](DOCS_STANDARD.md): required standards for future doc changes
- [../.docs-pr-checklist.md](../.docs-pr-checklist.md): short PR checklist for doc drift
- [refactor/final_architecture.md](refactor/final_architecture.md): implemented post-refactor architecture and migration notes

Historical material:
- [archive/README.md](archive/README.md): archived superseded docs
