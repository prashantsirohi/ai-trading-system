# AGENTS.md
## Codex Operational Guidelines for AI Trading System

This file provides persistent instructions for Codex when working on this repository.

---

## 📘 Primary Directive

Before making any architectural or structural changes, Codex MUST read:


Codex must execute tasks sequentially according to the phases defined in that document.

---

## 🎯 Repository Purpose

This repository implements a modular AI-driven trading system for NSE markets. It includes:

- Data ingestion and validation
- Feature engineering
- Technical ranking and pattern detection
- Paper-trading execution
- Multi-channel publishing
- Operator UI and APIs

---

## 🧭 Pipeline Architecture

ingest → features → rank → execute → publish → ui

Each stage produces versioned artifacts stored under:
data/pipeline_runs/

These artifacts serve as the system of record.

---

## 📌 Codex Rules of Engagement

### General Rules
- Follow the refactor plan strictly.
- Work phase-by-phase.
- Keep changes small and reviewable.
- Preserve backward compatibility.
- Maintain artifact formats unless instructed otherwise.
- Do not introduce breaking changes without justification.

### Development Standards
- Prefer `core.*` over `utils.*`.
- Use clear service boundaries.
- Keep stage wrappers thin.
- Introduce read models for UI.
- Add tests for new logic.
- Avoid unnecessary abstractions.

### Safety Constraints
Codex must NOT:
- Perform a full repository rewrite.
- Change artifact filenames or schemas.
- Modify trading logic unless instructed.
- Remove legacy code before replacement is verified.
- Replace DuckDB, Parquet, or the execution framework.
- Introduce live trading integrations.

---

## 📁 Architectural Guidelines

### Runtime Infrastructure
- Use `core.paths` for path resolution.
- Use `core.logging` for logging.
- Use `core.contracts` for shared data models.

### Service Layers
Business logic should reside in:
services/
ingest/
features/
rank/
execute/
publish/

### UI Data Access
The UI must use read models located in:
ui/services/readmodels/

Direct filesystem access to artifacts should be avoided outside these modules.

---

## 🧪 Testing Requirements

Codex must:
- Add tests for new modules.
- Preserve existing behavior.
- Ensure orchestrator and API imports remain valid.
- Validate artifact compatibility.

---

## 🧾 Documentation Responsibilities

Codex should update documentation when:
- Architectural changes are introduced.
- Services are extracted or refactored.
- Contracts are modified.

Relevant directories:

docs/refactor/
 docs/architecture/

---

## ▶️ Standard Codex Execution Prompt

When initiating work, use:
Read docs/refactor/CODEX_REFACTOR_PLAN.md and execute the next pending phase.


---

## 📊 Completion Criteria

The refactor is considered complete when:

- All phases in CODEX_REFACTOR_PLAN.md are executed.
- Runtime infrastructure is unified.
- Stage wrappers are thin and modular.
- UI read models are implemented.
- Execution contracts are normalized.
- Documentation is aligned with the codebase.

---

## 👤 Maintainer

**Prashant Sirohi**  
AI Trading System Architect

---

## 🚀 Final Instruction to Codex

Always begin by reading:

docs/refactor/CODEX_REFACTOR_PLAN.md

Then proceed with the next phase in sequence.

