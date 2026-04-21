# AGENTS.md — Refactor Rules for AI Trading System

## Purpose
This file defines how automated agents (Codex, Claude, etc.) should perform
refactoring and code changes in this repository.

---

## 🔒 Core Principle

> This is a **behavior-preserving refactor**, NOT a redesign.

If unsure → **preserve existing behavior**.

---

## 🧱 Architecture Reference

Pipeline stages (must remain unchanged):

ingest → features → rank → execute → publish

These are **hard contracts**, not suggestions.

---

## 🚫 DO NOT CHANGE (Critical Invariants)

### Runtime Contracts
- CLI flags and commands
- Stage ordering and naming
- Trust / DQ gating logic
- Execution modes (paper/live/preview)
- Publish retry + dedupe behavior

### Data Contracts
- DuckDB schema
- Table names
- Column names
- Data types

### Artifact Contracts
DO NOT rename or move:

- ingest_summary.json
- feature_snapshot.json
- ranked_signals.csv
- breakout_scan.csv
- pattern_scan.csv
- stock_scan.csv
- sector_dashboard.csv
- dashboard_payload.json
- execute_summary.json
- publish_summary.json

### API Contracts
- Endpoint paths
- Response JSON structure
- Query parameters

---

## ⚠️ Refactor Strategy (MANDATORY)

Every change MUST follow this sequence:

1. **Move files (path only)**
2. Add **compatibility shim**
3. Fix imports minimally
4. Run tests
5. Validate artifacts
6. THEN (in later phase) refactor internals

---

## 🔁 Compatibility Shim Rule

When moving a module:

Old file MUST remain as:

```python
from ai_trading_system.<new_path> import *  # noqa

Do NOT remove shim until:

All imports are migrated
Tests pass
Explicit cleanup phase
🧠 Decision Rules
If something is unclear:
Assume it is a public contract
Do NOT modify it
If refactor requires behavior change:
STOP
Document blocker
Do not guess
🧩 Large File Handling

Files like:

feature_store.py
dhan_collector.py
ranking orchestration

Must NOT be split during path migration.

Only split in dedicated decomposition phase.

🛑 Anti-Patterns to Avoid

❌ Renaming fields for “clarity”
❌ Changing JSON keys
❌ Reordering CSV columns
❌ Changing default config values
❌ Adding validation rules
❌ Introducing new abstractions prematurely
❌ Mixing multiple domains in one refactor

✅ What IS Allowed

✔ Moving files
✔ Fixing broken imports
✔ Adding shims
✔ Extracting helpers ONLY when necessary
✔ Small safe bugfixes explicitly identified

🧪 Validation Required After Every Change
Tests must run
Pipeline must still execute (canary mode acceptable)
Artifacts must match previous schema
📦 Output Requirements (for every task)

Agent MUST provide:

Files changed
Old → New path mapping
Shims added
Tests result
Risks / assumptions
🔚 Final Cleanup Phase

Only at the very end:

Remove shims
Remove dead imports
Update docs
Normalize imports
🧭 Golden Rule

If it might break production → don’t do it in refactor phase.