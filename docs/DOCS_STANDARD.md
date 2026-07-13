# Documentation Standard

- **Purpose:** Rules for writing documentation in this repo.
- **Audience:** Developer, anyone updating docs.
- **Last verified:** 2026-07-13
- **Source of truth:** This file.

---


## Rules

1. Code is the runtime source of truth. `docs/SYSTEM_GUIDE.md` is the single human-readable system orientation and operating contract.
2. Current behavior and planned behavior must be documented separately.
3. Canonical docs under `docs/` describe current behavior only unless a section is explicitly marked historical or planned.
4. Do not create duplicate architecture summaries at the same level of detail.
5. Do not describe ownership vaguely. Name the module, stage, table, path, flag, or endpoint that owns the behavior.
6. When runtime behavior changes, update the canonical docs in the same PR. Changes to system-level design also require `docs/SYSTEM_GUIDE.md`.
7. Mark legacy, scaffold, compatibility, and experimental modules explicitly.
8. Any provider or source-of-record change must update `docs/SYSTEM_GUIDE.md`, `docs/reference/data_sources.md`, the affected stage document, and the relevant runbook.
9. Any new or changed API endpoint must update `docs/reference/api_reference.md`.
10. Any new or changed artifact must update `docs/reference/artifacts.md`.
11. Any change to env vars, runtime flags, or safety controls must update the relevant part of `docs/SYSTEM_GUIDE.md` plus `docs/reference/configuration.md` or `docs/reference/environment_variables.md`.
12. Any change to UI surface status or startup flow must update `docs/SYSTEM_GUIDE.md` and `docs/architecture/ui_architecture.md`.
13. Archive or delete superseded docs. Do not leave parallel current docs that describe the same thing differently.
14. If code is internally inconsistent, document the inconsistency explicitly instead of guessing.
15. A pipeline-order/default, persistence-owner, execution-safety, public-interface, or common-operator-command change must update the guide and its linked detailed contract in the same commit.
16. New agents must be routed through `AGENTS.md` and `docs/SYSTEM_GUIDE.md`; do not create another jumpstart or high-level overview.

## Writing requirements

Use language that is:
- direct
- specific
- testable against code
- explicit about defaults, blockers, retries, writes, reads, and fallbacks

Avoid:
- aspirational architecture in current docs
- duplicated summaries
- vague ownership claims
- marketing language
