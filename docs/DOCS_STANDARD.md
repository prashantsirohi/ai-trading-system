# Documentation Standard

## Rules

1. Code is the source of truth.
2. Current behavior and planned behavior must be documented separately.
3. Canonical docs under `docs/` describe current behavior only unless a section is explicitly marked historical or planned.
4. Do not create duplicate architecture summaries at the same level of detail.
5. Do not describe ownership vaguely. Name the module, stage, table, path, flag, or endpoint that owns the behavior.
6. When runtime behavior changes, update the canonical docs in the same PR.
7. Mark legacy, scaffold, compatibility, and experimental modules explicitly.
8. Any provider or source-of-record change must update:
   - `docs/architecture/pipeline.md`
   - `docs/operations/installation.md`
   - `docs/operations/runbook.md`
   - `docs/reference/commands.md`
9. Any new or changed API endpoint must update `docs/interfaces/api.md`.
10. Any new or changed artifact must update `docs/reference/artifacts.md`.
11. Any change to env vars, runtime flags, or safety controls must update `docs/operations/configuration.md`.
12. Any change to UI surface status or startup flow must update `docs/interfaces/ui.md`.
13. Archive or delete superseded docs. Do not leave parallel current docs that describe the same thing differently.
14. If code is internally inconsistent, document the inconsistency explicitly instead of guessing.

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
