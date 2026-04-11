# React Operator Workspace Migration Plan

## Goal

Move the main operational workspace from Streamlit to the existing React + FastAPI stack while keeping the current pipeline artifacts as the first backend contract.

## Why

- Streamlit remains useful for research and admin flows.
- The operational workspace needs stronger layout control, denser tables, row detail flows, and more predictable app-shell behavior.
- The repo already contains a working React/Vite frontend and FastAPI backend, so we can migrate incrementally instead of rewriting from zero.

## Phase 1: Read-Only Operator Workspace

Scope:

- Add read-only operator endpoints on the FastAPI backend.
- Use existing rank artifacts and payload JSON as the source of truth.
- Build the first React `Pipeline` page.

Delivered in this phase:

- `/api/execution/workspace/pipeline`
- React `Pipeline` page as the default landing route
- Ops health ribbon
- Ranked / breakout / pattern / sector / stock scan tables

## Phase 2: Replace Streamlit Operational Tabs

Move these Streamlit surfaces into React:

- `Pipeline`
- `Ranking`
- `Chart`
- `Patterns`
- `Portfolio`

Recommended sequence:

1. `Pipeline`
2. `Ranking`
3. `Chart`
4. `Patterns`
5. `Portfolio`

Keep in Streamlit for now:

- research backtests
- ML diagnostics
- one-off admin/debug pages

## Backend Contract Strategy

Short term:

- Serve latest payload/CSV artifact snapshots through FastAPI.
- Keep the pipeline unchanged.

Medium term:

- Replace “latest artifact only” endpoints with richer read models backed by dedicated service functions.
- Preserve artifact compatibility so React and Streamlit can coexist during migration.

## UI Architecture

Recommended React shell:

- sidebar navigation
- top action bar
- page-level panels
- selectable data tables
- row-detail drawers/modals
- chart overlays on a dedicated chart route

Core component families:

- metric cards
- ops health ribbon
- dense data table
- detail drawer
- chart panel
- publish/task status panels

## Risks To Manage

- drift between Streamlit and React interpretations of payload fields
- missing artifact rows being confused with failed scanners
- chart interaction scope growing faster than the API contract

Mitigations:

- expose explicit task/artifact status from the backend
- keep tests on API snapshot payloads
- migrate page by page, not all at once

## Next Steps

1. Add row selection + detail drawer for React `Pipeline` breakout/pattern tables.
2. Add a dedicated React `Ranking` page that mirrors the operator workflow rather than the Streamlit layout.
3. Add chart endpoints and build the React `Chart` page with overlays for breakouts and patterns.
4. Move portfolio and execution context into the React shell.
