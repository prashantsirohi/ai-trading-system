# Adding a New API Endpoint

- **Purpose:** Add a route to the FastAPI execution console backend.
- **Audience:** Developer.
- **Last verified:** 2026-05-16
- **Source of truth:** `src/ai_trading_system/ui/execution_api/`.

---

## Layered architecture

The execution API enforces a 3-layer split (AST-lint enforced in `tests/lint/`):

```
HTTP (routes/)  →  service (services/)  →  readmodel (services/readmodels/)  →  domain
```

- Only the HTTP layer is allowed to import `fastapi` / `uvicorn` / `starlette`.
- Services delegate to readmodels.
- Readmodels are pure functions over the control-plane DuckDB and on-disk run artifacts.

## Checklist

### Route

- [ ] Add a router file under `src/ai_trading_system/ui/execution_api/routes/<area>.py` (or extend an existing one).
- [ ] Use an explicit `prefix` like `/api/<area>` on the `APIRouter`.
- [ ] Add `dependencies=[Depends(require_api_key)]` (or whatever the existing convention is) for `/api/*` routes.
- [ ] Register the router in `app.py::create_app()` (`app.include_router(...)`).

### Schemas

- [ ] Add Pydantic request/response models under `src/ai_trading_system/ui/execution_api/schemas/`.
- [ ] Reuse existing schemas where possible to keep the surface small.

### Service

- [ ] Implement the route's logic in `services/<area>.py`, not inline in the route handler.
- [ ] If the service needs a database read, put it in `services/readmodels/<name>.py`.

### Auth

- [ ] Verify the route requires `x-api-key` header (the `EXECUTION_API_KEY` env var server-side).

### Tests

- [ ] Route test: status code + response schema in `tests/integration/api/`.
- [ ] Service test: unit-level in `tests/integration/services/`.
- [ ] If a readmodel is added, test it against a fixture DuckDB.

### Docs

- [ ] Add the endpoint to `docs/reference/api_reference.md` under the correct router section (method, path, purpose, request schema, response schema).
- [ ] If the endpoint surfaces new data, link from `docs/architecture/ui_architecture.md` or relevant domain doc.

## See also

- [`docs/reference/api_reference.md`](../reference/api_reference.md)
- [`docs/architecture/ui_architecture.md`](../architecture/ui_architecture.md)
- [`docs/domains/ui_domain.md`](../domains/ui_domain.md)
