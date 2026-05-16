# Adding a New Publisher Channel

- **Purpose:** Add a new delivery channel (e.g. Discord, S3 upload, etc.) to the publish stage.
- **Audience:** Developer.
- **Last verified:** 2026-05-16
- **Source of truth:** `src/ai_trading_system/domains/publish/delivery_manager.py`, `domains/publish/channels/`.

---

## Checklist

### Channel implementation

- [ ] Add `src/ai_trading_system/domains/publish/channels/<channel>.py` exposing a class or function that takes a payload and dispatches it.
- [ ] Decide the **role**:
  - `publish_of_record` (blocking)
  - `publish_auxiliary` (blocking)
  - `publish_optional` (non-blocking)
  - `informational` (blocking)
  - `diagnostic` (non-blocking)
- [ ] Register the channel in `pipeline/stages/publish.py::PublishStage.CHANNEL_ROLES`.
- [ ] Implement idempotent send semantics — the channel should be safe to retry.

### Dedupe key

- [ ] The delivery manager uses `run_id + channel + artifact_hash`. Make sure the channel's payload hashes deterministically.

### Auth

- [ ] Document any new env vars in `docs/reference/environment_variables.md`.
- [ ] Read env vars via `platform/config/` patterns, not scattered `os.getenv(...)`.

### Retry / failure

- [ ] For blocking roles, raise on failure; the delivery manager will retry per its policy.
- [ ] For non-blocking roles, catch and log; return a status that delivery manager records.

### Tests

- [ ] Unit test the channel in `tests/publish/`.
- [ ] Integration test that the channel is dispatched by `delivery_manager` and records a row in `publisher_delivery_log`.
- [ ] Dedupe test: same payload twice → second is a no-op.

### Docs

- [ ] Add a row to `docs/reference/publish_contracts.md` (channel, module, input artifact, destination, role, dedupe key, retry behavior, failure behavior).
- [ ] Update `docs/stages/publish.md` if the channel changes stage behavior.
- [ ] Add the channel to `docs/domains/publishing_domain.md`.

## See also

- [`docs/stages/publish.md`](../stages/publish.md)
- [`docs/reference/publish_contracts.md`](../reference/publish_contracts.md)
