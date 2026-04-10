# Pending TODO

- Dhan auth follow-up:
  - investigate why `RenewToken` returns `DH-905` even when the access token is valid
  - keep `generateAccessToken` via `PIN + TOTP` as the current recovery path
  - add a pre-run auth health check to the operator workflow so token problems are caught before ingest
