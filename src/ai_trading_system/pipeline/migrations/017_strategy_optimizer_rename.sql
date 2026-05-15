-- Migration 017: benchmark-agnostic columns on strategy_iteration_result
--
-- Migration 015 hardcoded nifty_return_pct. With UNIV_TOP1000 (or any user-
-- chosen benchmark) replacing NIFTY as the default, the column name is
-- misleading. DuckDB's ALTER RENAME is brittle across versions, so we add
-- the new columns and backfill from the legacy column for historical rows.
-- nifty_return_pct stays NULL going forward; a later migration can DROP it.

ALTER TABLE strategy_iteration_result
    ADD COLUMN IF NOT EXISTS benchmark_return_pct DOUBLE;
ALTER TABLE strategy_iteration_result
    ADD COLUMN IF NOT EXISTS benchmark_symbol TEXT;

-- One-time idempotent backfill so historical rows have benchmark_return_pct
-- populated from the legacy column. New rows will write the new columns only.
UPDATE strategy_iteration_result
   SET benchmark_return_pct = nifty_return_pct
 WHERE benchmark_return_pct IS NULL
   AND nifty_return_pct IS NOT NULL;
