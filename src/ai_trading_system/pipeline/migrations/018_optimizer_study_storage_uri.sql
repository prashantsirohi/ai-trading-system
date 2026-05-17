-- Migration 018: persistent Optuna study storage for resumability
--
-- Wave 5a of the optimizer convenience plan: every fresh study now writes
-- its trial state to a per-run Optuna JournalStorage file on disk. Storing
-- the file's URI on the run row makes ``ai-trading-optimize resume <run_id>``
-- a self-contained lookup.
--
-- URI shape: a relative file path under ``data/optuna/`` (e.g.
-- ``data/optuna/<run_id>.log``). The runner resolves it against the
-- project_root the resume CLI was invoked with.

ALTER TABLE strategy_optimization_run
    ADD COLUMN IF NOT EXISTS study_storage_uri TEXT;
