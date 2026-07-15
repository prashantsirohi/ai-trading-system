# Phase 3C-5 Calibration and Phase 4 Readiness

- **Purpose:** Build immutable calibration eligibility and Phase 4 readiness evidence safely.
- **Audience:** Operators and developers.
- **Last verified:** 2026-07-15
- **Source of truth:** `domains/opportunities/calibration.py` and `interfaces/cli/build_phase3c5_calibration.py`.
- **Policy:** `phase3c5-calibration-policy-v1`

## Purpose and boundary

Phase 3C-5 creates reproducible, offline evidence for whether historical
opportunity decisions are safe to use in a future calibration exercise. It
does not choose thresholds, change strategy behavior, alter scoring or routing,
write an operator database, apply migrations, or implement Phase 4 surfaces.

Policy is `phase3c5-calibration-policy-v1`. Builder and readiness policy
versions are recorded in every manifest and readiness report.

## Eligibility contract

The builder assigns every input row one canonical status:

- `ELIGIBLE`: complete, point-in-time, authoritative evidence.
- `PENDING_OUTCOME`: the forward window is not mature; never treated as zero.
- `EXCLUDED`: known evidence is unsuitable for the declared dataset purpose.
- `QUARANTINED`: governance, attribution, identity, or integrity evidence needs
  review before it can become authoritative.

Eligibility fails closed when an input was available after `decision_at`, a
later correction leaks backward, stage authority conflicts or cycles, sector
membership is latest-only, correction impact is unresolved, entry history was
reconstructed only from a position, the price path or corporate-action
attribution is incomplete, historical universe/listing evidence is absent, or
the sample identity is invalid. Reason codes are preserved on every non-eligible
row.

Forward horizons are trading-session horizons. Pending, right-censored,
missing-price-path, and corporate-action-unresolved outcomes are separate
states. Delisted losers, failed candidates, renamed symbols with stable
identity, and repeated candidate episodes must remain representable; a
winner-only or current-universe-only population is a critical failure.

## Build a deterministic fixture

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.build_phase3c5_calibration \
  --profile small_fixture \
  --as-of 2026-07-15T23:59:59+00:00 \
  --output-root /tmp/phase3c5-small
```

The fixture is QA evidence, not a production-readiness sample. Run the same
command into another empty directory and compare `manifest_id`,
`eligible_dataset_hash`, and `phase3c5_calibration_replay_comparison.json`.

`--fail-on-not-ready` returns exit code 1 only for `NOT_READY`; without it, the
command still emits complete diagnostic artifacts and exits zero.

## Copied-realistic build

First create a temporary copy using the repository backup procedure and verify
that the copy is outside `$DATA_ROOT`. Then run:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.build_phase3c5_calibration \
  --profile copied_realistic \
  --copied-control-plane /tmp/operator-copy/control_plane.duckdb \
  --as-of 2026-07-15T23:59:59+00:00 \
  --output-root /tmp/phase3c5-copied
```

The command opens the source read-only. It rejects the configured operator
store and symlinked source/output paths. Sparse or pre-Phase-3B copies produce
honest exclusions and limitations; do not fabricate missing history.

## Readiness verdicts

Re-evaluate an existing manifest with:

```bash
PYTHONPATH=src ./.venv/bin/python -m ai_trading_system.interfaces.cli.check_phase4_readiness \
  --calibration-manifest /tmp/phase3c5-small/phase3c5_calibration_manifest.json \
  --output-root /tmp/phase3c5-readiness
```

- `READY`: all critical checks pass and production evidence is present.
- `READY_WITH_LIMITATIONS`: read-only development can proceed, but named
  evidence gaps block production claims.
- `NOT_READY`: at least one critical integrity, leakage, governance,
  survivorship, outcome, or sample-coverage check fails.

Development readiness and production readiness are separate booleans. Expected
limitation IDs include `COPIED_REALISTIC_BASELINE_MISSING`,
`OPERATOR_MIGRATIONS_NOT_APPLIED`, and `REAL_PHASE3B_HISTORY_EMPTY`. A fixture
may support development-path verification while production readiness remains
false.

## Operator review

Review all ten artifacts listed in [Artifacts](../reference/artifacts.md), with
particular attention to exclusion-reason counts, quarantined samples, class and
regime coverage, manifest source hashes, replay equivalence, and stable
limitation IDs. Treat any same-manifest/different-dataset result as an integrity
incident. Do not promote the eligible CSV into runtime behavior during this
phase.
