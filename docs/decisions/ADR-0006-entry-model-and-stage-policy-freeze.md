# ADR-0006: Entry Model and Stage Policy Freeze (D1–D7)

- **Purpose:** Freeze the v1 opportunity design against its trading objective — Weinstein stage analysis plus pattern evidence to find strong candidates among (a) high-RS / momentum leaders and (b) stocks emerging from Stage 1 / base consolidation — and bind the five implementation amendments (A1–A5) that operator review required.
- **Audience:** Operator (decision owner), developers, future agents.
- **Last verified:** 2026-07-15
- **Source of truth:** `src/ai_trading_system/domains/opportunities/orchestration/` (`admission.py`, `matching.py`, `transitions.py`, `retention.py`, `contracts.py`, `service.py`), `src/ai_trading_system/domains/opportunities/coverage.py`, `src/ai_trading_system/domains/opportunities/registry/models.py`, and the [opportunity lifecycle contracts](../architecture/opportunity_lifecycle_contracts.md).
- **Status:** Proposed — operator decisions D1–D7 recorded 2026-07-15 and frozen as design intent. The ADR moves to **Accepted** only when amendments A1–A5 are implemented and verified in code. Until then, the current behavior documented below remains in force and is known-deficient.

---

## Sign-off matrix

| # | Decision | Frozen choice | Condition |
|---|---|---|---|
| D1 | Entry model | **A — breakout-only** | Wording amendments in D1; v2 metrics pre-registered |
| D2 | Momentum → breakout handoff | **B — close momentum episode, open breakout episode** | Amendment A1 (first-class episode relation, atomic) |
| D3 | Sector gate on provisional S1→S2 entries | **A — hard gate, amended semantics** | Amendment A2 (completed-week locked sector snapshot) |
| D4 | Threshold freeze | **Freeze v1 priors** | Amendment A3 (policy snapshot fingerprint + enforcement) |
| D5 | Admission precedence | **A — fixed precedence** | Amendment A4 (evaluate-all + structured persistence) |
| D6 | Strategy side | **Long-only** | Contract declaration; no amendment |
| D7 | Retention | **Family-neutral, current limits** | Amendment A5 (trading-session counters) |

Verified code facts referenced throughout were confirmed against the working tree on 2026-07-15; file references are to that state.

---

## D1 — Entry model: breakout-only (FROZEN: Option A)

### Current logic

A lifecycle `TRIGGERED` transition requires a qualified, non-failed **breakout event** — nothing else counts ([transitions.py:54-56](../../src/ai_trading_system/domains/opportunities/orchestration/transitions.py); enforcement at line 163: `"missing legitimate breakout trigger"`). Momentum leaders admitted via `RANK_THRESHOLD` (percentile ≥ 90) or `RANK_VELOCITY` (≥ 5-position improvement at percentile ≥ 75) enter monitoring states and, absent a breakout, close under retention review.

### Options considered

- **A — breakout-only (chosen):** one trigger archetype; consistent pivot/follow-through attribution; clean false-positive and expectancy measurement; no pullback-stop semantics needed before the breakout model is validated; zero code change. Cost: no continuation entries in leaders; one entry per base.
- **B — add pullback-continuation trigger:** serves objective (a) directly and the vocabulary anticipates it, but requires new adapter evidence (MA-distance, pullback-volume character), different stop/invalidation semantics, family-aware retention (D7), all on an unvalidated shadow stack. Deferred, not rejected.

### Frozen semantics (binding wording)

- **`MOMENTUM_LEADER` is a watch-source family, not an entry setup in `lifecycle-policy-v1.1`.** Rank admission creates monitoring eligibility only and can never directly create an `ENTER` or `ADD` action.
- **`PULLBACK_REENTRY` is reserved in the contract but unsupported by `admission-rules-v1` and `lifecycle-policy-v1.1`.** Its presence in the `SetupFamily` enum ([contracts.py:58](../../src/ai_trading_system/domains/opportunities/orchestration/contracts.py)) must not be read as active functionality by agents, UI, or documentation.

### Pre-registered v2 metrics

Before any pullback-entry policy is proposed, the shadow/calibration history must answer:

1. Percentage of momentum leaders that later produce a qualified breakout.
2. Median time from leader admission to that breakout.
3. Percentage that advance materially without ever re-basing ("runaway" leaders).
4. Performance of runaway leaders versus later-breakout leaders.
5. Count and quality of 10-week/30-week pullback opportunities in monitored leaders.

These determine whether Option B is economically worth its policy complexity. No pullback policy may be adopted without them.

---

## D2 — Momentum leader progressing to breakout (FROZEN: Option B, via Amendment A1)

### Current logic (defective)

The progression ladder in [matching.py](../../src/ai_trading_system/domains/opportunities/orchestration/matching.py) is `early_accumulation → base_building → stage_1_to_2_transition → breakout → post_breakout_followthrough` (30-day continuity). `MOMENTUM_LEADER` is absent. A symbol with an open `momentum_leader` episode that prints a qualified breakout produces `CONFLICT` from `match_open_episode` — the most-wanted stock (monitored leader that bases and breaks out) manifests as a governance conflict.

### Options considered

- **A — insert `momentum_leader` into the progression ladder:** one-line change, but the immutable episode family would read `momentum_leader` for what is economically a breakout trade, polluting per-family attribution that D1's calibration plan depends on. Rejected.
- **B — auto-close the momentum episode, open a new `breakout` episode (chosen):** family-pure episodes; honest attribution; matches the registry's re-entry philosophy. Cost: one economic narrative spans two episodes — mitigated by a first-class relation (A1).
- **C — leave as conflict:** conflicts stop meaning "something is wrong." Rejected.

### Correction to the original proposal

The earlier draft proposed recording the predecessor in "opening metadata." **`OpenEpisodeRequest` has no metadata or predecessor field** ([models.py:109-118](../../src/ai_trading_system/domains/opportunities/registry/models.py)) — that proposal was not implementable. The binding design is Amendment A1. The existing generic `ClosureReason.SUPERSEDED_BY_NEW_EPISODE` ([contracts.py:72](../../src/ai_trading_system/domains/opportunities/orchestration/contracts.py)) is reused; no new closure reason is added.

---

## D3 — Sector gate on provisional S1→S2 entries (FROZEN: hard gate with amended temporal semantics, via Amendment A2)

### Current logic (defective — structurally unreachable, not merely strict)

`_trigger_blockers` requires the sector snapshot to be known, `LOCKED`, and locked in Stage 2 ([transitions.py:310-314](../../src/ai_trading_system/domains/opportunities/orchestration/transitions.py)). But sector aggregation in [coverage.py](../../src/ai_trading_system/domains/opportunities/coverage.py) sets `stage_status = "provisional"` when **any** constituent observation is provisional and `locked_stage = UNKNOWN` unless **all** constituents are locked. During an incomplete trading week — the only period in which a stock carries a provisional `transition_1_to_2` — the same-week sector aggregation is ordinarily provisional with `locked_stage = UNKNOWN`, so the early path fails the locked-sector requirement **by construction**, not by selectivity. Verified 2026-07-15.

### Options considered

- **A — hard gate (chosen, with amended semantics):** maximally fail-closed on the riskiest entry type; sector confirmation encodes a real prior; clean attribution. Cost: leaders emerge before sectors confirm, so the gate forfeits the earliest entries — accepted for v1 and pre-registered for calibration (below).
- **B — size-haircut tiering for sector-not-yet-Stage-2:** preserves earliest entries at reduced size but adds uncalibratable states today. Deferred to the calibration question.

### Frozen rule (v1, binding — Amendment A2 implements)

A provisional stock S1→S2 trigger evaluates the sector using the **latest completed-week locked sector snapshot**, never the current incomplete-week provisional aggregation:

| Prior completed-week locked sector state | v1 outcome |
|---|---|
| Locked Stage 2 | Pass |
| Locked Stage 1 with current-week provisional transition / improving breadth | **Block in v1, record cohort for calibration** |
| Locked Stage 3 or Stage 4 | Hard block |
| Missing or untrusted membership | Hard data-quality block |
| No valid completed-week sector snapshot | Explicit `sector_locked_snapshot_missing` block |

Current-week sector data is retained as monitoring evidence only: `sector_locked_stage_prior_completed_week`, `sector_provisional_stage_current_week`, `sector_stage_velocity_current_week`.

### Sector blocker taxonomy (binding)

Blocked early entries must be distinguishable by cause; these cohorts have different remediation and different calibration meaning:

- `missing_sector_mapping`
- `latest_only_untrusted_membership`
- `insufficient_constituent_coverage`
- `sector_not_stage_2`
- `sector_snapshot_not_locked` / `sector_locked_snapshot_missing`

### Pre-registered calibration question

First question the shadow campaign must answer: *how many provisional S1→S2 candidates with strong stock-level evidence were blocked solely by the sector gate (by taxonomy cohort), and what were their forward outcomes?* If the blocked Stage-2-pending cohort outperforms, Option B's haircuts get calibrated and adopted as `lifecycle-policy-v2`.

The sector-mapping coverage gap (77 unmatched symbols in the first real session) is design-critical: under the hard gate, unmapped sectors suppress the early path silently. Fixing mapping coverage precedes any conclusion from the calibration question.

---

## D4 — Threshold freeze and no-hand-tuning (FROZEN, via Amendment A3)

### Current logic (defective — version label does not bind content)

Version labels are constants (`admission-rules-v1`, `lifecycle-policy-v1`, `opportunity-retention-v1`), but seven admission thresholds are runtime-suppliable through `OpportunityShadowConfig.from_mapping` pipeline params (`opportunity_rank_admission_percentile`, `opportunity_rank_velocity_floor`, `opportunity_rank_velocity_percentile_floor`, `opportunity_investigator_admission_score`, `opportunity_accumulation_admission_score`, `opportunity_pattern_admission_score`, `opportunity_breakout_admission_score` — [contracts.py:299-305](../../src/ai_trading_system/domains/opportunities/orchestration/contracts.py)). Admission identity hashes the literal text `"admission-rules-v1"`, **not the threshold values used**. It is therefore currently possible to run percentile 90, change to 85, and keep producing history labelled `admission-rules-v1` — non-comparable samples under one version. This violates D4 as originally drafted; the freeze is unenforceable without Amendment A3.

### Frozen rules

1. Every numeric policy value is a **v1 prior**. Changes require a versioned policy bump (`admission-rules-v2`, …) backed by Phase 3C-5 calibration evidence or an explicit ADR.
2. No hand-tuning between runs. Five sessions of NSE data cannot distinguish a good threshold from a lucky one.
3. The sector breadth cutoffs (50/60/35/40) and stage-confidence weights are flagged as the least-grounded numbers in the system — designed, not fitted.
4. **Enforcement is technical, not procedural** (Amendment A3): a human-readable policy version may never be registered with two different canonical content hashes.

D4 is classified as **architecture plus enforcement**, not operator commitment.

---

## D5 — Admission precedence (FROZEN: Option A, via Amendment A4)

### Current logic

`evaluate_admission` ([admission.py](../../src/ai_trading_system/domains/opportunities/orchestration/admission.py)) short-circuits a fixed elif chain — qualified breakout → high-confidence S1→S2 → early accumulation → Investigator promotion → qualified pattern → rank velocity → rank threshold — assigning exactly one reason/family. Stage 3/4 (including 2→3, 3→4) blocks all admission first.

### Options considered

- **A — fixed precedence (chosen):** deterministic and replayable; the most actionable evidence defines the episode; correct for a breakout-entry system. Cost: family statistics undercount momentum origins because a stock qualifying on both paths is always classified by the base path.
- **B — multi-label admission identity:** unbiased origin statistics but changes admission-identity semantics and every downstream idempotency key. Rejected as a breaking registry change for a statistics improvement.

### Correction to the original proposal

The earlier draft suggested "appending other satisfied rules to `supporting`." **Not implementable as stated**: the elif chain never evaluates lower-precedence predicates once one passes, and the persisted admission record carries only candidate, reason, family, and rule version. The binding design is Amendment A4: evaluate all rules, choose primary by precedence, persist structured evaluations. Episode identity remains based on the primary reason/family and additionally binds the A3 policy snapshot ID.

---

## D6 — Long-only scope (FROZEN — declaration)

The system is **long-only through v1 and the entire calibration horizon**: `strategy_side = LONG_ONLY` at contract level. Stage 3/4 detection exists solely for exclusion, weakening, exit acceleration, and sector risk-off context — never short-side candidate generation. Enforced semantics:

- No negative position intent may originate from the opportunity lifecycle.
- No `SHORT` / `SELL_SHORT` candidate action exists in the taxonomy.
- Any future short-side work requires a new ADR, new setup families, downside follow-through and attribution models, and its own calibration track.

Consistent with current code: Stage 3/2→3/3→4/4 block new long admission; no short-side family, trigger, or attribution exists. No amendment required beyond the contract-doc declaration.

---

## D7 — Retention (FROZEN: family-neutral current limits, via Amendment A5)

### Current logic (defective — inconsistent time units)

Retention limits are state-specific, including 10 no-progress units for confirmed/advancing states. But the two age measures use different units ([service.py:535-543](../../src/ai_trading_system/domains/opportunities/orchestration/service.py)):

- `days_without_progress` increments by one on **every non-improving orchestration observation** — two non-identical runs on the same trading date add two "days"; reruns and extra shadow sessions accelerate closure.
- `days_in_state` is computed from **elapsed calendar days** since the last transition.

### Frozen decision

Family-neutral retention with current v1 limits is correct for a breakout-only system: stalled momentum-leader episodes are supposed to close — they are watchlist entries whose job is to be replaced by fresher leaders. Amendment A5 fixes the counting unit before the limits are meaningful.

If D1 ever moves to pullback entries (v2), family-aware retention (structural-violation-based closure for `momentum_leader`/`pullback_reentry` — close below rising 30-week MA, RS breakdown — instead of progress-day counting) **must ship in the same policy version**; the two changes are inseparable.

---

## Binding amendments

The amendments below are the acceptance criteria for this ADR: it moves from Proposed to Accepted when all five are implemented and verified. Each current behavior above remains in force — and known-deficient — until its amendment lands.

### A1 — First-class episode relation (implements D2)

New registry relation, append-only, control-plane:

```text
candidate_episode_relation
  relation_id                 (deterministic digest of the fields below)
  predecessor_candidate_id
  successor_candidate_id
  relation_type               -- v1: MOMENTUM_SUPERSEDED_BY_BREAKOUT
  related_at
  rule_version
  run_id
  source_artifact_hash
```

Closure of the predecessor uses the existing `ClosureReason.SUPERSEDED_BY_NEW_EPISODE`.

**Transactional invariant** — one registry transaction must:

1. verify exactly one compatible momentum episode is open for the symbol;
2. close it as superseded;
3. open the breakout episode;
4. write the predecessor–successor relation;
5. append the breakout observation and snapshot.

A crash must never leave both episodes open, or the old episode closed without its successor and relation.

**Required tests:**

- momentum leader + qualified breakout produces no registry conflict;
- old episode closed, new breakout episode open;
- relation queryable in both directions;
- replay is idempotent (no second relation, no second episode);
- injected failure rolls back close, open, and relation together;
- multiple open same-symbol episodes still produce a real conflict.

### A2 — Completed-week locked-sector semantics (implements D3)

> **Implementation status (2026-07-16): implemented.** Governed locked-only
> sector resolution, the binding blocker taxonomy, calibration-cohort tagging,
> shadow artifact/summary fields, `lifecycle-policy-v1.1` fingerprinting, and
> nullable decision-context evidence columns in migration 038 are implemented
> with resolver, reachability, rule-matrix, persistence, and replay tests.
> `lifecycle-policy-v2` remains reserved for the pre-registered future
> calibrated size-haircut policy. Migration 038 was applied to the operator
> store on 2026-07-16. Pending: the shadow campaign itself.

- The early-trigger sector check consumes the **latest completed-week locked sector snapshot**; the current incomplete-week provisional aggregation is never an input to the gate.
- Implement the v1 rule table and blocker taxonomy in D3 verbatim, including the `sector_locked_snapshot_missing` explicit state.
- Persist the three monitoring fields (`sector_locked_stage_prior_completed_week`, `sector_provisional_stage_current_week`, `sector_stage_velocity_current_week`) on decision contexts and shadow artifacts so the pre-registered calibration cohort is measurable from day one.
- Blocked-cohort recording must tag the taxonomy cause; an aggregate "sector blocked" counter is insufficient.

### A3 — Immutable policy snapshot and version/content enforcement (implements D4)

> **Implementation status (2026-07-16): implemented.** Migration 037,
> `domains/opportunities/policy_snapshot.py`, per-label runtime fingerprints
> over single-sourced constants, stage-start register-or-verify in all three
> Phase 3 stages, dedicated nullable stamp columns (episode open/close,
> transition, decision context), run-metadata audit events, and the approved
> stage-failure interpretation are in code with tests. Migration 037 was
> applied to the operator store on 2026-07-16. Pending: the deferred
> calibration-sample stamping that follows the Phase 3C-5 loader fix.

Canonical fingerprint:

```text
policy_snapshot_id = SHA256(canonical_json({
  admission_version, lifecycle_version, retention_version,
  setup_family_version, stage_classifier_version,
  confidence_formula_version, sector_aggregation_version,
  <all semantic numeric thresholds>,
  <all allowlists and tier rules>
}))
```

Persisted in: pipeline run metadata, candidate admission records, transition observations, closure/retention decisions, calibration samples, and Phase 4 read models.

**Registry rule:** one human-readable policy version may not be registered with a different canonical policy hash. On mismatch, startup fails:

```text
POLICY_VERSION_CONTENT_MISMATCH:
admission-rules-v1 already registered with another policy snapshot.
Create admission-rules-v2.
```

Runtime threshold overrides remain possible mechanically but can no longer masquerade as the same policy version — any changed value produces a different snapshot ID and is rejected under the old label.

### A4 — Evaluate-all admission with structured persistence (implements D5)

Two-step admission:

```text
satisfied_rules = evaluate_all_admission_rules(bundle, config)
primary_rule    = choose_by_precedence(satisfied_rules)
```

Persist machine-readable fields:

```text
primary_admission_reason
primary_setup_family
satisfied_admission_rules[]
rule_evaluations[ { rule, passed, observed_value, threshold, source_observation_ids } ]
```

Prose strings ("qualified breakout", "top rank percentile") may remain for operator display but are not the record of evaluation — calibration queries run on the structured fields. Episode identity stays bound to the primary reason/family plus the A3 `policy_snapshot_id`.

### A5 — Trading-session retention counters (implements D7)

One unit — **trading sessions** — for all retention ages. Increment only when:

```text
current_as_of_trading_date > last_counted_progress_date
```

Persist: `sessions_in_state`, `sessions_without_progress`, `last_progress_at`, `last_retention_counted_session`. If the existing field names are retained for compatibility, their semantics are explicitly redefined as trading sessions under a versioned retention policy bump (`opportunity-retention-v2` or a versioned semantics note within the A3 snapshot). Same-date reruns and multiple shadow sessions per date must not advance retention age.

---

## Consequences

- v1 is frozen as: **breakout-only, long-only, family-pure, strict on provisional entries, reproducible, calibration-ready.** Momentum admission is watchlist-semantics; pullback entries and sector-gate haircuts are deferred behind pre-registered measurements, not discarded.
- Implementation scope created by this ADR: A1 (registry relation + orchestration transaction), A2 (sector-gate temporal semantics + taxonomy), A3 (policy fingerprint + enforcement), A4 (admission refactor + persistence), A5 (session counters). A2 and A3 gate the usefulness of the shadow campaign and should land first; the early-entry path is currently unreachable (A2) and history produced without content-bound policy versions is not safely comparable (A3).
- The sector-mapping coverage gap (77 unmatched symbols) is elevated to design-critical: under the frozen hard gate it silently disables the early-entry path.
- Documentation propagation on acceptance: the contracts doc gains the `MOMENTUM_LEADER` watch-source and `PULLBACK_REENTRY` reserved-family declarations, the `LONG_ONLY` scope declaration, and the completed-week sector-gate semantics; the shadow-orchestration doc gains the A1 supersession flow.
