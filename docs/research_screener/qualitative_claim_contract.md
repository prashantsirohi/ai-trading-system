# Qualitative claim and low-cost agent contract

- **Purpose:** Define the deterministic boundary for converting annual-report discovery anchors into reviewable claims.
- **Audience:** Operator, research-data engineer, and investment-research reviewer.
- **Last verified:** 2026-08-14
- **Source of truth:** `configs/research_screener/qualitative_claim_contract.json`, its schemas, and `domains/research_screener/claim_contract.py`.

## Current scope

This milestone defines and tests the claim, independent-review, persistence,
cost-control, and escalation contracts. It does not submit model requests or
promote any of the existing 10,417 discovery anchors. A later runner must obey
this contract before it can write claim rows.

`NOT_DISCLOSED` and `SOURCE_UNAVAILABLE` remain evidence states. They are not
agent claims and cannot acquire invented excerpts, pages, or values. A claim
must retain the annual-report research run, document, company, security, ISIN,
source artifact, SHA-256, publication timestamp, fiscal period, statement
scope, exact excerpt, and page.

Company-type routing is deterministic. Industrial metrics cannot be applied to
banks or financial institutions, and the agent cannot override the KPI route
in `kpi_contracts.json`. Publication after the screen cutoff, missing
provenance, invalid scope, invalid units, or an unapproved model/prompt version
rejects the proposal before verification.

## Decision boundary

The extractor and verifier are separate structured-output requests. The
verifier receives independently assembled source context and must reproduce the
same normalized claim hash. Ordinary LOW/MEDIUM materiality factual claims may
reach `AGENT_VERIFIED` only when both reviews accept and every deterministic
check passes.

The following outcomes never auto-accept:

- guidance, targets, governance, ownership, and HIGH-materiality claims;
- conflicting source evidence, agent disagreement, or ambiguity;
- invalid provenance, cutoff, company-type, metric, schema, model, or prompt;
- a verifier that did not receive independently assembled context.

High-impact valid claims route to `HUMAN_REVIEW_REQUIRED`. Deterministically
invalid claims route to `AGENT_REJECTED`; source conflicts remain
`CONFLICT_DETECTED`. Human approval is a distinct `HUMAN_VERIFIED` state and is
not inferred from agent agreement.

## Low-cost operating policy

The repository pins `gpt-4o-mini-2024-07-18` and the OpenAI Batch API with a
24-hour completion window. Model escalation is disabled: exceptions go to a
human instead of a larger model. Prompts use only the cited page plus at most
one adjacent page on each side, capped at 18,000 characters; full-document
prompting is forbidden.

Before model submission, the future runner must deduplicate anchors and group
them into at most six topic packets per company. Each packet may return at most
five claims. Deterministically invalid extraction output is not sent to the
verifier. Each request has one retry, and token/request/batch usage is retained
with the review.

Rollout begins with 25 companies and budgets 1.5 million input tokens and
400,000 output tokens. New requests stop at 90% of either budget so in-flight
responses cannot silently exceed the calibration envelope. These are hard
request controls, not estimates of analytical completeness. The operator must
review observed acceptance, disagreement, false-positive, token, and cost rates
before widening the cohort.

No reasoning trace is stored. The durable audit record consists of structured
claims, exact source evidence, decisions, issue codes, model/prompt versions,
normalized hashes, and usage metadata.

## Persistence

Migration `008_qualitative_claim_contract.sql` adds three isolated tables to
`$DATA_ROOT/research_screener/control_plane.duckdb`:

- `qualitative_claim` stores the proposed claim and full structured payload;
- `qualitative_claim_review` stores extraction/verification decisions and usage;
- `qualitative_claim_policy_decision` stores the deterministic terminal or
  review-required status and reason codes.

The tables have no daily-pipeline, ranking, candidate, scheduling, publishing,
portfolio, execution, or broker consumer.
