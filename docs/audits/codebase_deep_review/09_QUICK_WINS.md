# Quick wins

- **Purpose:** List small, independently verifiable improvements that reduce immediate risk.
- **Audience:** Maintainers selecting the first remediation patches.
- **Last verified:** 2026-07-13
- **Source of truth:** Confirmed findings and verification gaps recorded by this audit.

---

These changes are small, independently reviewable, and should normally fit within one engineer-day each. They do not replace the structural remediation roadmap.

| Quick win | Benefit | Verification |
|---|---|---|
| Add the Telegram integration as an explicit optional project extra and install it in the full-test CI lane | restores clean pytest collection | build/install wheel in empty env; collect tests |
| Fix confirmed undefined names in AI analyzer, OAuth flow, Telegram metrics, and portfolio analyzer | removes reachable runtime failures | focused unit tests plus Ruff |
| Remove refresh-token prefix printing | prevents secret fragments in terminal/logs | capture output test contains no token data |
| Add a permission check/warning for `.env`, OAuth client, and token files | reduces local credential exposure | mode 0644 fails/warns; 0600 passes |
| Parameterize values in `analytics/feature_reader.py` | closes a concrete SQL policy violation | malicious/special-character values remain data |
| Bind API to loopback by default | reduces accidental LAN exposure | config/startup test |
| Replace wildcard CORS with configured explicit origins | removes unsafe browser trust default | middleware integration test |
| Add a historical-rank future-row regression test | makes AUD-001 permanently visible | old-date output unchanged after future append |
| Filter consumable artifacts to promoted, DQ-passed attempts | reduces failed-artifact exposure while full lifecycle is built | failed/retry integration test |
| Add a unique index/constraint for the durable execution idempotency key | creates a database guardrail | duplicate insert race test; migration on copied DB |
| Recompute projected heat after each accepted candidate | closes sequential batch undercount in the current process | two-order limit test |
| Stop deactivation only on confirmed filled quantity | reduces OPEN/partial-order stop loss | state table tests |
| Write JSON artifacts to temp siblings and atomically replace | prevents readers observing truncated files | failure-injection test |
| Add pagination maximums to history/list endpoints | caps memory and response sizes | boundary/API tests |
| Lazy-load large React routes | lowers initial operator bundle | bundle report and route smoke |
| Make Ruff “no new violations” a PR gate | prevents further quality debt without mass churn | baseline diff job |
| Add a clean-package smoke job | detects src-layout/root-import and missing dependency drift | wheel-only import/CLI help |
| Mark stale `_audit/current_code_truth_map.md` historical | prevents incorrect operational use | docs link/status check |

## Recommended first three patches

1. Test/dependency reproducibility plus the four undefined-name fixes.
2. Historical rank regression test followed by the cutoff-aware loader fix.
3. Failed-artifact promotion test followed by promoted-only resolution.

Those patches maximize immediate confidence and reduce the chance that later refactors preserve incorrect behavior.
