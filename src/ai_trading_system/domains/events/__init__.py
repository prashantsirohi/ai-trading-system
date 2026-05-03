"""Corporate-actions enrichment domain.

Joins trading-system signals (volume shocks, bulk deals, Tier A/B breakouts)
with corporate-action events sourced from the vendored ``market_intel``
package. Outputs enriched payloads consumed by the publish stage.
"""
