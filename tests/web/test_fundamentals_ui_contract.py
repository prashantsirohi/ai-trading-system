from __future__ import annotations

from pathlib import Path


FRONTEND_ROOT = Path("web/execution-console-v2/ai-trading-dashboard-starter/src")


def test_frontend_mapper_carries_fundamental_fields() -> None:
    mapper = (FRONTEND_ROOT / "lib/api/mappers.ts").read_text(encoding="utf-8")
    expected = [
        "fundamentalScore: optionalNumber(row.fundamental_score)",
        "fundamentalTier: normalizeFundamentalTier(row.fundamental_tier)",
        "qualityScore: optionalNumber(row.quality_score)",
        "growthScore: optionalNumber(row.growth_score)",
        "balanceSheetScore: optionalNumber(row.balance_sheet_score)",
        "valuationScore: optionalNumber(row.valuation_score)",
        "ownershipScore: optionalNumber(row.ownership_score)",
        "redFlags: optionalText(row.red_flags)",
        "watchlistBucket: optionalText(row.watchlist_bucket)",
        "nextAction: optionalText(row.next_action)",
    ]
    for snippet in expected:
        assert snippet in mapper


def test_frontend_tables_include_fundamental_columns_and_tier_colors() -> None:
    ranking = (FRONTEND_ROOT / "components/tables/RankingTable.tsx").read_text(encoding="utf-8")
    watchlist = (FRONTEND_ROOT / "components/watchlist/WatchlistTable.tsx").read_text(encoding="utf-8")
    combined = f"{ranking}\n{watchlist}"

    for header in ["'Fund'", "'Q'", "'G'", "'BS'", "'Val'", "'Own'", "'Flags'", "'Bucket'", "'Action'"]:
        assert header in combined
    assert "tier === 'A'" in combined and "emerald" in combined
    assert "tier === 'B'" in combined and "blue" in combined
    assert "tier === 'C'" in combined and "amber" in combined
    assert "tier === 'Reject'" in combined and "rose" in combined
