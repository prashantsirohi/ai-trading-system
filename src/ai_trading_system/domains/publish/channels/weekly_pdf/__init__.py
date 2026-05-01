"""Weekly PDF market report channel.

Phase 1: skeleton PDF assembled from existing rank artifacts
(ranked_signals, breakout_scan, pattern_scan, sector_dashboard,
dashboard_payload, rank_summary). No week-over-week diff or charts yet.
"""

from ai_trading_system.domains.publish.channels.weekly_pdf.channel import (
    publish_weekly_pdf,
)

__all__ = ["publish_weekly_pdf"]
