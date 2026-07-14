"""Pure adapters from current pipeline artifacts to canonical opportunity contracts."""

from .breakout import adapt_breakout_rows
from .investigator import adapt_investigator_rows
from .lifecycle import LifecycleEvidence, adapt_lifecycle_rows
from .pattern import adapt_pattern_rows
from .ranking import adapt_ranking_rows
from .sector_stage import adapt_sector_stage_rows
from .stock_stage import adapt_stock_stage_rows

__all__ = [
    "LifecycleEvidence", "adapt_breakout_rows", "adapt_investigator_rows", "adapt_lifecycle_rows",
    "adapt_pattern_rows", "adapt_ranking_rows", "adapt_sector_stage_rows", "adapt_stock_stage_rows",
]
