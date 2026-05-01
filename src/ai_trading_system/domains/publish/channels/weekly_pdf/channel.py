"""Weekly PDF channel entry point invoked by PublishStage."""

from __future__ import annotations

import logging
from typing import Any, Dict

import pandas as pd

from ai_trading_system.domains.publish.channels.weekly_pdf.builder import build_report
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext

logger = logging.getLogger(__name__)


def publish_weekly_pdf(
    context: StageContext,
    rank_artifact: StageArtifact,  # noqa: ARG001 — kept for handler signature parity
    datasets: Dict[str, pd.DataFrame],
) -> Dict[str, Any]:
    """Build the weekly PDF report under the publish stage attempt directory."""
    output_dir = context.output_dir() / "weekly_pdf"
    manifest = build_report(context, datasets, output_dir)
    if manifest.get("pdf_error"):
        logger.warning(
            "weekly_pdf rendered HTML but PDF generation failed: %s",
            manifest["pdf_error"],
        )
    return {
        "report_id": manifest["report_id"],
        "week_ending": manifest["week_ending"],
        "html_path": manifest.get("html_path"),
        "pdf_path": manifest.get("pdf_path"),
        "pdf_error": manifest.get("pdf_error"),
        "counts": manifest.get("counts", {}),
        "trust_status": manifest.get("trust_status"),
    }
