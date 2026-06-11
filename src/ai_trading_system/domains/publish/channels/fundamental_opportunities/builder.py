"""Build the standalone fundamental opportunities report artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.domains.publish.channels.fundamental_opportunities.classifier import (
    BUCKET_CARDS,
    bucket_counts,
    bucket_matrix,
    classify_fundamental_opportunities,
    metric_definitions,
    tracker_shortlist,
)
from ai_trading_system.domains.publish.channels.fundamental_opportunities.data_loader import (
    assemble_classifier_frame,
    load_inputs,
)
from ai_trading_system.domains.publish.channels.fundamental_opportunities.renderer import render
from ai_trading_system.domains.publish.channels.fundamental_opportunities.summary import (
    MAIN_BUCKETS,
    build_report_summary,
)
from ai_trading_system.domains.candidate_tracker import CandidateTrackerConfig, run_candidate_tracker


def build_fundamental_opportunity_report(
    *,
    as_of: str | None,
    fundamentals_db_path: str | Path,
    ohlcv_db_path: str | Path,
    output_dir: str | Path,
    tracker_db_path: str | Path | None = None,
    fundamental_scores_path: str | Path | None = None,
    universe_id: str = "UNIV_TOP1000_MCAP",
    limit_per_bucket: int = 25,
    update_tracker: bool = False,
) -> dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    inputs = load_inputs(
        as_of=as_of,
        fundamentals_db_path=fundamentals_db_path,
        ohlcv_db_path=ohlcv_db_path,
        tracker_db_path=tracker_db_path,
        fundamental_scores_path=fundamental_scores_path,
        universe_id=universe_id,
    )
    classifier_frame = assemble_classifier_frame(inputs)
    classified = classify_fundamental_opportunities(classifier_frame)
    shortlist = tracker_shortlist(classified)
    if not shortlist.empty:
        shortlist.loc[:, "bucket_as_of"] = inputs.as_of
    report_summary = build_report_summary(
        classified=classified,
        shortlist=shortlist,
        as_of=inputs.as_of,
        universe_id=universe_id,
        warnings=inputs.warnings,
        limit_per_bucket=int(limit_per_bucket),
    )

    context = {
        "as_of": inputs.as_of,
        "universe_id": universe_id,
        "cards": [card.__dict__ for card in BUCKET_CARDS],
        "metric_definitions": metric_definitions(),
        "bucket_matrix": bucket_matrix(),
        "bucket_counts": bucket_counts(classified),
        "bucket_tables": report_summary["main_bucket_tables"],
        "main_buckets": MAIN_BUCKETS,
        "total_rows": int(len(classified)),
        "shortlist_rows": int(len(shortlist)),
        "warnings": inputs.warnings,
        **report_summary,
    }
    html_path, pdf_path, pdf_error = render(context, output)

    shortlist_path = output / "fundamental_bucket_shortlist.csv"
    shortlist.to_csv(shortlist_path, index=False)
    classified_path = output / "fundamental_bucket_classified.csv"
    classified.to_csv(classified_path, index=False)
    manifest_path = output / "fundamental_bucket_report_manifest.json"
    manifest = {
        "report_id": f"fundamental_opportunities-{inputs.as_of}",
        "as_of": inputs.as_of,
        "universe_id": universe_id,
        "html_path": str(html_path),
        "pdf_path": str(pdf_path) if pdf_path else None,
        "pdf_error": pdf_error,
        "classified_path": str(classified_path),
        "shortlist_path": str(shortlist_path),
        "counts": {
            "total_rows": int(len(classified)),
            "shortlist_rows": int(len(shortlist)),
            "buckets": bucket_counts(classified),
        },
        "top_opportunities": report_summary["top_opportunities"],
        "data_quality": report_summary["data_quality"],
        "sector_map": report_summary["sector_map"],
        "no_candidate_buckets": report_summary["no_candidate_buckets"],
        "main_report_bucket_counts": report_summary["main_report_bucket_counts"],
        "appendix_bucket_counts": report_summary["appendix_bucket_counts"],
        "warnings": inputs.warnings,
    }
    if update_tracker:
        tracker_result = run_candidate_tracker(
            config=CandidateTrackerConfig(
                db_path=Path(tracker_db_path) if tracker_db_path is not None else output / "candidate_tracker.duckdb",
                ohlcv_db_path=Path(ohlcv_db_path),
                run_date=inputs.as_of,
                run_id=f"fundamental_opportunities-{inputs.as_of}",
            ),
            final_candidates=pd.DataFrame(),
            fundamental_bucket_shortlist=shortlist,
            stock_valuation_bands_latest=inputs.valuation_bands,
        )
        manifest["tracker_update"] = tracker_result.summary
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    manifest["manifest_path"] = str(manifest_path)
    return manifest


__all__ = ["build_fundamental_opportunity_report"]
