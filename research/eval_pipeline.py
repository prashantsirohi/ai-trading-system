"""Research evaluation entrypoint for model metadata and offline metrics."""

from __future__ import annotations

import argparse
from pathlib import Path

from analytics.alpha.policy import evaluate_promotion_candidate
from analytics.registry import RegistryStore
from utils.logger import log_context, logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research evaluation metadata writer")
    parser.add_argument("--model-id", required=True)
    parser.add_argument("--dataset-ref", required=True)
    parser.add_argument("--precision-at-10", type=float, required=True)
    parser.add_argument("--sharpe", type=float, required=True)
    parser.add_argument("--horizon", type=int, help="Optional horizon for promotion gate evaluation")
    parser.add_argument("--deployment-mode", default="shadow_ml")
    parser.add_argument("--lookback-days", type=int, default=60)
    parser.add_argument("--evaluate-promotion", action="store_true")
    parser.add_argument("--record-gates", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[1]
    registry = RegistryStore(project_root)
    with log_context(run_id="research-eval", model_id=args.model_id, stage_name="eval"):
        eval_id = registry.record_model_eval(
            args.model_id,
            {"precision_at_10": args.precision_at_10, "sharpe": args.sharpe},
            dataset_ref=args.dataset_ref,
        )
        logger.info("Recorded research evaluation eval_id=%s for model_id=%s", eval_id, args.model_id)
        if args.evaluate_promotion and args.horizon is not None:
            gate_result = evaluate_promotion_candidate(
                registry=registry,
                model_id=args.model_id,
                horizon=args.horizon,
                deployment_mode=args.deployment_mode,
                lookback_days=args.lookback_days,
            )
            if args.record_gates:
                registry.record_promotion_gate_results(args.model_id, gate_result["gate_results"])
            logger.info(
                "Promotion evaluation model_id=%s horizon=%s status=%s",
                args.model_id,
                args.horizon,
                gate_result["overall_status"],
            )


if __name__ == "__main__":
    main()
