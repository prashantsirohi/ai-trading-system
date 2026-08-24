from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path

from ai_trading_system.integrations.market_intel_client import resolve_db_path
from ai_trading_system.platform.db.paths import get_domain_paths, require_data_root_available
from ai_trading_system.domains.fundamentals.screener_store import default_screener_db_path

from .calibration import JCurveCalibrationService
from .cohort import BaselineCohort, DiscoveryRunCohort, SeedRunCohort
from .model_router import OpenRouterModelRouter
from .market_intel_adapter import HIGH_VALUE_FILTER_V1
from .service import JCurveEvaluationService, JCurveImportService
from .discovery_v2 import JCurveDiscoveryV2Service, ScreenerDiscoveryPolicyV2
from .screener_seed import (
    ScreenerHistoryReader,
    ScreenerSeedPolicy,
    ScreenerSeedService,
    profile_baseline,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the isolated J-curve research lifecycle.")
    parser.add_argument("--store-path", type=Path)
    parser.add_argument("--output-root", type=Path)
    sub = parser.add_subparsers(dest="command", required=True)

    bootstrap = sub.add_parser("bootstrap", help="Freeze a historical market_intel announcement baseline")
    bootstrap.add_argument("--as-of-date", required=True, type=date.fromisoformat)
    bootstrap.add_argument("--lookback-years", type=int, default=5)
    bootstrap.add_argument("--market-intel-db", type=Path)
    bootstrap.add_argument(
        "--upstream-filter-policy", choices=[HIGH_VALUE_FILTER_V1],
        help="Opt into a versioned market_intel shadow filter; omitted preserves legacy import behavior",
    )
    bootstrap.add_argument(
        "--cohort-config", type=Path,
        help="Versioned baseline cohort; defaults to capex_baseline_v1.json",
    )
    bootstrap.add_argument(
        "--all-companies", action="store_true",
        help="Disable the default curated baseline and import every eligible company",
    )
    bootstrap.add_argument(
        "--seed-run-id",
        help="Use resolved accepted candidates from one completed Screener seed run",
    )
    bootstrap.add_argument(
        "--discovery-run-id",
        help="Use the primary queue from one completed four-screen V2 discovery run",
    )

    seed = sub.add_parser(
        "seed-screener",
        help="Freeze a Screener capex screen and classify historical accounting transitions",
    )
    seed.add_argument("--as-of-date", required=True, type=date.fromisoformat)
    seed.add_argument("--screen-id", type=int, default=317873)
    seed.add_argument(
        "--screen-export", type=Path,
        help="Use a previously downloaded screen CSV/XLSX; omitted uses authenticated Playwright",
    )

    profile = sub.add_parser(
        "profile-baseline",
        help="Run the deterministic accounting classifier over a versioned baseline",
    )
    profile.add_argument("--as-of-date", required=True, type=date.fromisoformat)
    profile.add_argument("--cohort-config", type=Path)
    profile.add_argument("--fundamentals-db", type=Path)
    profile.add_argument("--policy-config", type=Path)
    seed.add_argument("--fundamentals-db", type=Path)
    seed.add_argument("--policy-config", type=Path)
    seed.add_argument(
        "--supplemental-cohort", type=Path,
        help="Additional versioned symbols; defaults to the 25-company capex baseline",
    )

    discover = sub.add_parser(
        "discover-v2",
        help="Freeze four Screener lanes and build the bounded J-curve research queue",
    )
    discover.add_argument("--as-of-date", required=True, type=date.fromisoformat)
    discover.add_argument(
        "--screen-export", action="append", default=[], type=_screen_export_arg,
        help="Offline export as SCREEN_ID=PATH; when used, all four screens are required",
    )
    discover.add_argument("--universe-run-id")
    discover.add_argument("--fundamentals-db", type=Path)
    discover.add_argument("--policy-config", type=Path)
    discover.add_argument("--accounting-policy-config", type=Path)
    discover.add_argument("--cohort-config", type=Path)

    ingest = sub.add_parser("ingest", help="Import recent announcements with an overlap window")
    ingest.add_argument("--as-of-date", required=True, type=date.fromisoformat)
    ingest.add_argument("--overlap-days", type=int, default=7)
    ingest.add_argument("--market-intel-db", type=Path)
    ingest.add_argument(
        "--upstream-filter-policy", choices=[HIGH_VALUE_FILTER_V1],
        help="Opt into a versioned market_intel shadow filter; omitted preserves legacy import behavior",
    )

    evaluate = sub.add_parser("evaluate", help="Extract, verify, and evaluate imported capex evidence")
    evaluate.add_argument("--parent-run-id", required=True)
    evaluate.add_argument("--as-of-date", required=True, type=date.fromisoformat)
    evaluate.add_argument("--materiality-inputs", type=Path, help="JSON keyed by company_id or announcement_id")
    evaluate.add_argument("--human-verifications", type=Path, help="Reviewed JSON rows with claim_id, reviewer, decision, and note")
    evaluate.add_argument("--company-limit", type=int, default=25)

    report = sub.add_parser("report", help="Print a completed immutable result JSON")
    report.add_argument("--run-id", required=True)

    calibrate = sub.add_parser("calibrate", help="Score an evaluation against reviewed labels")
    calibrate.add_argument("--evaluation-run-id", required=True)
    calibrate.add_argument("--labels", required=True, type=Path)
    calibrate.add_argument("--allow-nonstandard-cohort", action="store_true")

    args = parser.parse_args(argv)
    require_data_root_available()
    project_root = Path(__file__).resolve().parents[5]
    root = get_domain_paths(data_domain="operational").root_dir / "research_screener"
    store_path = args.store_path or root / "control_plane.duckdb"
    output_root = args.output_root or root / "jcurve_runs"
    if args.command == "discover-v2":
        discovery_policy = ScreenerDiscoveryPolicyV2.load(
            args.policy_config
            or project_root / "configs/research_screener/jcurve/screener_discovery_policy_v2.json"
        )
        accounting_policy = ScreenerSeedPolicy.load(
            args.accounting_policy_config
            or project_root / "configs/research_screener/jcurve/screener_seed_policy_v1.json"
        )
        cohort = BaselineCohort.load(
            args.cohort_config
            or project_root / "configs/research_screener/jcurve/capex_baseline_v2.json"
        )
        exports = dict(args.screen_export)
        if len(exports) != len(args.screen_export):
            parser.error("--screen-export contains a duplicate screen ID")
        result = JCurveDiscoveryV2Service(
            store_path=store_path,
            output_root=output_root,
            fundamentals_db=args.fundamentals_db or default_screener_db_path(project_root),
            policy=discovery_policy,
            accounting_policy=accounting_policy,
            cohort=cohort.raw,
        ).run(
            as_of_date=args.as_of_date,
            screen_exports=exports,
            universe_run_id=args.universe_run_id,
        )
    elif args.command == "profile-baseline":
        policy = ScreenerSeedPolicy.load(
            args.policy_config
            or project_root / "configs/research_screener/jcurve/screener_seed_policy_v1.json"
        )
        cohort = BaselineCohort.load(
            args.cohort_config
            or project_root / "configs/research_screener/jcurve/capex_baseline_v2.json"
        )
        result = profile_baseline(
            cohort.raw,
            reader=ScreenerHistoryReader(
                args.fundamentals_db or default_screener_db_path(project_root),
                policy,
                as_of_date=args.as_of_date,
            ),
        )
    elif args.command == "seed-screener":
        policy = ScreenerSeedPolicy.load(
            args.policy_config
            or project_root / "configs/research_screener/jcurve/screener_seed_policy_v1.json"
        )
        supplemental_raw = json.loads((
            args.supplemental_cohort
            or project_root / "configs/research_screener/jcurve/capex_baseline_v1.json"
        ).read_text(encoding="utf-8"))
        supplemental_symbols = tuple(
            str(member["nse_symbol"]).upper().strip()
            for member in supplemental_raw.get("members", [])
        )
        result = ScreenerSeedService(
            store_path=store_path,
            output_root=output_root,
            fundamentals_db=args.fundamentals_db or default_screener_db_path(project_root),
            policy=policy,
        ).run(
            as_of_date=args.as_of_date,
            screen_id=args.screen_id,
            screen_export=args.screen_export,
            supplemental_symbols=supplemental_symbols,
        )
    elif args.command in {"bootstrap", "ingest"}:
        service = JCurveImportService(store_path=store_path, output_root=output_root)
        if args.command == "bootstrap":
            selected_modes = sum(bool(value) for value in (
                args.all_companies, args.cohort_config, args.seed_run_id,
                args.discovery_run_id,
            ))
            if selected_modes > 1:
                parser.error(
                    "--all-companies, --cohort-config, --seed-run-id, and "
                    "--discovery-run-id are mutually exclusive"
                )
            if not 1 <= args.lookback_years <= 10:
                parser.error("lookback-years must be between 1 and 10")
            try:
                published_from = args.as_of_date.replace(year=args.as_of_date.year - args.lookback_years)
            except ValueError:
                published_from = args.as_of_date.replace(
                    year=args.as_of_date.year - args.lookback_years, day=28,
                )
            run_type = "BOOTSTRAP"
            if args.all_companies:
                cohort = None
            elif args.discovery_run_id:
                cohort = DiscoveryRunCohort(run_id=args.discovery_run_id)
            elif args.seed_run_id:
                cohort = SeedRunCohort(run_id=args.seed_run_id)
            else:
                cohort = BaselineCohort.load(
                    args.cohort_config
                    or project_root / "configs/research_screener/jcurve/capex_baseline_v1.json"
                )
        else:
            if not 1 <= args.overlap_days <= 90:
                parser.error("overlap-days must be between 1 and 90")
            published_from = service.incremental_from(as_of_date=args.as_of_date, overlap_days=args.overlap_days)
            run_type = "INCREMENTAL_IMPORT"
            cohort = None
        result = service.run(
            market_intel_db=args.market_intel_db or Path(resolve_db_path()), published_from=published_from,
            as_of_date=args.as_of_date, run_type=run_type, cohort=cohort,
            upstream_filter_policy=args.upstream_filter_policy,
        )
    elif args.command == "evaluate":
        api_key = os.environ.get("OPENROUTER_API_KEY") or os.environ.get("OPENROUTER_KEY")
        if not api_key:
            parser.error("evaluate requires OPENROUTER_API_KEY or OPENROUTER_KEY")
        materiality = json.loads(args.materiality_inputs.read_text(encoding="utf-8")) if args.materiality_inputs else {}
        human_verifications = json.loads(args.human_verifications.read_text(encoding="utf-8")) if args.human_verifications else []
        if not 1 <= args.company_limit <= 250:
            parser.error("company-limit must be between 1 and 250")
        router = OpenRouterModelRouter.from_project_root(project_root=project_root, api_key=api_key)
        result = JCurveEvaluationService(
            store_path=store_path, output_root=output_root, project_root=project_root, router=router,
        ).run(
            parent_run_id=args.parent_run_id, as_of_date=args.as_of_date,
            materiality_inputs=materiality, human_verifications=human_verifications,
            company_limit=args.company_limit,
        )
    elif args.command == "calibrate":
        result = JCurveCalibrationService(store_path=store_path, output_root=output_root).run(
            evaluation_run_id=args.evaluation_run_id, labels_path=args.labels,
            allow_nonstandard_cohort=args.allow_nonstandard_cohort,
        )
    else:
        result_path = output_root / args.run_id / "result.json"
        if not result_path.is_file():
            parser.error(f"completed result not found: {result_path}")
        result = json.loads(result_path.read_text(encoding="utf-8"))
    print(json.dumps(result, indent=2, default=str))
    return 0


def _screen_export_arg(value: str) -> tuple[int, Path]:
    screen_id, separator, path = value.partition("=")
    if not separator or not screen_id.isdigit() or not path:
        raise argparse.ArgumentTypeError("screen export must be SCREEN_ID=PATH")
    return int(screen_id), Path(path)


if __name__ == "__main__":
    raise SystemExit(main())
