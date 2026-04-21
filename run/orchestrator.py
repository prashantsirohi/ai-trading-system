from pathlib import Path
import sys


def _ensure_src_on_path() -> None:
    """Keep legacy script entrypoints working from a source checkout."""
    current_file = Path(__file__).resolve()
    project_root = current_file.parents[1]
    src_path = project_root / "src"

    project_root_str = str(project_root)
    src_path_str = str(src_path)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)
    if src_path_str not in sys.path:
        sys.path.insert(0, src_path_str)


_ensure_src_on_path()

from ai_trading_system.pipeline.orchestrator import *  # noqa: F401,F403,E402
from ai_trading_system.pipeline import orchestrator as _pipeline_orchestrator  # noqa: E402

_extract_quarantined_dates = _pipeline_orchestrator._extract_quarantined_dates
_run_auto_quarantine_repair = _pipeline_orchestrator._run_auto_quarantine_repair
_safe_stage_runs = _pipeline_orchestrator._safe_stage_runs


def main() -> None:
    _pipeline_orchestrator.PipelineOrchestrator = PipelineOrchestrator
    _pipeline_orchestrator._extract_quarantined_dates = _extract_quarantined_dates
    _pipeline_orchestrator._run_auto_quarantine_repair = _run_auto_quarantine_repair
    _pipeline_orchestrator._safe_stage_runs = _safe_stage_runs
    _pipeline_orchestrator.build_parser = build_parser
    _pipeline_orchestrator.main()


if __name__ == "__main__":
    main()
