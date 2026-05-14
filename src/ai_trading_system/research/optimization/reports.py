"""Markdown report builder for a finished optimisation run.

Reads from the control-plane DB and emits:
- summary line (run, champion, trials)
- per-fold metrics table for baseline vs champion
- top-10 trials by fitness
- parameter importance (when an Optuna study is supplied)
- champion guard verdict
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb

from ai_trading_system.platform.db.paths import ensure_domain_layout  # noqa: F401  (consumer may want it)
from ai_trading_system.pipeline.registry import RegistryStore


@dataclass(frozen=True)
class RunSummary:
    optimization_run_id: str
    strategy_id: str
    recipe_name: str
    status: str
    champion_rule_pack_id: str | None
    baseline_rule_pack_id: str
    from_date: date
    to_date: date
    trials: int


def _connect(project_root: Path | str) -> duckdb.DuckDBPyConnection:
    db_path = RegistryStore(project_root=Path(project_root)).db_path
    return duckdb.connect(str(db_path), read_only=True)


def load_run_summary(project_root: Path | str, optimization_run_id: str) -> RunSummary:
    with _connect(project_root) as con:
        row = con.execute(
            """
            SELECT
                r.optimization_run_id, r.strategy_id, r.recipe_name, r.status,
                r.champion_rule_pack_id, r.baseline_rule_pack_id, r.from_date, r.to_date,
                (SELECT COUNT(DISTINCT iteration)
                   FROM strategy_iteration_result
                  WHERE optimization_run_id = r.optimization_run_id
                    AND iteration >= 0) AS trial_count
            FROM strategy_optimization_run r
            WHERE r.optimization_run_id = ?
            """,
            [optimization_run_id],
        ).fetchone()
    if row is None:
        raise ValueError(f"unknown optimization_run_id: {optimization_run_id}")
    return RunSummary(*row)


def _fold_metrics(
    con: duckdb.DuckDBPyConnection, run_id: str, rule_pack_id: str
) -> list[tuple]:
    return con.execute(
        """
        SELECT fold_index, fitness, cagr, sharpe, max_drawdown_pct,
               win_rate, trade_count, total_return_pct, nifty_return_pct
        FROM strategy_iteration_result
        WHERE optimization_run_id = ?
          AND rule_pack_id = ?
          AND fold_index >= 0
        ORDER BY fold_index
        """,
        [run_id, rule_pack_id],
    ).fetchall()


def _top_trials(
    con: duckdb.DuckDBPyConnection, run_id: str, limit: int = 10
) -> list[tuple]:
    return con.execute(
        """
        SELECT iteration, rule_pack_id, fitness, cagr, sharpe,
               max_drawdown_pct, trade_count, accepted, rejection_reason
        FROM strategy_iteration_result
        WHERE optimization_run_id = ?
          AND fold_index = -1
          AND iteration >= 0
        ORDER BY fitness DESC NULLS LAST
        LIMIT ?
        """,
        [run_id, limit],
    ).fetchall()


def _format_fold_table(rows: list[tuple]) -> str:
    if not rows:
        return "_(no folds)_\n"
    out = [
        "| fold | fitness | CAGR | Sharpe | MDD% | win% | trades | total% | NIFTY% |",
        "|----:|--------:|-----:|------:|-----:|-----:|------:|------:|------:|",
    ]
    for r in rows:
        fold_idx, fit, cagr, sh, dd, wr, tc, ret, nif = r
        out.append(
            f"| {fold_idx} | {_n(fit)} | {_pct(cagr)} | {_n(sh)} | {_n(dd, 2)} | "
            f"{_pct(wr)} | {tc or 0} | {_n(ret, 2)} | {_n(nif, 2)} |"
        )
    return "\n".join(out) + "\n"


def _n(v, digits: int = 4) -> str:
    if v is None:
        return "—"
    return f"{v:.{digits}f}"


def _pct(v, digits: int = 2) -> str:
    if v is None:
        return "—"
    return f"{v * 100:.{digits}f}%" if abs(v) <= 1.0 else f"{v:.{digits}f}"


def build_markdown_report(
    project_root: Path | str,
    optimization_run_id: str,
) -> str:
    summary = load_run_summary(project_root, optimization_run_id)

    with _connect(project_root) as con:
        baseline_folds = _fold_metrics(con, summary.optimization_run_id, summary.baseline_rule_pack_id)
        champion_folds = (
            _fold_metrics(con, summary.optimization_run_id, summary.champion_rule_pack_id)
            if summary.champion_rule_pack_id
            else []
        )
        top = _top_trials(con, summary.optimization_run_id)

    lines: list[str] = []
    lines.append(f"# Strategy optimisation report — {summary.strategy_id}")
    lines.append("")
    lines.append(f"- run_id: `{summary.optimization_run_id}`")
    lines.append(f"- recipe: `{summary.recipe_name}`")
    lines.append(f"- status: **{summary.status}**")
    lines.append(f"- window: {summary.from_date} → {summary.to_date}")
    lines.append(f"- trials completed: {summary.trials}")
    lines.append(f"- baseline pack: `{summary.baseline_rule_pack_id}`")
    lines.append(
        f"- champion pack: "
        + (f"`{summary.champion_rule_pack_id}`" if summary.champion_rule_pack_id else "_none accepted_")
    )
    lines.append("")
    lines.append("## Baseline (per-fold)")
    lines.append("")
    lines.append(_format_fold_table(baseline_folds))
    if champion_folds:
        lines.append("## Champion (per-fold)")
        lines.append("")
        lines.append(_format_fold_table(champion_folds))
    lines.append("## Top trials by fitness")
    lines.append("")
    if top:
        lines.append(
            "| trial | pack_id | fitness | CAGR | Sharpe | MDD% | trades | accepted | reason |"
        )
        lines.append(
            "|----:|:--------|--------:|-----:|------:|-----:|------:|:---------|:-------|"
        )
        for it, pid, fit, cagr, sh, dd, tc, acc, reason in top:
            lines.append(
                f"| {it} | `{(pid or '')[:12]}…` | {_n(fit)} | {_pct(cagr)} | {_n(sh)} | "
                f"{_n(dd, 2)} | {tc or 0} | {'✅' if acc else '❌'} | {reason or ''} |"
            )
    else:
        lines.append("_(no trials)_")
    lines.append("")
    return "\n".join(lines)


def write_report(
    project_root: Path | str,
    optimization_run_id: str,
    out_path: Path | str,
) -> Path:
    text = build_markdown_report(project_root, optimization_run_id)
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    return out


def parameter_importances(study) -> dict[str, float]:
    """Wrap optuna.importance.get_param_importances for a finished study.

    Caller passes an active ``optuna.Study`` (importance requires the in-memory
    object — we don't persist Optuna's internal trial state in our DuckDB).
    """
    from optuna.importance import get_param_importances

    if not study.trials:
        return {}
    try:
        return dict(get_param_importances(study))
    except (ValueError, RuntimeError):
        return {}
