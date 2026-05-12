"""Service layer for engine-driven backtest endpoints."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.domains.risk import RiskPolicyConfig, load_profile
from ai_trading_system.domains.risk.config import profile_search_dirs
from ai_trading_system.research.backtesting import (
    EngineBacktestRunner,
    load_ranked_by_date,
    load_research_ranked_by_date,
)
from ai_trading_system.research.sync_operational_data import sync_operational_to_research


def list_risk_profiles() -> list[dict[str, Any]]:
    """Scan ``config/risk_profiles/*.yaml`` and return parsed configs."""
    seen: dict[str, dict[str, Any]] = {}
    for base in profile_search_dirs():
        if not base.exists():
            continue
        for path in sorted(base.glob("*.yaml")):
            name = path.stem
            if name in seen:
                continue
            try:
                cfg = load_profile(name, strict=True)
            except FileNotFoundError:
                continue
            seen[name] = {
                "name": cfg.name,
                "path": str(path),
                "entry": _to_jsonable(cfg.entry),
                "stop": _to_jsonable(cfg.stop),
                "exit": _to_jsonable(cfg.exit),
                "sizing": _to_jsonable(cfg.sizing),
                "constraints": _to_jsonable(cfg.constraints),
            }
    return sorted(seen.values(), key=lambda p: p["name"])


def _to_jsonable(obj: Any) -> dict[str, Any]:
    if hasattr(obj, "__dataclass_fields__"):
        return asdict(obj)
    return dict(obj or {})


def run_backtest(
    project_root: Path,
    *,
    profile_name: str,
    data_source: str = "pipeline_replay",
    from_date: date | None = None,
    to_date: date | None = None,
    equity: float = 1_000_000.0,
    persist: bool = True,
    custom_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile = _profile_with_overrides(profile_name, custom_config)
    source = (data_source or "pipeline_replay").strip().lower()
    sync_summary: dict[str, Any] | None = None
    if source == "research_dynamic":
        sync_summary = sync_operational_to_research(project_root=project_root, apply=True)
        ranked_by_date = load_research_ranked_by_date(
            project_root,
            from_date=from_date,
            to_date=to_date,
        )
        no_data_message = "no research OHLCV data available under data/research/research_ohlcv.duckdb"
    else:
        source = "pipeline_replay"
        pipeline_runs_dir = project_root / "data" / "pipeline_runs"
        ranked_by_date = load_ranked_by_date(
            pipeline_runs_dir, from_date=from_date, to_date=to_date
        )
        no_data_message = f"no ranked_signals.csv under {pipeline_runs_dir}"
    if not ranked_by_date:
        return {
            "status": "no_data",
            "profile": profile.name,
            "data_source": source,
            "sync": sync_summary,
            "trade_count": 0,
            "message": no_data_message,
        }

    runner = EngineBacktestRunner(risk_config=profile, starting_equity=equity)
    result = runner.run(ranked_by_date)
    trades_df = result.to_trades_df()
    equity_df = result.to_equity_df()

    summary = {
        "status": "ok",
        "profile": profile.name,
        "data_source": source,
        "sync": sync_summary,
        "from_date": str(from_date) if from_date else None,
        "to_date": str(to_date) if to_date else None,
        "starting_equity": equity,
        "ending_equity": float(equity_df["equity"].iloc[-1]) if not equity_df.empty else equity,
        "trading_days": int(len(equity_df)),
        "trade_count": int(len(result.trades)),
        "exit_reason_counts": (
            trades_df["exit_reason"].fillna("__open__").value_counts().to_dict()
            if not trades_df.empty
            else {}
        ),
        "trades": _trades_payload(result.trades, limit=500),
        "equity_curve": _equity_payload(equity_df),
    }

    if persist:
        out_dir = (
            project_root
            / "data"
            / "research"
            / "engine_backtests"
            / source
            / profile.name
            / datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        trades_df.to_csv(out_dir / "trades.csv", index=False)
        equity_df.to_csv(out_dir / "equity_curve.csv", index=False)
        (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
        summary["artifact_dir"] = str(out_dir.relative_to(project_root))

    return summary


def _profile_with_overrides(
    profile_name: str,
    custom_config: dict[str, Any] | None,
) -> RiskPolicyConfig:
    base = load_profile(profile_name)
    if not custom_config:
        return base

    payload = {
        "name": f"custom:{base.name}",
        "entry": asdict(base.entry),
        "stop": asdict(base.stop),
        "exit": asdict(base.exit),
        "sizing": asdict(base.sizing),
        "constraints": asdict(base.constraints),
    }
    for section in ("entry", "stop", "exit", "sizing", "constraints"):
        override = custom_config.get(section)
        if isinstance(override, dict):
            payload[section].update(override)
    return RiskPolicyConfig.from_dict(payload)


def _trades_payload(trades, *, limit: int) -> list[dict[str, Any]]:
    out = []
    for t in trades[:limit]:
        row = asdict(t)
        # Stringify dates for JSON.
        for k, v in row.items():
            if isinstance(v, date):
                row[k] = v.isoformat()
        out.append(row)
    return out


def _equity_payload(equity_df) -> list[dict[str, Any]]:
    if equity_df is None or equity_df.empty:
        return []
    out = []
    for row in equity_df.to_dict(orient="records"):
        d = row.get("date")
        if isinstance(d, date):
            row["date"] = d.isoformat()
        elif d is not None:
            row["date"] = str(d)
        out.append(row)
    return out
