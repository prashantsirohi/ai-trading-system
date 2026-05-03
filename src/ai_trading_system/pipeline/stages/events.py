"""Events pipeline stage — joins triggers with corporate-action context.

Sits between ``rank`` and ``execute`` in PIPELINE_ORDER. Inputs:

  - rank artifact ``breakout_scan.csv`` (Tier A/B breakout triggers)
  - rank artifact ``volume_shockers.csv`` (z-score shockers; optional)
  - market_intel DuckDB (queried via EventQueryService for bulk deals + events)

Outputs (all under ``data/pipeline_runs/<run_id>/events/attempt_<n>/``):

  - ``events_triggers.csv``    — consolidated trigger list (one row per
                                  symbol × trigger_type)
  - ``events_enrichment.json`` — per-trigger event lists + metadata
  - ``events_summary.json``    — counts by trigger_type / severity /
                                  category / materiality

When the feature flag is off (``params.events_enabled is False``, default
True at the stage level — orchestrator config gates this), the stage
short-circuits to a no-op result so the pipeline can move on.
"""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.pipeline.contracts import (
    StageArtifact,
    StageContext,
    StageResult,
)

logger = logging.getLogger(__name__)


@dataclass
class EventsStageConfig:
    enabled: bool = True
    # Trigger sources
    breakout_csv_relative: str = "breakout_scan.csv"
    volume_shockers_csv_relative: str = "volume_shockers.csv"
    # Trigger collection
    bulk_deal_lookback_days: int = 3
    bulk_deal_min_value_cr: float = 5.0
    breakout_tiers: tuple[str, ...] = ("A", "B")
    # Enrichment
    lookback_days: int = 30
    per_trigger_event_limit: int = 10
    min_trust: float = 80.0


class EventsStage:
    """Thin pipeline-stage wrapper around the events domain services."""

    name = "events"

    def __init__(
        self,
        *,
        config: EventsStageConfig | None = None,
        query_service_factory=None,        # () -> EventQuerier
        noise_filter=None,
    ):
        self.config = config or EventsStageConfig()
        self._query_service_factory = query_service_factory
        self._noise_filter = noise_filter

    # ----------------------------------------------------------------- run

    def run(self, context: StageContext) -> StageResult:
        cfg = self._merge_config(context)
        if not cfg.enabled:
            logger.info("events stage: disabled via config; skipping")
            return StageResult(
                metadata={"events_enabled": False, "skipped": True},
            )

        triggers = self._collect_triggers(context, cfg)
        if not triggers:
            logger.info("events stage: no triggers produced; emitting empty artifacts")
            return self._emit_empty(context)

        signals = self._enrich(triggers, cfg)
        return self._emit(context, triggers, signals, cfg)

    # ----------------------------------------------------------------- helpers

    def _merge_config(self, context: StageContext) -> EventsStageConfig:
        params = context.params or {}
        cfg = self.config
        # ``events_enabled`` is the single feature-flag escape hatch.
        # Default: enabled at stage level; orchestrator runs the stage when
        # it's part of PIPELINE_ORDER. Set events_enabled=False to opt out.
        if "events_enabled" in params:
            return EventsStageConfig(
                enabled=bool(params["events_enabled"]),
                breakout_csv_relative=cfg.breakout_csv_relative,
                volume_shockers_csv_relative=cfg.volume_shockers_csv_relative,
                bulk_deal_lookback_days=int(
                    params.get("events_bulk_lookback_days", cfg.bulk_deal_lookback_days)
                ),
                bulk_deal_min_value_cr=float(
                    params.get("events_bulk_min_value_cr", cfg.bulk_deal_min_value_cr)
                ),
                breakout_tiers=cfg.breakout_tiers,
                lookback_days=int(
                    params.get("events_lookback_days", cfg.lookback_days)
                ),
                per_trigger_event_limit=int(
                    params.get("events_per_trigger_limit", cfg.per_trigger_event_limit)
                ),
                min_trust=float(
                    params.get("events_min_trust", cfg.min_trust)
                ),
            )
        return cfg

    def _query_service(self):
        if self._query_service_factory is not None:
            return self._query_service_factory()
        from ai_trading_system.integrations.market_intel_client import (
            get_event_query_service,
        )
        return get_event_query_service()

    def _as_of_date(self, context: StageContext) -> date:
        if context.run_date:
            try:
                return datetime.fromisoformat(context.run_date).date()
            except ValueError:
                pass
        return datetime.utcnow().date()

    # ----------------------------------------------------------------- triggers

    def _collect_triggers(
        self,
        context: StageContext,
        cfg: EventsStageConfig,
    ):
        from ai_trading_system.domains.events.trigger_collector import (
            collect_breakout_triggers,
            collect_bulk_deal_triggers,
            merge_triggers,
        )
        from ai_trading_system.domains.events.triggers import Trigger

        as_of = self._as_of_date(context)

        # Breakout triggers from rank stage artifact
        rank_artifact = context.artifact_for("rank", "breakout_scan")
        breakout_path = Path(rank_artifact.uri) if rank_artifact else None
        if breakout_path is None:
            # Fall back to the conventional path under the rank attempt dir
            breakout_path = (
                self._sibling_stage_dir(context, "rank") / cfg.breakout_csv_relative
            )
        breakout_triggers = collect_breakout_triggers(
            breakout_path,
            as_of_date=as_of,
            tiers=cfg.breakout_tiers,
        )

        # Volume-shocker triggers from rank stage artifact (CSV produced by
        # ranking.volume_shocker.detect_volume_shockers)
        vs_artifact = context.artifact_for("rank", "volume_shockers")
        vs_path = Path(vs_artifact.uri) if vs_artifact else None
        if vs_path is None:
            vs_path = (
                self._sibling_stage_dir(context, "rank") / cfg.volume_shockers_csv_relative
            )
        volume_triggers: list[Trigger] = []
        if vs_path.exists():
            volume_triggers = self._read_volume_shocker_csv(vs_path, as_of)

        # Bulk-deal triggers via market_intel
        try:
            query_svc = self._query_service()
        except Exception as exc:
            logger.warning(
                "events stage: cannot reach market_intel (%s); proceeding with "
                "rank-only triggers", exc,
            )
            return merge_triggers(volume_triggers, breakout_triggers)

        bulk_triggers = collect_bulk_deal_triggers(
            as_of_date=as_of,
            query_service=query_svc,
            lookback_days=cfg.bulk_deal_lookback_days,
            min_value_cr=cfg.bulk_deal_min_value_cr,
        )

        return merge_triggers(volume_triggers, bulk_triggers, breakout_triggers)

    def _sibling_stage_dir(self, context: StageContext, sibling: str) -> Path:
        """Best-effort path to a sibling stage's latest attempt dir."""
        from ai_trading_system.platform.db.paths import ensure_domain_layout

        paths = ensure_domain_layout(
            project_root=context.project_root,
            data_domain=context.params.get("data_domain", "operational"),
        )
        base = paths.pipeline_runs_dir / context.run_id / sibling
        if not base.exists():
            return base / f"attempt_{context.attempt_number}"
        # Pick the highest attempt number that exists
        attempts = sorted(
            (p for p in base.iterdir() if p.is_dir() and p.name.startswith("attempt_")),
            key=lambda p: int(p.name.split("_", 1)[1]) if p.name.split("_", 1)[1].isdigit() else -1,
            reverse=True,
        )
        return attempts[0] if attempts else base / f"attempt_{context.attempt_number}"

    def _read_volume_shocker_csv(self, path: Path, as_of: date):
        """Optional input. Best-effort parse; on any error we fall back to []."""
        from ai_trading_system.domains.events.triggers import Trigger

        out: list[Trigger] = []
        try:
            with path.open("r", newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                if not reader.fieldnames:
                    return out
                lower = {f.lower(): f for f in reader.fieldnames}
                sym_col = lower.get("symbol") or lower.get("symbol_id")
                z_col = lower.get("volume_zscore_20") or lower.get("z_score")
                int_col = lower.get("shock_intensity")
                if not sym_col:
                    return out
                for row in reader:
                    symbol = (row.get(sym_col) or "").strip().upper()
                    if not symbol:
                        continue
                    z = _safe_float(row.get(z_col)) if z_col else None
                    intensity = _safe_float(row.get(int_col)) if int_col else None
                    meta: dict[str, Any] = {}
                    if z is not None:
                        meta["z_score"] = z
                    out.append(
                        Trigger(
                            symbol=symbol,
                            trigger_type="volume_shock",
                            as_of_date=as_of,
                            trigger_strength=float(intensity or 1.0),
                            trigger_metadata=meta,
                        )
                    )
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning("Failed to parse volume_shockers CSV at %s: %s", path, exc)
        return out

    # ----------------------------------------------------------------- enrich

    def _enrich(self, triggers, cfg: EventsStageConfig):
        from ai_trading_system.domains.events.enrichment_service import (
            EnrichmentService,
        )

        try:
            query_svc = self._query_service()
        except Exception as exc:
            logger.warning(
                "events stage: cannot reach market_intel for enrichment (%s); "
                "all triggers will have empty event lists", exc,
            )
            query_svc = _NullQuerier()

        # Build the default noise filter chain on first call. Tests can
        # inject a different chain via the constructor.
        noise_filter = self._noise_filter or self._build_default_chain()

        svc = EnrichmentService(
            query_service=query_svc,
            noise_filter=noise_filter,
            lookback_days=cfg.lookback_days,
            per_trigger_event_limit=cfg.per_trigger_event_limit,
            min_trust=cfg.min_trust,
        )
        return svc.enrich(triggers)

    def _build_default_chain(self):
        """Default filter chain; pure-function filters only by default.

        Phase-6 callers can extend by passing market_cap_provider /
        conn_provider via constructor.
        """
        from ai_trading_system.domains.events.noise_filter import (
            build_default_filter_chain,
        )
        return build_default_filter_chain()

    # ----------------------------------------------------------------- emit

    def _emit_empty(self, context: StageContext) -> StageResult:
        out_dir = context.output_dir()
        triggers_path = out_dir / "events_triggers.csv"
        triggers_path.write_text("symbol,trigger_type,as_of_date,trigger_strength\n")
        enrichment_path = context.write_json("events_enrichment.json", {"signals": []})
        summary_path = context.write_json(
            "events_summary.json",
            {
                "trigger_count": 0,
                "event_count": 0,
                "suppressed_count": 0,
                "by_trigger_type": {},
                "by_severity": {},
                "by_top_category": {},
                "by_materiality": {},
            },
        )
        return StageResult(
            artifacts=[
                StageArtifact.from_file("events_triggers", triggers_path, row_count=0),
                StageArtifact.from_file("events_enrichment", enrichment_path),
                StageArtifact.from_file("events_summary", summary_path),
            ],
            metadata={"trigger_count": 0, "event_count": 0},
        )

    def _emit(
        self,
        context: StageContext,
        triggers,
        signals,
        cfg: EventsStageConfig,
    ) -> StageResult:
        from ai_trading_system.domains.events.enrichment_service import summarize

        out_dir = context.output_dir()

        triggers_path = out_dir / "events_triggers.csv"
        with triggers_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "symbol", "trigger_type", "as_of_date",
                    "trigger_strength", "trigger_metadata_json",
                ],
            )
            writer.writeheader()
            for t in triggers:
                writer.writerow({
                    "symbol": t.symbol,
                    "trigger_type": t.trigger_type,
                    "as_of_date": t.as_of_date.isoformat(),
                    "trigger_strength": t.trigger_strength,
                    "trigger_metadata_json": json.dumps(
                        t.trigger_metadata, default=str, sort_keys=True,
                    ),
                })

        enrichment_payload = {"signals": [s.to_dict() for s in signals]}
        enrichment_path = context.write_json("events_enrichment.json", enrichment_payload)

        summary = summarize(signals)
        summary_path = context.write_json("events_summary.json", summary)

        # Persist to events_enrichment_log if the registry has a connection.
        self._persist_log(context, signals)

        artifacts = [
            StageArtifact.from_file(
                "events_triggers", triggers_path, row_count=len(triggers),
            ),
            StageArtifact.from_file(
                "events_enrichment", enrichment_path,
                row_count=len(signals),
                metadata={"event_count": summary["event_count"]},
            ),
            StageArtifact.from_file("events_summary", summary_path),
        ]
        return StageResult(
            artifacts=artifacts,
            metadata={
                "trigger_count": len(triggers),
                "event_count": summary["event_count"],
                "suppressed_count": summary["suppressed_count"],
                "by_severity": summary["by_severity"],
            },
        )

    def _persist_log(self, context: StageContext, signals) -> None:
        """Write one row per signal into events_enrichment_log.

        Best-effort: the table comes from migration 013, but we tolerate its
        absence so older deployments don't break the stage.
        """
        registry = getattr(context, "registry", None)
        if registry is None:
            return
        try:
            conn_ctx = (
                registry.connection() if hasattr(registry, "connection")
                else getattr(registry, "_connection", None)
            )
        except Exception:
            return
        if conn_ctx is None:
            return
        try:
            with conn_ctx as conn:
                for sig in signals:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO events_enrichment_log (
                            run_id, symbol, trigger_type, as_of_date,
                            trigger_strength, trigger_metadata_json,
                            event_hashes_json, materiality_label, top_category,
                            event_count, suppressed, suppress_reason, severity
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            context.run_id,
                            sig.trigger.symbol,
                            sig.trigger.trigger_type,
                            sig.trigger.as_of_date,
                            sig.trigger.trigger_strength,
                            json.dumps(sig.trigger.trigger_metadata, default=str),
                            json.dumps(sig.event_hashes),
                            sig.materiality_label,
                            sig.top_category,
                            len(sig.events),
                            sig.suppressed,
                            sig.suppress_reason,
                            sig.severity,
                        ],
                    )
        except Exception as exc:
            logger.debug(
                "Skipping events_enrichment_log persistence (%s) — "
                "stage continues without DB-side dedup",
                exc,
            )


# --------------------------------------------------------------------------- helpers


class _NullQuerier:
    """Fallback when market_intel is unreachable. Returns empty event lists."""

    def get_events_for_symbol(self, *args, **kwargs):
        return []

    def get_bulk_deals(self, *args, **kwargs):
        return []


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None
