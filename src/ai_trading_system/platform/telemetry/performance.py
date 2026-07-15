"""Observational Phase 3C-4 performance metrics and artifact persistence."""

from __future__ import annotations

import csv
import json
import math
import os
import platform
import statistics
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterator, Mapping

from ai_trading_system.pipeline.contracts import StageArtifact

PERFORMANCE_POLICY_VERSION = "phase3c4-performance-policy-v1"


class PerformanceStatus(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    NOT_EVALUATED = "NOT_EVALUATED"


class CacheMode(str, Enum):
    COLD = "COLD"
    WARM = "WARM"
    UNKNOWN = "UNKNOWN"


class ReplayMode(str, Enum):
    FIRST_RUN = "FIRST_RUN"
    EXACT_REPLAY = "EXACT_REPLAY"
    NON_IDENTICAL_REPLAY = "NON_IDENTICAL_REPLAY"


@dataclass(frozen=True, slots=True)
class PerformanceConfig:
    enabled: bool = True
    threshold_evaluation_enabled: bool = True
    fail_pipeline: bool = False
    policy_version: str = PERFORMANCE_POLICY_VERSION
    weekly_stage_warn_seconds: float = 120.0
    weekly_stage_fail_seconds: float = 300.0
    sector_aggregation_warn_seconds: float = 30.0
    sector_aggregation_fail_seconds: float = 90.0
    scan_router_warn_seconds: float = 30.0
    scan_router_fail_seconds: float = 90.0
    investigator_warn_seconds: float = 180.0
    investigator_fail_seconds: float = 600.0
    opportunities_warn_seconds: float = 60.0
    opportunities_fail_seconds: float = 180.0
    total_shadow_pipeline_warn_seconds: float = 420.0
    total_shadow_pipeline_fail_seconds: float = 900.0
    peak_rss_warn_mb: float = 2048.0
    peak_rss_fail_mb: float = 4096.0
    min_symbols_per_second_warn: float = 1.0
    min_symbols_per_second_fail: float = 0.1
    max_artifact_size_warn_mb: float = 100.0
    max_artifact_size_fail_mb: float = 500.0
    baseline_warn_pct: float = 10.0
    baseline_fail_pct: float = 25.0

    def __post_init__(self) -> None:
        upper_pairs = (
            ("weekly_stage", self.weekly_stage_warn_seconds, self.weekly_stage_fail_seconds),
            ("sector_aggregation", self.sector_aggregation_warn_seconds, self.sector_aggregation_fail_seconds),
            ("scan_router", self.scan_router_warn_seconds, self.scan_router_fail_seconds),
            ("investigator", self.investigator_warn_seconds, self.investigator_fail_seconds),
            ("opportunities", self.opportunities_warn_seconds, self.opportunities_fail_seconds),
            ("total_shadow_pipeline", self.total_shadow_pipeline_warn_seconds, self.total_shadow_pipeline_fail_seconds),
            ("peak_rss", self.peak_rss_warn_mb, self.peak_rss_fail_mb),
            ("artifact_size", self.max_artifact_size_warn_mb, self.max_artifact_size_fail_mb),
            ("baseline", self.baseline_warn_pct, self.baseline_fail_pct),
        )
        for name, warn, fail in upper_pairs:
            if warn < 0 or fail < 0 or warn > fail:
                raise ValueError(f"invalid upper-bound thresholds for {name}: warn must be <= fail and non-negative")
        if (
            self.min_symbols_per_second_warn < 0
            or self.min_symbols_per_second_fail < 0
            or self.min_symbols_per_second_warn < self.min_symbols_per_second_fail
        ):
            raise ValueError("invalid throughput thresholds: warn must be >= fail and non-negative")

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "PerformanceConfig":
        fields = cls.__dataclass_fields__
        payload: dict[str, Any] = {}
        for name in fields:
            key = {
                "enabled": "performance_instrumentation_enabled",
                "threshold_evaluation_enabled": "performance_threshold_evaluation_enabled",
                "fail_pipeline": "performance_fail_pipeline",
                "policy_version": "performance_policy_version",
            }.get(name, name)
            if key in values:
                payload[name] = values[key]
        return cls(**payload)

    def runtime_threshold(self, stage_name: str) -> tuple[str, float, float] | None:
        mapping = {
            "weekly_stage": ("weekly_stage_runtime", self.weekly_stage_warn_seconds, self.weekly_stage_fail_seconds),
            "scan_router": ("scan_router_runtime", self.scan_router_warn_seconds, self.scan_router_fail_seconds),
            "investigator": ("investigator_runtime", self.investigator_warn_seconds, self.investigator_fail_seconds),
            "opportunities": ("opportunities_runtime", self.opportunities_warn_seconds, self.opportunities_fail_seconds),
            "pipeline": ("total_shadow_pipeline_runtime", self.total_shadow_pipeline_warn_seconds, self.total_shadow_pipeline_fail_seconds),
        }
        return mapping.get(stage_name)


@dataclass(frozen=True, slots=True)
class PerformanceMetric:
    run_id: str
    stage_name: str
    operation_name: str
    started_at: datetime
    completed_at: datetime
    duration_ms: float
    rows_in: int | None = None
    rows_out: int | None = None
    symbols_in: int | None = None
    symbols_out: int | None = None
    rows_per_second: float | None = None
    symbols_per_second: float | None = None
    db_read_ms: float | None = None
    db_write_ms: float | None = None
    artifact_write_ms: float | None = None
    process_peak_rss_mb: float | None = None
    rss_delta_mb: float | None = None
    cache_mode: CacheMode = CacheMode.UNKNOWN
    replay_mode: ReplayMode = ReplayMode.FIRST_RUN
    status: PerformanceStatus = PerformanceStatus.NOT_EVALUATED
    threshold_name: str | None = None
    threshold_value: float | None = None
    policy_version: str = PERFORMANCE_POLICY_VERSION
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))
        if self.duration_ms < 0:
            raise ValueError("duration_ms must be non-negative")

    def as_row(self) -> dict[str, Any]:
        row = {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
            if name != "metadata"
        }
        row["cache_mode"] = self.cache_mode.value
        row["replay_mode"] = self.replay_mode.value
        row["status"] = self.status.value
        row["metadata"] = json.dumps(dict(self.metadata), sort_keys=True, default=str)
        return row


@dataclass(frozen=True, slots=True)
class ArtifactPerformanceMetric:
    artifact_name: str
    path: str
    row_count: int | None
    column_count: int | None
    size_bytes: int | None
    write_duration_ms: float | None
    content_hash: str | None
    presence_status: str = "present"
    performance_status: str = PerformanceStatus.NOT_EVALUATED.value


@dataclass(frozen=True, slots=True)
class DatabasePerformanceMetric:
    stage_name: str
    operation_name: str
    connection_open_ms: float = 0.0
    query_count: int = 0
    read_query_count: int = 0
    write_query_count: int = 0
    transaction_count: int = 0
    commit_count: int = 0
    rollback_count: int = 0
    db_read_ms: float = 0.0
    db_write_ms: float = 0.0
    rows_read: int | None = None
    rows_written: int | None = None


@dataclass(slots=True)
class PerformanceSpan:
    rows_in: int | None = None
    rows_out: int | None = None
    symbols_in: int | None = None
    symbols_out: int | None = None
    db_read_ms: float | None = None
    db_write_ms: float | None = None
    artifact_write_ms: float | None = None
    metadata: dict[str, object] = field(default_factory=dict)

    def counts(
        self, *, rows_in: int | None = None, rows_out: int | None = None,
        symbols_in: int | None = None, symbols_out: int | None = None,
    ) -> None:
        self.rows_in = rows_in if rows_in is not None else self.rows_in
        self.rows_out = rows_out if rows_out is not None else self.rows_out
        self.symbols_in = symbols_in if symbols_in is not None else self.symbols_in
        self.symbols_out = symbols_out if symbols_out is not None else self.symbols_out


class PerformanceCollector:
    """In-memory, failure-isolated collector persisted as attempt/run artifacts."""

    def __init__(
        self, *, run_id: str, as_of: str, config: PerformanceConfig | None = None,
        cache_mode: CacheMode = CacheMode.UNKNOWN,
        replay_mode: ReplayMode = ReplayMode.FIRST_RUN,
    ) -> None:
        self.run_id = run_id
        self.as_of = as_of
        self.config = config or PerformanceConfig()
        self.cache_mode = cache_mode
        self.replay_mode = replay_mode
        self.metrics: list[PerformanceMetric] = []
        self.artifact_metrics: list[ArtifactPerformanceMetric] = []
        self.database_metrics: list[DatabasePerformanceMetric] = []
        self.errors: list[str] = []
        self._depth = 0

    @contextmanager
    def timer(
        self, *, stage_name: str, operation_name: str,
        rows_in: int | None = None, symbols_in: int | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> Iterator[PerformanceSpan]:
        span = PerformanceSpan(rows_in=rows_in, symbols_in=symbols_in, metadata=dict(metadata or {}))
        if not self.config.enabled:
            yield span
            return
        started_wall = datetime.now(timezone.utc)
        started_ns = time.perf_counter_ns()
        rss_before, _ = process_memory_mb()
        self._depth += 1
        span.metadata.setdefault("nested_depth", self._depth)
        error: BaseException | None = None
        try:
            yield span
        except BaseException as exc:
            error = exc
            span.metadata["exception_class"] = exc.__class__.__name__
            span.metadata["operation_failed"] = True
            raise
        finally:
            completed_ns = time.perf_counter_ns()
            completed_wall = datetime.now(timezone.utc)
            rss_after, peak = process_memory_mb()
            duration_ms = max((completed_ns - started_ns) / 1_000_000.0, 0.0)
            status, threshold_name, threshold_value = self._evaluate(
                stage_name=stage_name,
                operation_name=operation_name,
                duration_ms=duration_ms,
                symbols_out=span.symbols_out,
            )
            if error is not None:
                status = PerformanceStatus.FAIL
            try:
                self.metrics.append(
                    PerformanceMetric(
                        run_id=self.run_id,
                        stage_name=stage_name,
                        operation_name=operation_name,
                        started_at=started_wall,
                        completed_at=completed_wall,
                        duration_ms=duration_ms,
                        rows_in=span.rows_in,
                        rows_out=span.rows_out,
                        symbols_in=span.symbols_in,
                        symbols_out=span.symbols_out,
                        rows_per_second=_throughput(span.rows_out, duration_ms),
                        symbols_per_second=_throughput(span.symbols_out, duration_ms),
                        db_read_ms=span.db_read_ms,
                        db_write_ms=span.db_write_ms,
                        artifact_write_ms=span.artifact_write_ms,
                        process_peak_rss_mb=peak,
                        rss_delta_mb=(max(rss_after - rss_before, 0.0) if rss_after is not None and rss_before is not None else None),
                        cache_mode=self.cache_mode,
                        replay_mode=self.replay_mode,
                        status=status,
                        threshold_name=threshold_name,
                        threshold_value=threshold_value,
                        policy_version=self.config.policy_version,
                        metadata=span.metadata,
                    )
                )
            except Exception as exc:  # instrumentation must not mask functional errors
                self.errors.append(f"metric_record_failed:{exc.__class__.__name__}:{exc}")
            self._depth -= 1

    @contextmanager
    def database_operation(
        self, *, stage_name: str, operation_name: str, write: bool = False,
        query_count: int = 1, transaction: bool = False,
    ) -> Iterator[PerformanceSpan]:
        started = time.perf_counter_ns()
        span = PerformanceSpan()
        failed = False
        try:
            yield span
        except BaseException:
            failed = True
            raise
        finally:
            duration = max((time.perf_counter_ns() - started) / 1_000_000.0, 0.0)
            self.database_metrics.append(
                DatabasePerformanceMetric(
                    stage_name=stage_name,
                    operation_name=operation_name,
                    query_count=query_count,
                    read_query_count=0 if write else query_count,
                    write_query_count=query_count if write else 0,
                    transaction_count=int(transaction),
                    commit_count=int(transaction and not failed),
                    rollback_count=int(transaction and failed),
                    db_read_ms=0.0 if write else duration,
                    db_write_ms=duration if write else 0.0,
                    rows_read=span.rows_out if not write else None,
                    rows_written=span.rows_out if write else None,
                )
            )

    def record_artifact(
        self, artifact: StageArtifact, *, column_count: int | None = None,
        write_duration_ms: float | None = None,
    ) -> None:
        try:
            path = Path(artifact.uri)
            if any(item.path == str(path) for item in self.artifact_metrics):
                return
            self.artifact_metrics.append(
                ArtifactPerformanceMetric(
                    artifact_name=artifact.artifact_type,
                    path=str(path),
                    row_count=artifact.row_count,
                    column_count=column_count,
                    size_bytes=path.stat().st_size if path.exists() else None,
                    write_duration_ms=write_duration_ms,
                    content_hash=artifact.content_hash,
                    presence_status="present" if path.exists() else "missing",
                    performance_status=(
                        evaluate_upper(
                            path.stat().st_size / (1024 * 1024),
                            self.config.max_artifact_size_warn_mb,
                            self.config.max_artifact_size_fail_mb,
                        ).value
                        if path.exists() and self.config.threshold_evaluation_enabled
                        else PerformanceStatus.NOT_EVALUATED.value
                    ),
                )
            )
        except Exception as exc:
            self.errors.append(f"artifact_metric_failed:{exc.__class__.__name__}:{exc}")

    def record_database_metric(self, metric: DatabasePerformanceMetric) -> None:
        try:
            if self.config.enabled:
                self.database_metrics.append(metric)
        except Exception as exc:
            self.errors.append(f"database_metric_failed:{exc.__class__.__name__}:{exc}")

    def record_duration(
        self, *, stage_name: str, operation_name: str, duration_ms: float,
        rows_in: int | None = None, rows_out: int | None = None,
        symbols_in: int | None = None, symbols_out: int | None = None,
        db_read_ms: float | None = None, db_write_ms: float | None = None,
        artifact_write_ms: float | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        """Record a duration already measured by an existing implementation."""
        try:
            self._record_duration(
                stage_name=stage_name, operation_name=operation_name,
                duration_ms=duration_ms, rows_in=rows_in, rows_out=rows_out,
                symbols_in=symbols_in, symbols_out=symbols_out,
                db_read_ms=db_read_ms, db_write_ms=db_write_ms,
                artifact_write_ms=artifact_write_ms, metadata=metadata,
            )
        except Exception as exc:
            self.errors.append(f"metric_record_failed:{exc.__class__.__name__}:{exc}")

    def _record_duration(
        self, *, stage_name: str, operation_name: str, duration_ms: float,
        rows_in: int | None = None, rows_out: int | None = None,
        symbols_in: int | None = None, symbols_out: int | None = None,
        db_read_ms: float | None = None, db_write_ms: float | None = None,
        artifact_write_ms: float | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        if not self.config.enabled:
            return
        completed = datetime.now(timezone.utc)
        started = completed - timedelta(milliseconds=max(duration_ms, 0.0))
        _, peak = process_memory_mb()
        status, threshold_name, threshold_value = self._evaluate(
            stage_name=stage_name,
            operation_name=operation_name,
            duration_ms=max(duration_ms, 0.0),
            symbols_out=symbols_out,
        )
        self.metrics.append(
            PerformanceMetric(
                run_id=self.run_id, stage_name=stage_name,
                operation_name=operation_name, started_at=started,
                completed_at=completed, duration_ms=max(duration_ms, 0.0),
                rows_in=rows_in, rows_out=rows_out, symbols_in=symbols_in,
                symbols_out=symbols_out,
                rows_per_second=_throughput(rows_out, duration_ms),
                symbols_per_second=_throughput(symbols_out, duration_ms),
                db_read_ms=db_read_ms, db_write_ms=db_write_ms,
                artifact_write_ms=artifact_write_ms,
                process_peak_rss_mb=peak, cache_mode=self.cache_mode,
                replay_mode=self.replay_mode, status=status,
                threshold_name=threshold_name, threshold_value=threshold_value,
                policy_version=self.config.policy_version,
                metadata=dict(metadata or {}),
            )
        )

    def stage_status(self, stage_name: str) -> PerformanceStatus:
        statuses = [metric.status for metric in self.metrics if metric.stage_name == stage_name]
        return _worst_status(statuses)

    def summary(self) -> dict[str, Any]:
        pipeline_totals = [metric.duration_ms for metric in self.metrics if metric.operation_name == "pipeline.total"]
        total_ms = pipeline_totals[-1] if pipeline_totals else sum(
            metric.duration_ms for metric in self.metrics
            if metric.operation_name.endswith(".total")
        )
        peak_values = [metric.process_peak_rss_mb for metric in self.metrics if metric.process_peak_rss_mb is not None]
        peak_status = (
            evaluate_upper(
                max(peak_values), self.config.peak_rss_warn_mb,
                self.config.peak_rss_fail_mb,
            )
            if peak_values and self.config.threshold_evaluation_enabled
            else PerformanceStatus.NOT_EVALUATED
        )
        artifact_statuses = [
            PerformanceStatus(item.performance_status) for item in self.artifact_metrics
        ]
        stage_metrics: dict[str, dict[str, Any]] = {}
        for stage in sorted({metric.stage_name for metric in self.metrics}):
            items = [metric for metric in self.metrics if metric.stage_name == stage]
            stage_metrics[stage] = {
                "duration_ms": sum(item.duration_ms for item in items if item.operation_name.endswith(".total"))
                or sum(item.duration_ms for item in items),
                "performance_status": self.stage_status(stage).value,
                "operations": len(items),
            }
        return {
            "run_id": self.run_id,
            "as_of": self.as_of,
            "policy_version": self.config.policy_version,
            "cache_mode": self.cache_mode.value,
            "replay_mode": self.replay_mode.value,
            "total_runtime_ms": total_ms,
            "peak_rss_mb": max(peak_values) if peak_values else None,
            "stage_metrics": stage_metrics,
            "database_metrics": [asdict(item) for item in self.database_metrics],
            "artifact_metrics": [asdict(item) for item in self.artifact_metrics],
            "symbols_processed": sum(item.symbols_out or 0 for item in self.metrics),
            "rows_processed": sum(item.rows_out or 0 for item in self.metrics),
            "rows_persisted": sum(item.rows_written or 0 for item in self.database_metrics),
            "threshold_evaluations": _status_counts(
                [item.status for item in self.metrics]
                + [peak_status]
                + artifact_statuses
            ),
            "resource_thresholds": {
                "peak_rss": peak_status.value,
                "artifact_size": _worst_status(artifact_statuses).value,
            },
            "warnings": [item.operation_name for item in self.metrics if item.status is PerformanceStatus.WARN],
            "failures": [item.operation_name for item in self.metrics if item.status is PerformanceStatus.FAIL],
            "instrumentation_errors": list(self.errors),
            "functional_status": "UNCHANGED",
            "performance_status": _worst_status(
                [item.status for item in self.metrics] + [peak_status] + artifact_statuses
            ).value,
            "output_equivalence": None,
            "replay_equivalence": None,
        }

    def write_artifacts(
        self, output_dir: Path, *, replay_comparison: Mapping[str, Any] | None = None,
    ) -> tuple[StageArtifact, ...]:
        output_dir.mkdir(parents=True, exist_ok=True)
        metrics_path = output_dir / "phase3c4_performance_metrics.csv"
        artifact_path = output_dir / "phase3c4_artifact_metrics.csv"
        database_path = output_dir / "phase3c4_database_metrics.csv"
        summary_path = output_dir / "phase3c4_performance_summary.json"
        replay_path = output_dir / "phase3c4_replay_comparison.json"
        _write_rows(metrics_path, [item.as_row() for item in self.metrics])
        _write_rows(artifact_path, [asdict(item) for item in self.artifact_metrics])
        _write_rows(database_path, [asdict(item) for item in self.database_metrics])
        summary_path.write_text(json.dumps(self.summary(), indent=2, sort_keys=True, default=str), encoding="utf-8")
        replay_path.write_text(
            json.dumps(dict(replay_comparison or _empty_replay()), indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        return tuple(
            StageArtifact.from_file(name, path, row_count=count, attempt_number=1)
            for name, path, count in (
                ("phase3c4_performance_metrics", metrics_path, len(self.metrics)),
                ("phase3c4_performance_summary", summary_path, None),
                ("phase3c4_artifact_metrics", artifact_path, len(self.artifact_metrics)),
                ("phase3c4_database_metrics", database_path, len(self.database_metrics)),
                ("phase3c4_replay_comparison", replay_path, None),
            )
        )

    def _evaluate(
        self, *, stage_name: str, operation_name: str, duration_ms: float,
        symbols_out: int | None,
    ) -> tuple[PerformanceStatus, str | None, float | None]:
        if not self.config.threshold_evaluation_enabled:
            return PerformanceStatus.NOT_EVALUATED, None, None
        threshold = self.config.runtime_threshold(stage_name) if operation_name.endswith(".total") else None
        if operation_name == "weekly_stage.aggregate_sector_stages":
            threshold = (
                "sector_aggregation_runtime",
                self.config.sector_aggregation_warn_seconds,
                self.config.sector_aggregation_fail_seconds,
            )
        if threshold:
            name, warn, fail = threshold
            status = evaluate_upper(duration_ms / 1000.0, warn, fail)
            return status, name, fail if status is PerformanceStatus.FAIL else warn
        if symbols_out is not None and duration_ms > 0:
            value = _throughput(symbols_out, duration_ms) or 0.0
            status = evaluate_lower(
                value,
                self.config.min_symbols_per_second_warn,
                self.config.min_symbols_per_second_fail,
            )
            return status, "min_symbols_per_second", self.config.min_symbols_per_second_warn
        return PerformanceStatus.NOT_EVALUATED, None, None


def evaluate_upper(value: float, warn: float, fail: float) -> PerformanceStatus:
    if value >= fail:
        return PerformanceStatus.FAIL
    if value >= warn:
        return PerformanceStatus.WARN
    return PerformanceStatus.PASS


def evaluate_lower(value: float, warn: float, fail: float) -> PerformanceStatus:
    if value <= fail:
        return PerformanceStatus.FAIL
    if value < warn:
        return PerformanceStatus.WARN
    return PerformanceStatus.PASS


def process_memory_mb(
    *, raw_rss: float | None = None, system: str | None = None,
) -> tuple[float | None, float | None]:
    """Return current RSS when available and process-lifetime peak RSS in MB."""
    current: float | None = None
    try:
        statm = Path("/proc/self/statm")
        if statm.exists():
            pages = int(statm.read_text(encoding="utf-8").split()[1])
            current = pages * os.sysconf("SC_PAGE_SIZE") / (1024 * 1024)
    except (OSError, ValueError, IndexError):
        current = None
    try:
        if raw_rss is None:
            import resource

            raw_rss = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        name = system or platform.system()
        peak = raw_rss / (1024 * 1024) if name == "Darwin" else raw_rss / 1024
        return current if current is not None else peak, max(peak, 0.0)
    except (ImportError, OSError, ValueError):
        return current, None


def compare_semantic_outputs(
    left: Mapping[str, Any], right: Mapping[str, Any],
    *, ignored_fields: tuple[str, ...] = (
        "run_id", "started_at", "completed_at", "duration_ms", "peak_rss_mb",
        "rss_delta_mb", "path", "temporary_path",
    ),
) -> dict[str, Any]:
    normalized_left = _normalize(left, set(ignored_fields))
    normalized_right = _normalize(right, set(ignored_fields))
    differences = [] if normalized_left == normalized_right else [
        {"left": normalized_left, "right": normalized_right}
    ]
    return {
        "equivalent": not differences,
        "differences": differences,
        "ignored_fields": list(ignored_fields),
        "artifact_hash_matches": _optional_match(_hash_map(left), _hash_map(right)),
        "decision_identity_matches": _optional_match(_identity_values(left), _identity_values(right)),
        "opportunity_identity_matches": _optional_match(_candidate_values(left), _candidate_values(right)),
    }


def compare_benchmark_summary(
    current: Mapping[str, Any], baseline: Mapping[str, Any],
    *, warn_pct: float = 10.0, fail_pct: float = 25.0,
) -> dict[str, Any]:
    comparisons: dict[str, Any] = {}
    for key, lower_is_better in (
        ("total_runtime_ms", True), ("peak_rss_mb", True),
        ("symbols_per_second", False), ("db_total_ms", True),
        ("artifact_size_bytes", True),
    ):
        current_value = _number(current.get(key))
        baseline_value = _number(baseline.get(key))
        if current_value is None or baseline_value in {None, 0.0}:
            continue
        change = ((current_value - baseline_value) / baseline_value) * 100.0
        regression = change if lower_is_better else -change
        comparisons[key] = {
            "current": current_value,
            "baseline": baseline_value,
            "change_pct": change,
            "status": evaluate_upper(max(regression, 0.0), warn_pct, fail_pct).value,
        }
    return {
        "comparisons": comparisons,
        "status": _worst_status(
            PerformanceStatus(item["status"]) for item in comparisons.values()
        ).value,
        "warn_pct": warn_pct,
        "fail_pct": fail_pct,
    }


def descriptive_statistics(values: list[float]) -> dict[str, Any]:
    if not values:
        return {}
    ordered = sorted(values)
    mean = statistics.fmean(ordered)
    result = {
        "minimum": ordered[0],
        "maximum": ordered[-1],
        "median": statistics.median(ordered),
        "p50": _percentile(ordered, 0.50),
        "coefficient_of_variation": (statistics.pstdev(ordered) / mean if len(ordered) > 1 and mean else 0.0),
        "sample_count": len(ordered),
        "percentiles_descriptive_only": len(ordered) < 10,
    }
    if len(ordered) >= 3:
        result["p90"] = _percentile(ordered, 0.90)
    return result


def _throughput(count: int | None, duration_ms: float) -> float | None:
    if count is None or duration_ms <= 0:
        return None
    return float(count) / (duration_ms / 1000.0)


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row}) or ["status"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _worst_status(statuses: Any) -> PerformanceStatus:
    order = {
        PerformanceStatus.NOT_EVALUATED: 0,
        PerformanceStatus.PASS: 1,
        PerformanceStatus.WARN: 2,
        PerformanceStatus.FAIL: 3,
    }
    values = list(statuses)
    return max(values, key=order.get) if values else PerformanceStatus.NOT_EVALUATED


def _status_counts(statuses: Any) -> dict[str, int]:
    result = {status.value: 0 for status in PerformanceStatus}
    for status in statuses:
        result[status.value] += 1
    return result


def _normalize(value: Any, ignored: set[str]) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _normalize(item, ignored)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key) not in ignored
        }
    if isinstance(value, (list, tuple)):
        return [_normalize(item, ignored) for item in value]
    return value


def _hash_map(value: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): item for key, item in value.items() if "hash" in str(key)}


def _identity_values(value: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(item for key, item in sorted(value.items()) if "decision_id" in str(key))


def _candidate_values(value: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(
        item for key, item in sorted(value.items())
        if any(token in str(key) for token in ("candidate_id", "setup_id", "opportunity_id"))
    )


def _optional_match(left: Any, right: Any) -> bool | None:
    if not left and not right:
        return None
    return left == right


def _number(value: Any) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _percentile(values: list[float], fraction: float) -> float:
    if len(values) == 1:
        return values[0]
    position = (len(values) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return values[lower]
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def _empty_replay() -> dict[str, Any]:
    return {
        "equivalent": None,
        "differences": [],
        "ignored_fields": [],
        "artifact_hash_matches": None,
        "decision_identity_matches": None,
        "opportunity_identity_matches": None,
    }
