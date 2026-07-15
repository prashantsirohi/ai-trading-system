"""Low-cardinality, process-local telemetry for the read-only API."""

from __future__ import annotations

import time
from collections import Counter, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Lock
from typing import Iterator


METRIC_LABEL_ALLOWLIST = frozenset({
    "route_template", "http_method", "status_class", "source_family", "cache_result",
})


@dataclass(slots=True)
class ApiMetrics:
    request_count: Counter[tuple[str, str]] = field(default_factory=Counter)
    request_duration_ms: dict[tuple[str, str], list[float]] = field(default_factory=lambda: defaultdict(list))
    status_code_count: Counter[tuple[str, str]] = field(default_factory=Counter)
    cache_hit_count: Counter[str] = field(default_factory=Counter)
    cache_miss_count: Counter[str] = field(default_factory=Counter)
    source_read_duration_ms: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    source_read_rows: Counter[str] = field(default_factory=Counter)
    source_unavailable_count: Counter[str] = field(default_factory=Counter)
    partial_response_count: Counter[str] = field(default_factory=Counter)
    authentication_failure_count: int = 0
    authorization_failure_count: int = 0
    rate_limit_count: int = 0
    governance_conflict_response_count: int = 0
    _lock: Lock = field(default_factory=Lock, repr=False)

    def record_request(self, route: str, method: str, status_code: int, duration_ms: float) -> None:
        status_class = f"{status_code // 100}xx"
        with self._lock:
            self.request_count[(route, method)] += 1
            self.request_duration_ms[(route, method)].append(duration_ms)
            self.status_code_count[(route, status_class)] += 1

    def record_partial(self, route: str) -> None:
        with self._lock:
            self.partial_response_count[route] += 1

    def record_source_unavailable(self, source_family: str) -> None:
        with self._lock:
            self.source_unavailable_count[source_family] += 1

    @contextmanager
    def source_read(self, source_family: str) -> Iterator[dict[str, int]]:
        started = time.monotonic_ns()
        observation = {"rows": 0}
        try:
            yield observation
        finally:
            duration = (time.monotonic_ns() - started) / 1_000_000.0
            with self._lock:
                self.source_read_duration_ms[source_family].append(duration)
                self.source_read_rows[source_family] += max(0, int(observation["rows"]))

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "request_count": dict(self.request_count),
                "request_duration_ms": {key: list(value) for key, value in self.request_duration_ms.items()},
                "status_code_count": dict(self.status_code_count),
                "cache_hit_count": dict(self.cache_hit_count),
                "cache_miss_count": dict(self.cache_miss_count),
                "source_read_duration_ms": {key: list(value) for key, value in self.source_read_duration_ms.items()},
                "source_read_rows": dict(self.source_read_rows),
                "source_unavailable_count": dict(self.source_unavailable_count),
                "partial_response_count": dict(self.partial_response_count),
                "authentication_failure_count": self.authentication_failure_count,
                "authorization_failure_count": self.authorization_failure_count,
                "rate_limit_count": self.rate_limit_count,
                "governance_conflict_response_count": self.governance_conflict_response_count,
                "label_allowlist": sorted(METRIC_LABEL_ALLOWLIST),
            }
