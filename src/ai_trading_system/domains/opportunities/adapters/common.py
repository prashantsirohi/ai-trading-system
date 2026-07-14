"""Shared pure adapter helpers."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time, timezone
from typing import Any, Mapping

from ai_trading_system.domains.opportunities.contracts import RiskLevel, StageConfidenceBand


def normalize_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def normalize_exchange(value: object) -> str:
    return str(value or "NSE").strip().upper()


def row_identity(row: Mapping[str, Any]) -> str:
    payload = json.dumps({str(k): _json_value(v) for k, v in sorted(row.items())}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def first(row: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        value = row.get(name)
        if value is not None and str(value).strip() not in {"", "nan", "None"}:
            return value
    return None


def as_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        number = float(value)
        return number if number == number else None
    except (TypeError, ValueError):
        return None


def as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in {"true", "1", "yes", "y", "qualified", "pass", "passed"}:
        return True
    if normalized in {"false", "0", "no", "n", "failed", "fail"}:
        return False
    return None


def as_datetime(value: Any, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, date):
        result = datetime.combine(value, time.min)
    elif value:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    else:
        result = fallback
    return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result.astimezone(timezone.utc)


def as_date(value: Any, fallback: date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)) if value else fallback


def confidence_band(score: float, *, unknown: bool = False) -> StageConfidenceBand:
    if unknown:
        return StageConfidenceBand.UNKNOWN
    if score < 50:
        return StageConfidenceBand.LOW
    if score < 65:
        return StageConfidenceBand.MEDIUM
    if score < 80:
        return StageConfidenceBand.HIGH
    return StageConfidenceBand.VERY_HIGH


def risk_level(value: Any) -> RiskLevel:
    normalized = str(value or "").strip().lower()
    return RiskLevel(normalized) if normalized in {item.value for item in RiskLevel} else RiskLevel.UNKNOWN


def text_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(str(item).strip() for item in value if str(item).strip())
    raw = str(value).strip()
    if not raw:
        return ()
    if raw.startswith("["):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return tuple(str(item).strip() for item in parsed if str(item).strip())
        except json.JSONDecodeError:
            pass
    return tuple(item.strip() for item in raw.replace(";", "|").split("|") if item.strip())
