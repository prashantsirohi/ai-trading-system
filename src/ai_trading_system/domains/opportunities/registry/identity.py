"""Deterministic identities and semantic hashes for registry records."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from ai_trading_system.domains.opportunities.serialization import to_dict


def normalize_exchange(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if not normalized:
        raise ValueError("exchange must be non-empty")
    return normalized


def normalize_symbol(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if not normalized:
        raise ValueError("symbol_id must be non-empty")
    return normalized


def normalize_setup_family(value: str) -> str:
    normalized = "_".join(str(value or "").strip().lower().replace("-", "_").split())
    if not normalized:
        raise ValueError("setup_family must be non-empty")
    return normalized


def require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def utc_iso(value: datetime) -> str:
    require_aware(value, "datetime")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    return json.dumps(to_dict(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def stable_digest(value: Any) -> str:
    payload = canonical_json(value).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def make_setup_id(
    *, exchange: str, symbol_id: str, setup_family: str, admission_identity: str, episode_started_at: datetime
) -> str:
    if not str(admission_identity or "").strip():
        raise ValueError("admission_identity must be non-empty")
    digest = stable_digest(
        {
            "exchange": normalize_exchange(exchange),
            "symbol_id": normalize_symbol(symbol_id),
            "setup_family": normalize_setup_family(setup_family),
            "admission_identity": str(admission_identity).strip(),
            "episode_started_at": utc_iso(episode_started_at),
        }
    )
    return f"setup_{digest}"


def make_candidate_id(setup_id: str) -> str:
    if not str(setup_id or "").strip():
        raise ValueError("setup_id must be non-empty")
    return f"candidate_{stable_digest({'setup_id': setup_id})}"


def make_record_identity(
    *,
    candidate_id: str,
    record_type: str,
    as_of: datetime,
    run_id: str,
    stage_attempt: int,
    source_artifact_hash: str,
    contract_version: str,
    semantic_payload: Any,
) -> tuple[str, str, str]:
    semantic_hash = stable_digest(semantic_payload)
    identity = {
        "candidate_id": candidate_id,
        "record_type": record_type,
        "as_of": utc_iso(as_of),
        "run_id": run_id,
        "stage_attempt": stage_attempt,
        "source_artifact_hash": source_artifact_hash,
        "contract_version": contract_version,
    }
    idempotency_key = stable_digest(identity)
    return f"{record_type}_{idempotency_key}", idempotency_key, semantic_hash
