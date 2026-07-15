"""Opaque, filter-bound cursor pagination."""

from __future__ import annotations

import base64
import hashlib
import json
from typing import Any, Mapping

from .errors import Phase4ApiError


def filter_hash(filters: Mapping[str, Any]) -> str:
    payload = json.dumps(dict(filters), sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def encode_cursor(*, sort: str, order: str, last_key: str, filters: Mapping[str, Any]) -> str:
    payload = {"v": 1, "sort": sort, "order": order, "last_key": last_key, "filters": filter_hash(filters)}
    return base64.urlsafe_b64encode(json.dumps(payload, sort_keys=True).encode()).decode().rstrip("=")


def decode_cursor(cursor: str, *, sort: str, order: str, filters: Mapping[str, Any]) -> str:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode())
    except Exception as exc:
        raise Phase4ApiError("INVALID_ARGUMENT", "Invalid pagination cursor", 400) from exc
    if payload.get("v") != 1 or payload.get("sort") != sort or payload.get("order") != order or payload.get("filters") != filter_hash(filters):
        raise Phase4ApiError("INVALID_ARGUMENT", "Cursor does not match this query", 400)
    return str(payload.get("last_key") or "")

