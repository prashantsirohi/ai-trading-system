"""Canonical serialization and deterministic journal identities."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any


def canonical_json(value: Any) -> str:
    def default(item: Any) -> str:
        if isinstance(item, (date, datetime)):
            return item.isoformat()
        if isinstance(item, Decimal):
            return format(item, "f")
        raise TypeError(type(item).__name__)

    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=default, ensure_ascii=False
    )


def stable_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha256(canonical_json(parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:32]}"


def is_valid_isin(value: str | None) -> bool:
    text = str(value or "").strip().upper()
    return (
        len(text) == 12
        and text[:2] == "IN"
        and text[:-1].isalnum()
        and text[-1].isdigit()
    )
