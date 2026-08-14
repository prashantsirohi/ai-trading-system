"""Response envelope and the point-in-time safety invariant.

Every MCP tool returns ``envelope(data, **meta)``. The envelope carries the
``as_of`` status explicitly so an agent can tell "no data at that date" apart
from "this surface cannot answer historically" apart from "here is the latest".

``assert_not_future`` is the enforcement for invariant I2: a tool that forgets
its cutoff raises instead of handing back data from after the requested date.
"""

from __future__ import annotations

import math
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd

# as_of status vocabulary. See docs/reference/mcp_tools.md.
AS_OF_LATEST = "LATEST"
AS_OF_EXACT = "EXACT"
AS_OF_NO_DATA = "NO_DATA_AS_OF"
AS_OF_UNSUPPORTED = "AS_OF_UNSUPPORTED"

AS_OF_STATUSES = frozenset(
    {AS_OF_LATEST, AS_OF_EXACT, AS_OF_NO_DATA, AS_OF_UNSUPPORTED}
)

DEFAULT_LIMIT = 250
MAX_LIMIT = 2000


class FutureDataError(RuntimeError):
    """A response would carry a row dated after the requested ``as_of``."""


def clamp_limit(
    limit: int | None,
    *,
    default: int = DEFAULT_LIMIT,
    maximum: int = MAX_LIMIT,
) -> int:
    """Clamp a caller-supplied row limit into ``[1, maximum]``."""

    if limit is None:
        return default
    try:
        value = int(limit)
    except (TypeError, ValueError):
        return default
    return max(1, min(value, maximum))


def coerce_date(value: Any) -> date | None:
    """Best-effort conversion of a stored timestamp/date/string to ``date``."""

    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if stamp is pd.NaT or pd.isna(stamp):
        return None
    return stamp.date()


def json_safe(value: Any) -> Any:
    """Convert DuckDB/pandas/numpy scalars into JSON-serializable values."""

    if value is None:
        return None
    if isinstance(value, (str, bool)):
        return value
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, pd.Timestamp)):
        if pd.isna(value):
            return None
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, np.ndarray)):
        return [json_safe(item) for item in value]
    if value is pd.NaT:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def records(frame: pd.DataFrame, *, limit: int | None = None) -> list[dict[str, Any]]:
    """Convert a DataFrame to JSON-safe records, optionally keeping the tail."""

    if frame is None or frame.empty:
        return []
    subset = frame if limit is None else frame.tail(limit)
    return [
        {str(key): json_safe(value) for key, value in row.items()}
        for row in subset.to_dict(orient="records")
    ]


def assert_not_future(
    rows: Iterable[dict[str, Any]] | None,
    as_of: str | date | None,
    date_fields: Sequence[str],
) -> None:
    """Raise if any row carries a ``date_fields`` value after ``as_of``.

    This is the point-in-time guarantee (invariant I2). It is deliberately a
    hard failure rather than a filter: a tool that silently drops leaked rows
    would hide the bug that produced them.
    """

    cutoff = coerce_date(as_of)
    if cutoff is None or not rows:
        return

    for row in rows:
        for field in date_fields:
            if field not in row:
                continue
            observed = coerce_date(row.get(field))
            if observed is not None and observed > cutoff:
                raise FutureDataError(
                    f"Row field {field!r}={observed.isoformat()} is after "
                    f"as_of={cutoff.isoformat()}; the tool's cutoff is missing "
                    "or incorrect."
                )


def envelope(
    data: Any,
    *,
    source: str,
    as_of_status: str,
    as_of_requested: str | date | None = None,
    as_of_effective: str | date | None = None,
    date_fields: Sequence[str] = (),
    notes: Sequence[str] | None = None,
    **extra_meta: Any,
) -> dict[str, Any]:
    """Build the standard tool response.

    ``date_fields`` names the row columns that carry an observation date; they
    are checked against ``as_of_requested`` before the response is returned.
    """

    if as_of_status not in AS_OF_STATUSES:
        raise ValueError(f"Unknown as_of_status: {as_of_status!r}")

    requested = coerce_date(as_of_requested)
    if as_of_requested is not None and requested is None:
        raise ValueError(
            f"Invalid as_of date: {as_of_requested!r}; expected an ISO date "
            "such as YYYY-MM-DD."
        )

    if isinstance(data, list):
        assert_not_future(data, requested, date_fields)
        row_count = len(data)
    else:
        row_count = None

    effective = coerce_date(as_of_effective)

    meta: dict[str, Any] = {
        "source": source,
        "as_of_status": as_of_status,
        "as_of_requested": requested.isoformat() if requested else None,
        "as_of_effective": effective.isoformat() if effective else None,
        "notes": list(notes or []),
    }
    if row_count is not None:
        meta["row_count"] = row_count
    meta.update({key: json_safe(value) for key, value in extra_meta.items()})

    return {"data": data, "meta": meta}


__all__ = [
    "AS_OF_EXACT",
    "AS_OF_LATEST",
    "AS_OF_NO_DATA",
    "AS_OF_STATUSES",
    "AS_OF_UNSUPPORTED",
    "DEFAULT_LIMIT",
    "MAX_LIMIT",
    "FutureDataError",
    "assert_not_future",
    "clamp_limit",
    "coerce_date",
    "envelope",
    "json_safe",
    "records",
]
