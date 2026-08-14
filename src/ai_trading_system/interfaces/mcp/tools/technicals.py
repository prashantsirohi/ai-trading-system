"""Technical indicator history from the per-symbol Parquet feature store.

Two surfaces are merged: the nine per-symbol indicator families
(``rsi``, ``sma``, ``adx``, ...) and the Phase 1 derived risk/liquidity table
``feat_phase1_symbol_features`` in ``ohlcv.duckdb``, where several analysis
questions actually land (realized volatility, beta, drawdown, liquidity score,
delivery trend).

These features are computed on the split-adjusted price basis, which is why
``get_ohlcv`` defaults to the same basis — mixing the two would compare an
indicator against prices it was never derived from.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Sequence

from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT,
    AS_OF_LATEST,
    AS_OF_NO_DATA,
    clamp_limit,
    coerce_date,
    envelope,
    json_safe,
)
from ai_trading_system.interfaces.mcp.readers import featurestore

PHASE1_TABLE = "feat_phase1_symbol_features"

DATE_FIELDS = ("date",)

_PHASE1_COLUMNS = (
    "realized_vol_20",
    "realized_vol_60",
    "beta_to_nifty_60",
    "max_drawdown_63",
    "max_drawdown_126",
    "atr_pct",
    "avg_value_traded_20",
    "liquidity_score",
    "delivery_pct_latest",
    "delivery_pct_5d_avg",
    "delivery_pct_20d_avg",
    "delivery_pct_change_5d",
    "delivery_pct_vs_20d",
    "delivery_trend_score",
)


def _phase1_rows(
    ctx: McpContext,
    symbol_id: str,
    exchange: str,
    from_date: str | date | None,
    upper: date | None,
) -> dict[str, dict[str, Any]]:
    """Phase 1 derived features keyed by ISO date, or empty when absent."""

    with ctx.ohlcv() as conn:
        exists = conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            [PHASE1_TABLE],
        ).fetchone()
        if not exists:
            return {}

        present = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = ?",
                [PHASE1_TABLE],
            ).fetchall()
        }
        columns = [column for column in _PHASE1_COLUMNS if column in present]
        if not columns:
            return {}

        clauses = ["symbol_id = ?", "exchange = ?"]
        params: list[Any] = [symbol_id, exchange]
        start = coerce_date(from_date)
        if start is not None:
            clauses.append("CAST(timestamp AS DATE) >= CAST(? AS DATE)")
            params.append(start.isoformat())
        if upper is not None:
            clauses.append("CAST(timestamp AS DATE) <= CAST(? AS DATE)")
            params.append(upper.isoformat())

        projection = ", ".join(columns)
        rows = conn.execute(
            f"SELECT CAST(timestamp AS DATE) AS date, {projection} "
            f"FROM {PHASE1_TABLE} WHERE {' AND '.join(clauses)} ORDER BY timestamp",
            params,
        ).fetchall()

    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        observed = coerce_date(row[0])
        if observed is None:
            continue
        result[observed.isoformat()] = {
            column: json_safe(value) for column, value in zip(columns, row[1:])
        }
    return result


def get_technical_features(
    ctx: McpContext,
    symbol: str,
    *,
    exchange: str = "NSE",
    families: Sequence[str] | None = None,
    from_date: str | date | None = None,
    to_date: str | date | None = None,
    as_of: str | date | None = None,
    include_phase1: bool = True,
    limit: int | None = None,
) -> dict[str, Any]:
    """Return one row per session with the requested indicator families."""

    symbol_id = ctx.normalize_symbol(symbol)
    exchange_code = ctx.resolve_exchange(exchange)
    row_limit = clamp_limit(limit)

    bounds = [value for value in (coerce_date(to_date), coerce_date(as_of)) if value]
    upper = min(bounds) if bounds else None

    frame, present = featurestore.read_symbol_features(
        ctx,
        symbol_id,
        exchange=exchange_code,
        families=families,
        from_date=from_date,
        to_date=upper,
    )

    rows: list[dict[str, Any]] = []
    for record in frame.to_dict(orient="records"):
        observed = coerce_date(record.pop("timestamp", None))
        if observed is None:
            continue
        row = {"date": observed.isoformat()}
        row.update(
            {
                key: json_safe(value)
                for key, value in record.items()
                if key not in {"symbol_id", "exchange"}
            }
        )
        rows.append(row)

    phase1_present = False
    if include_phase1:
        phase1 = _phase1_rows(ctx, symbol_id, exchange_code, from_date, upper)
        phase1_present = bool(phase1)
        if phase1:
            known = {row["date"] for row in rows}
            for row in rows:
                row.update(phase1.get(row["date"], {}))
            # Sessions that only Phase 1 covers still deserve a row.
            for day in sorted(set(phase1) - known):
                rows.append({"date": day, **phase1[day]})
            rows.sort(key=lambda item: item["date"])

    truncated = len(rows) > row_limit
    if truncated:
        rows = rows[-row_limit:]

    notes: list[str] = []
    if truncated:
        notes.append(
            f"Truncated to the most recent {row_limit} sessions; raise 'limit' "
            "or narrow the date window for more."
        )
    if not present:
        notes.append(
            f"No technical feature partitions exist for {symbol_id} on "
            f"{exchange_code} under {ctx.feature_store_dir}."
        )
    missing = sorted(set(featurestore.resolve_families(families)) - set(present))
    if present and missing:
        notes.append(
            f"These families have no partition for this symbol and were "
            f"skipped: {missing}."
        )

    if as_of is None:
        status = AS_OF_LATEST
    elif rows:
        status = AS_OF_EXACT
    else:
        status = AS_OF_NO_DATA

    return envelope(
        rows,
        source=f"feature_store/<family>/{exchange_code}/{symbol_id}.parquet",
        as_of_status=status,
        as_of_requested=as_of,
        as_of_effective=rows[-1]["date"] if rows else None,
        date_fields=DATE_FIELDS,
        notes=notes,
        symbol=symbol_id,
        exchange=exchange_code,
        families=present,
        phase1_included=phase1_present,
        price_basis="adjusted",
        truncated=truncated,
        data_domain=ctx.paths.domain,
    )


__all__ = ["PHASE1_TABLE", "get_technical_features"]
