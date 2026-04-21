"""Persistent execution store for orders, fills, and trade journal notes."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, List, Optional

import duckdb

from ai_trading_system.domains.execution.models import FillRecord, OrderRecord


def _load_json(value: str | None) -> dict:
    if not value:
        return {}
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return {}


class ExecutionStore:
    """Lightweight DuckDB-backed store for execution events."""

    def __init__(self, project_root: Path | str, db_path: Optional[Path | str] = None):
        self.project_root = Path(project_root)
        self.db_path = Path(db_path) if db_path else self.project_root / "data" / "execution.duckdb"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path))

    def _init_db(self) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_order (
                    order_id TEXT PRIMARY KEY,
                    broker TEXT NOT NULL,
                    symbol_id TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    side TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    product_type TEXT NOT NULL,
                    validity TEXT NOT NULL,
                    status TEXT NOT NULL,
                    submitted_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    correlation_id TEXT,
                    broker_order_id TEXT,
                    limit_price DOUBLE,
                    stop_price DOUBLE,
                    requested_price DOUBLE,
                    avg_fill_price DOUBLE,
                    filled_quantity INTEGER NOT NULL,
                    metadata_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_fill (
                    fill_id TEXT PRIMARY KEY,
                    order_id TEXT NOT NULL,
                    broker TEXT NOT NULL,
                    symbol_id TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    price DOUBLE NOT NULL,
                    filled_at TIMESTAMP NOT NULL,
                    side TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    broker_fill_id TEXT,
                    metadata_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_trade_note (
                    trade_ref TEXT PRIMARY KEY,
                    symbol_id TEXT,
                    exchange TEXT,
                    thesis TEXT,
                    setup_note TEXT,
                    exit_note TEXT,
                    lesson_learned TEXT,
                    tags TEXT,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    metadata_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_position_stop (
                    position_key TEXT PRIMARY KEY,
                    symbol_id TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    entry_price DOUBLE NOT NULL,
                    stop_price DOUBLE NOT NULL,
                    atr_multiplier DOUBLE NOT NULL,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    metadata_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_drawdown (
                    run_id TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    snapshot_type TEXT NOT NULL DEFAULT 'intraday',
                    portfolio_value DOUBLE NOT NULL,
                    peak_value DOUBLE NOT NULL,
                    drawdown_pct DOUBLE NOT NULL,
                    portfolio_heat DOUBLE,
                    metadata_json TEXT,
                    PRIMARY KEY (run_id, timestamp, snapshot_type)
                )
                """
            )
        finally:
            conn.close()

    def upsert_order(self, order: OrderRecord) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO execution_order (
                    order_id, broker, symbol_id, quantity, side, exchange, order_type,
                    product_type, validity, status, submitted_at, updated_at,
                    correlation_id, broker_order_id, limit_price, stop_price,
                    requested_price, avg_fill_price, filled_quantity, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    broker = excluded.broker,
                    symbol_id = excluded.symbol_id,
                    quantity = excluded.quantity,
                    side = excluded.side,
                    exchange = excluded.exchange,
                    order_type = excluded.order_type,
                    product_type = excluded.product_type,
                    validity = excluded.validity,
                    status = excluded.status,
                    submitted_at = excluded.submitted_at,
                    updated_at = excluded.updated_at,
                    correlation_id = excluded.correlation_id,
                    broker_order_id = excluded.broker_order_id,
                    limit_price = excluded.limit_price,
                    stop_price = excluded.stop_price,
                    requested_price = excluded.requested_price,
                    avg_fill_price = excluded.avg_fill_price,
                    filled_quantity = excluded.filled_quantity,
                    metadata_json = excluded.metadata_json
                """,
                [
                    order.order_id,
                    order.broker,
                    order.symbol_id,
                    order.quantity,
                    order.side,
                    order.exchange,
                    order.order_type,
                    order.product_type,
                    order.validity,
                    order.status,
                    order.submitted_at,
                    order.updated_at,
                    order.correlation_id,
                    order.broker_order_id,
                    order.limit_price,
                    order.stop_price,
                    order.requested_price,
                    order.avg_fill_price,
                    order.filled_quantity,
                    json.dumps(order.metadata or {}, sort_keys=True),
                ],
            )
        finally:
            conn.close()

    def append_fills(self, fills: Iterable[FillRecord]) -> None:
        rows = list(fills)
        if not rows:
            return
        conn = self._connect()
        try:
            for fill in rows:
                conn.execute(
                    """
                    INSERT INTO execution_fill (
                        fill_id, order_id, broker, symbol_id, quantity, price,
                        filled_at, side, exchange, broker_fill_id, metadata_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(fill_id) DO NOTHING
                    """,
                    [
                        fill.fill_id,
                        fill.order_id,
                        fill.broker,
                        fill.symbol_id,
                        fill.quantity,
                        fill.price,
                        fill.filled_at,
                        fill.side,
                        fill.exchange,
                        fill.broker_fill_id,
                        json.dumps(fill.metadata or {}, sort_keys=True),
                    ],
                )
        finally:
            conn.close()

    def get_order(self, order_id: str) -> Optional[OrderRecord]:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT *
                FROM execution_order
                WHERE order_id = ?
                """,
                [order_id],
            ).fetchone()
            if row is None:
                return None
            columns = [item[0] for item in conn.description]
            return self._order_from_row(dict(zip(columns, row)))
        finally:
            conn.close()

    def list_orders(self) -> List[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT *
                FROM execution_order
                ORDER BY submitted_at DESC, order_id DESC
                """
            ).fetchall()
            columns = [item[0] for item in conn.description]
            return [self._order_from_row(dict(zip(columns, row))).to_dict() for row in rows]
        finally:
            conn.close()

    def list_fills(self, order_id: Optional[str] = None) -> List[dict]:
        conn = self._connect()
        try:
            if order_id:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM execution_fill
                    WHERE order_id = ?
                    ORDER BY filled_at ASC, fill_id ASC
                    """,
                    [order_id],
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM execution_fill
                    ORDER BY filled_at DESC, fill_id DESC
                    """
                ).fetchall()
            columns = [item[0] for item in conn.description]
            return [self._fill_from_row(dict(zip(columns, row))).to_dict() for row in rows]
        finally:
            conn.close()

    def list_position_snapshot(self) -> List[dict]:
        """Return simple net position state derived from fills."""
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT
                    symbol_id,
                    exchange,
                    SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) AS net_quantity,
                    MAX(filled_at) AS last_filled_at
                FROM execution_fill
                GROUP BY 1, 2
                HAVING SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) <> 0
                ORDER BY symbol_id
                """
            ).fetchall()
            return [
                {
                    "symbol_id": row[0],
                    "exchange": row[1],
                    "net_quantity": int(row[2]),
                    "last_filled_at": _coerce_dt(row[3]).isoformat() if row[3] is not None else None,
                }
                for row in rows
            ]
        finally:
            conn.close()

    def upsert_trade_note(
        self,
        *,
        trade_ref: str,
        symbol_id: str | None = None,
        exchange: str | None = None,
        thesis: str | None = None,
        setup_note: str | None = None,
        exit_note: str | None = None,
        lesson_learned: str | None = None,
        tags: str | list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        normalized_ref = str(trade_ref or "").strip()
        if not normalized_ref:
            raise ValueError("trade_ref is required")

        now = datetime.now(timezone.utc)
        existing = self.get_trade_note(normalized_ref) or {}
        tags_value = tags
        if isinstance(tags_value, list):
            tags_value = ", ".join(str(item).strip() for item in tags_value if str(item).strip())
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO execution_trade_note (
                    trade_ref, symbol_id, exchange, thesis, setup_note, exit_note,
                    lesson_learned, tags, created_at, updated_at, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_ref) DO UPDATE SET
                    symbol_id = excluded.symbol_id,
                    exchange = excluded.exchange,
                    thesis = excluded.thesis,
                    setup_note = excluded.setup_note,
                    exit_note = excluded.exit_note,
                    lesson_learned = excluded.lesson_learned,
                    tags = excluded.tags,
                    updated_at = excluded.updated_at,
                    metadata_json = excluded.metadata_json
                """,
                [
                    normalized_ref,
                    (symbol_id if symbol_id is not None else existing.get("symbol_id")) or None,
                    (exchange if exchange is not None else existing.get("exchange")) or None,
                    thesis if thesis is not None else existing.get("thesis"),
                    setup_note if setup_note is not None else existing.get("setup_note"),
                    exit_note if exit_note is not None else existing.get("exit_note"),
                    lesson_learned if lesson_learned is not None else existing.get("lesson_learned"),
                    tags_value if tags is not None else existing.get("tags"),
                    _coerce_dt(existing["created_at"]) if existing.get("created_at") else now,
                    now,
                    json.dumps(metadata or existing.get("metadata") or {}, sort_keys=True),
                ],
            )
        finally:
            conn.close()

    def get_trade_note(self, trade_ref: str) -> Optional[dict]:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT *
                FROM execution_trade_note
                WHERE trade_ref = ?
                """,
                [trade_ref],
            ).fetchone()
            if row is None:
                return None
            columns = [item[0] for item in conn.description]
            return self._trade_note_from_row(dict(zip(columns, row)))
        finally:
            conn.close()

    def list_trade_notes(self) -> List[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT *
                FROM execution_trade_note
                ORDER BY updated_at DESC, trade_ref DESC
                """
            ).fetchall()
            columns = [item[0] for item in conn.description]
            return [self._trade_note_from_row(dict(zip(columns, row))) for row in rows]
        finally:
            conn.close()

    def upsert_position_stop(
        self,
        *,
        position_key: str,
        symbol_id: str,
        exchange: str,
        quantity: int,
        entry_price: float,
        stop_price: float,
        atr_multiplier: float,
        status: str = "ACTIVE",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO execution_position_stop (
                    position_key, symbol_id, exchange, quantity, entry_price,
                    stop_price, atr_multiplier, status, created_at, updated_at, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(position_key) DO UPDATE SET
                    quantity = excluded.quantity,
                    entry_price = excluded.entry_price,
                    stop_price = excluded.stop_price,
                    atr_multiplier = excluded.atr_multiplier,
                    status = excluded.status,
                    updated_at = excluded.updated_at,
                    metadata_json = excluded.metadata_json
                """,
                [
                    position_key,
                    symbol_id,
                    exchange,
                    quantity,
                    entry_price,
                    stop_price,
                    atr_multiplier,
                    status,
                    now,
                    now,
                    json.dumps(metadata or {}, sort_keys=True),
                ],
            )
        finally:
            conn.close()

    def get_position_stop(self, position_key: str) -> Optional[dict]:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT *
                FROM execution_position_stop
                WHERE position_key = ?
                """,
                [position_key],
            ).fetchone()
            if row is None:
                return None
            columns = [item[0] for item in conn.description]
            return dict(zip(columns, row))
        finally:
            conn.close()

    def list_active_stops(self) -> List[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT *
                FROM execution_position_stop
                WHERE status = 'ACTIVE'
                ORDER BY symbol_id
                """
            ).fetchall()
            columns = [item[0] for item in conn.description]
            return [dict(zip(columns, row)) for row in rows]
        finally:
            conn.close()

    def deactivate_stop(self, position_key: str) -> None:
        now = datetime.now(timezone.utc)
        conn = self._connect()
        try:
            conn.execute(
                """
                UPDATE execution_position_stop
                SET status = 'INACTIVE', updated_at = ?
                WHERE position_key = ?
                """,
                [now, position_key],
            )
        finally:
            conn.close()

    def record_drawdown_snapshot(
        self,
        *,
        run_id: str,
        portfolio_value: float,
        peak_value: float,
        portfolio_heat: float | None = None,
        snapshot_type: str = "intraday",
        metadata: dict[str, Any] | None = None,
    ) -> dict:
        now = datetime.now(timezone.utc)
        drawdown_pct = ((portfolio_value - peak_value) / peak_value * 100) if peak_value > 0 else 0.0
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO execution_drawdown (
                    run_id, timestamp, snapshot_type, portfolio_value, peak_value,
                    drawdown_pct, portfolio_heat, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, timestamp, snapshot_type) DO UPDATE SET
                    portfolio_value = excluded.portfolio_value,
                    peak_value = excluded.peak_value,
                    drawdown_pct = excluded.drawdown_pct,
                    portfolio_heat = excluded.portfolio_heat,
                    metadata_json = excluded.metadata_json
                """,
                [
                    run_id,
                    now,
                    snapshot_type,
                    portfolio_value,
                    peak_value,
                    drawdown_pct,
                    portfolio_heat,
                    json.dumps(metadata or {}, sort_keys=True),
                ],
            )
        finally:
            conn.close()
        return {
            "run_id": run_id,
            "timestamp": now.isoformat(),
            "snapshot_type": snapshot_type,
            "portfolio_value": portfolio_value,
            "peak_value": peak_value,
            "drawdown_pct": round(drawdown_pct, 2),
            "portfolio_heat": portfolio_heat,
        }

    def get_latest_drawdown(self, run_id: str, snapshot_type: str | None = None) -> dict | None:
        conn = self._connect()
        try:
            if snapshot_type:
                row = conn.execute(
                    """
                    SELECT * FROM execution_drawdown
                    WHERE run_id = ? AND snapshot_type = ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    [run_id, snapshot_type],
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT * FROM execution_drawdown
                    WHERE run_id = ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    [run_id],
                ).fetchone()
            if row is None:
                return None
            columns = [item[0] for item in conn.description]
            return dict(zip(columns, row))
        finally:
            conn.close()

    def get_drawdown_history(
        self,
        run_id: str,
        snapshot_type: str | None = None,
        limit: int = 100,
    ) -> List[dict]:
        conn = self._connect()
        try:
            if snapshot_type:
                rows = conn.execute(
                    """
                    SELECT * FROM execution_drawdown
                    WHERE run_id = ? AND snapshot_type = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    [run_id, snapshot_type, limit],
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM execution_drawdown
                    WHERE run_id = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    [run_id, limit],
                ).fetchall()
            columns = [item[0] for item in conn.description]
            return [dict(zip(columns, row)) for row in rows]
        finally:
            conn.close()

    @staticmethod
    def _order_from_row(row: dict) -> OrderRecord:
        return OrderRecord(
            order_id=row["order_id"],
            broker=row["broker"],
            symbol_id=row["symbol_id"],
            quantity=int(row["quantity"]),
            side=row["side"],
            exchange=row["exchange"],
            order_type=row["order_type"],
            product_type=row["product_type"],
            validity=row["validity"],
            status=row["status"],
            submitted_at=_coerce_dt(row["submitted_at"]),
            updated_at=_coerce_dt(row["updated_at"]),
            correlation_id=row.get("correlation_id"),
            broker_order_id=row.get("broker_order_id"),
            limit_price=row.get("limit_price"),
            stop_price=row.get("stop_price"),
            requested_price=row.get("requested_price"),
            avg_fill_price=row.get("avg_fill_price"),
            filled_quantity=int(row.get("filled_quantity") or 0),
            metadata=_load_json(row.get("metadata_json")),
        )

    @staticmethod
    def _fill_from_row(row: dict) -> FillRecord:
        return FillRecord(
            fill_id=row["fill_id"],
            order_id=row["order_id"],
            broker=row["broker"],
            symbol_id=row["symbol_id"],
            quantity=int(row["quantity"]),
            price=float(row["price"]),
            filled_at=_coerce_dt(row["filled_at"]),
            side=row["side"],
            exchange=row["exchange"],
            broker_fill_id=row.get("broker_fill_id"),
            metadata=_load_json(row.get("metadata_json")),
        )

    @staticmethod
    def _trade_note_from_row(row: dict) -> dict:
        created_at = row.get("created_at")
        updated_at = row.get("updated_at")
        return {
            "trade_ref": row.get("trade_ref"),
            "symbol_id": row.get("symbol_id"),
            "exchange": row.get("exchange"),
            "thesis": row.get("thesis") or "",
            "setup_note": row.get("setup_note") or "",
            "exit_note": row.get("exit_note") or "",
            "lesson_learned": row.get("lesson_learned") or "",
            "tags": row.get("tags") or "",
            "created_at": _coerce_dt(created_at).isoformat() if created_at is not None else None,
            "updated_at": _coerce_dt(updated_at).isoformat() if updated_at is not None else None,
            "metadata": _load_json(row.get("metadata_json")),
        }


def _coerce_dt(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
