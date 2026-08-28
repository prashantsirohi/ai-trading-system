from __future__ import annotations

import hashlib
from collections import defaultdict
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import duckdb

from ai_trading_system.domains.research_screener.store import content_hash

from .models import AnnouncementRecord


OFFICIAL_SOURCES = ("nse_rss", "bse_corp", "nse_api")
CAPEX_CATEGORIES = ("capex_expansion", "major_order_win", "guidance", "results")
HIGH_VALUE_FILTER_V1 = "market-intel-high-value-filter-v1"
JCURVE_FILTER_SIGNALS = (
    "CAPEX", "CAPACITY", "NEW_FACILITY", "COMMERCIALISATION",
    "PROJECT_FINANCE", "DEMAND_PATH", "ORDER_AWARD", "PROJECT_ADVERSE",
)


class MarketIntelAnnouncementAdapter:
    """Strictly read-only bridge from market_intel into screener-owned evidence."""

    def __init__(self, *, market_intel_db: Path, screener_db: Path):
        self.market_intel_db = Path(market_intel_db)
        self.screener_db = Path(screener_db)

    def read(
        self, *, published_from: date, as_of_date: date,
        categories: tuple[str, ...] = CAPEX_CATEGORIES,
        filter_policy_version: str | None = None,
    ) -> list[AnnouncementRecord]:
        if not self.market_intel_db.is_file():
            raise FileNotFoundError(f"market_intel DuckDB not found: {self.market_intel_db}")
        if not self.screener_db.is_file():
            raise FileNotFoundError(f"research_screener DuckDB not found: {self.screener_db}")
        # market_intel currently stores DuckDB TIMESTAMP values without timezone
        # metadata. Its collectors normalize to UTC before insertion, so bind UTC
        # wall-clock values without tzinfo to avoid session-timezone conversion.
        lower = datetime.combine(published_from, time.min)
        upper = datetime.combine(as_of_date, time.max)
        placeholders = ",".join("?" for _ in categories)
        category_clause = "" if filter_policy_version else f"AND re.primary_category IN ({placeholders})"
        filter_clause = ""
        if filter_policy_version:
            self._require_filter_contract(filter_policy_version)
            signal_clause = " OR ".join(
                f"afd.matched_signals_json LIKE '%\"{signal}\"%'"
                for signal in JCURVE_FILTER_SIGNALS
            )
            filter_clause = f"""
              AND EXISTS (
                  SELECT 1 FROM announcement_filter_decision afd
                  WHERE afd.raw_event_id = r.raw_event_id
                    AND afd.policy_version = ?
                    AND afd.decision IN ('KEEP', 'FETCH_ATTACHMENT')
                    AND ({signal_clause})
              )
            """
        sql = f"""
            SELECT r.raw_event_id, r.event_hash, r.source, r.external_id, r.symbol,
                   r.isin, r.company_name, r.title, r.description, r.published_at,
                   r.event_date, r.ingested_at, r.raw_payload_json, r.link, r.attachment_url,
                   re.primary_category, fd.local_path, fd.content_hash, fd.pdf_status
            FROM raw_event r
            JOIN resolved_event re ON re.raw_event_id = r.raw_event_id
            LEFT JOIN filing_document fd ON fd.raw_event_id = r.raw_event_id
            WHERE r.source IN ('nse_rss', 'bse_corp', 'nse_api')
              AND r.published_at >= ? AND r.published_at <= ?
              AND r.ingested_at <= ?
              {category_clause}
              AND coalesce(re.is_official, FALSE) = TRUE
              {filter_clause}
            ORDER BY r.published_at, r.raw_event_id
        """
        params = [lower, upper, upper]
        if not filter_policy_version:
            params.extend(categories)
        if filter_policy_version:
            params.append(filter_policy_version)
        source = duckdb.connect(str(self.market_intel_db), read_only=True)
        try:
            rows = source.execute(sql, params).fetchall()
        finally:
            source.close()
        identity = duckdb.connect(str(self.screener_db), read_only=True)
        try:
            return [self._record(identity, row) for row in rows]
        finally:
            identity.close()

    def coverage_receipts(
        self, *, published_from: date, as_of_date: date, filter_policy_version: str,
        required_sources: tuple[str, ...] = ("nse_api", "bse_corp"),
    ) -> dict:
        """Return conservative upstream coverage evidence for the requested window."""
        self._require_filter_contract(filter_policy_version)
        supported_sources = {"nse_api", "bse_corp"}
        required_source_set = set(required_sources)
        if not required_source_set or not required_source_set <= supported_sources:
            raise ValueError(
                f"invalid required coverage sources: {sorted(required_source_set)}"
            )
        lower = datetime.combine(published_from, time.min)
        upper = datetime.combine(as_of_date, time.max)
        source = duckdb.connect(str(self.market_intel_db), read_only=True)
        try:
            rows = source.execute(
                """
                SELECT collection_run_id, source, requested_from, requested_to,
                       status, pages_complete, item_count, failure_count,
                       policy_hash
                FROM announcement_collection_run
                WHERE policy_version = ?
                  AND requested_to >= ? AND requested_from <= ?
                ORDER BY source, requested_from, collection_run_id
                """,
                [filter_policy_version, lower, upper],
            ).fetchall()
        finally:
            source.close()
        receipts = [
            {
                "collection_run_id": row[0], "source": row[1],
                "requested_from": row[2], "requested_to": row[3],
                "status": row[4], "pages_complete": bool(row[5]),
                "item_count": int(row[6]), "failure_count": int(row[7]),
                "policy_hash": row[8],
            }
            for row in rows
        ]
        intervals: dict[str, list[tuple[datetime, datetime]]] = defaultdict(list)
        for row in receipts:
            if row["status"] == "COMPLETED" and row["pages_complete"]:
                intervals[str(row["source"])].append(
                    (row["requested_from"], row["requested_to"])
                )
        covered_sources = {
            source for source, source_intervals in intervals.items()
            if self._intervals_cover(source_intervals, lower=lower, upper=upper)
        }
        return {
            "policy_version": filter_policy_version,
            "requested_from": lower, "requested_to": upper,
            "coverage_scope": "COHORT_PRIMARY_LISTING_V1",
            "required_sources": sorted(required_source_set),
            "coverage_proven": required_source_set <= covered_sources,
            "covered_sources": sorted(covered_sources),
            "missing_sources": sorted(required_source_set - covered_sources),
            "receipts": receipts,
        }

    @staticmethod
    def _intervals_cover(
        intervals: list[tuple[datetime, datetime]], *, lower: datetime, upper: datetime,
    ) -> bool:
        if not intervals:
            return False
        cursor = lower
        tolerance = timedelta(microseconds=1)
        for start, end in sorted(intervals):
            if end < cursor:
                continue
            if start > cursor + tolerance:
                return False
            cursor = max(cursor, end)
            if cursor >= upper:
                return True
        return False

    def _require_filter_contract(self, filter_policy_version: str) -> None:
        if filter_policy_version != HIGH_VALUE_FILTER_V1:
            raise ValueError(f"unsupported market_intel filter policy: {filter_policy_version}")
        source = duckdb.connect(str(self.market_intel_db), read_only=True)
        try:
            tables = {
                row[0] for row in source.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                ).fetchall()
            }
        finally:
            source.close()
        required = {"announcement_collection_run", "announcement_filter_decision"}
        missing = required - tables
        if missing:
            raise ValueError(f"market_intel filter V1 tables are missing: {sorted(missing)}")

    def _record(self, conn, row: tuple) -> AnnouncementRecord:
        (
            raw_id, event_hash, source, external_id, symbol, isin, company_name,
            title, description, published_at, event_at, retrieved_at, raw_payload,
            source_url, attachment_url,
            category, attachment_path, upstream_attachment_hash, pdf_status,
        ) = row
        published = self._aware(published_at)
        event = self._aware(event_at) if event_at else None
        retrieved = self._aware(retrieved_at)
        matches = self._resolve_identity(
            conn, source=str(source), symbol=str(symbol or ""), isin=str(isin or ""),
            company_name=str(company_name or ""), as_of=published.date(),
        )
        identity_status = "RESOLVED" if len(matches) == 1 else "UNRESOLVED" if not matches else "AMBIGUOUS"
        match = matches[0] if len(matches) == 1 else (None, None, None)
        payload = str(raw_payload or "{}").encode("utf-8")
        attachment_hash = None
        valid_attachment_path = None
        attachment_status = "ABSENT" if not attachment_path else "UPSTREAM_INVALID"
        if attachment_path and str(pdf_status or "").lower() in {"ok", "cached", "extracted"}:
            path = Path(str(attachment_path))
            if path.is_file():
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
                if not upstream_attachment_hash or digest == str(upstream_attachment_hash):
                    attachment_hash = digest
                    valid_attachment_path = str(path)
                    attachment_status = "VALID"
                else:
                    attachment_status = "HASH_MISMATCH"
            else:
                attachment_status = "MISSING_FILE"
        announcement_id = f"jcurve-announcement:{content_hash([source, event_hash])[:28]}"
        return AnnouncementRecord(
            announcement_id=announcement_id, upstream_raw_event_id=int(raw_id),
            upstream_event_hash=str(event_hash), source=str(source), external_id=external_id,
            category=category, title=str(title or ""), description=description,
            source_url=source_url, attachment_url=attachment_url,
            published_at=published, event_at=event, retrieved_at=retrieved,
            raw_payload=payload, raw_content_hash=hashlib.sha256(payload).hexdigest(),
            attachment_path=valid_attachment_path, attachment_content_hash=attachment_hash,
            attachment_validation_status=attachment_status,
            company_id=match[0], security_id=match[1], listing_id=match[2],
            identity_status=identity_status,
            identity_candidates=tuple(sorted({str(candidate[1]) for candidate in matches})),
        )

    @staticmethod
    def _resolve_identity(
        conn, *, source: str, symbol: str, isin: str, company_name: str, as_of: date
    ) -> list[tuple[str, str, str]]:
        exchange = "BSE" if source == "bse_corp" else "NSE"
        if isin:
            rows = conn.execute(
                """SELECT DISTINCT s.company_id, s.security_id, l.listing_id
                   FROM security_master s JOIN listing_master l ON l.security_id = s.security_id
                   WHERE s.isin = ? AND l.exchange = ?
                     AND s.valid_from <= ? AND (s.valid_to IS NULL OR s.valid_to >= ?)
                     AND l.valid_from <= ? AND (l.valid_to IS NULL OR l.valid_to >= ?)""",
                [isin, exchange, as_of, as_of, as_of, as_of],
            ).fetchall()
            if rows:
                return rows
            rows = conn.execute(
                """SELECT DISTINCT s.company_id, s.security_id, l.listing_id
                   FROM security_master s JOIN listing_master l ON l.security_id = s.security_id
                   WHERE s.isin = ? AND l.exchange = ?
                     AND s.valid_to IS NULL AND l.valid_to IS NULL
                   ORDER BY s.company_id, s.security_id, l.listing_id""",
                [isin, exchange],
            ).fetchall()
            if rows:
                return rows
        value = symbol.strip().upper()
        if not value:
            return []
        rows = conn.execute(
            """SELECT DISTINCT s.company_id, s.security_id, l.listing_id
               FROM listing_master l JOIN security_master s ON s.security_id = l.security_id
               WHERE l.exchange = ? AND l.valid_from <= ? AND (l.valid_to IS NULL OR l.valid_to >= ?)
                 AND (upper(coalesce(l.symbol, '')) = ? OR upper(coalesce(l.bse_code, '')) = ?
                      OR upper(coalesce(l.exchange_security_id, '')) = ?)
               ORDER BY s.company_id, s.security_id, l.listing_id""",
            [exchange, as_of, as_of, value, value, value],
        ).fetchall()
        if rows:
            return rows
        return conn.execute(
            """SELECT DISTINCT s.company_id, s.security_id, l.listing_id
               FROM listing_master l JOIN security_master s ON s.security_id = l.security_id
               WHERE l.exchange = ? AND l.valid_to IS NULL AND s.valid_to IS NULL
                 AND (upper(coalesce(l.symbol, '')) = ? OR upper(coalesce(l.bse_code, '')) = ?
                      OR upper(coalesce(l.exchange_security_id, '')) = ?)
               ORDER BY s.company_id, s.security_id, l.listing_id""",
            [exchange, value, value, value],
        ).fetchall()

    @staticmethod
    def _aware(value: datetime) -> datetime:
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
