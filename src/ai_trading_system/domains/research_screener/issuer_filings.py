from __future__ import annotations

import hashlib
import io
import json
import re
from datetime import date, datetime
from typing import Any

import pdfplumber
import requests

from .filings import completeness, mandatory_metrics
from .providers import OfficialExchangeClient, _artifact


FORMULA_VERSION = "issuer-pdf-curated-v1"


class IssuerFilingRepairClient:
    """Complete missing periods from checksum-locked official issuer documents.

    Values are curated from fixed pages in the reviewed document contract. Runtime
    acquisition validates the exact PDF bytes and document identity, reconciles any
    configured overlap, and adds missing periods only. Existing exchange periods are
    never replaced.
    """

    def __init__(self, exchange_client: OfficialExchangeClient, contract: dict[str, Any]):
        self.exchange = exchange_client
        self.contract = contract

    def augment(self, symbol: str, expected_isin: str, snapshot: dict, as_of_date: date,
                *, company_type: str) -> tuple[dict, list[dict]]:
        repair = self.contract.get("symbols", {}).get(symbol)
        if repair is None:
            return snapshot, []
        document = repair["document"]
        url = document["url"]
        published_at = datetime.fromisoformat(document["published_at"])
        if published_at.date() > as_of_date:
            return snapshot, []

        artifacts: list[dict] = []
        raw: bytes | None = None
        try:
            self._validate_contract_identity(repair, expected_isin, snapshot)
            response = self.exchange._get(url, referer=document["referer"])
            raw = self.exchange._validate(
                response, expected=("application/pdf", "octet-stream"), source=url,
            )
            if not raw.startswith(b"%PDF-"):
                raise RuntimeError("ISSUER_FILING_NOT_PDF")
            observed_hash = hashlib.sha256(raw).hexdigest()
            if observed_hash != document["sha256"]:
                raise RuntimeError(
                    f"ISSUER_FILING_HASH_MISMATCH:{observed_hash}"
                )
            page_text = self._extract_pages(
                raw,
                sorted({
                    *document["identity_pages"],
                    *(page for row in repair["statements"] for page in row["evidence_pages"]),
                }),
            )
            identity_text = " ".join(page_text[page] for page in document["identity_pages"])
            normalized_identity = self._normalized_text(identity_text)
            missing_markers = [
                marker for marker in document["required_text_markers"]
                if self._normalized_text(marker) not in normalized_identity
            ]
            if missing_markers:
                raise RuntimeError(f"ISSUER_FILING_IDENTITY_MISMATCH:{missing_markers}")

            artifact = _artifact(
                "issuer_filing_pdf", document["provider"], raw, url=url,
                effective_date=max(date.fromisoformat(row["period_end"]) for row in repair["statements"]),
                row_count=len(repair["statements"]),
                metadata={
                    "symbol": symbol, "expected_isin": expected_isin,
                    "document_kind": document["document_kind"],
                    "scope": repair["scope"], "published_at": published_at.isoformat(),
                    "contract_version": self.contract["contract_version"],
                    "evidence_pages": sorted(page_text),
                },
            )
            artifact["published_at"] = published_at
            artifacts.append(artifact)
            curated = [
                self._statement(row, repair, document, raw, artifact["artifact_id"], published_at)
                for row in repair["statements"]
            ]
            result = self._merge_missing_periods(
                snapshot, curated, company_type=company_type, as_of_date=as_of_date,
                document_artifact_id=artifact["artifact_id"],
            )
            return result, artifacts
        except (OSError, ValueError, TypeError, RuntimeError, requests.RequestException) as exc:
            artifacts.append(_artifact(
                "issuer_filing_pdf", document["provider"], raw or str(exc).encode(), url=url,
                effective_date=as_of_date, row_count=0, status="FAILED",
                metadata={
                    "symbol": symbol, "expected_isin": expected_isin,
                    "document_kind": document["document_kind"], "error": str(exc),
                    "contract_version": self.contract["contract_version"],
                },
            ))
            failed = dict(snapshot)
            failed["issuer_repair"] = {
                "status": "FAILED", "reason": str(exc),
                "contract_version": self.contract["contract_version"],
            }
            return failed, artifacts

    @staticmethod
    def _validate_contract_identity(repair: dict, expected_isin: str, snapshot: dict) -> None:
        if repair["expected_isin"].upper() != expected_isin.upper():
            raise RuntimeError("ISSUER_REPAIR_EXPECTED_ISIN_MISMATCH")
        if snapshot.get("scope") != repair["scope"]:
            raise RuntimeError(
                f"ISSUER_REPAIR_SCOPE_MISMATCH:{snapshot.get('scope')}:{repair['scope']}"
            )

    @staticmethod
    def _extract_pages(raw: bytes, pages: list[int]) -> dict[int, str]:
        result: dict[int, str] = {}
        with pdfplumber.open(io.BytesIO(raw)) as pdf:
            for page_number in pages:
                if page_number < 1 or page_number > len(pdf.pages):
                    raise RuntimeError(f"ISSUER_FILING_PAGE_OUT_OF_RANGE:{page_number}")
                result[page_number] = pdf.pages[page_number - 1].extract_text() or ""
        return result

    @staticmethod
    def _normalized_text(value: str) -> str:
        return re.sub(r"[^A-Z0-9]+", " ", value.upper()).strip()

    def _statement(self, row: dict, repair: dict, document: dict, raw: bytes,
                   artifact_id: str, published_at: datetime) -> dict:
        period_end = date.fromisoformat(row["period_end"])
        material = json.dumps(row, sort_keys=True, separators=(",", ":")).encode()
        return {
            "period_type": row["period_type"], "period_end": period_end,
            "published_at": published_at, "scope": repair["scope"],
            "source_document_url": document["url"],
            "source_provider": f"{document['provider']}_{document['document_kind']}",
            "identity_evidence": "CHECKSUM_LOCKED_PDF_LEGAL_NAME_AND_CIN",
            "metrics": dict(row["metrics"]),
            "raw_values": {
                metric: {
                    "normalized_value_inr_crore": value,
                    "evidence_pages": row["evidence_pages"],
                    "curation_contract": self.contract["contract_version"],
                }
                for metric, value in row["metrics"].items() if value is not None
            },
            "formula_version": FORMULA_VERSION,
            "source_row_hash": hashlib.sha256(
                raw + b"|" + row["period_type"].encode() + b"|"
                + row["period_end"].encode() + b"|" + material
            ).hexdigest(),
            "source_artifact_id": artifact_id,
            "reconcile_metrics": row.get("reconcile_metrics"),
        }

    def _merge_missing_periods(self, snapshot: dict, curated: list[dict], *, company_type: str,
                               as_of_date: date, document_artifact_id: str) -> dict:
        result = dict(snapshot)
        result["missing_target_periods"] = dict(snapshot.get("missing_target_periods", {}))
        result["latest_parsed_periods"] = dict(snapshot.get("latest_parsed_periods", {}))
        added_periods: dict[str, list[date]] = {"annual": [], "quarterly": []}
        filled_empty_periods: dict[str, list[date]] = {"annual": [], "quarterly": []}
        reconciled_periods: dict[str, list[date]] = {"annual": [], "quarterly": []}
        for period_type in ("annual", "quarterly"):
            key = f"{period_type}_statements"
            existing = list(snapshot.get(key, []))
            by_period = {row["period_end"]: row for row in existing}
            for candidate in (row for row in curated if row["period_type"] == period_type):
                overlap = by_period.get(candidate["period_end"])
                if overlap is not None:
                    required = mandatory_metrics(company_type, period_type)
                    if not any(overlap.get("metrics", {}).get(metric) is not None for metric in required):
                        by_period[candidate["period_end"]] = candidate
                        filled_empty_periods[period_type].append(candidate["period_end"])
                        continue
                    self._reconcile_overlap(overlap, candidate, company_type, period_type)
                    reconciled_periods[period_type].append(candidate["period_end"])
                    continue
                by_period[candidate["period_end"]] = candidate
                added_periods[period_type].append(candidate["period_end"])
            result[key] = sorted(by_period.values(), key=lambda item: item["period_end"], reverse=True)

        target_periods = snapshot.get("target_periods", {})
        for period_type, count in (("annual", 6), ("quarterly", 12)):
            statements = result[f"{period_type}_statements"]
            targets = list(target_periods.get(period_type, []))
            result[f"{period_type}_completeness"] = completeness(
                statements, company_type=company_type, period_type=period_type, periods=count,
                target_period_ends=targets,
            )
            result[f"{period_type}_period_count"] = len(statements)
            parsed = {row["period_end"] for row in statements}
            result.setdefault("missing_target_periods", {})[period_type] = [
                period for period in targets if period not in parsed
            ]
            result.setdefault("latest_parsed_periods", {})[period_type] = max(parsed, default=None)

        disclosed = result.get("latest_disclosed_periods", {})
        parsed = result.get("latest_parsed_periods", {})
        latest_matched = all(
            disclosed.get(kind) is not None and parsed.get(kind) == disclosed.get(kind)
            for kind in ("annual", "quarterly")
        )
        usable = (
            min(result["annual_completeness"], result["quarterly_completeness"]) >= 0.70
            and latest_matched
        )
        result["state"] = "PRESENT" if usable else "DATA_REPAIR_REQUIRED"
        providers = sorted({
            row["source_provider"]
            for kind in ("annual", "quarterly")
            for row in result[f"{kind}_statements"]
        })
        result["provenance_validation"] = {
            "provider": providers,
            "available_at": all(
                row["published_at"].date() <= as_of_date
                for kind in ("annual", "quarterly")
                for row in result[f"{kind}_statements"]
            ),
            "source_row_hash": all(
                row.get("source_row_hash")
                for kind in ("annual", "quarterly")
                for row in result[f"{kind}_statements"]
            ),
            "filing_source": bool(providers),
            "reason": (
                "official exchange history completed with checksum-locked issuer filing periods"
                if usable else
                "issuer filing repair applied but required coverage remains below the admission contract"
            ),
        }
        result["issuer_repair"] = {
            "status": "APPLIED", "contract_version": self.contract["contract_version"],
            "source_artifact_id": document_artifact_id,
            "added_periods": added_periods, "filled_empty_periods": filled_empty_periods,
            "reconciled_periods": reconciled_periods,
            "policy": "missing-period-only-same-scope-v1",
        }
        return result

    @staticmethod
    def _reconcile_overlap(exchange_row: dict, issuer_row: dict, company_type: str,
                           period_type: str) -> None:
        configured = issuer_row.get("reconcile_metrics")
        metrics = set(configured or mandatory_metrics(company_type, period_type))
        compared = 0
        for metric in metrics:
            left = exchange_row.get("metrics", {}).get(metric)
            right = issuer_row.get("metrics", {}).get(metric)
            if left is None or right is None:
                continue
            compared += 1
            tolerance = max(0.02, abs(float(right)) * 0.002)
            if abs(float(left) - float(right)) > tolerance:
                raise RuntimeError(
                    f"ISSUER_FILING_OVERLAP_MISMATCH:{period_type}:"
                    f"{issuer_row['period_end']}:{metric}:{left}:{right}"
                )
        if compared < 4:
            raise RuntimeError(
                f"ISSUER_FILING_OVERLAP_INSUFFICIENT:{period_type}:"
                f"{issuer_row['period_end']}:{compared}"
            )
