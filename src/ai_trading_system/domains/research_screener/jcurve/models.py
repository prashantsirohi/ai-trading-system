from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class ModelRoute(StrEnum):
    TEXT = "text"
    VISION = "vision"
    VISION_FALLBACK = "vision_fallback"


@dataclass(frozen=True)
class EvidencePage:
    page: int
    text: str = ""
    image_data_url: str | None = None


@dataclass(frozen=True)
class EvidencePacket:
    announcement_id: str
    source_artifact_id: str
    source_content_hash: str
    company_id: str
    security_id: str
    isin: str
    published_at: datetime
    title: str
    pages: tuple[EvidencePage, ...]
    pdf_data_url: str | None = None


@dataclass(frozen=True)
class AgentCallRecord:
    request_id: str
    role: str
    route: str
    model_id: str
    provider: str | None
    prompt_version: str
    prompt_hash: str
    source_page_hashes: tuple[str, ...]
    input_tokens: int
    output_tokens: int
    cost_usd: float | None
    schema_valid: bool
    attempt_count: int
    retry_history: tuple[dict[str, Any], ...]
    response_hash: str | None
    status: str
    error_code: str | None


@dataclass(frozen=True)
class AgentResult:
    payload: dict[str, Any]
    call: AgentCallRecord


@dataclass(frozen=True)
class AnnouncementRecord:
    announcement_id: str
    upstream_raw_event_id: int
    upstream_event_hash: str
    source: str
    external_id: str | None
    category: str | None
    title: str
    description: str | None
    source_url: str | None
    attachment_url: str | None
    published_at: datetime
    event_at: datetime | None
    retrieved_at: datetime
    raw_payload: bytes
    raw_content_hash: str
    attachment_path: str | None
    attachment_content_hash: str | None
    attachment_validation_status: str
    company_id: str | None
    security_id: str | None
    listing_id: str | None
    identity_status: str
    identity_candidates: tuple[str, ...] = field(default_factory=tuple)
