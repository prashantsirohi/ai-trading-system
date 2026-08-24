from __future__ import annotations

import base64
from dataclasses import dataclass
from pathlib import Path

import pdfplumber

from .models import EvidencePage, ModelRoute


TERMS = (
    "capex", "capital expenditure", "capacity", "commission", "commercial production",
    "greenfield", "brownfield", "plant", "facility", "utilisation", "utilization",
    "customer qualification", "commercial supply", "incremental revenue",
)


@dataclass(frozen=True)
class SelectedEvidence:
    pages: tuple[EvidencePage, ...]
    route: ModelRoute
    reason_codes: tuple[str, ...]


def select_relevant_pages(
    pdf_path: str | Path, *, max_pages: int = 8, max_characters: int = 18000
) -> SelectedEvidence:
    path = Path(pdf_path)
    candidates: list[tuple[int, int, str, bool]] = []
    with pdfplumber.open(path) as pdf:
        for index, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            lowered = text.lower()
            score = sum(lowered.count(term) for term in TERMS)
            if score:
                complex_layout = len(page.extract_tables() or []) > 1
                candidates.append((score, index, text, complex_layout))
    candidates.sort(key=lambda row: (-row[0], row[1]))
    selected = sorted(candidates[:max_pages], key=lambda row: row[1])
    if not selected:
        return SelectedEvidence((), ModelRoute.TEXT, ("NO_RELEVANT_PAGES",))
    remaining = max_characters
    pages: list[EvidencePage] = []
    vision_needed = False
    for _, page_number, text, complex_layout in selected:
        bounded = text[:remaining]
        remaining -= len(bounded)
        alpha_ratio = sum(char.isalpha() for char in bounded) / max(len(bounded), 1)
        weak_text = len(bounded.strip()) < 200 or alpha_ratio < 0.30
        vision_needed = vision_needed or weak_text or complex_layout
        image = _render_page(path, page_number) if weak_text or complex_layout else None
        pages.append(EvidencePage(page=page_number, text=bounded, image_data_url=image))
        if remaining <= 0:
            break
    route = ModelRoute.VISION if vision_needed and any(page.image_data_url for page in pages) else ModelRoute.TEXT
    reasons = ("VISION_SELECTED_FOR_LAYOUT",) if route == ModelRoute.VISION else ("CLEAN_TEXT_SELECTED",)
    return SelectedEvidence(tuple(pages), route, reasons)


def _render_page(path: Path, page_number: int) -> str | None:
    try:
        import fitz

        with fitz.open(path) as document:
            pixmap = document[page_number - 1].get_pixmap(dpi=160, alpha=False)
            encoded = base64.b64encode(pixmap.tobytes("png")).decode("ascii")
            return f"data:image/png;base64,{encoded}"
    except Exception:
        return None
