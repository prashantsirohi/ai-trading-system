"""Render the weekly market report HTML and (optionally) PDF.

PDF rendering uses WeasyPrint when available; if the dependency or its
native libs are missing, the HTML is still produced and the PDF step is
skipped with a warning surfaced to the caller.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from jinja2 import Environment, FileSystemLoader, select_autoescape

logger = logging.getLogger(__name__)

_TEMPLATE_DIR = Path(__file__).parent / "templates"
_STATIC_DIR = Path(__file__).parent / "static"


def _build_env() -> Environment:
    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATE_DIR)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    env.filters["fmt_num"] = _fmt_num
    env.filters["fmt_pct"] = _fmt_pct
    return env


def _fmt_num(value: Any, digits: int = 2) -> str:
    if value is None or value == "":
        return "—"
    try:
        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def _fmt_pct(value: Any, digits: int = 1) -> str:
    if value is None or value == "":
        return "—"
    try:
        return f"{float(value) * 100:,.{digits}f}%"
    except (TypeError, ValueError):
        return str(value)


def render_html(context: Dict[str, Any]) -> str:
    env = _build_env()
    template = env.get_template("weekly_report.html")
    css_path = _STATIC_DIR / "report.css"
    inline_css = css_path.read_text(encoding="utf-8") if css_path.exists() else ""
    return template.render(**context, inline_css=inline_css)


def render(context: Dict[str, Any], output_dir: Path) -> Tuple[Path, Optional[Path], Optional[str]]:
    """Render HTML, then attempt PDF. Returns (html_path, pdf_path_or_None, pdf_error_or_None)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    html = render_html(context)
    html_path = output_dir / "weekly_market_report.html"
    html_path.write_text(html, encoding="utf-8")

    pdf_path: Optional[Path] = None
    pdf_error: Optional[str] = None
    try:
        from weasyprint import HTML  # type: ignore

        pdf_path = output_dir / "weekly_market_report.pdf"
        HTML(string=html, base_url=str(_STATIC_DIR)).write_pdf(str(pdf_path))
    except ImportError as exc:
        pdf_error = f"weasyprint not installed: {exc}"
        logger.warning(pdf_error)
    except Exception as exc:  # noqa: BLE001 — weasyprint can raise OS-level errors for missing libs
        pdf_error = f"weasyprint failed: {exc}"
        logger.warning(pdf_error)

    return html_path, pdf_path, pdf_error
