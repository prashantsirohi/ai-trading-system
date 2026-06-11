"""Render the fundamental opportunities HTML/PDF report."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from jinja2 import ChainableUndefined, Environment, FileSystemLoader, Undefined, select_autoescape

logger = logging.getLogger(__name__)

_TEMPLATE_DIR = Path(__file__).parent / "templates"
_STATIC_DIR = Path(__file__).parent / "static"


def render_html(context: dict[str, Any]) -> str:
    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATE_DIR)),
        autoescape=select_autoescape(["html", "xml"]),
        undefined=ChainableUndefined,
    )
    env.filters["fmt_num"] = _fmt_num
    env.filters["fmt_pct_points"] = _fmt_pct_points
    template = env.get_template("fundamental_opportunities.html")
    css_path = _STATIC_DIR / "report.css"
    inline_css = css_path.read_text(encoding="utf-8") if css_path.exists() else ""
    return template.render(**context, inline_css=inline_css)


def render(context: dict[str, Any], output_dir: Path) -> tuple[Path, Path | None, str | None]:
    output_dir.mkdir(parents=True, exist_ok=True)
    as_of = str(context.get("as_of") or "latest")
    stem = f"fundamental_opportunities_{as_of}"
    html = render_html(context)
    html_path = output_dir / f"{stem}.html"
    html_path.write_text(html, encoding="utf-8")

    pdf_path: Path | None = None
    pdf_error: str | None = None
    try:
        from weasyprint import HTML  # type: ignore

        pdf_path = output_dir / f"{stem}.pdf"
        HTML(string=html, base_url=str(output_dir)).write_pdf(str(pdf_path))
    except ImportError as exc:
        pdf_error = f"weasyprint not installed: {exc}"
        logger.warning(pdf_error)
    except Exception as exc:  # noqa: BLE001
        pdf_error = f"weasyprint failed: {exc}"
        logger.warning(pdf_error)
    return html_path, pdf_path, pdf_error


def _fmt_num(value: Any, digits: int = 2) -> str:
    if _is_blank(value):
        return "-"
    try:
        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def _fmt_pct_points(value: Any, digits: int = 1) -> str:
    if _is_blank(value):
        return "-"
    try:
        return f"{float(value):,.{digits}f}%"
    except (TypeError, ValueError):
        return str(value)


def _is_blank(value: Any) -> bool:
    return value is None or value == "" or isinstance(value, Undefined)


__all__ = ["render", "render_html"]
