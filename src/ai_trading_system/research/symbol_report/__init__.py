"""Single-symbol diagnostic reports for emitted system behavior."""

from .dataset import SymbolReportData, build_symbol_report
from .renderer import render_symbol_report

__all__ = ["SymbolReportData", "build_symbol_report", "render_symbol_report"]
