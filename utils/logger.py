"""Compatibility shim for legacy logging imports."""

from core.logging import (
    clear_log_context,
    configure_terminal_output,
    get_log_context,
    get_logger,
    log_context,
    logger,
    set_log_context,
)

__all__ = [
    "logger",
    "get_logger",
    "get_log_context",
    "set_log_context",
    "clear_log_context",
    "log_context",
    "configure_terminal_output",
]
