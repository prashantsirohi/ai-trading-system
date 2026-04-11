"""Central logging configuration with shared pipeline context support."""

from __future__ import annotations

import contextlib
import contextvars
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Iterator

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

_LOG_CONTEXT: contextvars.ContextVar[Dict[str, Any]] = contextvars.ContextVar(
    "ai_trading_log_context",
    default={},
)
_CONTEXT_KEYS = ("run_id", "stage_name", "attempt_number", "channel", "model_id")


def get_log_context() -> Dict[str, Any]:
    """Return the current structured logging context."""
    return dict(_LOG_CONTEXT.get())


def set_log_context(**values: Any) -> Dict[str, Any]:
    """Merge new context fields into the current logging context."""
    current = get_log_context()
    current.update({key: value for key, value in values.items() if value is not None})
    _LOG_CONTEXT.set(current)
    return current


def clear_log_context(*keys: str) -> Dict[str, Any]:
    """Clear selected context keys or reset the whole context when no keys are given."""
    if not keys:
        _LOG_CONTEXT.set({})
        return {}
    current = get_log_context()
    for key in keys:
        current.pop(key, None)
    _LOG_CONTEXT.set(current)
    return current


@contextlib.contextmanager
def log_context(**values: Any) -> Iterator[None]:
    """Temporarily attach structured fields to all logs in the current context."""
    token = _LOG_CONTEXT.set({**get_log_context(), **{k: v for k, v in values.items() if v is not None}})
    try:
        yield
    finally:
        _LOG_CONTEXT.reset(token)


def _context_string() -> str:
    context = get_log_context()
    parts = [f"{key}={context[key]}" for key in _CONTEXT_KEYS if key in context]
    return " ".join(parts)


class _ContextFilter(logging.Filter):
    """Inject pipeline context fields into stdlib log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        context = get_log_context()
        for key in _CONTEXT_KEYS:
            setattr(record, key, context.get(key, "-"))
        record.context = _context_string() or "-"
        return True


_ROOT_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d"
    " | %(context)s - %(message)s"
)

logging.basicConfig(
    level=logging.INFO,
    format=_ROOT_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stderr),
        logging.FileHandler(LOG_DIR / "app_fallback.log"),
    ],
    force=True,
)

_context_filter = _ContextFilter()
for _handler in logging.getLogger().handlers:
    _handler.addFilter(_context_filter)


def _set_stdlib_stream_level(level: int) -> None:
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setLevel(level)


def get_logger(name: str | None = None):
    """Return a logger instance consistent with the shared runtime configuration."""
    logger_name = name or "ai_trading_system"
    return logging.getLogger(logger_name)


try:
    from loguru import logger as _loguru_logger

    _LOGURU_STDERR_SINK_ID: int | None = None
    _LOGURU_FILE_SINK_IDS: list[int] = []

    def _patch_record(record: Dict[str, Any]) -> Dict[str, Any]:
        record["extra"]["context"] = _context_string() or "-"
        return record

    logger = _loguru_logger.patch(_patch_record)

    def _configure_loguru_sinks(stderr_level: str = "INFO") -> None:
        global _LOGURU_STDERR_SINK_ID, _LOGURU_FILE_SINK_IDS
        logger.remove()
        _LOGURU_STDERR_SINK_ID = logger.add(
            sys.stderr,
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                "<magenta>{extra[context]}</magenta> - <level>{message}</level>"
            ),
            level=stderr_level,
        )
        _LOGURU_FILE_SINK_IDS = [
            logger.add(
                LOG_DIR / "app_{time:YYYY-MM-DD}.log",
                rotation="1 day",
                retention="30 days",
                compression="zip",
                format=(
                    "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | "
                    "{extra[context]} - {message}"
                ),
                level="DEBUG",
            ),
            logger.add(
                LOG_DIR / "error_{time:YYYY-MM-DD}.log",
                rotation="1 day",
                retention="90 days",
                format=(
                    "{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | "
                    "{extra[context]} - {message}"
                ),
                level="ERROR",
                backtrace=True,
                diagnose=True,
            ),
        ]

    _configure_loguru_sinks()
except ModuleNotFoundError:
    class _FallbackLogger:
        """Adapter that exposes the subset of Loguru APIs used in this repo."""

        def __init__(self, inner: logging.Logger):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def disable(self, *_args, **_kwargs):
            return None

        def bind(self, **kwargs):
            return _FallbackLoggerAdapter(self._inner, kwargs)

        def opt(self, **_kwargs):
            return self

        def patch(self, *_args, **_kwargs):
            return self

        def success(self, message, *args, **kwargs):
            self._inner.info(message, *args, **kwargs)


    class _FallbackLoggerAdapter(_FallbackLogger):
        """Logger adapter that mirrors Loguru's bind semantics via contextvars."""

        def __init__(self, inner: logging.Logger, bound_values: Dict[str, Any]):
            super().__init__(inner)
            self._bound_values = bound_values

        def _call_with_context(self, method_name: str, message, *args, **kwargs):
            with log_context(**self._bound_values):
                return getattr(self._inner, method_name)(message, *args, **kwargs)

        def debug(self, message, *args, **kwargs):
            return self._call_with_context("debug", message, *args, **kwargs)

        def info(self, message, *args, **kwargs):
            return self._call_with_context("info", message, *args, **kwargs)

        def warning(self, message, *args, **kwargs):
            return self._call_with_context("warning", message, *args, **kwargs)

        def error(self, message, *args, **kwargs):
            return self._call_with_context("error", message, *args, **kwargs)

        def critical(self, message, *args, **kwargs):
            return self._call_with_context("critical", message, *args, **kwargs)

        def exception(self, message, *args, **kwargs):
            return self._call_with_context("exception", message, *args, **kwargs)

        def success(self, message, *args, **kwargs):
            return self._call_with_context("info", message, *args, **kwargs)


    logger = _FallbackLogger(get_logger("ai_trading_system"))


def configure_terminal_output(mode: str = "verbose") -> None:
    """Tighten terminal noise without affecting file logs."""

    normalized = str(mode or "verbose").strip().lower()
    if normalized == "compact":
        _set_stdlib_stream_level(logging.WARNING)
        if "_configure_loguru_sinks" in globals():
            _configure_loguru_sinks(stderr_level="WARNING")
        return
    if normalized in {"verbose", "json"}:
        _set_stdlib_stream_level(logging.INFO)
        if "_configure_loguru_sinks" in globals():
            _configure_loguru_sinks(stderr_level="INFO")
        return
    raise ValueError(f"Unsupported terminal logging mode: {mode}")

__all__ = [
    "logger",
    "get_logger",
    "get_log_context",
    "set_log_context",
    "clear_log_context",
    "log_context",
    "configure_terminal_output",
]
