"""Telegram delivery adapter boundary."""

from __future__ import annotations

from services.publish.publish_payloads import format_rows_for_channel
from channel.telegram_reporter import TelegramReporter


def format_telegram_rows(rows: list[dict]) -> list[dict]:
    """Return concise telegram rows via shared publish formatter."""
    return format_rows_for_channel(rows, "telegram")["rows"]


__all__ = ["TelegramReporter", "format_telegram_rows"]
