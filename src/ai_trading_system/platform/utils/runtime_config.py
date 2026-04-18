"""Typed runtime configuration for external integrations and providers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from ai_trading_system.platform.utils.env import load_project_env

load_project_env(__file__)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off", ""}


@dataclass(frozen=True)
class DhanRuntimeConfig:
    api_key: str
    client_id: str
    access_token: str
    refresh_token: str
    totp: str
    pin: str

    @classmethod
    def from_env(cls) -> "DhanRuntimeConfig":
        return cls(
            api_key=os.getenv("DHAN_API_KEY", ""),
            client_id=os.getenv("DHAN_CLIENT_ID", ""),
            access_token=os.getenv("DHAN_ACCESS_TOKEN", ""),
            refresh_token=os.getenv("DHAN_REFRESH_TOKEN", ""),
            totp=os.getenv("DHAN_TOTP", ""),
            pin=os.getenv("DHAN_PIN", ""),
        )


@dataclass(frozen=True)
class TelegramRuntimeConfig:
    bot_token: str
    chat_id: str
    connect_timeout_seconds: float
    read_timeout_seconds: float
    write_timeout_seconds: float
    pool_timeout_seconds: float
    send_attempts: int
    dns_precheck_enabled: bool

    @classmethod
    def from_env(cls) -> "TelegramRuntimeConfig":
        return cls(
            bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
            chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
            connect_timeout_seconds=float(os.getenv("TELEGRAM_CONNECT_TIMEOUT_SECONDS", "5.0")),
            read_timeout_seconds=float(os.getenv("TELEGRAM_READ_TIMEOUT_SECONDS", "10.0")),
            write_timeout_seconds=float(os.getenv("TELEGRAM_WRITE_TIMEOUT_SECONDS", "10.0")),
            pool_timeout_seconds=float(os.getenv("TELEGRAM_POOL_TIMEOUT_SECONDS", "2.0")),
            send_attempts=max(int(os.getenv("TELEGRAM_SEND_ATTEMPTS", "1")), 1),
            dns_precheck_enabled=_env_bool("TELEGRAM_DNS_PRECHECK_ENABLED", True),
        )


@dataclass(frozen=True)
class GoogleSheetsRuntimeConfig:
    spreadsheet_id: str
    credentials_path: Path
    token_path: Path

    @classmethod
    def from_env(cls, project_root: str | Path | None = None) -> "GoogleSheetsRuntimeConfig":
        root = Path(project_root).resolve() if project_root else Path(__file__).resolve().parents[5]
        credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
        token_path = os.getenv("GOOGLE_TOKEN_PATH")
        return cls(
            spreadsheet_id=os.getenv("GOOGLE_SPREADSHEET_ID", ""),
            credentials_path=Path(credentials_path) if credentials_path else root / "client_secret.json",
            token_path=Path(token_path) if token_path else root / "token.json",
        )
