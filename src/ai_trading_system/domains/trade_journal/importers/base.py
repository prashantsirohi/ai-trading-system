"""Broker parser protocol."""

from pathlib import Path
from typing import Protocol

from ..models import ParseResult


class BrokerParser(Protocol):
    format_version: str

    def parse(self, path: Path) -> ParseResult: ...
