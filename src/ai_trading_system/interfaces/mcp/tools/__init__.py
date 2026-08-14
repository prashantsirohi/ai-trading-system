"""MCP tool implementations.

Every module here exposes plain functions that take primitives and return
dicts. None of them import the ``mcp`` package: only ``server.py`` does. That
keeps the whole tool surface testable without an MCP client and leaves an HTTP
adapter possible without touching tool logic.
"""

from __future__ import annotations

__all__: list[str] = []
