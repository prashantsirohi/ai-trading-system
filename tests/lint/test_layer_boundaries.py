"""Layer-boundary lint: keep FastAPI/uvicorn out of non-HTTP layers.

The execution-console refactor (PR #1, PR #2) established a clean separation
between the HTTP transport (``ui/execution_api/app.py`` + ``routes/``) and
the rest of the codebase (services, read-models, domains, pipeline, etc.).

This test ratchets that boundary so future changes can't accidentally
re-couple a service or domain module to FastAPI primitives. Routes can
still import HTTPException; services should raise plain Python exceptions
and let routes translate them.
"""

from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_PKG = REPO_ROOT / "src" / "ai_trading_system"

# Modules whose contents must NOT import fastapi/uvicorn/starlette.
FORBIDDEN_LAYERS: tuple[Path, ...] = (
    SRC_PKG / "domains",
    SRC_PKG / "pipeline",
    SRC_PKG / "platform",
    SRC_PKG / "research",
    SRC_PKG / "ui" / "execution_api" / "services",
    SRC_PKG / "ui" / "execution_api" / "schemas",
)

# Top-level module roots that count as "a transport" and must not leak into
# the layers above. `mcp` is the Model Context Protocol SDK: like fastapi it
# belongs only in an interface module, and only `interfaces/mcp/server.py`
# imports it.
FORBIDDEN_ROOTS: frozenset[str] = frozenset(
    {"fastapi", "uvicorn", "starlette", "mcp"}
)

# Tool and reader modules under interfaces/mcp/ stay transport-agnostic so they
# can be unit-tested without the SDK and reused behind another transport.
MCP_TRANSPORT_FREE_LAYERS: tuple[Path, ...] = (
    SRC_PKG / "interfaces" / "mcp" / "tools",
    SRC_PKG / "interfaces" / "mcp" / "readers",
)

# The read-only MCP interface must never reach execution, broker, or pipeline
# orchestration code, whose import side effects can open writable stores.
MCP_FORBIDDEN_IMPORTS: tuple[str, ...] = (
    "ai_trading_system.domains.execution",
    "ai_trading_system.domains.trade_journal",
    "ai_trading_system.pipeline.orchestrator",
    "ai_trading_system.integrations",
)


def _iter_python_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(p for p in root.rglob("*.py") if "__pycache__" not in p.parts)


def _module_root(name: str) -> str:
    """Return the top-level package name from a dotted module path."""

    return name.split(".", 1)[0]


def _violations_in_file(path: Path) -> list[str]:
    """Return human-readable violation strings for ``path`` (empty if clean)."""

    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError as exc:  # pragma: no cover - defensive
        return [f"{path}: syntax error parsing for boundary lint: {exc}"]

    findings: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _module_root(alias.name) in FORBIDDEN_ROOTS:
                    findings.append(
                        f"{path.relative_to(REPO_ROOT)}:{node.lineno}: "
                        f"import {alias.name}"
                    )
        elif isinstance(node, ast.ImportFrom):
            if node.module is None:
                continue
            if _module_root(node.module) in FORBIDDEN_ROOTS:
                findings.append(
                    f"{path.relative_to(REPO_ROOT)}:{node.lineno}: "
                    f"from {node.module} import ..."
                )
    return findings


def test_no_fastapi_imports_in_non_http_layers() -> None:
    """Forbid fastapi/uvicorn/starlette imports in service/domain layers."""

    all_findings: list[str] = []
    for layer in FORBIDDEN_LAYERS:
        for py_file in _iter_python_files(layer):
            all_findings.extend(_violations_in_file(py_file))

    assert not all_findings, (
        "FastAPI/uvicorn/starlette imports leaked into a non-HTTP layer. "
        "Services and domains should raise plain exceptions and let the "
        "route layer translate them. Findings:\n  - "
        + "\n  - ".join(all_findings)
    )


def _imported_modules(path: Path) -> list[tuple[int, str]]:
    """Return (lineno, dotted module) for every import in ``path``."""

    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:  # pragma: no cover - defensive
        return []

    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.extend((node.lineno, alias.name) for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.append((node.lineno, node.module))
    return found


def test_mcp_tools_and_readers_are_transport_agnostic() -> None:
    """Only interfaces/mcp/server.py may import the MCP SDK."""

    findings: list[str] = []
    for layer in MCP_TRANSPORT_FREE_LAYERS:
        for py_file in _iter_python_files(layer):
            for lineno, module in _imported_modules(py_file):
                if _module_root(module) == "mcp":
                    findings.append(
                        f"{py_file.relative_to(REPO_ROOT)}:{lineno}: {module}"
                    )

    assert not findings, (
        "The MCP SDK leaked into a tool or reader module. Tools must stay "
        "plain functions so they can be tested without an MCP client and "
        "reused behind another transport; only server.py imports 'mcp'. "
        "Findings:\n  - " + "\n  - ".join(findings)
    )


def test_mcp_interface_never_imports_execution_or_orchestration() -> None:
    """The read-only interface must not reach code that opens writable stores."""

    mcp_layer = SRC_PKG / "interfaces" / "mcp"
    findings: list[str] = []
    for py_file in _iter_python_files(mcp_layer):
        for lineno, module in _imported_modules(py_file):
            for forbidden in MCP_FORBIDDEN_IMPORTS:
                if module == forbidden or module.startswith(f"{forbidden}."):
                    findings.append(
                        f"{py_file.relative_to(REPO_ROOT)}:{lineno}: {module}"
                    )

    assert not findings, (
        "The read-only MCP interface imported execution, trade-journal, "
        "broker, or pipeline-orchestration code. Those modules can open "
        "writable stores or mutate broker state. Findings:\n  - "
        + "\n  - ".join(findings)
    )


def test_mcp_server_is_the_only_sdk_entry_point() -> None:
    """Sanity check: if server.py stops importing mcp, the lint is vacuous."""

    server = SRC_PKG / "interfaces" / "mcp" / "server.py"
    assert server.exists()
    assert "from mcp.server" in server.read_text(encoding="utf-8"), (
        "Expected interfaces/mcp/server.py to import the MCP SDK; if it no "
        "longer does, the transport-agnostic lint above proves nothing."
    )


def test_http_layer_is_intact() -> None:
    """Sanity check: the HTTP layer DOES import fastapi (catches accidental gut-checks)."""

    http_layer = SRC_PKG / "ui" / "execution_api"
    bootstrap_and_routes = [http_layer / "app.py"] + _iter_python_files(
        http_layer / "routes"
    )
    has_any_fastapi_import = False
    for py_file in bootstrap_and_routes:
        if not py_file.exists():
            continue
        text = py_file.read_text(encoding="utf-8")
        if "fastapi" in text:
            has_any_fastapi_import = True
            break

    assert has_any_fastapi_import, (
        "Expected at least one fastapi import inside ui/execution_api/{app.py,routes/}; "
        "if this fails the boundary lint is no longer meaningful."
    )
