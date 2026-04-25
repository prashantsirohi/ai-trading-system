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

# Top-level module roots that count as "the HTTP framework" and must not
# leak into the layers above.
FORBIDDEN_ROOTS: frozenset[str] = frozenset({"fastapi", "uvicorn", "starlette"})


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
