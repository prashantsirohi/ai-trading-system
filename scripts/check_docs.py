#!/usr/bin/env python3
"""Lightweight validation for docs/.

Checks:
  1. No broken relative links in docs/**/*.md (excluding docs/_legacy/).
  2. Frontmatter is present (every doc has Purpose / Audience / Last verified / Source of truth).
  3. No "Status: STUB" markers in current docs (only allowed under docs/_legacy/).
  4. No forbidden stale terms in current docs unless inside a fenced code block or explicit
     "Current code status: unknown" or "verify" disclaimer.
  5. Required sections exist in docs/stages/*.md and docs/domains/*.md.
  6. Every `python -m ai_trading_system.<mod>` invocation references a real module
     under src/ai_trading_system/.

Exit code:
  0 — all checks passed.
  1 — at least one check failed; details printed.

Usage:
  python scripts/check_docs.py            # validate
  python scripts/check_docs.py --fix      # not implemented (placeholder for future)

This is intentionally simple — it's a lint, not a full doc engine.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Iterable

REPO = Path(__file__).resolve().parent.parent
DOCS = REPO / "docs"
LEGACY = DOCS / "_legacy"
SRC_PACKAGE_ROOT = REPO / "src" / "ai_trading_system"

# Forbidden stale terms in NON-legacy docs (case-sensitive substring match outside code fences)
FORBIDDEN_TERMS = [
    # Module paths that no longer exist as top-level packages
    "from collectors.",
    "import collectors.",
    "from run.stages",
    "from run.orchestrator",
    "from core.",
    "ui.execution.app",
    # 5-stage pipeline myth in non-legacy contexts
    "5-stage pipeline",
    # Dhan-first claim
    "Dhan-first",
    "Dhan as primary source",
    # Streamlit operator UI claim
    "Streamlit is the UI",
    "Streamlit operator console",
]

REQUIRED_STAGE_SECTIONS = [
    "## Purpose",
    "## Entrypoints",
    "## Input data",
    "## Output artifacts",
    "## Main modules",
    "## Process flow",
    "## DQ",  # matches "## DQ / trust gates" or similar
    "## Failure modes",
    "## Retry behavior",
    "## Downstream consumers",
    "## Commands",
]

REQUIRED_DOMAIN_SECTIONS = [
    "## Responsibility",
    "## Package / module ownership",
    "## Public contracts",
    "## Storage ownership",
    "## Dependencies",
    "## Extension points",
    "## Known gaps",
]

FRONTMATTER_FIELDS = [
    "**Purpose:**",
    "**Audience:**",
    "**Last verified:**",
    "**Source of truth:**",
]

LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
CODE_FENCE_RE = re.compile(r"```")
PYTHON_M_RE = re.compile(r"python -m (ai_trading_system\.[a-zA-Z0-9_.]+)")


def iter_current_docs() -> Iterable[Path]:
    for p in DOCS.rglob("*.md"):
        try:
            p.relative_to(LEGACY)
            continue
        except ValueError:
            yield p


def strip_code_fences(text: str) -> str:
    out = []
    in_fence = False
    for line in text.splitlines():
        if CODE_FENCE_RE.search(line):
            in_fence = not in_fence
            continue
        if not in_fence:
            out.append(line)
    return "\n".join(out)


def check_links(doc: Path) -> list[str]:
    errors = []
    text = doc.read_text()
    for m in LINK_RE.finditer(text):
        target = m.group(1).split("#")[0].strip()
        if not target:
            continue
        if target.startswith(("http://", "https://", "mailto:")):
            continue
        # Skip absolute path / fragment-only
        if target.startswith("/"):
            continue
        resolved = (doc.parent / target).resolve()
        if not resolved.exists():
            errors.append(f"{doc.relative_to(REPO)}: broken link → {target}")
    return errors


def check_frontmatter(doc: Path) -> list[str]:
    text = doc.read_text()
    head = "\n".join(text.splitlines()[:25])
    return [
        f"{doc.relative_to(REPO)}: missing frontmatter field {field!r}"
        for field in FRONTMATTER_FIELDS
        if field not in head
    ]


def check_no_stub(doc: Path) -> list[str]:
    if "Status: STUB" in doc.read_text():
        return [f"{doc.relative_to(REPO)}: contains 'Status: STUB' marker (current docs must have real content)"]
    return []


def check_forbidden_terms(doc: Path) -> list[str]:
    body = strip_code_fences(doc.read_text())
    errors = []
    for term in FORBIDDEN_TERMS:
        if term in body:
            # Allow if accompanied by an explicit disclaimer/historical note in the same paragraph
            # (paragraphs separated by blank lines).
            allowed = False
            for para in body.split("\n\n"):
                if term in para and any(
                    marker in para
                    for marker in (
                        "Current code status: unknown",
                        "verify before",
                        "historical",
                        "legacy",
                        "was wrong",
                        "retracted",
                        "do NOT",
                        "must NOT",
                    )
                ):
                    allowed = True
                    break
            if not allowed:
                errors.append(f"{doc.relative_to(REPO)}: forbidden term {term!r} (wrap in code, archive the doc, or add a disclaimer)")
    return errors


def check_required_sections(doc: Path, required: list[str]) -> list[str]:
    text = doc.read_text()
    errors = []
    for section in required:
        # Match any header line starting with the section prefix (allow trailing words)
        if not any(line.strip().startswith(section) for line in text.splitlines()):
            errors.append(f"{doc.relative_to(REPO)}: missing required section {section!r}")
    return errors


def check_python_m_modules(doc: Path) -> list[str]:
    text = doc.read_text()
    errors = []
    for m in PYTHON_M_RE.finditer(text):
        mod = m.group(1)
        # ai_trading_system.foo.bar → src/ai_trading_system/foo/bar.py or src/ai_trading_system/foo/bar/__init__.py
        parts = mod.split(".")
        if parts[0] != "ai_trading_system":
            continue
        rel = SRC_PACKAGE_ROOT.joinpath(*parts[1:])
        candidate_py = rel.with_suffix(".py")
        candidate_init = rel / "__init__.py"
        if not candidate_py.exists() and not candidate_init.exists():
            errors.append(f"{doc.relative_to(REPO)}: `python -m {mod}` references non-existent module")
    return errors


def main() -> int:
    errors: list[str] = []
    docs = list(iter_current_docs())

    for doc in docs:
        errors.extend(check_links(doc))
        errors.extend(check_frontmatter(doc))
        errors.extend(check_no_stub(doc))
        # The _audit/ directory is *by design* a catalog of stale claims.
        # Skip forbidden-term checks there.
        try:
            doc.relative_to(DOCS / "_audit")
        except ValueError:
            errors.extend(check_forbidden_terms(doc))
        errors.extend(check_python_m_modules(doc))

    for doc in (DOCS / "stages").glob("*.md"):
        errors.extend(check_required_sections(doc, REQUIRED_STAGE_SECTIONS))
    for doc in (DOCS / "domains").glob("*.md"):
        errors.extend(check_required_sections(doc, REQUIRED_DOMAIN_SECTIONS))

    if errors:
        print(f"\n{len(errors)} issue(s) found:\n")
        for e in errors:
            print(f"  - {e}")
        return 1

    print(f"\nOK — validated {len(docs)} current docs, no issues.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
