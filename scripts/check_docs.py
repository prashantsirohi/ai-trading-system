#!/usr/bin/env python3
"""Lightweight validation for docs/.

Checks:
  1. No broken relative links in docs/**/*.md (excluding docs/_legacy/ and docs/evidence/).
  2. Frontmatter is present (every doc has Purpose / Audience / Last verified / Source of truth).
  3. No "Status: STUB" markers in current docs (only allowed under docs/_legacy/).
  4. No forbidden stale terms in current docs unless inside a fenced code block or explicit
     "Current code status: unknown" or "verify" disclaimer.
  5. Required sections exist in docs/stages/*.md and docs/domains/*.md.
  6. Every `python -m ai_trading_system.<mod>` invocation references a real module
     under src/ai_trading_system/.
  7. The System Guide matches the orchestrator's logical stages and feature substages.
  8. Every logical stage has a detailed stage document.
  9. Current docs and AGENTS.md route readers through the System Guide.
 10. Optional git change-impact checks require canonical docs with design changes.
 11. Every current doc is linked from docs/INDEX.md.
 12. Every `[project.scripts]` console script appears in docs/reference/commands.md.
 13. Every env var read in src/ appears in docs/reference/environment_variables.md.

Advisory (warns, never fails): a doc whose git history shows it changed after
the "Last verified" date it claims. Re-stamping without re-reading the content
against code would make the claim worse, so this is a prompt for a human.

Exit code:
  0 — all checks passed.
  1 — at least one check failed; details printed.

Usage:
  python scripts/check_docs.py                 # validate
  python scripts/check_docs.py --no-warnings   # errors only
  python scripts/check_docs.py --fix           # not implemented (placeholder for future)

This is intentionally simple — it's a lint, not a full doc engine.
"""

from __future__ import annotations

import argparse
import ast
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Iterable

REPO = Path(__file__).resolve().parent.parent
DOCS = REPO / "docs"
LEGACY = DOCS / "_legacy"
EVIDENCE = DOCS / "evidence"
# Frozen trees are excluded from current-doc lint. `_legacy` is archived; `evidence`
# holds immutable audit bundles whose files are covered by their own `bundle.sha256`,
# so editing them (e.g. to add frontmatter) would invalidate the proof.
FROZEN_TREES = (LEGACY, EVIDENCE)
SRC_PACKAGE_ROOT = REPO / "src" / "ai_trading_system"
SYSTEM_GUIDE = DOCS / "SYSTEM_GUIDE.md"
INDEX = DOCS / "INDEX.md"
COMMANDS_DOC = DOCS / "reference" / "commands.md"
ENV_VARS_DOC = DOCS / "reference" / "environment_variables.md"
ORCHESTRATOR = SRC_PACKAGE_ROOT / "pipeline" / "orchestrator.py"
PYPROJECT = REPO / "pyproject.toml"

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
GUIDE_MARKER_RE = re.compile(r"<!-- system-guide-([a-z-]+):\s*([^>]+?)\s*-->")
ENV_NAME_RE = re.compile(r"[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+")
LAST_VERIFIED_RE = re.compile(r"\*\*Last verified:\*\*\s*(\d{4}-\d{2}-\d{2})\s*$", re.M)
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

GUIDE_REQUIRED_SECTIONS = [
    "## System purpose and boundaries",
    "## Safety and operating invariants",
    "## Operational design and stages",
    "## Persistence and lineage",
    "## Operator quick start",
    "## Where to go deeper",
    "## Maintenance contract",
]

GUIDE_REQUIRED_TOKENS = [
    "$DATA_ROOT/ohlcv.duckdb",
    "$DATA_ROOT/control_plane.duckdb",
    "$DATA_ROOT/execution.duckdb",
    "$DATA_ROOT/candidate_tracker.duckdb",
    "$DATA_ROOT/masterdata.db",
    "$DATA_ROOT/feature_store/<symbol_id>/",
    "$DATA_ROOT/pipeline_runs/<run_id>/<stage>/attempt_<n>/",
    "ai_trading_system.pipeline.orchestrator",
    "ai_trading_system.ui.execution_api.app",
]


def iter_current_docs() -> Iterable[Path]:
    for p in DOCS.rglob("*.md"):
        if any(_is_under(p, frozen) for frozen in FROZEN_TREES):
            continue
        yield p


def _is_under(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


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
    lines = [line.strip().lower() for line in doc.read_text().splitlines()]
    errors = []
    for section in required:
        # Match any header line starting with the section prefix (allow trailing words)
        if not any(line.startswith(section.lower()) for line in lines):
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


def _literal_string_list(node: ast.AST, values: dict[str, list[str]]) -> list[str]:
    """Evaluate the simple string-list declarations used by the orchestrator."""
    if isinstance(node, (ast.List, ast.Tuple)):
        result: list[str] = []
        for item in node.elts:
            if isinstance(item, ast.Constant) and isinstance(item.value, str):
                result.append(item.value)
            elif isinstance(item, ast.Starred) and isinstance(item.value, ast.Name):
                result.extend(values[item.value.id])
            elif isinstance(item, ast.Name):
                result.extend(values[item.id])
            else:
                raise ValueError(f"Unsupported list expression: {ast.dump(item)}")
        return result
    raise ValueError(f"Unsupported assignment: {ast.dump(node)}")


def extract_orchestrator_stages(path: Path = ORCHESTRATOR) -> tuple[list[str], list[str], list[str]]:
    """Return (logical stages, feature substages, persisted pipeline order)."""
    tree = ast.parse(path.read_text(), filename=str(path))
    values: dict[str, list[str]] = {}
    for statement in tree.body:
        if not isinstance(statement, ast.Assign) or len(statement.targets) != 1:
            continue
        target = statement.targets[0]
        if not isinstance(target, ast.Name) or target.id not in {"FEATURE_SUBSTAGES", "PIPELINE_ORDER"}:
            continue
        values[target.id] = _literal_string_list(statement.value, values)

    feature_substages = values.get("FEATURE_SUBSTAGES", [])
    pipeline_order = values.get("PIPELINE_ORDER", [])
    if not feature_substages or not pipeline_order:
        raise ValueError("Could not extract FEATURE_SUBSTAGES and PIPELINE_ORDER")
    first_feature = pipeline_order.index(feature_substages[0])
    logical_stages = pipeline_order[:first_feature] + ["features"] + pipeline_order[first_feature + len(feature_substages) :]
    return logical_stages, feature_substages, pipeline_order


def _guide_markers(text: str) -> dict[str, list[str]]:
    return {
        name: [item.strip() for item in value.split(",") if item.strip()]
        for name, value in GUIDE_MARKER_RE.findall(text)
    }


def check_stage_documents(logical_stages: list[str], docs_root: Path = DOCS) -> list[str]:
    errors: list[str] = []
    for stage in logical_stages:
        stage_doc = docs_root / "stages" / f"{stage}.md"
        if not stage_doc.exists():
            errors.append(f"docs/stages/{stage}.md: missing detailed document for logical stage {stage!r}")
    return errors


def check_system_guide() -> list[str]:
    if not SYSTEM_GUIDE.exists():
        return ["docs/SYSTEM_GUIDE.md: canonical System Guide is missing"]

    text = SYSTEM_GUIDE.read_text()
    errors = [
        f"docs/SYSTEM_GUIDE.md: missing required section {section!r}"
        for section in GUIDE_REQUIRED_SECTIONS
        if section not in text
    ]
    errors.extend(
        f"docs/SYSTEM_GUIDE.md: missing canonical token {token!r}"
        for token in GUIDE_REQUIRED_TOKENS
        if token not in text
    )

    try:
        logical_stages, feature_substages, _ = extract_orchestrator_stages()
    except (OSError, SyntaxError, KeyError, ValueError) as exc:
        return errors + [f"Could not inspect orchestrator stages: {exc}"]

    markers = _guide_markers(text)
    if markers.get("logical-stages") != logical_stages:
        errors.append(
            "docs/SYSTEM_GUIDE.md: logical-stage marker differs from PIPELINE_ORDER; "
            f"expected {','.join(logical_stages)}"
        )
    if markers.get("feature-substages") != feature_substages:
        errors.append(
            "docs/SYSTEM_GUIDE.md: feature-substage marker differs from FEATURE_SUBSTAGES; "
            f"expected {','.join(feature_substages)}"
        )

    errors.extend(check_stage_documents(logical_stages))
    return errors


def check_index_completeness(docs: list[Path]) -> list[str]:
    """Every current doc must be reachable from the doc index.

    Compares against resolved link targets rather than raw text, so a short path
    like `README.md` is not satisfied by an unrelated `_legacy/README.md` link.
    """
    linked = {
        (INDEX.parent / m.group(1).split("#")[0].strip()).resolve()
        for m in LINK_RE.finditer(INDEX.read_text())
        if not m.group(1).startswith(("http://", "https://", "mailto:", "/"))
    }
    return [
        f"docs/INDEX.md: does not link {doc.relative_to(DOCS).as_posix()} "
        "(the index claims to be a complete map)"
        for doc in sorted(docs)
        if doc != INDEX and doc.resolve() not in linked
    ]


def console_scripts(path: Path = PYPROJECT) -> list[str]:
    """Return the console-script aliases declared in pyproject."""
    with path.open("rb") as handle:
        return sorted(tomllib.load(handle).get("project", {}).get("scripts", {}))


def check_console_scripts_documented() -> list[str]:
    text = COMMANDS_DOC.read_text()
    return [
        f"docs/reference/commands.md: console script {alias!r} is declared in pyproject but not documented"
        for alias in console_scripts()
        if alias not in text
    ]


def _env_names_from_module(tree: ast.Module) -> set[str]:
    """Env var names read by one module.

    Handles the direct forms (`os.getenv("X")`, `os.environ.get("X")`,
    `os.environ["X"]`) plus module-local wrappers: a function that reads the env
    using one of its own parameters is treated as an env reader, so calls like
    `_bool("PHASE4_API_AUTH_ENABLED", True)` are counted too. Without this,
    helper-wrapped settings silently escape the check.
    """
    names: set[str] = set()
    wrappers: dict[str, int] = {}

    def direct_read(node: ast.AST) -> ast.AST | None:
        """Return the env-name argument node if this is a direct env read."""
        if isinstance(node, ast.Subscript) and _is_os_environ(node.value):
            return node.slice
        if not isinstance(node, ast.Call):
            return None
        func = node.func
        if not isinstance(func, ast.Attribute) or not node.args:
            return None
        if func.attr == "getenv" and isinstance(func.value, ast.Name) and func.value.id == "os":
            return node.args[0]
        if func.attr == "get" and _is_os_environ(func.value):
            return node.args[0]
        return None

    # Pass 1: module-local wrappers, and direct reads with literal names.
    for parent in ast.walk(tree):
        if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
            params = [arg.arg for arg in parent.args.args]
            for node in ast.walk(parent):
                arg = direct_read(node)
                if isinstance(arg, ast.Name) and arg.id in params:
                    wrappers[parent.name] = params.index(arg.id)
    for node in ast.walk(tree):
        arg = direct_read(node)
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            names.add(arg.value)

    # Pass 2: calls into those wrappers with a literal name.
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        index = wrappers.get(node.func.id)
        if index is None or index >= len(node.args):
            continue
        arg = node.args[index]
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            names.add(arg.value)

    return {name for name in names if ENV_NAME_RE.fullmatch(name)}


def _is_os_environ(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "environ"
        and isinstance(node.value, ast.Name)
        and node.value.id == "os"
    )


def env_vars_in_source(root: Path = SRC_PACKAGE_ROOT) -> set[str]:
    names: set[str] = set()
    for path in sorted(root.rglob("*.py")):
        try:
            names |= _env_names_from_module(ast.parse(path.read_text(), filename=str(path)))
        except (OSError, SyntaxError):
            continue
    return names


def check_env_vars_documented() -> list[str]:
    text = ENV_VARS_DOC.read_text()
    return [
        f"docs/reference/environment_variables.md: {name} is read in src/ but not documented"
        for name in sorted(env_vars_in_source())
        if name not in text
    ]


def last_commit_dates(paths: Iterable[Path]) -> dict[Path, str]:
    """Map each path to its most recent commit date (YYYY-MM-DD), in one git call."""
    result = subprocess.run(
        ["git", "log", "--format=%ad", "--date=short", "--name-only", "--", str(DOCS)],
        cwd=REPO,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "git log failed")

    dates: dict[str, str] = {}
    current = ""
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        if DATE_RE.fullmatch(line):
            current = line
        elif current:
            dates.setdefault(line, current)  # log is newest-first
    known = {p.relative_to(REPO).as_posix(): p for p in paths}
    return {known[rel]: date for rel, date in dates.items() if rel in known}


def check_verification_stamps(docs: list[Path]) -> list[str]:
    """Warn when a doc changed after the date it claims to have been verified.

    This is advisory: it means someone edited content without re-verifying it.
    Re-stamping without actually checking the content against code would make
    the claim worse, so this never fails the build.
    """
    try:
        committed = last_commit_dates(docs)
    except RuntimeError as exc:
        return [f"could not read git history for verification stamps: {exc}"]

    warnings = []
    for doc in sorted(docs):
        match = LAST_VERIFIED_RE.search(doc.read_text())
        rel = doc.relative_to(REPO)
        if not match:
            warnings.append(f"{rel}: 'Last verified' is not a bare YYYY-MM-DD date")
            continue
        changed = committed.get(doc)
        if changed and changed > match.group(1):
            warnings.append(f"{rel}: last verified {match.group(1)} but last changed {changed}")
    return warnings


def check_canonical_routing() -> list[str]:
    errors: list[str] = []
    agents = (REPO / "AGENTS.md").read_text()
    if "docs/SYSTEM_GUIDE.md" not in agents:
        errors.append("AGENTS.md: required read order must include docs/SYSTEM_GUIDE.md")

    retired_terms = ("docs/CODEX_JUMPSTART.md", "high_level_operational_data_flow.md")
    paths = [REPO / "AGENTS.md"]
    for doc in iter_current_docs():
        try:
            doc.relative_to(DOCS / "_audit")
            continue
        except ValueError:
            paths.append(doc)
    for path in paths:
        text = path.read_text()
        for term in retired_terms:
            if term in text:
                errors.append(f"{path.relative_to(REPO)}: routes readers to retired document {term!r}")
    return errors


def check_change_impact(changed_paths: set[str]) -> list[str]:
    """Require canonical doc changes when central design authorities change."""
    required: set[str] = set()
    if changed_paths & {
        "src/ai_trading_system/pipeline/orchestrator.py",
        "src/ai_trading_system/pipeline/stages/__init__.py",
    }:
        required.update({"docs/SYSTEM_GUIDE.md", "docs/architecture/operational_data_flow.md"})
    if any(
        path in {
            "src/ai_trading_system/platform/db/paths.py",
            "src/ai_trading_system/pipeline/registry.py",
            "src/ai_trading_system/domains/execution/store.py",
            "src/ai_trading_system/pipeline/stages/candidate_tracker.py",
        }
        or path.startswith("src/ai_trading_system/pipeline/migrations/")
        for path in changed_paths
    ):
        required.update({"docs/SYSTEM_GUIDE.md", "docs/architecture/storage_and_lineage.md"})
    if "pyproject.toml" in changed_paths:
        required.update({"docs/SYSTEM_GUIDE.md", "docs/reference/commands.md"})

    return [
        f"design change requires {path} to change in the same commit"
        for path in sorted(required - changed_paths)
    ]


def changed_paths_from_git(base_ref: str) -> set[str]:
    if not base_ref or set(base_ref) == {"0"}:
        return set()
    result = subprocess.run(
        ["git", "diff", "--name-only", f"{base_ref}...HEAD"],
        cwd=REPO,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"git diff failed for {base_ref}")
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate current documentation")
    parser.add_argument(
        "--base-ref",
        help="Optional git base ref used to enforce design-change documentation impact rules.",
    )
    parser.add_argument(
        "--no-warnings",
        action="store_true",
        help="Skip advisory checks (stale verification stamps). Errors are unaffected.",
    )
    args = parser.parse_args(argv)
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

    errors.extend(check_system_guide())
    errors.extend(check_canonical_routing())
    errors.extend(check_index_completeness(docs))
    errors.extend(check_console_scripts_documented())
    errors.extend(check_env_vars_documented())
    if args.base_ref:
        try:
            errors.extend(check_change_impact(changed_paths_from_git(args.base_ref)))
        except RuntimeError as exc:
            errors.append(str(exc))

    warnings = [] if args.no_warnings else check_verification_stamps(docs)

    if errors:
        print(f"\n{len(errors)} issue(s) found:\n")
        for e in errors:
            print(f"  - {e}")
    if warnings:
        print(f"\n{len(warnings)} doc(s) changed since they were last verified (advisory):\n")
        for w in warnings:
            print(f"  - {w}")
        print("\nRe-verify the content against code before bumping 'Last verified'.")
    if errors:
        return 1

    print(f"\nOK — validated {len(docs)} current docs, no issues.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
