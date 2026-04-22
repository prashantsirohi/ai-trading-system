from __future__ import annotations

from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[2]
ALLOWLIST_PATH = Path(__file__).with_name("path_hygiene_allowlist.txt")
SCAN_TARGETS = [
    REPO_ROOT / "collectors",
    REPO_ROOT / "tools",
    REPO_ROOT / "dashboard",
    REPO_ROOT / "main.py",
]
PATTERNS: dict[str, re.Pattern[str]] = {
    "literal_data_ohlcv_db": re.compile(r"""["']data/ohlcv\.duckdb["']"""),
    "literal_data_master_db": re.compile(r"""["']data/masterdata\.db["']"""),
    "literal_data_feature_store": re.compile(r"""["']data/feature_store[^"']*["']"""),
    "hardcoded_repo_data_root": re.compile(r"""["']ai-trading-system/data/[^"']*["']"""),
}


def _iter_python_files() -> list[Path]:
    files: list[Path] = []
    for target in SCAN_TARGETS:
        if target.is_file():
            files.append(target)
            continue
        if target.exists():
            files.extend(sorted(target.rglob("*.py")))
    return files


def _load_allowlist() -> set[str]:
    if not ALLOWLIST_PATH.exists():
        return set()
    entries: set[str] = set()
    for raw_line in ALLOWLIST_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        entries.add(line)
    return entries


def _scan_findings() -> set[str]:
    findings: set[str] = set()
    for path in _iter_python_files():
        text = path.read_text(encoding="utf-8")
        rel = path.relative_to(REPO_ROOT).as_posix()
        for pattern_id, pattern in PATTERNS.items():
            if pattern.search(text):
                findings.add(f"{rel}|{pattern_id}")
    return findings


def test_path_hygiene_ratchet_no_new_violations() -> None:
    allowlist = _load_allowlist()
    findings = _scan_findings()
    unexpected = sorted(findings - allowlist)
    assert not unexpected, (
        "Path hygiene ratchet detected new non-canonical hardcoded path patterns.\n"
        f"New violations: {unexpected}\n"
        f"Add only intentional debt to {ALLOWLIST_PATH.relative_to(REPO_ROOT)}."
    )
