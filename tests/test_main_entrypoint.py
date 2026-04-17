from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_main_entrypoint_is_deprecated_shim() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "main.py"],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 2
    assert "main.py is deprecated" in proc.stderr
    assert "python -m run.orchestrator" in proc.stderr
