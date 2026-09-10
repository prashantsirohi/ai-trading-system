"""Exercise the shell wrapper without running a pipeline or opening live stores."""
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


@pytest.mark.parametrize('pipeline_rc', [0, 7])
def test_review_form_preserves_notes_and_pipeline_exit(tmp_path, pipeline_rc):
    scripts = tmp_path / 'scripts'
    scripts.mkdir()
    source = Path(__file__).resolve().parents[2] / 'scripts/run_daily_shadow.sh'
    shutil.copy2(source, scripts / source.name)
    interpreter = tmp_path / '.venv/bin/python'
    interpreter.parent.mkdir(parents=True)
    interpreter.write_text(
        f'#!{sys.executable}\n'
        'import os, sys\n'
        'if sys.argv[1:2] == ["-m"]:\n'
        '    if sys.argv[2].endswith("pipeline.orchestrator"):\n'
        '        sys.exit(int(os.environ["TEST_PIPELINE_RC"]))\n'
        '    sys.exit(1)  # No session-gate verdict: still leave a review form.\n'
        'os.execv(sys.executable, [sys.executable, *sys.argv[1:]])\n'
    )
    interpreter.chmod(0o755)
    env = {**os.environ, 'RUN_ID': 'review-run-1', 'RUN_DATE': '2026-09-09',
           'TMPDIR': str(tmp_path), 'TEST_PIPELINE_RC': str(pipeline_rc)}

    def run():
        return subprocess.run(['bash', str(scripts / source.name)], env=env,
                              capture_output=True, text=True, timeout=20)

    first = run()
    assert first.returncode == pipeline_rc, first.stderr
    out = tmp_path / 'reports/research/shadow_sessions/2026-09-09'
    form, = out.glob('operator_review_*.md')
    assert 'Run ID: review-run-1' in form.read_text()
    assert 'Review status: NOT RECORDED' in form.read_text()
    assert '- Active review minutes (exclude interruptions):\n' in form.read_text()
    assert str(form) in first.stdout
    form.write_text(form.read_text() + '\nOperator notes: 12 minutes, two unresolved listings.\n')
    before = form.read_bytes()
    assert run().returncode == pipeline_rc
    assert form.read_bytes() == before
    env['RUN_ID'] = 'review-run-2'
    assert run().returncode == pipeline_rc
    assert len(list(out.glob('operator_review_*.md'))) == 2
    assert form.read_bytes() == before
