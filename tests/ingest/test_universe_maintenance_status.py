from datetime import date
import json

import pytest

from ai_trading_system.domains.ingest.universe_refresh import maintenance_status
from ai_trading_system.platform.db.paths import get_domain_paths


def checkpoint(root, payload):
    path = (
        get_domain_paths(project_root=root, data_domain="operational").stage_store_dir
        / "universe_refresh"
        / "state.json"
    )
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload))
    return path


def test_separate_dates_cooldown_and_same_day_changes_are_retained(tmp_path):
    path = checkpoint(
        tmp_path,
        {
            "last_success": "2026-09-11",
            "updated_on": "2026-10-02",
            "updated_symbols": ["DONE"],
            "pending": {
                "NEWCO": {
                    "exchange": "NSE",
                    "onboarding_failure": {"retry_after": "2026-10-09"},
                }
            },
        },
    )
    before = path.read_bytes()
    result = maintenance_status(
        project_root=tmp_path, as_of=date(2026, 10, 2), decision_session="2026-10-01"
    )
    assert result["maintenance_date"] == "2026-10-02"
    assert result["decision_session"] == "2026-10-01"
    assert result["reasons"] == ["cadence_due"]
    assert result["quarantined_symbols"] == ["NEWCO"]
    assert result["updated_symbols"] == ["DONE"]
    assert path.read_bytes() == before
    assert (
        maintenance_status(
            project_root=tmp_path,
            as_of=date(2026, 10, 3),
            decision_session="2026-10-01",
        )["updated_symbols"]
        == []
    )


def test_no_state_requests_maintenance_without_creating_store(tmp_path, monkeypatch):
    monkeypatch.delenv("DATA_ROOT", raising=False)
    result = maintenance_status(
        project_root=tmp_path, as_of=date(2026, 10, 2), decision_session="2026-10-01"
    )
    assert result["status"] == "maintenance_required"
    assert not (tmp_path / "data").exists()


@pytest.mark.parametrize(
    "payload",
    [
        {"pending": []},
        {"pending": {"X": None}},
        {"pending": {"X": {"exchange": "INVALID"}}},
        {"pending": {}, "last_success": "2026-10-03"},
        {"pending": {}, "discovery_quarantine": []},
    ],
)
def test_bad_or_future_maintenance_state_fails_closed(tmp_path, payload):
    checkpoint(tmp_path, payload)
    with pytest.raises(ValueError):
        maintenance_status(
            project_root=tmp_path,
            as_of=date(2026, 10, 2),
            decision_session="2026-10-01",
        )
