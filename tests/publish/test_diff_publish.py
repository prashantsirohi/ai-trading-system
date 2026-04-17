from __future__ import annotations

from services.publish.publish_payloads import add_rank_diff


def test_add_rank_diff_tracks_previous_position_and_new_entries() -> None:
    current_rows = [
        {"symbol_id": "AAA"},
        {"symbol_id": "BBB"},
        {"symbol_id": "CCC"},
    ]
    previous_rows = [
        {"symbol_id": "BBB"},
        {"symbol_id": "AAA"},
    ]

    out = add_rank_diff(current_rows, previous_rows)

    assert out[0]["previous_rank"] == 2
    assert out[0]["rank_change"] == 1
    assert out[2]["previous_rank"] is None
    assert out[2]["new_entry"] is True

