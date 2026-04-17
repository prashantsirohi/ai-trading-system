from __future__ import annotations

import pandas as pd

from analytics.data_trust import annotate_provider_reconciliation, reconcile_provider_row


def test_reconcile_provider_row_flags_discrepancy() -> None:
    reconciled = reconcile_provider_row({"close": 100.0}, {"close": 101.0})
    assert reconciled["provider_discrepancy_flag"] is True
    assert reconciled["provider_confidence"] == 0.8
    assert "primary_vs_fallback_close_diff" in str(reconciled["provider_discrepancy_note"])


def test_annotate_provider_reconciliation_marks_primary_rows() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol_id": "ABC",
                "exchange": "NSE",
                "timestamp": pd.Timestamp("2026-04-07"),
                "provider": "nse_bhavcopy",
                "close": 100.0,
            },
            {
                "symbol_id": "ABC",
                "exchange": "NSE",
                "timestamp": pd.Timestamp("2026-04-07"),
                "provider": "yfinance",
                "close": 101.0,
            },
        ]
    )

    output = annotate_provider_reconciliation(frame)
    primary = output[output["provider"] == "nse_bhavcopy"].iloc[0]
    fallback = output[output["provider"] == "yfinance"].iloc[0]
    assert bool(primary["provider_discrepancy_flag"]) is True
    assert float(primary["provider_confidence"]) == 0.8
    assert fallback["provider_confidence"] == 1.0
