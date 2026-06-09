from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.fundamentals.screener_client import _section_dates, _values_by_date


def test_sparse_report_date_columns_keep_values_aligned() -> None:
    frame = pd.DataFrame(
        [
            ["Report Date", None, None, "2025-03-31", "2026-03-31"],
            ["Net profit", None, None, 49.23, 43.96],
        ]
    )

    dates = _section_dates(frame, 0)
    values = _values_by_date(dates, frame.iloc[1])

    assert values == {"2025-03-31": 49.23, "2026-03-31": 43.96}
