import json

import numpy as np
import pandas as pd

from ai_trading_system.research import daily_review_outcomes as dro


def _bars(prices: dict[str, list[float]], start="2026-01-01") -> pd.DataFrame:
    rows = []
    for symbol, series in prices.items():
        dates = pd.bdate_range(start, periods=len(series))
        for d, p in zip(dates, series):
            rows.append(
                {"symbol_id": symbol, "d": d, "adjusted_close": p, "close": p, "volume": 1000}
            )
    return pd.DataFrame(rows)


def test_only_matured_horizons_and_jump_windows_flagged():
    flat = [100.0] * 40
    rising = [100.0 + i for i in range(40)]
    jumpy = [100.0] * 25 + [10.0] * 15  # unexplained 90% drop
    out = dro.compute_outcomes(_bars({"A": flat, "B": rising, "J": jumpy}), horizons=(5,))
    # last 5 bars of each symbol have no 5-session future
    assert out.groupby("symbol_id")["d"].count().eq(35).all()
    day = pd.bdate_range("2026-01-01", periods=40)[22]
    row = out[(out.symbol_id == "J") & (out.d == day)].iloc[0]
    assert row["unverified_jump"]
    clean = out[(out.symbol_id == "B") & (out.d == day)].iloc[0]
    assert not clean["unverified_jump"]
    # universe median excludes the jump name: median of A (0) and B (+5%) ~ 2.6
    assert 0 < clean["universe_median"] < clean["ret"]


def test_score_queues_dedups_episodes_and_excludes_jumps():
    dates = pd.bdate_range("2026-01-01", periods=60)
    bars = _bars(
        {
            "UP": [100.0 + 2 * i for i in range(60)],
            "FLAT": [100.0] * 60,
            "JMP": [100.0] * 45 + [10.0] * 15,
        }
    )
    outcomes = dro.compute_outcomes(bars, horizons=(5,))
    listings = pd.DataFrame(
        {
            "exchange": "NSE",
            "symbol": ["UP", "UP", "FLAT", "JMP"],
            "session": [dates[30], dates[31], dates[30], dates[42]],
            "queue": ["ENTRY_REVIEW", "ENTRY_REVIEW", "WATCH", "WATCH"],
        }
    )
    board = dro.score_queues(listings, outcomes)
    entry = board[(board.queue == "ENTRY_REVIEW") & (board.horizon == 5)].iloc[0]
    assert entry["names"] == 1  # repeat day is not a second observation
    assert entry["median_excess"] > 0
    assert entry["evidence"] == "THIN"
    watch = board[(board.queue == "WATCH") & (board.horizon == 5)].iloc[0]
    assert watch["names"] == 1 and watch["jump_excluded"] == 1


def test_load_listings_requires_calendar_pass(tmp_path):
    def pack(run, status, attempts=1):
        for n in range(1, attempts + 1):
            d = tmp_path / run / "daily_review" / f"attempt_{n}"
            d.mkdir(parents=True)
            pd.DataFrame(
                {"exchange": ["NSE", "BSE"], "symbol": ["AAA", "BBB"],
                 "session": ["2026-01-05"] * 2, "queue": ["WATCH"] * 2}
            ).to_csv(d / "daily_review_list.csv", index=False)
            (d / "daily_review_summary.json").write_text(
                json.dumps({"calendar": {"status": status}})
            )

    pack("run-ok", "PASS", attempts=2)
    pack("run-bad", "FAIL")
    out = dro.load_listings(tmp_path)
    assert out["run_id"].unique().tolist() == ["run-ok"]
    assert out["exchange"].unique().tolist() == ["NSE"]  # BSE calendar unsupported
    assert np.issubdtype(out["session"].dtype, np.datetime64)
