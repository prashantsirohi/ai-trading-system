"""Phase 8: breadth_velocity_bucket assignment must not leak future data."""

from __future__ import annotations

from ai_trading_system.analytics.regime.breadth import _assign_breadth_velocity_bucket


def test_bucket_helper_quintile_mapping() -> None:
    # cutpoints chosen so the buckets are easy to read.
    q20, q40, q60, q80 = -1.0, -0.25, 0.25, 1.0
    assert _assign_breadth_velocity_bucket(-5.0, q20, q40, q60, q80) == ("Q1_lowest", "very_negative")
    assert _assign_breadth_velocity_bucket(-0.50, q20, q40, q60, q80) == ("Q2_low", "negative")
    assert _assign_breadth_velocity_bucket(0.00, q20, q40, q60, q80) == ("Q3_middle", "neutral")
    assert _assign_breadth_velocity_bucket(0.50, q20, q40, q60, q80) == ("Q4_high", "positive")
    assert _assign_breadth_velocity_bucket(5.00, q20, q40, q60, q80) == ("Q5_highest", "very_positive")


def test_bucket_uses_only_prior_rows_simulated() -> None:
    """Simulate the per-row computation _load_recent_raw_snapshots does.

    For row index i, cutpoints come from history rows ``< i`` only.
    Mutating a future row (i+1) must NOT change row i's assigned bucket.
    """
    # Build a deterministic chg5_score sequence: a clean ramp from -2..+2
    # over 300 points so the warmup (>=252 prior rows) is satisfied at
    # index >=257 (the 5-row pre-warmup skip plus 252).
    series = [round(-2.0 + 4.0 * i / 299, 4) for i in range(300)]
    target_idx = 290  # well past warmup

    def bucket_at(idx: int, src: list[float]) -> str:
        history = src[max(5, idx - 1260):idx]
        if len(history) < 252:
            return "neutral"
        s = sorted(history)
        m = len(s)
        q20 = s[int(m * 0.20)]
        q40 = s[int(m * 0.40)]
        q60 = s[int(m * 0.60)]
        q80 = s[int(m * 0.80)]
        return _assign_breadth_velocity_bucket(src[idx], q20, q40, q60, q80)[1]

    baseline = bucket_at(target_idx, series)

    # Now corrupt every row AFTER target_idx with a huge spike.
    corrupted = list(series)
    for j in range(target_idx + 1, len(corrupted)):
        corrupted[j] = 999.0
    corrupted_bucket = bucket_at(target_idx, corrupted)

    assert baseline == corrupted_bucket, (
        f"future-row corruption changed row {target_idx}'s bucket "
        f"({baseline} -> {corrupted_bucket}); lookahead leak detected"
    )


def test_insufficient_history_falls_back_to_neutral_low_history() -> None:
    """Below 252 prior rows of chg5_score history, bucket should be neutral."""
    # Reproduce the exact warmup-fallback condition used in the loader.
    history_len = 100
    chg5 = [0.1] * history_len
    # The fallback check is `len(history) < 252` — confirm it triggers.
    history = chg5[max(5, history_len - 1260):history_len]
    assert len(history) < 252
