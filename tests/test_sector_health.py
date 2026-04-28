"""Tests for the sector_health rule engine."""
from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.ranking.sector_health import (
    classify_sector_health,
)


def _frame(rows):
    return pd.DataFrame(rows, columns=["symbol", "sector", "stage_label",
                                       "stage_transition"])


def test_healthy_when_dominant_s2_and_positive_rs():
    df = _frame([
        ("A", "Capital Goods", "S2", "NONE"),
        ("B", "Capital Goods", "S2", "NONE"),
        ("C", "Capital Goods", "S2", "NONE"),
        ("D", "Capital Goods", "S2", "NONE"),
        ("E", "Capital Goods", "S1", "S1_TO_S2"),
        ("F", "Capital Goods", "S1", "NONE"),
        ("G", "Capital Goods", "S4", "NONE"),
    ])
    out = {h.sector: h for h in classify_sector_health(
        df, sector_rs={"Capital Goods": 1.5})}
    # 4 S2 + 1 S1_TO_S2 = 5/7 ≈ 71% > 60% with rs > 0
    assert out["Capital Goods"].health == "Healthy"


def test_unhealthy_when_high_s4_share():
    df = _frame([
        ("A", "IT", "S4", "NONE"),
        ("B", "IT", "S4", "NONE"),
        ("C", "IT", "S4", "NONE"),
        ("D", "IT", "S4", "NONE"),
        ("E", "IT", "S2", "NONE"),
        ("F", "IT", "S1", "NONE"),
    ])
    out = {h.sector: h for h in classify_sector_health(df)}
    assert out["IT"].health == "Unhealthy"


def test_unhealthy_when_rs_strongly_negative():
    df = _frame([("A", "Realty", "S2", "NONE")])
    out = {h.sector: h for h in classify_sector_health(
        df, sector_rs={"Realty": -1.5})}
    assert out["Realty"].health == "Unhealthy"


def test_improving_when_s2_share_and_rs_rising():
    df = _frame([
        ("A", "Pharma", "S2", "NONE"),
        ("B", "Pharma", "S1", "NONE"),
        ("C", "Pharma", "S1", "NONE"),
        ("D", "Pharma", "S1", "NONE"),
        ("E", "Pharma", "S1", "NONE"),
    ])
    out = {h.sector: h for h in classify_sector_health(
        df, sector_rs={"Pharma": 0.2},
        sector_rs_trend={"Pharma": 0.3},
        s2_share_trend={"Pharma": 0.05},
    )}
    assert out["Pharma"].health == "Improving"


def test_weakening_on_rising_s3():
    df = _frame([
        ("A", "Auto", "S3", "NONE"),
        ("B", "Auto", "S3", "NONE"),
        ("C", "Auto", "S2", "NONE"),
        ("D", "Auto", "S1", "NONE"),
        ("E", "Auto", "S1", "NONE"),
        ("F", "Auto", "S1", "NONE"),
    ])
    out = {h.sector: h for h in classify_sector_health(df)}
    assert out["Auto"].health == "Weakening"


def test_neutral_default():
    df = _frame([
        ("A", "Misc", "S1", "NONE"),
        ("B", "Misc", "S1", "NONE"),
        ("C", "Misc", "S2", "NONE"),
    ])
    out = {h.sector: h for h in classify_sector_health(df)}
    assert out["Misc"].health == "Neutral"


def test_per_sector_counts_populated():
    df = _frame([
        ("A", "X", "S1", "NONE"),
        ("B", "X", "S2", "S1_TO_S2"),
        ("C", "X", "S3", "NONE"),
        ("D", "X", "S4", "NONE"),
    ])
    out = classify_sector_health(df)[0]
    assert (out.s1, out.s2, out.s3, out.s4, out.s1_to_s2) == (1, 1, 1, 1, 1)
    assert out.total == 4
    assert out.to_dict()["s2_pct"] == 0.25
