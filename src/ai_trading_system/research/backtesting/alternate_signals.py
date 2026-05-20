"""Alternate-signal investigation (Phase 7 follow-up).

Phase 7 found that breadth-LEVEL regime does not predict forward returns
on UNIV_TOP1000 — risk_off (= sharp breadth drawdowns) mean-reverts
strongly. This module tests whether other transforms of the same breadth
series carry predictive signal.

Hypotheses tested:
  H1 — Breadth momentum (Δ pct_above_200dma over N days). Improving
       breadth may predict returns even when the level doesn't.
  H2 — Regime-transition days. The DAY OF a regime change might behave
       differently than steady-state days within a regime.
  H3 — Combined signal: regime × breadth-momentum quintile.
  H4 — Leadership-expansion rate (Δ pct_at_52w_high over N days).
  H5 — Regime persistence (days-in-current-regime).

All hypotheses test forward returns of the same UNIV_TOP1000 series the
regime is measured against, at 5/10/20/60-day horizons. A pass is
*monotone non-decreasing mean forward returns* across the signal
quintiles (or across signal direction for binary signals).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.research.backtesting.regime_report import (
    DEFAULT_BENCHMARK,
    DEFAULT_HORIZONS,
    _attach_forward_returns,
    _load_benchmark_closes,
    _resolve_db_path,
    build_regime_forward_return_report,
)

LOG = logging.getLogger(__name__)

# Quintile labels (lowest to highest signal value)
_QUINTILE_LABELS = ("Q1_lowest", "Q2", "Q3", "Q4", "Q5_highest")


@dataclass(frozen=True)
class AlternateSignalsReport:
    daily: pd.DataFrame
    findings: dict[str, Any]


def _quintile_bucket(series: pd.Series, labels: tuple[str, ...] = _QUINTILE_LABELS) -> pd.Series:
    """Assign each value to a quintile label. Drops NaN inputs."""
    clean = pd.to_numeric(series, errors="coerce")
    try:
        buckets = pd.qcut(clean, 5, labels=list(labels), duplicates="drop")
    except ValueError:
        # Not enough unique values to form 5 quantiles
        return pd.Series([pd.NA] * len(series), index=series.index, dtype="object")
    return buckets.astype("object")


def _signal_ordering_pass(
    daily: pd.DataFrame,
    *,
    signal_col: str,
    horizons: tuple[int, ...],
    bucket_order: tuple[str, ...] = _QUINTILE_LABELS,
) -> dict[str, Any]:
    """Group by signal bucket, compute fwd-return means, verdict ordering.

    Pass = mean forward returns monotone non-decreasing from Q1 → Q5.
    """
    out_per_horizon: dict[str, Any] = {}
    for n in horizons:
        ret_col = f"fwd_{n}_return"
        if ret_col not in daily.columns:
            continue
        grouped = daily.groupby(signal_col, dropna=True, observed=True)[ret_col]
        means_by_bucket: dict[str, float] = {}
        sample_sizes: dict[str, int] = {}
        for bucket, vals in grouped:
            cleaned = pd.to_numeric(vals, errors="coerce").dropna()
            sample_sizes[str(bucket)] = int(len(cleaned))
            means_by_bucket[str(bucket)] = (
                round(float(cleaned.mean()), 4) if len(cleaned) else None
            )

        # Order means by the configured bucket order
        ordered = [
            (b, means_by_bucket.get(b))
            for b in bucket_order
            if b in means_by_bucket and means_by_bucket[b] is not None
        ]
        monotone = (
            all(ordered[i][1] <= ordered[i + 1][1] for i in range(len(ordered) - 1))
            if len(ordered) >= 2
            else False
        )
        # Also compute Q5 - Q1 spread — the simplest "does the top quintile
        # outperform the bottom?" question.
        spread = None
        if (
            len(ordered) == len(bucket_order)
            and ordered[0][1] is not None
            and ordered[-1][1] is not None
        ):
            spread = round(ordered[-1][1] - ordered[0][1], 4)
        out_per_horizon[f"{n}d"] = {
            "monotone_non_decreasing": monotone,
            "by_bucket": [
                {"bucket": b, "mean_return_pct": m, "sample_size": sample_sizes.get(b, 0)}
                for b, m in ordered
            ],
            "q5_minus_q1_pct": spread,
        }
    return out_per_horizon


def _binary_ordering(
    daily: pd.DataFrame,
    *,
    signal_col: str,
    true_label: str,
    false_label: str,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    """Compare forward returns on signal=True vs signal=False days."""
    out: dict[str, Any] = {}
    for n in horizons:
        ret_col = f"fwd_{n}_return"
        if ret_col not in daily.columns:
            continue
        true_mask = daily[signal_col].astype(bool)
        true_rets = pd.to_numeric(daily.loc[true_mask, ret_col], errors="coerce").dropna()
        false_rets = pd.to_numeric(daily.loc[~true_mask, ret_col], errors="coerce").dropna()
        out[f"{n}d"] = {
            true_label: {
                "sample_size": int(len(true_rets)),
                "mean_return_pct": round(float(true_rets.mean()), 4) if len(true_rets) else None,
                "win_rate_pct": round(float((true_rets > 0).mean() * 100.0), 2) if len(true_rets) else None,
            },
            false_label: {
                "sample_size": int(len(false_rets)),
                "mean_return_pct": round(float(false_rets.mean()), 4) if len(false_rets) else None,
                "win_rate_pct": round(float((false_rets > 0).mean() * 100.0), 2) if len(false_rets) else None,
            },
            "true_minus_false_pct": (
                round(float(true_rets.mean() - false_rets.mean()), 4)
                if len(true_rets) and len(false_rets)
                else None
            ),
        }
    return out


def build_alternate_signals_report(
    *,
    project_root: Path | str | None = None,
    from_date: str,
    to_date: str,
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
    benchmark: str = DEFAULT_BENCHMARK,
    db_path: str | Path | None = None,
    momentum_windows: tuple[int, ...] = (5, 20),
) -> AlternateSignalsReport:
    """Test alternate transforms of the breadth series for predictive signal.

    Reuses build_regime_forward_return_report() for the daily breadth +
    regime + forward-returns frame, then derives extra signal columns
    and buckets forward returns by each.
    """
    base = build_regime_forward_return_report(
        project_root=project_root,
        from_date=from_date,
        to_date=to_date,
        horizons=horizons,
        benchmark=benchmark,
        db_path=db_path,
    )
    daily = base.daily.copy()

    # ── H1 / H4: signal momentum (Δ over N days) ────────────────────────
    for window in momentum_windows:
        daily[f"breadth_200dma_change_{window}d"] = daily["pct_above_200dma"].diff(window)
        daily[f"at_high_change_{window}d"] = daily["pct_at_52w_high"].diff(window)
        daily[f"regime_score_change_{window}d"] = daily["regime_score"].diff(window)

    # ── H2: regime-transition days ──────────────────────────────────────
    daily["regime_changed"] = daily["regime"] != daily["regime"].shift(1)
    # Run-length of consecutive days in the current regime
    daily["regime_run_id"] = (
        (daily["regime"] != daily["regime"].shift(1)).cumsum()
    )
    daily["days_in_regime"] = daily.groupby("regime_run_id").cumcount()

    # ── Hypothesis tests ───────────────────────────────────────────────
    findings: dict[str, Any] = {
        "from_date": from_date,
        "to_date": to_date,
        "benchmark": benchmark,
        "total_days": int(len(daily)),
        "hypotheses": {},
    }

    # H1 — breadth momentum 5d
    for window in momentum_windows:
        signal_col = f"breadth_200dma_change_{window}d"
        quintile_col = f"{signal_col}_quintile"
        daily[quintile_col] = _quintile_bucket(daily[signal_col])
        findings["hypotheses"][f"H1_breadth_200dma_change_{window}d"] = {
            "description": (
                f"Quintiles of Δ pct_above_200dma over {window} trading days. "
                f"Pass = improving breadth → higher forward returns."
            ),
            "results": _signal_ordering_pass(
                daily, signal_col=quintile_col, horizons=horizons
            ),
        }

    # H4 — at-high expansion rate
    for window in momentum_windows:
        signal_col = f"at_high_change_{window}d"
        quintile_col = f"{signal_col}_quintile"
        daily[quintile_col] = _quintile_bucket(daily[signal_col])
        findings["hypotheses"][f"H4_at_high_change_{window}d"] = {
            "description": (
                f"Quintiles of Δ pct_at_52w_high over {window} trading days. "
                f"Pass = widening new-high participation → higher forward returns."
            ),
            "results": _signal_ordering_pass(
                daily, signal_col=quintile_col, horizons=horizons
            ),
        }

    # H1b — regime_score momentum (combines all three breadth signals)
    for window in momentum_windows:
        signal_col = f"regime_score_change_{window}d"
        quintile_col = f"{signal_col}_quintile"
        daily[quintile_col] = _quintile_bucket(daily[signal_col])
        findings["hypotheses"][f"H1b_regime_score_change_{window}d"] = {
            "description": (
                f"Quintiles of Δ regime_score over {window} trading days. "
                f"Pass = blended-breadth improvement → higher forward returns."
            ),
            "results": _signal_ordering_pass(
                daily, signal_col=quintile_col, horizons=horizons
            ),
        }

    # H2 — regime-transition day vs steady-state
    findings["hypotheses"]["H2_regime_transition_day"] = {
        "description": (
            "Forward returns on the day a regime CHANGE was classified, "
            "vs days when the regime was unchanged."
        ),
        "results": _binary_ordering(
            daily,
            signal_col="regime_changed",
            true_label="transition_day",
            false_label="steady_state",
            horizons=horizons,
        ),
    }

    # H5 — regime persistence (newer vs older entry into the current regime)
    daily["regime_age_bucket"] = pd.cut(
        daily["days_in_regime"],
        bins=[-1, 2, 7, 20, 60, 10_000],
        labels=["day_0_2", "day_3_7", "day_8_20", "day_21_60", "day_60plus"],
    ).astype("object")
    findings["hypotheses"]["H5_regime_persistence"] = {
        "description": (
            "Forward returns bucketed by number of trading days since the "
            "current regime began. Pass = sustained regimes outperform "
            "fresh ones (or vice versa — either direction is informative)."
        ),
        "results": _signal_ordering_pass(
            daily,
            signal_col="regime_age_bucket",
            horizons=horizons,
            bucket_order=("day_0_2", "day_3_7", "day_8_20", "day_21_60", "day_60plus"),
        ),
    }

    return AlternateSignalsReport(daily=daily, findings=findings)


def write_alternate_signals_report(
    report: AlternateSignalsReport,
    *,
    out_dir: Path | str,
    stem: str = "alternate_signals",
) -> dict[str, Path]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    findings_path = out / f"{stem}.json"
    daily_path = out / f"{stem}_daily.csv"
    findings_path.write_text(
        json.dumps(report.findings, indent=2, default=str), encoding="utf-8"
    )
    report.daily.to_csv(daily_path, index=False)
    return {"findings": findings_path, "daily": daily_path}
