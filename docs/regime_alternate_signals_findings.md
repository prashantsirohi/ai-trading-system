# Alternate-signal investigation — findings

Phase 7 found that **breadth-LEVEL regime** does not predict UNIV_TOP1000
forward returns — `risk_off` (= sharp breadth drawdowns) mean-reverts
strongly, so the naïve "size up in bull, size down in risk_off" rule is
wrong on this index.

This document records what *does* carry signal in the same daily
breadth series over the 2005-01-01 → 2025-12-31 window (5,151 trading
days). Reproducible via:

```
python scripts/investigate_alternate_signals.py \
    --from 2005-01-01 --to 2025-12-31 \
    --out reports/alternate_signals_20yr
```

---

## TL;DR — what works

| Signal | Best horizon | Q5−Q1 spread | Verdict |
|---|---|---|---|
| **Δ pct_above_200dma over 5 days** | **5d / 10d** | **+0.79% / +1.24%** | rate-of-change of breadth (short window) |
| **Δ pct_at_52w_high over 20 days** | 5d / 10d | +0.68% / +1.11% | leadership expansion rate |
| **Δ regime_score over 5 days** | 5d / 20d | +0.88% / +1.50% | blended-breadth velocity |
| Regime-transition day (binary) | all horizons | +0.11 to +0.31% | small but consistent edge |

## What doesn't work

| Signal | Issue |
|---|---|
| Breadth LEVEL (original Phase 7) | Monotone fail — risk_off mean-reverts |
| Δ pct_at_52w_high over 5 days | Random; needs longer accumulation window |
| Regime persistence (day_60+ bucket) | Strongly NEGATIVE — stale regimes underperform fresh ones across all horizons |

---

## Best signal: breadth momentum (Δ pct_above_200dma, 5-day window)

Bucketing all 5,151 days into quintiles by *change* in `pct_above_200dma`
over the trailing 5 trading days:

| Quintile | 5d mean return | 10d mean return |
|---|---|---|
| Q1 (worst Δ) | +0.20% | +0.34% |
| Q2 | +0.30% | +0.55% |
| Q3 | +0.53% | +1.31% |
| Q4 | +0.80% | +1.34% |
| Q5 (best Δ) | +0.99% | +1.58% |
| **Q5 − Q1** | **+0.79%** | **+1.24%** |
| **Verdict** | **PASS (monotone)** | **PASS-ish (Q3 slightly above Q4)** |

**Interpretation.** Breadth turning up — even from a low base — is what
predicts the next 1-2 weeks. This is the rate-of-change vs level
distinction that Phase 7's level-based gate missed. A market with
40% above 200DMA and *rising* outperforms one at 65% and *falling*.

The signal degrades past 10 days, presumably because mean-reversion
catches up with momentum at the 20-60 day horizons (consistent with
Phase 7's finding that `risk_off` mean-reverts hardest at 60d).

---

## Second-best signal: at-high expansion (Δ pct_at_52w_high, 20-day window)

| Quintile | 5d | 10d |
|---|---|---|
| Q1 (worst Δ) | +0.25% | +0.61% |
| Q5 (best Δ) | +0.93% | +1.72% |
| **Q5 − Q1** | **+0.68%** | **+1.11%** |
| Verdict | PASS | PASS |

Leadership widening — more stocks hitting new highs over the trailing
month — predicts the next 1-2 weeks. Notably uses the longer (20-day)
accumulation window; the 5-day variant of the same signal is noise.

---

## Combined signal: Δ regime_score (the blended metric)

`regime_score` is the Phase-4b weighted blend
(0.5·pct_above_200dma + 0.3·pct_at_52w_high + 0.2·pct_above_50dma).
Its 5-day change shows the cleanest 5d and 20d ordering:

| Quintile | 5d | 20d |
|---|---|---|
| Q1 | +0.21% | +1.42% |
| Q5 | +1.09% | +2.92% |
| **Q5 − Q1** | **+0.88%** | **+1.50%** |
| Verdict | PASS | PASS |

The blended-breadth momentum metric subsumes both individual signals
above. This is the most operationally useful version: a single number
that captures all three breadth axes' velocity.

---

## Regime transitions are weakly positive

Days where the confirmed regime *changed* (n=339 out of 5,151)
outperformed steady-state days by 11–31 bps depending on horizon, with
materially higher win rates (transition_day 65–76% vs steady_state
62–68%). Small effect; could be sampled-bias from a few large transition
events. Not strong enough to act on alone.

---

## Regime persistence — NEGATIVE finding

Days bucketed by how long the current regime has held:

| Days in regime | 20d mean return | 60d mean return |
|---|---|---|
| 0–2 | +2.15% | +6.97% |
| 3–7 | +1.47% | +6.33% |
| 8–20 | +2.20% | +6.58% |
| 21–60 | +3.71% | +7.83% |
| **60+** | **+0.73%** | **+5.21%** |

The longest-held regimes systematically underperform. Stale regimes
should probably trigger a "needs re-check" signal rather than
contribute additional confidence.

---

## Recommended next steps

1. **Add `breadth_momentum_5d` and `regime_score_velocity` to the
   snapshot.** Both are pure functions of the existing breadth series;
   the cost is one extra `diff(5)` per day. They'd live alongside
   `regime_score` and `regime_confidence`.

2. **Don't size off `risk_off` regime alone.** Phase 5/6 rebalancing
   should gate on `regime_score velocity`, not the categorical
   `risk_off` label. A `risk_off + falling` day is still defensive;
   a `risk_off + rising` day is the recovery setup.

3. **Cap regime confidence above day-60.** When `days_in_regime > 60`,
   downweight the regime profile's risk-on tilt — the data says
   sustained regimes lose predictive power.

4. **Per-stock conditioning (not done yet).** This investigation tested
   index-level forward returns only. The next worthwhile test is
   whether regime affects *cross-sectional dispersion* — does
   high-RS outperform low-RS more in one regime than another? That
   would justify the Phase-5 factor-weight shifts at a per-stock
   level even if the index doesn't move.

5. **Phase 7 gate stays NO-GO until either of the above is shipped.**
   The original framework as proposed (size by regime label alone)
   should not be enabled for live trading.
