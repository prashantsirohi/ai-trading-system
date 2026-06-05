"""Quarterly result acceleration scoring from company growth features."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import duckdb
import pandas as pd


OUTPUT_COLUMNS = [
    "symbol",
    "report_date",
    "available_at",
    "sales_yoy_pct",
    "sales_qoq_pct",
    "operating_profit_yoy_pct",
    "operating_profit_qoq_pct",
    "profit_yoy_pct",
    "profit_qoq_pct",
    "opm_pct",
    "opm_yoy_change_bps",
    "opm_qoq_change_bps",
    "positive_profit_quarters_4q",
    "sales_growth_positive_quarters_4q",
    "profit_growth_positive_quarters_4q",
    "margin_expansion_quarters_4q",
    "quarterly_result_score",
    "quarterly_result_bucket",
    "quarterly_result_reason",
    "low_base_flag",
    "margin_expansion_flag",
    "quality_penalty",
]


def build_quarterly_result_scores(
    *,
    fundamentals_db_path: str | Path,
    asof_date: str,
    lookback_days: int = 150,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """Build latest available quarterly result scores per symbol."""

    db_path = Path(fundamentals_db_path)
    if not db_path.exists():
        frame = pd.DataFrame(columns=OUTPUT_COLUMNS)
        _write(frame, output_path)
        return frame
    asof = pd.Timestamp(asof_date).date()
    start = asof - timedelta(days=int(lookback_days))
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        exists = bool(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_name = 'company_growth_features'
                """
            ).fetchone()[0]
        )
        if not exists:
            frame = pd.DataFrame(columns=OUTPUT_COLUMNS)
            _write(frame, output_path)
            return frame
        source = conn.execute(
            """
            SELECT *
            FROM company_growth_features
            WHERE available_at <= CAST(? AS DATE)
              AND available_at >= CAST(? AS DATE)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY available_at DESC, report_date DESC
            ) = 1
            """,
            [str(asof), str(start)],
        ).df()
    finally:
        conn.close()
    if source.empty:
        frame = pd.DataFrame(columns=OUTPUT_COLUMNS)
        _write(frame, output_path)
        return frame
    frame = _score(source)
    _write(frame, output_path)
    return frame


def _score(source: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=source.index)
    out.loc[:, "symbol"] = source["symbol"].astype(str).str.upper().str.strip()
    out.loc[:, "report_date"] = pd.to_datetime(source["report_date"]).dt.date.astype(str)
    out.loc[:, "available_at"] = pd.to_datetime(source["available_at"]).dt.date.astype(str)
    mappings = {
        "sales_yoy_pct": ("sales_yoy_growth", 100.0),
        "sales_qoq_pct": ("sales_qoq_growth", 100.0),
        "operating_profit_yoy_pct": ("operating_profit_yoy_growth", 100.0),
        "operating_profit_qoq_pct": ("operating_profit_qoq_growth", 100.0),
        "profit_yoy_pct": ("profit_yoy_growth", 100.0),
        "profit_qoq_pct": ("profit_qoq_growth", 100.0),
        "opm_pct": ("opm_pct", 1.0),
        "opm_yoy_change_bps": ("opm_yoy_change", 100.0),
        "opm_qoq_change_bps": ("opm_qoq_change", 100.0),
    }
    for output, (source_col, scale) in mappings.items():
        out.loc[:, output] = _num(source, source_col) * scale
    for column in (
        "positive_profit_quarters_4q",
        "sales_growth_positive_quarters_4q",
        "profit_growth_positive_quarters_4q",
        "margin_expansion_quarters_4q",
    ):
        out.loc[:, column] = _num(source, column).fillna(0).astype(int)

    out.loc[:, "low_base_flag"] = _low_base_flag(out)
    out.loc[:, "margin_expansion_flag"] = out["opm_yoy_change_bps"].ge(200).fillna(False)
    penalty = pd.Series(0.0, index=out.index)
    penalty = penalty + out["low_base_flag"].astype(float) * 10.0
    penalty = penalty + (out["sales_yoy_pct"].lt(5) & out["profit_yoy_pct"].gt(50)).astype(float) * 10.0
    penalty = penalty + out["opm_yoy_change_bps"].lt(-200).astype(float) * 15.0
    out.loc[:, "quality_penalty"] = penalty

    consistency = (
        out[[
            "sales_growth_positive_quarters_4q",
            "profit_growth_positive_quarters_4q",
            "margin_expansion_quarters_4q",
        ]]
        .clip(0, 4)
        .mean(axis=1)
        / 4.0
        * 100.0
    )
    score = (
        0.15 * _clip_score(out["sales_yoy_pct"], 0, 25)
        + 0.08 * _clip_score(out["sales_qoq_pct"], -5, 10)
        + 0.20 * _clip_score(out["operating_profit_yoy_pct"], 0, 40)
        + 0.10 * _clip_score(out["operating_profit_qoq_pct"], -5, 20)
        + 0.12 * _clip_score(out["profit_yoy_pct"], 0, 40)
        + 0.05 * _clip_score(out["profit_qoq_pct"], -5, 20)
        + 0.20 * _clip_score(out["opm_yoy_change_bps"], -200, 400)
        + 0.10 * consistency
        - out["quality_penalty"]
    )
    out.loc[:, "quarterly_result_score"] = score.clip(0, 100).round(2)
    buckets = out.apply(_bucket, axis=1)
    out.loc[:, "quarterly_result_bucket"] = buckets
    out.loc[:, "quarterly_result_reason"] = out.apply(_reason, axis=1)
    return out[OUTPUT_COLUMNS].sort_values("quarterly_result_score", ascending=False, kind="stable").reset_index(drop=True)


def _num(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(float("nan"), index=frame.index)
    return pd.to_numeric(frame[column], errors="coerce")


def _clip_score(values: pd.Series, low: float, high: float) -> pd.Series:
    return ((pd.to_numeric(values, errors="coerce") - low) / (high - low) * 100.0).clip(0, 100).fillna(50.0)


def _low_base_flag(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["profit_yoy_pct"].isna()
        | frame["profit_yoy_pct"].gt(150)
        | (frame["profit_yoy_pct"].gt(80) & frame["profit_qoq_pct"].gt(40))
    )


def _bucket(row: pd.Series) -> str:
    score = float(row.get("quarterly_result_score") or 0)
    sales_yoy = _float(row.get("sales_yoy_pct"))
    sales_qoq = _float(row.get("sales_qoq_pct"))
    op_yoy = _float(row.get("operating_profit_yoy_pct"))
    op_qoq = _float(row.get("operating_profit_qoq_pct"))
    profit_yoy = _float(row.get("profit_yoy_pct"))
    opm_yoy = _float(row.get("opm_yoy_change_bps"))
    opm_qoq = _float(row.get("opm_qoq_change_bps"))
    if score < 45 or opm_yoy <= -200 or profit_yoy < 0:
        return "DETERIORATING"
    if score >= 85 and sales_yoy >= 25 and op_yoy >= 40 and opm_yoy >= 300:
        return "BLOWOUT_RESULT"
    if score >= 75 and sales_yoy >= 15 and op_yoy >= 25 and opm_yoy >= 100:
        return "GREAT_RESULT"
    if score >= 70 and sales_qoq >= 5 and op_qoq >= 10 and opm_qoq >= 75:
        return "RESULT_ACCELERATION"
    if opm_yoy >= 200 and op_yoy >= 20:
        return "MARGIN_EXPANSION"
    if profit_yoy >= 50 and sales_yoy >= 10 and opm_yoy >= 300:
        return "TURNAROUND"
    return "IGNORE"


def _reason(row: pd.Series) -> str:
    bucket = str(row.get("quarterly_result_bucket") or "IGNORE")
    bits: list[str] = []
    if _float(row.get("sales_yoy_pct")) >= 15:
        bits.append("sales growth")
    if _float(row.get("operating_profit_yoy_pct")) >= 25:
        bits.append("operating profit growth")
    if _float(row.get("opm_yoy_change_bps")) >= 100:
        bits.append("OPM expansion")
    if bool(row.get("low_base_flag")):
        bits.append("low base")
    if _float(row.get("opm_yoy_change_bps")) <= -200:
        bits.append("margin contraction")
    detail = " + ".join(bits) if bits else "no clear result acceleration"
    return f"{bucket}: {detail}"


def _float(value: object) -> float:
    try:
        if pd.isna(value):
            return float("nan")
        return float(value)
    except Exception:
        return float("nan")


def _write(frame: pd.DataFrame, output_path: str | Path | None) -> None:
    if output_path is None:
        return
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


__all__ = ["OUTPUT_COLUMNS", "build_quarterly_result_scores"]
