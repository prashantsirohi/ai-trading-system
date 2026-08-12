from datetime import date

import pytest

from ai_trading_system.domains.research_screener.filings import (
    CORPORATE_ANNUAL,
    completeness,
    parse_xbrl_statement,
)


def test_inline_xbrl_uses_nested_contexts_concept_names_and_scale():
    raw = b"""<html xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
        xmlns:xbrli="http://www.xbrl.org/2003/instance">
      <body><ix:resources>
        <xbrli:context id="OneD"><xbrli:entity><xbrli:identifier>508486</xbrli:identifier></xbrli:entity>
          <xbrli:period><xbrli:startDate>2026-01-01</xbrli:startDate><xbrli:endDate>2026-03-31</xbrli:endDate></xbrli:period>
        </xbrli:context>
        <xbrli:context id="FourD"><xbrli:entity><xbrli:identifier>508486</xbrli:identifier></xbrli:entity>
          <xbrli:period><xbrli:startDate>2025-04-01</xbrli:startDate><xbrli:endDate>2026-03-31</xbrli:endDate></xbrli:period>
        </xbrli:context>
      </ix:resources>
      <ix:nonNumeric name="in-capmkt:ScripCode" contextRef="OneD">508486</ix:nonNumeric>
      <ix:nonNumeric name="in-capmkt:ISIN" contextRef="OneD">INE979B01015</ix:nonNumeric>
      <ix:nonFraction name="in-capmkt:RevenueFromOperations" contextRef="FourD" unitRef="INR" scale="7">1,252.93</ix:nonFraction>
      <ix:nonFraction name="in-capmkt:ProfitBeforeExceptionalItemsAndTax" contextRef="FourD" unitRef="INR" scale="7">176.62</ix:nonFraction>
      <ix:nonFraction name="in-capmkt:FinanceCosts" contextRef="FourD" unitRef="INR" scale="7">3.52</ix:nonFraction>
      <ix:nonFraction name="in-capmkt:DepreciationDepletionAndAmortisationExpense" contextRef="FourD" unitRef="INR" scale="7">13.60</ix:nonFraction>
      </body></html>"""

    parsed = parse_xbrl_statement(raw, period_end=date(2026, 3, 31), period_type="annual")

    assert parsed["document_identity"] == {"ScripCode": "508486", "ISIN": "INE979B01015"}
    assert parsed["metrics"]["sales"] == pytest.approx(1252.93)
    assert parsed["metrics"]["operating_profit"] == pytest.approx(193.74)
    assert parsed["formula_version"] == "india-xbrl-normalization-v2"


def test_completeness_does_not_backfill_missing_target_period_with_older_year():
    target = [date(year, 3, 31) for year in range(2026, 2020, -1)]
    complete_metrics = {metric: 1.0 for metric in CORPORATE_ANNUAL}
    statements = [
        {"period_end": period_end, "metrics": complete_metrics}
        for period_end in target if period_end != date(2025, 3, 31)
    ]
    statements.append({"period_end": date(2020, 3, 31), "metrics": complete_metrics})

    score = completeness(
        statements, company_type="CORPORATE", period_type="annual", periods=6,
        target_period_ends=target,
    )

    assert score == pytest.approx(5 / 6, abs=0.0001)
