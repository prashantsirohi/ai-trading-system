from datetime import date

from ai_trading_system.domains.research_screener.filing_replay import rebuild_fundamentals
from ai_trading_system.domains.research_screener.reporting import _statement_rows


def _xbrl(period_end: str, start: str, *, complete: bool) -> bytes:
    facts = '<RevenueFromOperations contextRef="D" unitRef="INR">10000000</RevenueFromOperations>'
    if complete:
        facts += """
        <ProfitLossForPeriod contextRef="D" unitRef="INR">1000000</ProfitLossForPeriod>
        <DilutedEarningsLossPerShareFromContinuingOperations contextRef="D" unitRef="INRPerShare">1</DilutedEarningsLossPerShareFromContinuingOperations>
        <OtherEquity contextRef="I" unitRef="INR">10000000</OtherEquity>
        <BorrowingsCurrent contextRef="I" unitRef="INR">10000000</BorrowingsCurrent>
        <CashAndCashEquivalents contextRef="I" unitRef="INR">10000000</CashAndCashEquivalents>
        """
    return f"""<xbrl xmlns="http://example.com" xmlns:xbrli="http://www.xbrl.org/2003/instance">
      <xbrli:context id="D"><xbrli:entity/><xbrli:period><xbrli:startDate>{start}</xbrli:startDate><xbrli:endDate>{period_end}</xbrli:endDate></xbrli:period></xbrli:context>
      <xbrli:context id="I"><xbrli:entity/><xbrli:period><xbrli:instant>{period_end}</xbrli:instant></xbrli:period></xbrli:context>
      {facts}
    </xbrl>""".encode()


def _artifact(tmp_path, provider, period_type, period_end, raw, index):
    path = tmp_path / f"{provider}_{period_type}_{index}.xml"
    path.write_bytes(raw)
    return {
        "artifact_id": f"artifact:{provider}:{period_type}:{index}",
        "source_key": "filing_xbrl", "provider": provider,
        "source_url": f"https://example.test/{path.name}",
        "effective_date": period_end, "published_at": "2026-05-01T00:00:00",
        "content_hash": str(index), "validation_status": "VALID",
        "metadata": {"period_type": period_type, "scope": "consolidated" if provider == "NSE" else "standalone"},
        "_raw_path": str(path),
    }


def test_replay_reselects_whole_provider_after_new_company_contract(tmp_path):
    artifacts = [
        _artifact(tmp_path, "NSE", "annual", "2026-03-31", _xbrl("2026-03-31", "2025-04-01", complete=False), 1),
        _artifact(tmp_path, "NSE", "quarterly", "2026-06-30", _xbrl("2026-06-30", "2026-04-01", complete=False), 2),
        _artifact(tmp_path, "BSE", "annual", "2026-03-31", _xbrl("2026-03-31", "2025-04-01", complete=True), 3),
        _artifact(tmp_path, "BSE", "quarterly", "2026-06-30", _xbrl("2026-06-30", "2026-04-01", complete=True), 4),
    ]

    rebuilt = rebuild_fundamentals(
        {"annual_statements": [], "quarterly_statements": []}, artifacts,
        company_type="FINANCIAL_INSTITUTION", as_of_date=date(2026, 8, 12),
    )

    assert rebuilt["provider_selection"]["selected"] == "fallback"
    assert rebuilt["scope"] == "standalone"
    assert rebuilt["annual_completeness"] > 0
    assert rebuilt["provenance_validation"]["evidence_replay"].endswith("v1.1.0")


def test_statement_output_normalizes_mixed_checkpoint_date_types():
    member = {
        "company_id": "company:test", "security_id": "security:test",
        "statement_scope": "standalone", "annual_completeness": 1.0,
    }
    fundamentals = {
        "state": "PRESENT", "annual_statements": [
            {
                "period_type": "annual", "period_end": date(2026, 3, 31),
                "published_at": "2026-05-01T00:00:00", "scope": "standalone",
                "metrics": {}, "raw_values": {},
            },
            {
                "period_type": "annual", "period_end": "2025-03-31",
                "published_at": "2025-05-01T00:00:00", "scope": "standalone",
                "metrics": {}, "raw_values": {},
            },
        ],
    }

    rows = _statement_rows(member, fundamentals, "annual")

    assert [row["period_end"] for row in rows] == ["2026-03-31", "2025-03-31"]
