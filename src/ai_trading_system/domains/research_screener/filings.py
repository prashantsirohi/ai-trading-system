from __future__ import annotations

import hashlib
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


CORPORATE_ANNUAL = {
    "sales", "operating_profit", "net_profit", "eps", "reserves", "borrowings",
    "cash_and_bank", "cash_from_operations", "capex",
}
CORPORATE_QUARTERLY = {"sales", "operating_profit", "net_profit", "exceptional_items", "eps"}
BANK_METRICS = {"nim", "gnpa", "nnpa", "slippages", "credit_cost", "roa", "roe", "cet1"}
FINANCIAL_INSTITUTION_ANNUAL = {
    "sales", "net_profit", "eps", "reserves", "borrowings", "cash_and_bank",
}
FINANCIAL_INSTITUTION_QUARTERLY = {"sales", "net_profit", "eps"}


TAG_ALIASES = {
    "sales": ("RevenueFromOperations",),
    "net_profit": ("ProfitLossForPeriod", "ProfitLossForPeriodFromContinuingOperations"),
    "eps": (
        "DilutedEarningsLossPerShareFromContinuingAndDiscontinuedOperations",
        "DilutedEarningsLossPerShareFromContinuingOperations",
    ),
    "exceptional_items": ("ExceptionalItemsBeforeTax",),
    "depreciation": ("DepreciationDepletionAndAmortisationExpense",),
    "finance_costs": ("FinanceCosts",),
    "profit_before_exceptional_items_and_tax": ("ProfitBeforeExceptionalItemsAndTax",),
    "reserves": ("OtherEquity", "ReserveExcludingRevaluationReserves"),
    "borrowings_current": ("BorrowingsCurrent",),
    "borrowings_noncurrent": ("BorrowingsNoncurrent",),
    "cash": ("CashAndCashEquivalents", "CashAndCashEquivalentsCashFlowStatement"),
    "bank_balance": ("BankBalanceOtherThanCashAndCashEquivalents",),
    "cash_from_operations": ("CashFlowsFromUsedInOperatingActivities",),
    "capex": ("PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities",),
    "nim": ("NetInterestMargin", "NetInterestMarginPercentage"),
    "gnpa": ("GrossNonPerformingAssetsRatio", "PercentageOfGrossNonPerformingAssets", "PercentageOfGrossNpa"),
    "nnpa": ("NetNonPerformingAssetsRatio", "PercentageOfNetNonPerformingAssets", "PercentageOfNpa"),
    "slippages": ("Slippages", "FreshSlippages"),
    "credit_cost": ("CreditCost", "CreditCostRatio"),
    "roa": ("ReturnOnAssets", "ReturnOnAverageAssets"),
    "roe": ("ReturnOnEquity", "ReturnOnAverageEquity"),
    "cet1": ("CommonEquityTier1CapitalRatio", "CommonEquityTierOneCapitalRatio"),
}


def parse_exchange_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text or text == "-":
        return None
    text = re.sub(r"\s+", " ", text)
    for fmt in (
        "%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M", "%b %d %Y %I:%M%p",
        "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def parse_exchange_date(value: Any) -> date | None:
    text = str(value or "").strip()
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d-%b-%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def mandatory_metrics(company_type: str, period_type: str) -> set[str]:
    if company_type == "BANK":
        return BANK_METRICS
    if company_type == "FINANCIAL_INSTITUTION":
        return (
            FINANCIAL_INSTITUTION_ANNUAL
            if period_type == "annual" else FINANCIAL_INSTITUTION_QUARTERLY
        )
    if company_type in {"CORPORATE", "INDUSTRIAL", "MARKET_INFRASTRUCTURE"}:
        return CORPORATE_ANNUAL if period_type == "annual" else CORPORATE_QUARTERLY
    return set()


def parse_xbrl_statement(raw: bytes, *, period_end: date, period_type: str) -> dict:
    root = ET.fromstring(raw)
    xbrli = "{http://www.xbrl.org/2003/instance}"
    contexts: dict[str, dict] = {}
    for context in root.findall(".//" + xbrli + "context"):
        period = context.find(xbrli + "period")
        if period is None:
            continue
        start = period.find(xbrli + "startDate")
        end = period.find(xbrli + "endDate")
        instant = period.find(xbrli + "instant")
        contexts[str(context.get("id"))] = {
            "start": date.fromisoformat(start.text) if start is not None and start.text else None,
            "end": date.fromisoformat(end.text) if end is not None and end.text else None,
            "instant": date.fromisoformat(instant.text) if instant is not None and instant.text else None,
            "dimensioned": context.find(".//" + xbrli + "segment") is not None,
        }

    by_name: dict[str, list[dict]] = {}
    document_identity: dict[str, str] = {}
    for element in root.iter():
        context_id = element.get("contextRef")
        text = "".join(element.itertext()).strip()
        if not context_id or not text or context_id not in contexts:
            continue
        local_name = element.tag.rsplit("}", 1)[-1]
        # Inline XBRL facts use ix:nonFraction/ix:nonNumeric and carry the
        # taxonomy concept in the name attribute. Instance XBRLs use the
        # taxonomy concept as the element name directly.
        qualified_name = element.get("name") if local_name in {"nonFraction", "nonNumeric", "fraction"} else None
        name = str(qualified_name or local_name).split(":")[-1]
        if name in {"ISIN", "ScripCode", "Symbol"} and name not in document_identity:
            document_identity[name] = text.strip().upper()
        by_name.setdefault(name, []).append({
            "value": text, "context": contexts[context_id], "unit": element.get("unitRef"),
            "decimals": element.get("decimals"), "context_id": context_id,
            "scale": element.get("scale"), "sign": element.get("sign"),
        })

    expected_days = 365 if period_type == "annual" else 91

    def fact(tag_names: tuple[str, ...]) -> dict | None:
        candidates = []
        for priority, tag in enumerate(tag_names):
            for item in by_name.get(tag, []):
                ctx = item["context"]
                fact_end = ctx["end"] or ctx["instant"]
                if fact_end != period_end:
                    continue
                duration = (ctx["end"] - ctx["start"]).days + 1 if ctx["start"] and ctx["end"] else expected_days
                candidates.append((ctx["dimensioned"], abs(duration - expected_days), priority, item))
        return min(candidates, default=(None, None, None, None), key=lambda row: row[:3])[3]

    raw_values: dict[str, dict] = {}
    normalized: dict[str, float | None] = {}
    for metric, tags in TAG_ALIASES.items():
        selected = fact(tags)
        if selected is None:
            normalized[metric] = None
            continue
        raw_values[metric] = selected
        number = _decimal_value(selected)
        if number is None:
            normalized[metric] = None
            continue
        unit = str(selected.get("unit") or "")
        normalized[metric] = float(number / Decimal("10000000")) if unit.upper() == "INR" else float(number)

    normalized["borrowings"] = _sum_optional(normalized.get("borrowings_current"), normalized.get("borrowings_noncurrent"))
    normalized["cash_and_bank"] = _sum_optional(normalized.get("cash"), normalized.get("bank_balance"))
    normalized["operating_profit"] = _sum_optional(
        normalized.get("profit_before_exceptional_items_and_tax"),
        normalized.get("finance_costs"), normalized.get("depreciation"),
    )
    return {
        "metrics": normalized,
        "raw_values": raw_values,
        "formula_version": "india-xbrl-normalization-v2",
        "document_identity": document_identity,
        "source_row_hash": hashlib.sha256(raw + f"|{period_type}|{period_end}".encode()).hexdigest(),
    }


def completeness(statements: list[dict], *, company_type: str, period_type: str, periods: int,
                 target_period_ends: list[date] | None = None) -> float:
    required = mandatory_metrics(company_type, period_type)
    if not required:
        return 0.0
    if target_period_ends is None:
        selected = sorted(statements, key=lambda row: row["period_end"], reverse=True)[:periods]
    else:
        by_period = {row["period_end"]: row for row in statements}
        selected = [by_period.get(period_end, {"period_end": period_end, "metrics": {}})
                    for period_end in target_period_ends[:periods]]
    present = sum(
        1 for statement in selected for metric in required
        if statement.get("metrics", {}).get(metric) is not None
    )
    return round(present / (len(required) * periods), 4)


def _sum_optional(*values: float | None) -> float | None:
    present = [value for value in values if value is not None]
    return sum(present) if present else None


def _decimal_value(fact: dict) -> Decimal | None:
    text = str(fact.get("value") or "").strip().replace(",", "")
    if not text or text in {"-", "—"}:
        return None
    negative_parentheses = text.startswith("(") and text.endswith(")")
    if negative_parentheses:
        text = text[1:-1]
    try:
        number = Decimal(text)
        scale = int(fact.get("scale") or 0)
    except (InvalidOperation, TypeError, ValueError):
        return None
    if scale:
        number *= Decimal(10) ** scale
    if negative_parentheses or str(fact.get("sign") or "").strip() == "-":
        number = -abs(number)
    return number
