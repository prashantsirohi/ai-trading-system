"""End-to-end engine tests covering exit-before-entry sequencing."""

from dataclasses import replace
from datetime import date

from ai_trading_system.domains.risk import (
    CandidateSignal,
    MarketSnapshot,
    PortfolioSnapshot,
    TradingRuleEngine,
)


def test_exits_emitted_before_entries(make_candidate, make_market, make_position, base_config):
    held = make_position(symbol_id="OLD", sector="TECH", stop_price=99.0)
    portfolio = PortfolioSnapshot(
        cash=500_000.0,
        equity=1_000_000.0,
        positions=(held,),
        sector_exposure={"TECH": 0.10},
    )
    new_cand = make_candidate(symbol_id="NEW", sector="HEALTH")
    markets = {
        "OLD": make_market(symbol_id="OLD", close=70.0),  # triggers hard_stop
        "NEW": make_market(symbol_id="NEW"),
    }

    engine = TradingRuleEngine(base_config)
    intents = engine.generate_order_intents([new_cand], markets, portfolio)

    assert len(intents) >= 2
    assert intents[0].intent_kind == "exit"
    assert intents[0].symbol_id == "OLD"
    assert intents[0].reason == "hard_stop"
    assert any(i.intent_kind == "entry" and i.symbol_id == "NEW" for i in intents)


def test_freed_slot_used_by_entry_same_tick(make_candidate, make_market, make_position, base_config):
    # Fill portfolio to capacity, then exit one and add a new candidate.
    cfg = replace(base_config)
    held = tuple(
        make_position(symbol_id=f"H{i}", sector="TECH", stop_price=99.0)
        for i in range(cfg.constraints.max_concurrent_positions)
    )
    # First held position will exit via hard stop
    portfolio = PortfolioSnapshot(
        cash=200_000.0,
        equity=1_000_000.0,
        positions=held,
        sector_exposure={"TECH": 0.50},
    )
    new_cand = make_candidate(symbol_id="NEW", sector="HEALTH")
    markets = {
        "H0": make_market(symbol_id="H0", close=70.0),  # triggers exit
        "NEW": make_market(symbol_id="NEW"),
    }
    for i in range(1, cfg.constraints.max_concurrent_positions):
        markets[f"H{i}"] = make_market(symbol_id=f"H{i}")  # default close=100, no exit

    engine = TradingRuleEngine(cfg)
    intents = engine.generate_order_intents([new_cand], markets, portfolio)

    exit_intents = [i for i in intents if i.intent_kind == "exit"]
    entry_intents = [i for i in intents if i.intent_kind == "entry"]
    assert len(exit_intents) == 1
    assert exit_intents[0].symbol_id == "H0"
    assert any(i.symbol_id == "NEW" for i in entry_intents)


def test_sector_cap_accumulates_across_same_bar_entries(
    make_candidate, make_market, base_config
):
    """Same-sector candidates evaluated in one bar must accumulate against the
    sector cap. Locks the running_portfolio update inside generate_order_intents.
    """
    portfolio = PortfolioSnapshot(
        cash=1_000_000.0,
        equity=1_000_000.0,
        positions=(),
        sector_exposure={},
    )
    # Each candidate sized to ~12% (max_stock_weight default). Five same-sector
    # candidates would push sector to 60% if accumulation broke; cap is 30%.
    candidates = [
        make_candidate(symbol_id=f"T{i}", sector="TECH", rank=i)
        for i in range(1, 6)
    ]
    markets = {f"T{i}": make_market(symbol_id=f"T{i}") for i in range(1, 6)}

    engine = TradingRuleEngine(base_config)
    intents = engine.generate_order_intents(candidates, markets, portfolio)

    entry_intents = [i for i in intents if i.intent_kind == "entry"]
    # Total TECH exposure across emitted entries must respect the 30% cap.
    total_value = sum(
        markets[i.symbol_id].close * i.quantity for i in entry_intents
    )
    assert (total_value / portfolio.equity) <= (
        base_config.constraints.max_sector_exposure_pct / 100.0
    ) + 1e-6


def test_no_entry_if_candidate_already_held(make_candidate, make_market, make_position, base_config):
    held = make_position(symbol_id="ACME", sector="TECH")
    portfolio = PortfolioSnapshot(
        cash=900_000.0, equity=1_000_000.0, positions=(held,), sector_exposure={"TECH": 0.10}
    )
    markets = {"ACME": make_market()}
    engine = TradingRuleEngine(base_config)
    intents = engine.generate_order_intents([make_candidate()], markets, portfolio)
    assert all(i.intent_kind == "exit" or i.symbol_id != "ACME" for i in intents)


def test_symbol_exited_this_tick_is_not_reentered_same_tick(
    make_candidate, make_market, make_position, base_config
):
    held = make_position(symbol_id="ACME", sector="TECH", stop_price=99.0)
    portfolio = PortfolioSnapshot(
        cash=900_000.0,
        equity=1_000_000.0,
        positions=(held,),
        sector_exposure={"TECH": 0.10},
    )
    candidates = [
        make_candidate(symbol_id="ACME", sector="TECH", rank=1),
        make_candidate(symbol_id="NEW", sector="HEALTH", rank=2),
    ]
    markets = {
        "ACME": make_market(symbol_id="ACME", close=70.0),
        "NEW": make_market(symbol_id="NEW"),
    }

    engine = TradingRuleEngine(base_config)
    intents = engine.generate_order_intents(candidates, markets, portfolio)

    assert any(i.intent_kind == "exit" and i.symbol_id == "ACME" for i in intents)
    assert not any(i.intent_kind == "entry" and i.symbol_id == "ACME" for i in intents)
    assert any(i.intent_kind == "entry" and i.symbol_id == "NEW" for i in intents)
