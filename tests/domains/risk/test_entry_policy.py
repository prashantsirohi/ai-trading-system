"""Entry-gate tests — one per gate plus the happy path."""

from ai_trading_system.domains.risk.entry_policy import evaluate_entry


def test_happy_path_entry_confirmed(candidate, market, empty_portfolio, base_config):
    decision = evaluate_entry(candidate, market, empty_portfolio, base_config)
    assert decision.should_enter is True
    assert decision.reasons == ("entry_confirmed",)
    assert decision.initial_stop is not None
    assert decision.stop_method == "atr"


def test_rejects_when_not_stage_2(make_candidate, market, empty_portfolio, base_config):
    decision = evaluate_entry(
        make_candidate(is_stage_2=False), market, empty_portfolio, base_config
    )
    assert decision.should_enter is False
    assert "not_stage_2" in decision.reasons


def test_rejects_when_below_sma200(candidate, make_market, empty_portfolio, base_config):
    decision = evaluate_entry(
        candidate, make_market(close=80.0, sma_200=85.0), empty_portfolio, base_config
    )
    assert decision.should_enter is False
    assert "below_sma200" in decision.reasons


def test_rejects_when_sector_weak(make_candidate, market, empty_portfolio, base_config):
    decision = evaluate_entry(
        make_candidate(sector_strength=-0.2), market, empty_portfolio, base_config
    )
    assert decision.should_enter is False
    assert "weak_sector" in decision.reasons


def test_rejects_when_volume_below_threshold(candidate, make_market, empty_portfolio, base_config):
    decision = evaluate_entry(
        candidate, make_market(volume_ratio_20=0.5), empty_portfolio, base_config
    )
    assert decision.should_enter is False
    assert "volume_not_confirmed" in decision.reasons


def test_rejects_when_portfolio_full(candidate, market, empty_portfolio, base_config, make_position):
    full_positions = tuple(
        make_position(symbol_id=f"S{i}") for i in range(base_config.constraints.max_concurrent_positions)
    )
    portfolio = empty_portfolio.__class__(
        cash=empty_portfolio.cash,
        equity=empty_portfolio.equity,
        positions=full_positions,
        sector_exposure={},
    )
    decision = evaluate_entry(candidate, market, portfolio, base_config)
    assert decision.should_enter is False
    assert "portfolio_full" in decision.reasons


def test_rejects_when_already_held(candidate, market, empty_portfolio, base_config, position):
    portfolio = empty_portfolio.__class__(
        cash=empty_portfolio.cash,
        equity=empty_portfolio.equity,
        positions=(position,),
        sector_exposure={},
    )
    decision = evaluate_entry(candidate, market, portfolio, base_config)
    assert decision.should_enter is False
    assert "already_held" in decision.reasons


def test_accumulates_multiple_reasons(make_candidate, make_market, empty_portfolio, base_config):
    decision = evaluate_entry(
        make_candidate(is_stage_2=False, sector_strength=-0.1),
        make_market(close=80.0, sma_200=85.0, volume_ratio_20=0.5),
        empty_portfolio,
        base_config,
    )
    assert decision.should_enter is False
    assert {"not_stage_2", "below_sma200", "weak_sector", "volume_not_confirmed"} <= set(
        decision.reasons
    )
