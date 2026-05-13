"""Entry-gate tests — one per gate plus the happy path."""

from ai_trading_system.domains.risk.entry_policy import evaluate_entry
from ai_trading_system.domains.risk.config import EntryConfig, RiskPolicyConfig


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


def test_stage1_watchlist_extra_gates_confirm_when_present(candidate, make_market, empty_portfolio):
    cfg = RiskPolicyConfig(
        name="stage1_watchlist_test",
        entry=EntryConfig(
            require_stage_2=False,
            require_price_above_sma200=False,
            require_price_above_sma50=True,
            require_price_above_ema20=True,
            require_sma50_above_sma200_or_rising_20d=True,
            min_close_to_52w_high=0.75,
            min_return_20_pct=8.0,
            min_return_50_pct=15.0,
            max_drawdown_from_recent_high_pct=25.0,
            max_below_ema20_days_20=6,
        ),
    )
    market = make_market(
        close=100.0,
        sma_50=90.0,
        sma_200=95.0,
        ema_20=96.0,
        high_52w=120.0,
        return_20_pct=9.0,
        return_50_pct=16.0,
        sma50_rising_20d=True,
        drawdown_from_recent_high_pct=20.0,
        below_ema20_days_20=3,
        volume_ratio_20=1.6,
    )
    decision = evaluate_entry(candidate, market, empty_portfolio, cfg)
    assert decision.should_enter is True


def test_stage1_watchlist_extra_gates_reject_when_missing(candidate, make_market, empty_portfolio):
    cfg = RiskPolicyConfig(
        name="stage1_watchlist_test",
        entry=EntryConfig(
            require_stage_2=False,
            require_price_above_sma200=False,
            require_price_above_sma50=True,
            require_price_above_ema20=True,
            require_sma50_above_sma200_or_rising_20d=True,
            min_close_to_52w_high=0.75,
            min_return_20_pct=8.0,
            min_return_50_pct=15.0,
            max_drawdown_from_recent_high_pct=25.0,
            max_below_ema20_days_20=6,
        ),
    )
    market = make_market(
        close=90.0,
        sma_50=92.0,
        sma_200=95.0,
        ema_20=96.0,
        high_52w=140.0,
        return_20_pct=5.0,
        return_50_pct=10.0,
        sma50_rising_20d=False,
        drawdown_from_recent_high_pct=30.0,
        below_ema20_days_20=7,
        volume_ratio_20=1.0,
    )
    decision = evaluate_entry(candidate, market, empty_portfolio, cfg)
    assert decision.should_enter is False
    assert {
        "below_sma50",
        "below_ema20",
        "sma50_not_above_sma200_or_rising",
        "not_near_52w_high",
        "return_20_too_low",
        "return_50_too_low",
        "drawdown_too_deep",
        "too_many_closes_below_ema20",
        "volume_not_confirmed",
    } <= set(decision.reasons)


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
