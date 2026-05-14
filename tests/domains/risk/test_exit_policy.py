"""Exit priority tests — one per priority level + tie-break check."""

from ai_trading_system.domains.risk.exit_policy import evaluate_exit


def test_hold_when_nothing_fires(position, market, base_config):
    decision = evaluate_exit(position, market, None, base_config)
    assert decision.should_exit is False
    assert decision.reason == "hold"


def test_hard_stop_wins_over_dma(make_position, make_market, base_config):
    pos = make_position(stop_price=99.0)
    mk = make_market(close=90.0, sma_20=95.0)  # below stop AND below 20DMA
    decision = evaluate_exit(pos, mk, None, base_config)
    assert decision.should_exit is True
    assert decision.reason == "hard_stop"
    assert decision.priority == 0


def test_hard_stop_fires_on_intrabar_low_even_when_close_recovers(
    make_position, make_market, base_config
):
    """Bar with low piercing stop must trigger hard_stop even if close > stop."""
    pos = make_position(stop_price=95.0)
    mk = make_market(close=98.0, low=90.0, high=99.0, open=97.0)
    decision = evaluate_exit(pos, mk, None, base_config)
    assert decision.should_exit is True
    assert decision.reason == "hard_stop"
    assert decision.priority == 0


def test_hard_stop_does_not_fire_when_low_is_above_stop(
    make_position, make_market, base_config
):
    pos = make_position(stop_price=88.0)
    mk = make_market(close=98.0, low=95.0, high=99.0, open=97.0)
    decision = evaluate_exit(pos, mk, None, base_config)
    assert decision.should_exit is False or decision.reason != "hard_stop"


def test_close_below_200dma_emergency(make_position, make_market, base_config):
    pos = make_position(stop_price=70.0)
    mk = make_market(close=80.0, sma_200=85.0, sma_50=92.0, sma_20=97.0, sma_11=98.0)
    decision = evaluate_exit(pos, mk, None, base_config)
    assert decision.should_exit is True
    assert decision.reason == "close_below_200dma"
    assert decision.priority == 1


def test_close_below_configured_dma(make_position, make_market, base_config):
    pos = make_position(stop_price=80.0)
    # close below 20-DMA (default) but above 200-DMA and stop
    mk = make_market(close=95.0, sma_20=97.0)
    decision = evaluate_exit(pos, mk, None, base_config)
    assert decision.should_exit is True
    assert decision.reason == "close_below_20dma"
    assert decision.priority == 2


def test_dma_whipsaw_buffer_blocks_marginal_close(make_position, make_market, base_config):
    pos = make_position(stop_price=80.0)
    # 0.5% buffer → close must be < 97.0 * 0.995 = 96.515. 96.6 holds.
    mk = make_market(close=96.6, sma_20=97.0)
    decision = evaluate_exit(pos, mk, None, base_config)
    assert decision.should_exit is False


def test_rank_deterioration_streak_fires(make_position, market, base_config):
    pos = make_position(
        stop_price=80.0,
        rank_above_threshold_streak=base_config.exit.rank_deterioration_bars,
    )
    decision = evaluate_exit(pos, market, None, base_config)
    assert decision.should_exit is True
    assert decision.reason == "rank_deterioration_streak"
    assert decision.priority == 3


def test_score_deterioration_streak_fires(make_position, market, base_config):
    pos = make_position(
        stop_price=80.0,
        score_below_threshold_streak=base_config.exit.score_deterioration_bars,
    )
    decision = evaluate_exit(pos, market, None, base_config)
    assert decision.should_exit is True
    assert decision.reason == "score_deterioration_streak"
    assert decision.priority == 4


def test_time_stop_fires(make_position, market, base_config):
    pos = make_position(stop_price=80.0, bars_held=base_config.exit.time_stop_days)
    decision = evaluate_exit(pos, market, None, base_config)
    assert decision.should_exit is True
    assert decision.reason == "time_stop"
    assert decision.priority == 5


def test_priority_ordering_lowest_wins(make_position, make_market, base_config):
    # All conditions fire simultaneously — hard_stop (0) must win.
    pos = make_position(
        stop_price=99.0,
        bars_held=base_config.exit.time_stop_days + 10,
        rank_above_threshold_streak=base_config.exit.rank_deterioration_bars + 1,
        score_below_threshold_streak=base_config.exit.score_deterioration_bars + 1,
    )
    mk = make_market(close=70.0, sma_20=95.0, sma_200=90.0)
    decision = evaluate_exit(pos, mk, None, base_config)
    assert decision.reason == "hard_stop"
    assert decision.priority == 0
