"""
Telegram Reporter Example

This script demonstrates how to use the TelegramReporter to send
QuantStats tearsheets and performance metrics via Telegram.

Setup:
1. Create a Telegram Bot:
   - Open Telegram and search for @BotFather
   - Send /newbot and follow the instructions
   - Copy the bot token

2. Get your Chat ID:
   - Open Telegram and search for @userinfobot
   - Send any message to get your chat ID
   - Or create a channel/group and get its ID

3. Set environment variables:
   export TELEGRAM_BOT_TOKEN="your_bot_token"
   export TELEGRAM_CHAT_ID="your_chat_id"

   Or add them to .env file

Usage:
    from publishers.telegram import TelegramReporter

    reporter = TelegramReporter()

    # Send metrics
    reporter.send_metrics(returns, name="My Strategy")

    # Generate and send tearsheet
    reporter.send_tearsheet(returns, name="My Strategy")

    # Send daily summary
    reporter.send_daily_summary(returns, name="My Portfolio", top_holdings=[("RELIANCE", 0.15), ("TCS", 0.10)])

    # Full report
    reporter.send_full_report(returns, name="My Strategy")
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path

from core.bootstrap import ensure_project_root_on_path

ensure_project_root_on_path(__file__)

from publishers.telegram import TelegramReporter


def create_sample_returns(start_date: str = "2024-01-01", days: int = 252) -> pd.Series:
    np.random.seed(42)
    dates = pd.date_range(start_date, periods=days, freq="B")
    daily_returns = np.random.randn(days) / 100 + 0.0003
    return pd.Series(daily_returns, index=dates, name="returns")


def example_basic_usage():
    print("=" * 50)
    print("Example 1: Basic Usage")
    print("=" * 50)

    reporter = TelegramReporter()
    returns = create_sample_returns()

    if reporter.bot:
        print("Sending metrics to Telegram...")
        reporter.send_metrics(returns, name="Sample Strategy")
        print("Done!")
    else:
        print("Telegram not configured. Generating report locally...")
        path = reporter.generate_tearsheet(returns, "Sample Strategy")
        print(f"Tearsheet saved to: {path}")


def example_with_portfolio():
    print("\n" + "=" * 50)
    print("Example 2: Portfolio with Holdings")
    print("=" * 50)

    reporter = TelegramReporter()
    returns = create_sample_returns()

    top_holdings = [
        ("RELIANCE", 0.15),
        ("TCS", 0.12),
        ("HDFCBANK", 0.10),
        ("INFY", 0.08),
        ("ICICIBANK", 0.07),
    ]

    if reporter.bot:
        print("Sending daily summary with holdings...")
        reporter.send_daily_summary(
            returns, name="My Portfolio", top_holdings=top_holdings
        )
        print("Done!")
    else:
        print("Telegram not configured. Generating report locally...")
        path = reporter.generate_tearsheet(returns, "My Portfolio")
        print(f"Tearsheet saved to: {path}")


def example_backtest_results():
    print("\n" + "=" * 50)
    print("Example 3: Backtest Results")
    print("=" * 50)

    reporter = TelegramReporter()

    dates = pd.date_range("2023-01-01", periods=756, freq="B")
    strategy_returns = pd.Series(np.random.randn(756) / 100 + 0.0004, index=dates)
    benchmark_returns = pd.Series(np.random.randn(756) / 100 + 0.0002, index=dates)

    if reporter.bot:
        print("Sending full backtest report...")
        reporter.send_full_report(
            strategy_returns,
            benchmark=benchmark_returns,
            name="RS Strategy Backtest",
            include_metrics=True,
            include_tearsheet=True,
        )
        print("Done!")
    else:
        print("Telegram not configured. Generating report locally...")
        path = reporter.generate_tearsheet(strategy_returns, "RS Strategy Backtest")
        print(f"Tearsheet saved to: {path}")


def example_sector_report():
    print("\n" + "=" * 50)
    print("Example 4: Sector Report")
    print("=" * 50)

    reporter = TelegramReporter()

    sectors = ["Power", "Pharma", "Metals", "IT", "Banks"]

    if reporter.bot:
        for sector in sectors:
            returns = create_sample_returns(days=60)
            reporter.send_daily_summary(returns, name=f"{sector} Sector")
            print(f"Sent report for {sector}")
    else:
        print("Telegram not configured.")


if __name__ == "__main__":
    example_basic_usage()
    example_with_portfolio()
    example_backtest_results()
    example_sector_report()

    print("\n" + "=" * 50)
    print("Examples complete!")
    print("=" * 50)
