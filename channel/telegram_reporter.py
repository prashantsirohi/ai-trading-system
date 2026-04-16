import os
import io
import asyncio
import socket
import ssl
from pathlib import Path
from typing import Awaitable, Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
import quantstats as qs
from core.runtime_config import TelegramRuntimeConfig
from utils.env import load_project_env

load_project_env(__file__)

try:
    from telegram import Bot, InputFile
    from telegram.error import TelegramError
    from telegram.request import HTTPXRequest
except ImportError:
    raise ImportError(
        "python-telegram-bot is required. Install with: pip install python-telegram-bot"
    )
from core.logging import logger


class TelegramReporter:
    TELEGRAM_HOST = "api.telegram.org"

    def __init__(
        self,
        bot_token: Optional[str] = None,
        chat_id: Optional[str] = None,
        report_dir: Optional[Path] = None,
    ):
        runtime = TelegramRuntimeConfig.from_env()
        self.bot_token = bot_token or runtime.bot_token
        self.chat_id = chat_id or runtime.chat_id
        self.report_dir = Path(report_dir) if report_dir else Path("reports")
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.bot = None
        self._loop = None
        self.last_error: Optional[str] = None
        self.last_error_code: Optional[str] = None
        self.last_health_check: Optional[Dict[str, str]] = None
        self.send_attempts = runtime.send_attempts
        self.dns_precheck_enabled = runtime.dns_precheck_enabled

        if self.bot_token:
            request = HTTPXRequest(
                connect_timeout=runtime.connect_timeout_seconds,
                read_timeout=runtime.read_timeout_seconds,
                write_timeout=runtime.write_timeout_seconds,
                pool_timeout=runtime.pool_timeout_seconds,
            )
            self.bot = Bot(token=self.bot_token, request=request)

    def _get_or_create_loop(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop

    def _validate_config(self) -> bool:
        if not self.bot:
            self.last_error_code = "telegram_not_configured"
            self.last_error = "Telegram bot not configured. Set TELEGRAM_BOT_TOKEN"
            logger.error(self.last_error)
            return False
        if not self.chat_id:
            self.last_error_code = "telegram_chat_missing"
            self.last_error = "Telegram chat_id not set. Set TELEGRAM_CHAT_ID"
            logger.error(self.last_error)
            return False
        return True

    def _record_error(self, code: str, message: str) -> None:
        self.last_error_code = code
        self.last_error = message

    def _reset_last_error(self) -> None:
        self.last_error = None
        self.last_error_code = None

    def _health_check(self) -> bool:
        if not self.dns_precheck_enabled:
            self.last_health_check = {
                "status": "skipped",
                "kind": "telegram_dns_precheck_disabled",
                "detail": "DNS precheck disabled by configuration.",
            }
            return True
        try:
            socket.getaddrinfo(self.TELEGRAM_HOST, 443)
            self.last_health_check = {
                "status": "ok",
                "kind": "telegram_dns_ok",
                "detail": f"Resolved {self.TELEGRAM_HOST}",
            }
            return True
        except OSError as exc:
            detail = f"Unable to resolve {self.TELEGRAM_HOST}: {exc}"
            self.last_health_check = {
                "status": "failed",
                "kind": "telegram_dns_failure",
                "detail": detail,
            }
            self._record_error("telegram_dns_failure", detail)
            logger.warning("Telegram DNS precheck failed for %s: %s", self.TELEGRAM_HOST, exc)
            return False

    def _classify_exception(self, exc: Exception) -> Tuple[str, str]:
        message = str(exc).strip() or exc.__class__.__name__
        lowered = message.lower()
        if isinstance(exc, ssl.SSLError) or "ssl" in lowered or "tls" in lowered or "record_layer_failure" in lowered:
            return "telegram_ssl_failure", f"Telegram SSL failure: {message}"
        if isinstance(exc, (TimeoutError, asyncio.TimeoutError)) or "timed out" in lowered or "timeout" in lowered:
            return "telegram_timeout", f"Telegram timeout: {message}"
        if isinstance(exc, socket.gaierror) or "getaddrinfo" in lowered or "name or service not known" in lowered or "nodename nor servname provided" in lowered:
            return "telegram_dns_failure", f"Telegram DNS failure: {message}"
        if isinstance(exc, OSError) and ("temporary failure in name resolution" in lowered or "name resolution" in lowered):
            return "telegram_dns_failure", f"Telegram DNS failure: {message}"
        if isinstance(exc, TelegramError):
            return "telegram_api_error", f"Telegram API error: {message}"
        return "telegram_network_error", f"Telegram network failure: {message}"

    async def _send_with_retry(
        self,
        sender: Callable[[], Awaitable[None]],
    ) -> bool:
        if not self._validate_config():
            return False
        if not self._health_check():
            return False
        for attempt in range(1, self.send_attempts + 1):
            try:
                await sender()
                self._reset_last_error()
                return True
            except Exception as exc:
                code, detail = self._classify_exception(exc)
                self._record_error(code, detail)
                logger.error(
                    "Telegram send failed on attempt %s/%s [%s]: %s",
                    attempt,
                    self.send_attempts,
                    code,
                    exc,
                )
                if attempt >= self.send_attempts:
                    return False
        return False

    async def _send_message_async(self, text: str, parse_mode: str = "HTML") -> bool:
        async def _sender() -> None:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode=parse_mode,
            )

        return await self._send_with_retry(_sender)

    def send_message(self, text: str, parse_mode: str = "HTML") -> bool:
        loop = self._get_or_create_loop()
        return loop.run_until_complete(self._send_message_async(text, parse_mode))

    async def _send_photo_async(
        self,
        photo_path: Union[str, Path],
        caption: Optional[str] = None,
    ) -> bool:
        async def _sender() -> None:
            with open(photo_path, "rb") as f:
                await self.bot.send_photo(
                    chat_id=self.chat_id,
                    photo=InputFile(f, filename=Path(photo_path).name),
                    caption=caption,
                )

        return await self._send_with_retry(_sender)

    def send_photo(
        self, photo_path: Union[str, Path], caption: Optional[str] = None
    ) -> bool:
        loop = self._get_or_create_loop()
        return loop.run_until_complete(self._send_photo_async(photo_path, caption))

    async def _send_document_async(
        self,
        doc_path: Union[str, Path],
        caption: Optional[str] = None,
    ) -> bool:
        async def _sender() -> None:
            with open(doc_path, "rb") as f:
                await self.bot.send_document(
                    chat_id=self.chat_id,
                    document=InputFile(f, filename=Path(doc_path).name),
                    caption=caption,
                )

        return await self._send_with_retry(_sender)

    def send_document(
        self, doc_path: Union[str, Path], caption: Optional[str] = None
    ) -> bool:
        loop = self._get_or_create_loop()
        return loop.run_until_complete(self._send_document_async(doc_path, caption))

    async def _send_html_report_async(
        self,
        html_path: Union[str, Path],
        caption: Optional[str] = None,
    ) -> bool:
        return await self._send_document_async(html_path, caption)

    def send_html_report(
        self, html_path: Union[str, Path], caption: Optional[str] = None
    ) -> bool:
        loop = self._get_or_create_loop()
        return loop.run_until_complete(self._send_html_report_async(html_path, caption))

    def generate_tearsheet(
        self,
        returns: pd.Series,
        name: str = "Strategy",
        periods_per_year: int = 252,
        match_dates: bool = True,
        save_path: Optional[Path] = None,
    ) -> Path:
        save_path = save_path or self.report_dir / f"{name}_tearsheet.html"
        qs.reports.html(
            returns=returns,
            name=name,
            periods_per_year=periods_per_year,
            match_dates=match_dates,
            output=save_path,
        )
        return save_path

    async def send_tearsheet_async(
        self,
        returns: pd.Series,
        name: str = "Strategy",
        caption: Optional[str] = None,
        periods_per_year: int = 252,
    ) -> bool:
        save_path = self.generate_tearsheet(returns, name, periods_per_year)
        default_caption = f"<b>{name}</b> QuantStats Tearsheet"
        return await self._send_html_report_async(save_path, caption or default_caption)

    def send_tearsheet(
        self,
        returns: pd.Series,
        name: str = "Strategy",
        caption: Optional[str] = None,
        periods_per_year: int = 252,
    ) -> bool:
        loop = self._get_or_create_loop()
        return loop.run_until_complete(
            self.send_tearsheet_async(returns, name, caption, periods_per_year)
        )

    async def send_metrics_async(
        self,
        returns: pd.Series,
        name: str = "Strategy",
        periods_per_year: int = 252,
    ) -> bool:
        try:
            total_return = qs.stats.compsum(returns)
            sharpe = qs.stats.sharpe(returns, periods=periods_per_year)
            sortino = qs.stats.sortino(returns, periods=periods_per_year)
            max_dd = qs.stats.max_drawdown(returns)
            volatility = qs.stats.volatility(returns, periods=periods_per_year)
            win_rate = qs.stats.win_rate(returns)
            cagr = qs.stats.cagr(returns, periods=periods_per_year)
        except Exception as e:
            logger.warning(f"Error computing stats: {e}")
            total_return = sharpe = sortino = max_dd = volatility = win_rate = cagr = (
                None
            )

        metrics = [
            f"<b>{name}</b>",
            f"━━━━━━━━━━━━━━━━━━━━━━",
            f"📈 <b>CAGR:</b> {cagr:.2%}"
            if isinstance(cagr, (int, float))
            else f"📈 <b>CAGR:</b> N/A",
            f"💰 <b>Return:</b> {total_return:.2%}"
            if isinstance(total_return, (int, float))
            else f"💰 <b>Return:</b> N/A",
            f"📊 <b>Sharpe:</b> {sharpe:.2f}"
            if isinstance(sharpe, (int, float))
            else f"📊 <b>Sharpe:</b> N/A",
            f"📉 <b>Sortino:</b> {sortino:.2f}"
            if isinstance(sortino, (int, float))
            else f"📉 <b>Sortino:</b> N/A",
            f"⚠️ <b>MaxDD:</b> {max_dd:.2%}"
            if isinstance(max_dd, (int, float))
            else f"⚠️ <b>MaxDD:</b> N/A",
            f"📅 <b>Vol:</b> {volatility:.2%}"
            if isinstance(volatility, (int, float))
            else f"📅 <b>Vol:</b> N/A",
            f"🎯 <b>Win%:</b> {win_rate:.1%}"
            if isinstance(win_rate, (int, float))
            else f"🎯 <b>Win%:</b> N/A",
            "",
            f"<i>AI Trading System</i>",
        ]

        text = "\n".join(metrics)
        return await self._send_message_async(text)

    def send_metrics(
        self,
        returns: pd.Series,
        name: str = "Strategy",
        periods_per_year: int = 252,
    ) -> bool:
        loop = self._get_or_create_loop()
        return loop.run_until_complete(
            self.send_metrics_async(returns, name, periods_per_year)
        )

    async def send_daily_summary_async(
        self,
        returns: pd.Series,
        name: str = "Strategy",
        top_holdings: Optional[List[tuple]] = None,
    ) -> bool:
        today_return = returns.iloc[-1] if len(returns) > 0 else 0
        week_return = returns.iloc[-5:].sum() if len(returns) >= 5 else 0
        month_return = returns.iloc[-20:].sum() if len(returns) >= 20 else 0

        summary = [
            f"<b>📊 {name} Daily Summary</b>",
            f"",
            f"📅 <b>Date:</b> {returns.index[-1].strftime('%Y-%m-%d')}"
            if len(returns) > 0
            else "",
            f"📈 <b>Today:</b> {today_return:+.2%}"
            if isinstance(today_return, (int, float))
            else "",
            f"📈 <b>This Week:</b> {week_return:+.2%}"
            if isinstance(week_return, (int, float))
            else "",
            f"📈 <b>This Month:</b> {month_return:+.2%}"
            if isinstance(month_return, (int, float))
            else "",
        ]

        if top_holdings:
            summary.append("")
            summary.append("<b>🏆 Top Holdings:</b>")
            for i, (sym, weight) in enumerate(top_holdings[:5], 1):
                summary.append(f"  {i}. {sym}: {weight:.1%}")

        summary.extend(
            [
                "",
                f"<i>Generated by AI Trading System</i>",
            ]
        )

        text = "\n".join([line for line in summary if line])
        return await self._send_message_async(text)

    def send_daily_summary(
        self,
        returns: pd.Series,
        name: str = "Strategy",
        top_holdings: Optional[List[tuple]] = None,
    ) -> bool:
        loop = self._get_or_create_loop()
        return loop.run_until_complete(
            self.send_daily_summary_async(returns, name, top_holdings)
        )

    async def send_full_report_async(
        self,
        returns: pd.Series,
        benchmark: Optional[pd.Series] = None,
        name: str = "Strategy",
        periods_per_year: int = 252,
        include_tearsheet: bool = True,
        include_metrics: bool = True,
    ) -> dict:
        results = {"metrics": None, "tearsheet": None}

        if include_metrics:
            results["metrics"] = await self.send_metrics_async(
                returns, name, periods_per_year
            )

        if include_tearsheet:
            save_path = self.generate_tearsheet(returns, name, periods_per_year)
            results["tearsheet"] = await self._send_html_report_async(
                save_path,
                f"<b>{name}</b> QuantStats Tearsheet",
            )

        return results

    def send_full_report(
        self,
        returns: pd.Series,
        benchmark: Optional[pd.Series] = None,
        name: str = "Strategy",
        periods_per_year: int = 252,
        include_tearsheet: bool = True,
        include_metrics: bool = True,
    ) -> dict:
        loop = self._get_or_create_loop()
        return loop.run_until_complete(
            self.send_full_report_async(
                returns,
                benchmark,
                name,
                periods_per_year,
                include_tearsheet,
                include_metrics,
            )
        )

    async def send_sector_report_async(
        self,
        sector_rs: pd.DataFrame,
        lookback_days: int = 20,
        top_n: int = 10,
    ) -> bool:
        latest_rank = sector_rs.iloc[-1].sort_values(ascending=False)

        lines = [
            "<b>AI TRADING SYSTEM</b>",
            "<b>SECTOR STRENGTH REPORT</b>",
            "========================",
            "",
            "<b>STRONG SECTORS:</b>",
        ]

        for i, (sector, rs_val) in enumerate(latest_rank.head(top_n).items(), 1):
            returns = sector_rs[sector].iloc[-lookback_days:].sum()
            lines.append(
                f"{i}. {sector}: RS={rs_val:.3f} | {lookback_days}d={returns:+.1%}"
            )

        lines.extend(["", "<b>WEAK SECTORS:</b>"])

        for i, (sector, rs_val) in enumerate(latest_rank.tail(5).items(), 1):
            returns = sector_rs[sector].iloc[-lookback_days:].sum()
            lines.append(
                f"{i}. {sector}: RS={rs_val:.3f} | {lookback_days}d={returns:+.1%}"
            )

        lines.extend(
            [
                "",
                "========================",
                f"<i>RS = Relative Strength Rank | {lookback_days}d = {lookback_days}-day momentum</i>",
            ]
        )

        text = "\n".join(lines)
        return await self._send_message_async(text)

    def send_sector_report(
        self,
        sector_rs: pd.DataFrame,
        lookback_days: int = 20,
        top_n: int = 10,
    ) -> bool:
        loop = self._get_or_create_loop()
        return loop.run_until_complete(
            self.send_sector_report_async(sector_rs, lookback_days, top_n)
        )

    async def send_sector_tearsheet_async(
        self,
        sector_rs: pd.DataFrame,
        sector: str,
        lookback_days: int = 252,
        name: Optional[str] = None,
    ) -> bool:
        latest_rank = sector_rs.iloc[-1].sort_values(ascending=False)
        rs_val = latest_rank.get(sector, 0)

        returns = sector_rs[sector].iloc[-lookback_days:]
        returns = returns.pct_change(fill_method=None).dropna()
        returns.name = sector

        save_path = self.generate_tearsheet(
            returns, name or f"{sector} Sector", periods_per_year=252
        )

        file_size = save_path.stat().st_size / 1024 / 1024
        if file_size > 10:
            logger.warning(f"Tearsheet too large ({file_size:.1f} MB). Skipping.")
            return False

        return await self._send_html_report_async(
            save_path,
            f"<b>{sector} Sector RS Tearsheet</b>\nRank: {rs_val:.3f}",
        )

    def send_sector_tearsheet(
        self,
        sector_rs: pd.DataFrame,
        sector: str,
        lookback_days: int = 126,
        name: Optional[str] = None,
    ) -> bool:
        loop = self._get_or_create_loop()
        return loop.run_until_complete(
            self.send_sector_tearsheet_async(sector_rs, sector, lookback_days, name)
        )


def create_telegram_reporter() -> TelegramReporter:
    return TelegramReporter()


if __name__ == "__main__":
    import numpy as np

    dates = pd.date_range("2024-01-01", periods=252, freq="B")
    returns = pd.Series(np.random.randn(252) / 100 + 0.0005, index=dates)

    reporter = create_telegram_reporter()

    print("Generating sample tearsheet...")
    path = reporter.generate_tearsheet(returns, "Sample Strategy")
    print(f"Saved to: {path}")

    if reporter.bot_token and reporter.chat_id:
        print("Sending metrics to Telegram...")
        reporter.send_metrics(returns, "Sample Strategy")
    else:
        print("Telegram not configured. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")
