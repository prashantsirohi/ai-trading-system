"""Weekly PDF market report channel."""


def __getattr__(name: str):
    if name == "publish_weekly_pdf":
        from ai_trading_system.domains.publish.channels.weekly_pdf.channel import (
            publish_weekly_pdf,
        )

        return publish_weekly_pdf
    raise AttributeError(name)

__all__ = ["publish_weekly_pdf"]
