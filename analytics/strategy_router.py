from ai_trading_system.domains.ranking.strategy_router import *  # noqa
from ai_trading_system.domains.ranking import strategy_router as _strategy_router
import sys as _sys

_sys.modules[__name__] = _strategy_router
