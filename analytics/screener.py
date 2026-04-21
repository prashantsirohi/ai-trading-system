from ai_trading_system.domains.ranking.screener import *  # noqa
from ai_trading_system.domains.ranking import screener as _screener
import sys as _sys

_sys.modules[__name__] = _screener

