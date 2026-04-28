from ai_trading_system.domains.ranking.weekly import *  # noqa
from ai_trading_system.domains.ranking import weekly as _weekly
import sys as _sys

_sys.modules[__name__] = _weekly
