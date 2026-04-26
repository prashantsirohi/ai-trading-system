from ai_trading_system.domains.ranking.ranker import *  # noqa
from ai_trading_system.domains.ranking import ranker as _ranker
import sys as _sys

_sys.modules[__name__] = _ranker

