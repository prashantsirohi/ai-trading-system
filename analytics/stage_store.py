from ai_trading_system.domains.ranking.stage_store import *  # noqa
from ai_trading_system.domains.ranking import stage_store as _stage_store
import sys as _sys

_sys.modules[__name__] = _stage_store
