from ai_trading_system.domains.ranking.patterns.detectors import *  # noqa
from ai_trading_system.domains.ranking.patterns import detectors as _detectors
import sys as _sys

_sys.modules[__name__] = _detectors

