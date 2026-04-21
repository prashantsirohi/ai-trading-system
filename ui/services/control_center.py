from ai_trading_system.interfaces.api.services.control_center import *  # noqa: F401,F403
from ai_trading_system.interfaces.api.services import control_center as _control_center
import sys as _sys

_sys.modules[__name__] = _control_center

