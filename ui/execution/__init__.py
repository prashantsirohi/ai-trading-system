from ai_trading_system.interfaces.streamlit.execution import *  # noqa: F401,F403
from ai_trading_system.interfaces.streamlit import execution as _execution
import sys as _sys

_sys.modules[__name__] = _execution
