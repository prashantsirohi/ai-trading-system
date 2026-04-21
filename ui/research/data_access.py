from ai_trading_system.interfaces.streamlit.research.data_access import *  # noqa: F401,F403
from ai_trading_system.interfaces.streamlit.research import data_access as _data_access
import sys as _sys

_sys.modules[__name__] = _data_access

