from ai_trading_system.interfaces.streamlit.research.widgets import *  # noqa: F401,F403
from ai_trading_system.interfaces.streamlit.research import widgets as _widgets
import sys as _sys

_sys.modules[__name__] = _widgets

