from ai_trading_system.interfaces.streamlit.research import *  # noqa: F401,F403
from ai_trading_system.interfaces.streamlit import research as _research
import sys as _sys

_sys.modules[__name__] = _research

