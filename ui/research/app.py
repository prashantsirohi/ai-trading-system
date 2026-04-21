from ai_trading_system.interfaces.streamlit.research.app import *  # noqa: F401,F403
from ai_trading_system.interfaces.streamlit.research.app import main as _main


if __name__ in {"__main__", "__mp_main__"}:
    _main()
