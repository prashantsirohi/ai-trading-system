from ai_trading_system.interfaces.streamlit.execution.app import *  # noqa: F401,F403
from ai_trading_system.interfaces.streamlit.execution.app import build_parser as _build_parser
from ai_trading_system.interfaces.streamlit.execution.app import main as _main


if __name__ in {"__main__", "__mp_main__"}:
    _args = _build_parser().parse_args()
    _main(host=_args.host, port=_args.port, show=_args.show)
