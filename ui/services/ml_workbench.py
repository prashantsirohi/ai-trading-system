from ai_trading_system.ui.execution_api.services.ml_workbench import *  # noqa: F401,F403
from ai_trading_system.ui.execution_api.services import ml_workbench as _ml_workbench
import sys as _sys

_sys.modules[__name__] = _ml_workbench
