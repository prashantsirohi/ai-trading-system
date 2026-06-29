from __future__ import annotations

import warnings

import pandas as pd

from ai_trading_system.research.perf_tracker import digest, reports
from ai_trading_system.ui.execution_api.routes import perf_tracker as perf_tracker_api


def test_constant_rank_ic_inputs_return_none_without_runtime_warning() -> None:
    x = pd.Series([1.0] * 30)
    y = pd.Series(range(30))
    valid = pd.DataFrame({"factor": x, "fwd_20d_return": y})

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        assert digest._rank_ic(valid) is None
        assert digest._rank_ic_xy(x, y) is None
        assert reports._spearman_ic(x, y) is None
        assert perf_tracker_api._rank_ic_xy(x, y) is None
