"""Compatibility wrapper for shared stage contracts.

New code should import these types from ``core.contracts`` instead of
``run.stages.base`` so non-runtime layers do not depend on the run package.
"""

from ai_trading_system.pipeline.contracts import *  # noqa: F401,F403
