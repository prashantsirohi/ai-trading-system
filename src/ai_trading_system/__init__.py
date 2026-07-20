"""Placeholder package for the future src-based migration.

This package is intentionally minimal in the baseline step so the repository can
recognize a ``src/`` package root without moving current runtime modules.
"""

import pandas as pd

# Opt into the pandas 3.0 behaviour where ``.fillna``/``.ffill``/``.bfill`` on
# object-dtype columns no longer silently downcast the result. Every call site
# in this codebase already applies an explicit ``.astype(...)`` afterwards, so
# the observable result is unchanged; this only removes the noisy pandas 2.2
# "Downcasting object dtype arrays on .fillna" FutureWarning. Remove once the
# project is on pandas >= 3.0 (where this is the default).
pd.set_option("future.no_silent_downcasting", True)
