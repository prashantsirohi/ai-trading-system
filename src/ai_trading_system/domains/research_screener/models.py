from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path


class RunMode(StrEnum):
    REGRESSION_REPLAY = "regression_replay"
    LIVE_CANARY = "live_canary"
    FULL_UNIVERSE = "full_universe"
    FILING_DISCOVERY = "filing_discovery"


class Disposition(StrEnum):
    QUALIFIED = "QUALIFIED"
    BOUNDARY_REVIEW = "BOUNDARY_REVIEW"
    DATA_REPAIR_REQUIRED = "DATA_REPAIR_REQUIRED"
    ELIGIBILITY_UNKNOWN = "ELIGIBILITY_UNKNOWN"
    INELIGIBLE_MARKET_CAP = "INELIGIBLE_MARKET_CAP"
    INELIGIBLE_BOARD_OR_INSTRUMENT = "INELIGIBLE_BOARD_OR_INSTRUMENT"
    REJECTED_HARD_FAIL = "REJECTED_HARD_FAIL"
    WATCHLIST = "WATCHLIST"


class DataState(StrEnum):
    PRESENT = "PRESENT"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    NOT_DISCLOSED = "NOT_DISCLOSED"
    DATA_REPAIR_REQUIRED = "DATA_REPAIR_REQUIRED"
    ELIGIBILITY_UNKNOWN = "ELIGIBILITY_UNKNOWN"
    SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
    SCOPE_UNRESOLVED = "SCOPE_UNRESOLVED"
    ADJUSTMENT_INCOMPLETE = "ADJUSTMENT_INCOMPLETE"
    IDENTITY_CONFLICT = "IDENTITY_CONFLICT"


@dataclass(frozen=True)
class ScreeningParameters:
    as_of_date: date
    run_mode: RunMode
    min_market_cap_cr: float = 1_000.0
    max_market_cap_cr: float = 100_000.0
    canary_file: Path | None = None
    screen_definition: str | None = None
    screen_version: str | None = None
    parent_run_id: str | None = None
    batch_size: int = 25
    workers: int = 1

    def __post_init__(self) -> None:
        if self.min_market_cap_cr < 0 or self.max_market_cap_cr <= self.min_market_cap_cr:
            raise ValueError("market-cap bounds must be non-negative and increasing")
        if self.canary_file is not None and self.run_mode in {RunMode.FULL_UNIVERSE, RunMode.FILING_DISCOVERY}:
            raise ValueError(f"{self.run_mode.value} mode does not accept a canary fixture")
        if self.batch_size < 1:
            raise ValueError("batch_size must be positive")
        if self.workers < 1 or self.workers > 64:
            raise ValueError("workers must be between 1 and 64")
        if self.screen_definition is None:
            object.__setattr__(
                self, "screen_definition",
                "persistent_screener_phase1" if self.run_mode == RunMode.FULL_UNIVERSE
                else "persistent_screener_filing_discovery" if self.run_mode == RunMode.FILING_DISCOVERY
                else "persistent_screener_canary",
            )
        if self.screen_version is None:
            object.__setattr__(
                self, "screen_version",
                "1.0.0" if self.run_mode == RunMode.FULL_UNIVERSE
                else "1.2.0" if self.run_mode == RunMode.FILING_DISCOVERY
                else "0.2.5",
            )
