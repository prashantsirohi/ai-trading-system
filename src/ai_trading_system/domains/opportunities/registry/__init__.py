"""Persistent, append-oriented opportunity registry."""

from .identity import make_candidate_id, make_record_identity, make_setup_id, stable_digest
from . import models as _models
from .models import *  # noqa: F403
from .service import OpportunityRegistryService
from .store import DuckDBOpportunityRegistryStore, OpportunityRegistryStore

__all__ = [
    *_models.__all__,
    "DuckDBOpportunityRegistryStore",
    "OpportunityRegistryService",
    "OpportunityRegistryStore",
    "make_candidate_id",
    "make_record_identity",
    "make_setup_id",
    "stable_digest",
]
