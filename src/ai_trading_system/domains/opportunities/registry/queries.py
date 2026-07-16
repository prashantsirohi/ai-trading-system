"""Typed registry query helpers.

The query implementation lives in ``store`` so every read shares the existing
control-plane connection lifecycle. This module provides stable named helpers
for callers that prefer functions over the service facade.
"""

from __future__ import annotations

from datetime import datetime

from .store import DuckDBOpportunityRegistryStore


def get_candidate_state_as_of(store: DuckDBOpportunityRegistryStore, candidate_id: str, as_of: datetime):
    return store.state_as_of(candidate_id, as_of)


def get_candidate_timeline(store: DuckDBOpportunityRegistryStore, candidate_id: str):
    return store.timeline(candidate_id)


def get_episode_relations(
    store: DuckDBOpportunityRegistryStore, candidate_id: str
):
    return store.list_episode_relations(candidate_id)
