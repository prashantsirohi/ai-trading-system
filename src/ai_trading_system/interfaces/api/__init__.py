"""Phase 4A read-only operator API.

This package is deliberately separate from the legacy execution-console API,
which contains command endpoints.  Nothing in this package imports execution
adapters or write-capable domain stores.
"""

from .app import create_app

__all__ = ["create_app"]
