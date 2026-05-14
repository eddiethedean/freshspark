"""
freshspark
~~~~~~~~~~

Utilities for creating *fresh* local Spark sessions with isolated temp dirs
and reliable teardown.

Public API:
- fresh_local_spark
- get_fresh_local_spark
- reset_active_session
- ensure_fresh
"""

from .core import (
    ensure_fresh,
    fresh_local_spark,
    get_fresh_local_spark,
    reset_active_session,
)

__all__ = [
    "fresh_local_spark",
    "get_fresh_local_spark",
    "reset_active_session",
    "ensure_fresh",
]

__version__ = "0.2.1"
