"""
Centralized data-lake path contracts.

This module standardizes how jobs and UI reference shared Delta tables.
All paths here are *table paths within a container* (not abfss URIs).
"""

# Ranking container tables.
CANONICAL_RANKINGS_PATH = "platinum/rankings"

# Canonical derived signals tables (within the ranking container).
CANONICAL_COMPOSITE_SIGNALS_PATH = "platinum/signals/daily"
CANONICAL_RANKING_SIGNALS_PATH = "platinum/signals/ranking_signals"

