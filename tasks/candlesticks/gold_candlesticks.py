"""Compatibility wrapper for candlestick feature generation.

The implementation currently lives in
`tasks.technical_analysis.technical_indicators`. Keep this module so legacy
imports (`tasks.candlesticks.gold_candlesticks`) continue to work.
"""

from tasks.technical_analysis import technical_indicators as _impl

# Explicit exports commonly used by tests/callers.
compute_features = _impl.compute_features
_process_ticker = _impl._process_ticker
_to_snake_case = _impl._to_snake_case
main = _impl.main
run_feature_job = getattr(_impl, "run_feature_job", _impl.main)
materialize_main = getattr(_impl, "materialize_main", getattr(_impl, "by_date_main", _impl.main))


def __getattr__(name: str):
    """Forward any non-explicit attribute access to the implementation module."""
    return getattr(_impl, name)
