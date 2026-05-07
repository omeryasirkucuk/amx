"""Perf-only pytest configuration.

This subtree is opt-in: install with ``pip install -e ".[perf]"`` and run
``pytest tests/perf -m perf``. The default ``pytest`` invocation skips
everything here via the ``perf`` marker filter in pyproject.toml.

Nothing in here imports the runtime ``amx`` package — that keeps regular
test runs unaffected even when the optional perf dependencies are not
installed.
"""

from __future__ import annotations

import importlib.util

_HAS_BENCHMARK = importlib.util.find_spec("pytest_benchmark") is not None
_HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


def pytest_ignore_collect(collection_path, config):
    """Skip the entire ``tests/perf`` subtree when optional deps are missing.

    We use ``pytest_ignore_collect`` (not ``pytest_collection_modifyitems``)
    because individual benchmark modules import ``duckdb`` at module level —
    without this guard, collection would fail with ``ImportError`` on a
    checkout that has not run ``pip install -e ".[perf]"``.
    """
    if _HAS_BENCHMARK and _HAS_DUCKDB:
        return None
    # ``collection_path`` is a ``pathlib.Path`` (pytest >=7); skip every
    # file in this directory tree, but allow conftest.py + __init__.py
    # themselves so pytest can still report the skip cleanly.
    name = collection_path.name
    if name in {"conftest.py", "__init__.py"}:
        return None
    return True
