"""AMX test suite.

This file exists so pytest can resolve absolute imports like
``from tests.eval.metrics import ...`` (used by the eval harness in
``tests/eval/test_smoke.py`` and ``tests/eval/test_retrieval_metrics.py``).
Without it ``tests`` is treated as a "rootless" directory and the eval
modules fail to collect with::

    ModuleNotFoundError: No module named 'tests.eval'

Marker file only — no test fixtures or shared setup belong here. Suite-wide
fixtures live in ``tests/conftest.py``; per-package fixtures should live in
the corresponding ``conftest.py``.
"""
