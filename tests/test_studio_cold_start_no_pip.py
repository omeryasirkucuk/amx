"""Regression test for the Studio cold-start pip-install hang.

v0.15.0 shipped with module-level ``_ensure(...)`` calls in four RAG
modules (``amx/docs/rag.py``, ``amx/search/index.py``,
``amx/search/embeddings.py``, ``amx/codebase/code_rag.py``). The
Studio launcher's child process imports ``amx.web.server``, whose
transitive import graph passes through ``docs.rag`` and ``search.
index``. On a fresh ``pip install amx-cli`` (no extras) that import
chain triggered a ~150 MB pip subprocess at module load time and
blocked uvicorn from binding the port — Studio appeared dead until
the install finished.

This test pins the import-light contract for the modules in
Studio's cold-start path: importing them must not spawn any pip
subprocess. New ``_ensure(...)`` calls belong inside class
constructors, factory functions, or runtime entry points — never
at module top level in this import chain.
"""

from __future__ import annotations

import subprocess

import pytest


@pytest.fixture
def trap_pip_subprocess(monkeypatch):
    """Replace :class:`subprocess.Popen` so any pip invocation
    raises immediately with the offending argv. Tests that hit a
    real subprocess for unrelated reasons (none in this file)
    would need to scope the trap more tightly.
    """
    real_popen = subprocess.Popen
    pip_invocations: list[list[str]] = []

    def _trap(args, *rest, **kwargs):
        # Cover both list-form (sys.executable, -m, pip, ...) and
        # string-form invocations. The ``optional_deps`` module uses
        # list-form, but the trap is permissive on purpose.
        if isinstance(args, (list, tuple)):
            argv = [str(x) for x in args]
        else:
            argv = [str(args)]
        if any("pip" in tok for tok in argv):
            pip_invocations.append(argv)
            raise AssertionError(f"Module-level _ensure leaked into Studio cold start: pip {argv}")
        return real_popen(args, *rest, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", _trap)
    return pip_invocations


COLD_START_MODULES = (
    # Direct fixes in this PR.
    "amx.docs.rag",
    "amx.search.index",
    "amx.codebase.code_rag",
    # The end-to-end target. Importing this is what the Studio
    # launcher's child process does at startup; if any transitive
    # module reintroduces a module-level _ensure, the assertion
    # below fires before uvicorn is even reached.
    "amx.web.server",
)


@pytest.mark.parametrize("module_name", COLD_START_MODULES)
def test_import_does_not_spawn_pip(module_name: str, trap_pip_subprocess) -> None:
    """Importing each Studio cold-start module must not invoke pip."""
    import importlib

    importlib.import_module(module_name)
    assert trap_pip_subprocess == [], (
        f"{module_name} import triggered a pip subprocess; module-level "
        "_ensure() must be moved into a runtime entry point"
    )
