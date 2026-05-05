"""Tests for the on-demand pip-installer used by feature-gated modules.

The ``ensure()`` helper is the single boundary between AMX's slim
default install and the heavy feature clusters (RAG, AMX Studio,
cloud sources, Batch APIs). These tests cover the three behaviours
the helper has to get right:

1. Already-installed → no pip subprocess, cached so subsequent calls
   are O(1).
2. Missing → invokes ``sys.executable -m pip install`` exactly once
   for the union of missing packages, with quiet flags.
3. pip failure → raises ``RuntimeError`` whose message tells the user
   the manual fallback command, and removes the cache entries so a
   later retry can attempt again (network may have come back).

The third behaviour matters because a half-cached failure used to
let the next call think the package was fine and crash deeper inside
the import chain.
"""

from __future__ import annotations

import subprocess
import sys
from unittest.mock import patch

import pytest

from amx.utils import optional_deps as od


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    od._VERIFIED.clear()
    yield
    od._VERIFIED.clear()


@pytest.fixture
def _no_sys_modules_shortcut(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force ``ensure()`` to consult find_spec instead of short-
    circuiting on ``sys.modules``. The shortcut exists for production
    (and for fixtures that inject SimpleNamespace doubles via
    ``sys.modules[…] = …``) but it would mask the find_spec mock
    these tests need to exercise."""

    real_modules = sys.modules
    test_packages = {
        "chromadb",
        "docx",
        "uvicorn",
        "uvicorn[standard]",
        "python-docx",
        "bogus-package",
    }

    class _ProxyModules(dict):
        def __contains__(self, key: object) -> bool:
            if isinstance(key, str) and key in test_packages:
                return False
            return super().__contains__(key)

    proxy = _ProxyModules(real_modules)
    monkeypatch.setattr(sys, "modules", proxy)


def test_ensure_noops_when_already_installed(_no_sys_modules_shortcut) -> None:
    """find_spec returns a non-None spec → no subprocess invoked."""
    fake_spec = object()
    with (
        patch.object(od.importlib.util, "find_spec", return_value=fake_spec) as fs,
        patch.object(od.subprocess, "run") as run,
    ):
        od.ensure(["chromadb", ("docx", "python-docx")], feature="x")

    assert fs.call_count == 2
    run.assert_not_called()
    # Cache populated so a second call is O(0) — neither find_spec
    # nor subprocess hit again.
    with (
        patch.object(od.importlib.util, "find_spec") as fs2,
        patch.object(od.subprocess, "run") as run2,
    ):
        od.ensure(["chromadb", ("docx", "python-docx")], feature="x")
    fs2.assert_not_called()
    run2.assert_not_called()


def test_ensure_pip_installs_missing_packages(_no_sys_modules_shortcut) -> None:
    completed = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
    with (
        patch.object(od.importlib.util, "find_spec", return_value=None),
        patch.object(od.subprocess, "run", return_value=completed) as run,
        patch.object(od.importlib, "invalidate_caches") as invalidate,
    ):
        od.ensure(
            ["chromadb", ("docx", "python-docx"), ("uvicorn", "uvicorn[standard]")],
            feature="document RAG",
        )

    assert run.call_count == 1
    cmd = run.call_args.args[0]
    # Single pip call for the union; lands in the same interpreter
    # as AMX. Output is NOT captured and ``--quiet`` is NOT passed —
    # a multi-package install on a fresh machine takes long enough
    # that hidden output reads as a frozen CLI to the user, so pip's
    # native progress bars stream through the terminal.
    assert cmd[0] == sys.executable
    assert cmd[1:4] == ["-m", "pip", "install"]
    assert "--quiet" not in cmd
    assert "--disable-pip-version-check" in cmd
    # capture_output=True / stdout=… would suppress pip's progress;
    # the call must inherit the parent's stdio.
    kwargs = run.call_args.kwargs
    assert "capture_output" not in kwargs or kwargs.get("capture_output") is False
    assert "stdout" not in kwargs
    assert "chromadb" in cmd
    assert "python-docx" in cmd
    assert "uvicorn[standard]" in cmd
    invalidate.assert_called_once()


def test_ensure_raises_with_manual_command_when_pip_fails(_no_sys_modules_shortcut) -> None:
    failed = subprocess.CompletedProcess(
        args=[],
        returncode=1,
        stdout="",
        stderr="ERROR: Could not find a version that satisfies the requirement bogus",
    )
    with (
        patch.object(od.importlib.util, "find_spec", return_value=None),
        patch.object(od.subprocess, "run", return_value=failed),
    ):
        with pytest.raises(RuntimeError) as exc_info:
            od.ensure(["bogus-package"], feature="bogus feature")

    msg = str(exc_info.value)
    assert "pip install bogus-package" in msg
    assert "exit code 1" in msg
    # Cache must NOT retain the failed entries — a retry is expected
    # to call find_spec/pip again.
    assert "bogus-package|bogus-package" not in od._VERIFIED


def test_ensure_raises_when_pip_executable_missing(_no_sys_modules_shortcut) -> None:
    """Catches OSError (no such executable, permissions) and surfaces
    the same actionable manual-install hint."""
    with (
        patch.object(od.importlib.util, "find_spec", return_value=None),
        patch.object(od.subprocess, "run", side_effect=OSError("no pip on PATH")),
    ):
        with pytest.raises(RuntimeError) as exc_info:
            od.ensure(["chromadb"], feature="search index")

    msg = str(exc_info.value)
    assert "Could not invoke pip" in msg
    assert "pip install chromadb" in msg


def test_ensure_resolves_tuple_form_correctly(_no_sys_modules_shortcut) -> None:
    """``("docx", "python-docx")`` checks the import name but installs
    the pip distribution name — confusion between these two has bitten
    every other "ensure-on-demand" pattern in the wild."""
    completed = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
    captured: dict[str, object] = {}

    def fake_find_spec(name: str, *args, **kwargs):
        captured.setdefault("find_spec_name", name)
        return None

    with (
        patch.object(od.importlib.util, "find_spec", side_effect=fake_find_spec),
        patch.object(od.subprocess, "run", return_value=completed) as run,
        patch.object(od.importlib, "invalidate_caches"),
    ):
        od.ensure([("docx", "python-docx")], feature="docs")

    assert captured["find_spec_name"] == "docx"
    cmd = run.call_args.args[0]
    assert "python-docx" in cmd
    assert "docx" not in cmd
