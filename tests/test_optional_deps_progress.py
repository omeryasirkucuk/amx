"""Tests for the captured-output pip helper used by ``ensure()``.

The helper has to do three things on top of a plain ``subprocess`` call:

1. Parse pip's stable milestone lines into structured ``phase`` events
   on the install bus. These drive the Studio banner's secondary line
   ("Downloading openai-1.51.0-py3-none-any.whl (387 kB)") and the
   CLI spinner's tail.
2. Emit ``pip.install.begin`` / ``done`` / ``failed`` lifecycle events
   so consumers can render a start, an end, and an error state.
3. Buffer the full pip output so a failure produces a ``RuntimeError``
   that includes enough context to debug without re-running.
"""

from __future__ import annotations

import io
from typing import Any

import pytest

from amx.utils import optional_deps as od
from amx.web import install_bus


@pytest.fixture(autouse=True)
def _reset_bus() -> None:
    install_bus.reset_for_tests()
    yield
    install_bus.reset_for_tests()


PIP_OUTPUT_SUCCESS = """\
Collecting openai
  Downloading openai-1.51.0-py3-none-any.whl (387 kB)
Collecting httpx
  Downloading httpx-0.27.2-py3-none-any.whl (76 kB)
Installing collected packages: httpx, openai
Successfully installed httpx-0.27.2 openai-1.51.0
"""

PIP_OUTPUT_FAILURE = """\
Collecting bogus-package
ERROR: Could not find a version that satisfies the requirement bogus-package
ERROR: No matching distribution found for bogus-package
"""


class _FakePopen:
    """Minimal Popen stand-in: yields the canned output and exit code."""

    def __init__(self, output: str, returncode: int):
        self.stdout = io.StringIO(output)
        self.returncode = returncode

    def wait(self) -> int:
        return self.returncode


def test_parses_pip_milestones_into_phase_events(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[dict[str, Any]] = []
    install_bus.subscribe(events.append)

    monkeypatch.setattr(
        od.subprocess, "Popen", lambda *a, **k: _FakePopen(PIP_OUTPUT_SUCCESS, 0)
    )
    # The Rich Live region tries to paint to the terminal in spinner
    # mode; force the headless path so the test stays deterministic.
    monkeypatch.setattr(od, "_stdout_is_a_tty", lambda: False)

    rc, captured = od._run_pip_with_progress(
        ["pip", "install", "openai"], feature="OpenAI Batch API", packages=["openai"]
    )

    assert rc == 0
    assert captured[0].startswith("Collecting openai")
    assert any(line.startswith("Successfully installed") for line in captured)

    phases = [(e["type"], e.get("phase")) for e in events if "phase" in e or e["type"].endswith(
        ("begin", "done", "failed")
    )]
    # Lifecycle bookends present.
    assert phases[0][0] == "pip.install.begin"
    assert phases[-1][0] == "pip.install.done"
    # All four parsed milestones surface at least once.
    seen_phases = {e.get("phase") for e in events if e["type"] == "pip.install.progress"}
    assert {"collecting", "downloading", "installing", "installed"} <= seen_phases


def test_emits_specific_payload_for_collecting_and_downloading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[dict[str, Any]] = []
    install_bus.subscribe(events.append)

    monkeypatch.setattr(
        od.subprocess, "Popen", lambda *a, **k: _FakePopen(PIP_OUTPUT_SUCCESS, 0)
    )
    monkeypatch.setattr(od, "_stdout_is_a_tty", lambda: False)

    od._run_pip_with_progress(["pip", "install", "openai"], feature="x", packages=["openai"])

    collecting = [
        e for e in events if e["type"] == "pip.install.progress" and e.get("phase") == "collecting"
    ]
    downloading = [
        e for e in events if e["type"] == "pip.install.progress" and e.get("phase") == "downloading"
    ]
    assert any(e["package"] == "openai" for e in collecting)
    assert any(
        e["artifact"].startswith("openai-1.51.0") and e["size"] == "387 kB" for e in downloading
    )


def test_failure_emits_failed_event_and_returns_captured_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[dict[str, Any]] = []
    install_bus.subscribe(events.append)

    monkeypatch.setattr(
        od.subprocess, "Popen", lambda *a, **k: _FakePopen(PIP_OUTPUT_FAILURE, 1)
    )
    monkeypatch.setattr(od, "_stdout_is_a_tty", lambda: False)

    rc, captured = od._run_pip_with_progress(
        ["pip", "install", "bogus-package"], feature="bogus", packages=["bogus-package"]
    )

    assert rc == 1
    assert any("No matching distribution found" in line for line in captured)

    failed = [e for e in events if e["type"] == "pip.install.failed"]
    assert len(failed) == 1
    assert failed[0]["returncode"] == 1
    # ``tail`` carries the last lines so the browser banner can render
    # the actionable pip error inline without a separate fetch.
    assert any("No matching distribution found" in line for line in failed[0]["tail"])


def test_failure_path_includes_captured_tail_in_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: ensure() raises RuntimeError whose message includes
    the captured pip tail returned by ``_run_pip_with_progress``."""
    # Force the find_spec / sys.modules paths so ensure() believes the
    # package is missing and reaches the install step.
    monkeypatch.setattr(od.importlib.util, "find_spec", lambda *a, **k: None)
    monkeypatch.setattr(
        od,
        "_run_pip_with_progress",
        lambda *a, **k: (1, ["Collecting bogus-package", "ERROR: distribution not found"]),
    )

    with pytest.raises(RuntimeError) as exc:
        od.ensure(["bogus-package"], feature="bogus")

    msg = str(exc.value)
    assert "exit code 1" in msg
    assert "ERROR: distribution not found" in msg
    assert "pip install bogus-package" in msg
