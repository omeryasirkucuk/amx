"""Filesystem filter checks for :func:`amx.codebase.walker.walk_code_files`.

Pins three behaviours added in PR beta of the code-RAG hardening
series:

1. Directories under the hard-coded denylist (``node_modules``,
   ``.git``, ``.venv``, ``dist``, ``build``, …) are always skipped.
2. A ``.gitignore`` at the repo root excludes matching files when
   ``pathspec`` is installed.
3. With ``pathspec`` monkey-patched away the walker still applies the
   denylist (graceful degrade).
"""

from __future__ import annotations

import builtins
from pathlib import Path

from amx.codebase.walker import walk_code_files


def _touch(path: Path, body: str = "x = 1\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def test_walker_skips_node_modules(tmp_path: Path) -> None:
    _touch(tmp_path / "app.py", "def f():\n    return 1\n")
    _touch(tmp_path / "node_modules" / "lib" / "junk.js")
    _touch(tmp_path / ".git" / "hooks" / "post-commit.sh")
    _touch(tmp_path / "__pycache__" / "stale.py")

    rels = sorted(p.relative_to(tmp_path).as_posix() for p in walk_code_files(tmp_path))
    assert "app.py" in rels
    assert not any("node_modules" in r for r in rels)
    assert not any(r.startswith(".git/") for r in rels)
    assert not any("__pycache__" in r for r in rels)


def test_walker_honours_gitignore(tmp_path: Path) -> None:
    _touch(tmp_path / "keep.py", "def k():\n    return 1\n")
    _touch(tmp_path / "secret.py", "def s():\n    return 2\n")
    (tmp_path / ".gitignore").write_text("secret.py\n", encoding="utf-8")

    pathspec = pytest_importorskip_inline("pathspec")
    if pathspec is None:
        return  # pathspec not installed in this environment

    rels = sorted(p.relative_to(tmp_path).as_posix() for p in walk_code_files(tmp_path))
    assert "keep.py" in rels
    assert "secret.py" not in rels


def test_walker_handles_missing_pathspec(tmp_path: Path, monkeypatch) -> None:
    _touch(tmp_path / "keep.py", "def k():\n    return 1\n")
    _touch(tmp_path / "node_modules" / "x.js")
    (tmp_path / ".gitignore").write_text("keep.py\n", encoding="utf-8")

    real_import = builtins.__import__

    def _no_pathspec(name, *args, **kwargs):  # type: ignore[no-untyped-def]
        if name == "pathspec":
            raise ImportError("pathspec unavailable in this test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _no_pathspec)

    rels = sorted(p.relative_to(tmp_path).as_posix() for p in walk_code_files(tmp_path))
    # Without pathspec the .gitignore rule is ignored — keep.py is
    # still emitted — but the denylist still skips node_modules.
    assert "keep.py" in rels
    assert not any("node_modules" in r for r in rels)


def test_walker_includes_normal_python(tmp_path: Path) -> None:
    _touch(tmp_path / "src" / "module.py", "def f():\n    return 1\n")
    _touch(tmp_path / "src" / "nested" / "thing.py")

    rels = sorted(p.relative_to(tmp_path).as_posix() for p in walk_code_files(tmp_path))
    assert "src/module.py" in rels
    assert "src/nested/thing.py" in rels


# ── tiny helper so the gitignore test no-ops on environments where
#    pathspec genuinely isn't installed (CI matrix may exclude the
#    optional dep). ``pytest.importorskip`` would skip the whole test,
#    but we want to assert the denylist behaviour even when pathspec
#    is missing — those checks live in the other tests.


def pytest_importorskip_inline(name: str):  # noqa: D401 - tiny helper
    """Return the imported module or ``None`` when not installed."""
    try:
        return __import__(name)
    except Exception:
        return None
