"""Shared filesystem walker for codebase scans.

Both :func:`amx.codebase.analyzer.analyze_codebase` and
:func:`amx.codebase.code_rag.index_codebase_tree` walk a repository
root and pick code files. Historically each ran a naive
``root.rglob("*")``, which dragged ``node_modules``, ``.git``, packed
vendor directories, and build artefacts into the index. PR beta of
the code-RAG hardening series consolidates the walk here so both
sides apply the same two-layer filter:

1. Hard-coded denylist of directory names that are never indexed.
2. Optional ``.gitignore`` matcher (when ``pathspec`` is installed) —
   reuses the helper from :mod:`amx.docs.scanner`.

Either layer alone is enough to skip a path, mirroring how the docs
scanner already behaves.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from amx.codebase.analyzer import CODE_EXTENSIONS

# Directory basenames that should never be traversed regardless of any
# ``.gitignore`` content. Keeping this list small and unsurprising on
# purpose: extending it is a behavioural change and needs a follow-up
# PR / test, not a silent edit.
_NEVER_INDEX_DIRS: frozenset[str] = frozenset(
    {
        "node_modules",
        ".git",
        ".venv",
        "venv",
        "dist",
        "build",
        "target",
        "vendor",
        ".next",
        ".cache",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        ".mypy_cache",
        "site-packages",
        "egg-info",
    }
)


def _load_gitignore_matcher(root: Path):
    """Thin re-export of the docs-scanner helper.

    Kept as a wrapper so a future move (e.g. to ``amx/utils/gitignore.py``)
    only changes one import site. ``pathspec`` is optional; the helper
    already degrades to ``None`` when it isn't installed.
    """
    from amx.docs.scanner import _load_gitignore_matcher as _impl

    return _impl(root)


def _is_under_denylist(rel_parts: tuple[str, ...]) -> bool:
    """Return ``True`` when any path segment matches a denylisted dir."""
    return any(part in _NEVER_INDEX_DIRS for part in rel_parts)


def walk_code_files(root: Path) -> Iterator[Path]:
    """Yield code files under ``root`` honouring the denylist + ``.gitignore``.

    Yields absolute ``Path`` objects whose suffix is in
    :data:`amx.codebase.analyzer.CODE_EXTENSIONS`. The traversal order
    is sorted for deterministic test fixtures and reproducible chunk
    ids.
    """
    matcher = _load_gitignore_matcher(root)
    for f in sorted(root.rglob("*")):
        if not f.is_file():
            continue
        if f.suffix.lower() not in CODE_EXTENSIONS:
            continue
        try:
            rel_parts = f.relative_to(root).parts
        except ValueError:
            continue
        if _is_under_denylist(rel_parts):
            continue
        if matcher is not None:
            rel_posix = f.relative_to(root).as_posix()
            if rel_posix and matcher.match_file(rel_posix):
                continue
        yield f
