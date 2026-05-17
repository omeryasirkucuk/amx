"""Lazy chroma open for :class:`amx.search.index.SearchIndex`.

PR #498 surfaced ``CollectionIdentityMismatch`` cleanly when the
``/ask`` worker opened the search catalog. The follow-up reproduction
revealed that the legacy (empty-profile) Chroma collection was opened
inside ``SearchIndex.__init__`` — so a ``/ask`` request that explicitly
turned both Docs and Code RAG off STILL absorbed the chroma open + identity
check, and the user could never get past the embedding-mismatch error
without a manual rebuild that itself was broken (see
``test_reset_profile_drops_collection``).

This module pins the new contract:

* ``SearchIndex()`` does not open the legacy collection at construction
  time.
* The legacy collection materialises on first attribute access (via the
  ``.collection`` property or any per-profile ``_collection_for`` call).
* After ``reset_profile`` the on-disk Chroma collection is removed
  outright — not just emptied — so the next reopen registers the
  active embedding identity from scratch.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import patch

from amx.search import index as index_module


def test_construction_does_not_call_get_or_create_collection() -> None:
    """``SearchIndex()`` must not touch any Chroma collection at
    construction time. Without this, a docs/code-off ``/ask`` still
    opens the legacy collection and re-runs ``reconcile_identity``."""
    captured: list[dict[str, object]] = []

    class FakeClient:
        def __init__(self, *, path: str) -> None:
            self._path = path

        def get_or_create_collection(self, **kwargs: object) -> object:
            captured.append(kwargs)
            return object()

    with tempfile.TemporaryDirectory() as td:
        with patch("chromadb.PersistentClient", FakeClient):
            _ = index_module.SearchIndex(persist_dir=td)

    assert captured == [], f"SearchIndex opened a Chroma collection at construction: {captured}"


def test_collection_property_triggers_lazy_open() -> None:
    """Accessing ``index.collection`` materialises the legacy
    collection on demand, so callers that depend on the historical
    attribute keep working."""
    captured: list[dict[str, object]] = []

    class FakeCollection:
        metadata: dict[str, object] = {}

    class FakeClient:
        def __init__(self, *, path: str) -> None:
            self._path = path

        def get_or_create_collection(self, **kwargs: object) -> FakeCollection:
            captured.append(kwargs)
            return FakeCollection()

    with tempfile.TemporaryDirectory() as td:
        with patch("chromadb.PersistentClient", FakeClient):
            idx = index_module.SearchIndex(persist_dir=td)
            assert captured == []
            _ = idx.collection
            assert len(captured) == 1
            assert captured[0]["name"] == "amx_search"


def test_reset_profile_drops_collection_so_identity_can_change(tmp_path: Path) -> None:
    """``reset_profile`` must drop the chroma collection so the next
    ``_collection_for`` re-runs ``get_or_create_collection`` with the
    active identity metadata. The previous body deleted documents but
    left the recorded provider/model/dim alone, so ``/search rebuild``
    after an ``/embeddings`` swap could not unstick the user."""
    deleted: list[str] = []
    created: list[dict[str, object]] = []

    class FakeCollection:
        def __init__(self, name: str) -> None:
            self.name = name
            self.metadata: dict[str, object] = {}

        def get(self, **_: object) -> dict[str, list[str]]:
            return {"ids": []}

        def delete(self, **_: object) -> None:  # documents-only delete
            raise AssertionError(
                "reset_profile must drop the whole collection, not just delete documents"
            )

    class FakeClient:
        def __init__(self, *, path: str) -> None:
            self._path = path
            self._collections: dict[str, FakeCollection] = {}

        def get_or_create_collection(self, **kwargs: object) -> FakeCollection:
            created.append(kwargs)
            name = str(kwargs.get("name") or "")
            col = self._collections.get(name) or FakeCollection(name)
            self._collections[name] = col
            return col

        def delete_collection(self, *, name: str) -> None:
            deleted.append(name)
            self._collections.pop(name, None)

    with patch("chromadb.PersistentClient", FakeClient):
        idx = index_module.SearchIndex(persist_dir=str(tmp_path / "chroma"))
        # Populate the in-process cache to mirror the real flow:
        # something queried this profile before the swap.
        _ = idx._collection_for("prod")
        assert "prod" in {kw.get("metadata", {}).get("amx_db_profile") for kw in created}

        idx.reset_profile("prod")

    profile_collection_name = index_module._collection_name_for("prod")
    assert profile_collection_name in deleted, (
        f"reset_profile should have dropped {profile_collection_name}, got {deleted}"
    )
