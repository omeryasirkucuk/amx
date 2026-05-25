"""chromadb embedding-function ``name()`` contract.

chromadb >= 1.5 calls ``embedding_function.name()`` as a METHOD during
its ef-conflict validation on collection get/create
(``validate_embedding_function_conflict_on_get``). AMX's embedding
classes used to declare ``name`` as a plain string class attribute,
which shadowed chromadb's ``name()`` method — so chromadb called a str
and raised ``TypeError: 'str' object is not callable``, surfacing in
Studio as "Docs unavailable: 'str' object is not callable …".

Every AMX collection persists its ef config name as ``"default"`` (AMX
governs embedding identity through collection metadata
+ reconcile_identity, not chromadb's name check), and ``"default"`` is
the one value chromadb's conflict check ignores. These tests pin that
``name()`` is a callable returning ``"default"`` so the regression
cannot return.
"""

from __future__ import annotations

import pytest

from amx.search.embeddings import (
    MiniLMEmbedding,
    OpenAICompatibleEmbedding,
    SentenceTransformerEmbedding,
)

_EF_CLASSES = [MiniLMEmbedding, OpenAICompatibleEmbedding, SentenceTransformerEmbedding]


@pytest.mark.parametrize("cls", _EF_CLASSES)
def test_name_is_callable_not_a_string(cls: type) -> None:
    # The bug: ``name`` was a str attribute, so ``ef.name()`` raised
    # "'str' object is not callable".
    assert callable(cls.name), f"{cls.__name__}.name must be a method, not a string"
    assert not isinstance(cls.name, str)


@pytest.mark.parametrize("cls", _EF_CLASSES)
def test_name_returns_default(cls: type) -> None:
    # "default" is the value chromadb's ef-conflict check skips, and it
    # matches what every AMX collection persisted.
    assert cls.name() == "default"


def test_chromadb_conflict_check_does_not_raise_with_default() -> None:
    """The real chromadb validator must accept an AMX ef against a
    persisted config whose name is 'default' (no false conflict, no
    TypeError)."""
    try:
        from chromadb.api.collection_configuration import (
            validate_embedding_function_conflict_on_get,
        )
    except Exception:
        pytest.skip("chromadb conflict validator not importable in this environment")

    class _StubEF:
        @staticmethod
        def name() -> str:
            return SentenceTransformerEmbedding.name()

    # Mirrors the persisted ef_config AMX collections carry.
    validate_embedding_function_conflict_on_get(_StubEF(), {"type": "known", "name": "default"})
