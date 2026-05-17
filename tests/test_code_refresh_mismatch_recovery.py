"""``/code-refresh`` mismatch recovery.

When the user swaps the code embedding model via ``/embeddings``, the
on-disk ``amx_code`` Chroma collection's stamped identity stops
matching the active config. The pre-fix ``/code-refresh`` invoked
``delete_code_collection(source_filters=[code_path])``, which only
deleted documents whose ``source_root`` matched the active code path
and left the recorded identity triple alone — so the very next
``/ask`` opened the unchanged collection and raised
:class:`CodeEmbeddingMismatch` again. Users had no path out.

This regression pins the new behaviour: when the probe detects a
mismatch, ``/code-refresh`` calls ``delete_code_collection`` with
``source_filters=None`` so Chroma drops the whole collection and the
next scan recreates it with the active identity.
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

from click.testing import CliRunner

from amx.cli import main
from amx.codebase.code_rag import CodeEmbeddingMismatch


class FakeChromaCollection:
    name = "amx_code"


class FakeChromaClient:
    def __init__(self, *, path: str) -> None:
        self._path = path

    def list_collections(self):
        return [FakeChromaCollection()]


class CodeRefreshMismatchRecoveryTests(unittest.TestCase):
    def test_mismatch_probe_routes_through_full_collection_drop(self) -> None:
        runner = CliRunner()

        def _raise_mismatch(*_a, **_k):
            raise CodeEmbeddingMismatch(
                recorded_provider="minilm",
                recorded_model="minilm-l6-v2",
                active_provider="sentence_transformers",
                active_model="thenlper/gte-small",
            )

        with (
            patch("amx.config.AMXConfig.resolve_code_path", return_value="."),
            patch("amx.codebase.cache.invalidate_cache"),
            patch("amx.codebase.code_rag.delete_code_collection") as delete_code_collection,
            patch("chromadb.PersistentClient", FakeChromaClient),
            patch(
                "amx.codebase.code_rag._resolve_code_embedding",
                return_value=("sentence_transformers", "thenlper/gte-small", None),
            ),
            patch(
                "amx.codebase.code_rag._open_collection",
                side_effect=_raise_mismatch,
            ),
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "code", "refresh"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        # Mismatch detected → full collection drop, not per-path delete.
        delete_code_collection.assert_called_once_with(source_filters=None)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
