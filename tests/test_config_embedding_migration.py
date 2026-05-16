"""Regression tests for the docs/code embedding-config split.

Pre-0.15 AMX stored a single ``embedding:`` block in ``config.yml`` and
used it for both docs RAG and code RAG. 0.15+ keeps two independent
fields, ``embedding_docs`` and ``embedding_code``. These tests pin:

* the new fields default to MiniLM and survive a save/load round trip,
* secrets externalise to the OS keyring under per-side keys
  (``embedding_docs/api_key`` and ``embedding_code/api_key``),
* a legacy single-``embedding:`` YAML migrates transparently to both
  new fields on first load, with both plaintext and keyring-reference
  api_keys preserved, and the legacy keyring entry is best-effort
  cleaned up so it does not linger as an orphan.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from amx.config import AMXConfig, EmbeddingConfig
from amx.storage.secrets import InMemorySecretStore, set_default_store


class EmbeddingSplitConfigTests(unittest.TestCase):
    """``cfg.embedding_docs`` and ``cfg.embedding_code`` are independent
    ``EmbeddingConfig`` blocks that survive save/load and externalise
    api_keys to per-side keyring entries."""

    def setUp(self) -> None:
        self._store = InMemorySecretStore()
        set_default_store(self._store)

    def tearDown(self) -> None:
        set_default_store(None)

    def test_both_sides_default_to_minilm(self) -> None:
        cfg = AMXConfig()
        for side in ("embedding_docs", "embedding_code"):
            emb = getattr(cfg, side)
            self.assertEqual(emb.kind, "minilm", side)
            self.assertEqual(emb.model, "", side)
            self.assertEqual(emb.api_key, "", side)
            self.assertTrue(emb.is_configured(), side)

    def test_save_and_load_round_trip_preserves_both_sides(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig()
            cfg.embedding_docs = EmbeddingConfig(
                kind="openai_compatible",
                model="text-embedding-3-large",
                api_key="sk-docs-1234",
                base_url="https://api.openai.com/v1",
            )
            cfg.embedding_code = EmbeddingConfig(
                kind="sentence_transformers",
                model="BAAI/bge-m3",
            )
            cfg.save(str(cfg_path))

            reloaded = AMXConfig.load(str(cfg_path))
            self.assertEqual(reloaded.embedding_docs.kind, "openai_compatible")
            self.assertEqual(reloaded.embedding_docs.model, "text-embedding-3-large")
            self.assertEqual(reloaded.embedding_docs.api_key, "sk-docs-1234")
            self.assertEqual(reloaded.embedding_docs.base_url, "https://api.openai.com/v1")
            self.assertEqual(reloaded.embedding_code.kind, "sentence_transformers")
            self.assertEqual(reloaded.embedding_code.model, "BAAI/bge-m3")

    def test_api_keys_externalised_under_per_side_keyring_paths(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig()
            cfg.embedding_docs = EmbeddingConfig(
                kind="openai_compatible",
                model="text-embedding-3-small",
                api_key="sk-docs-must-not-leak",
                base_url="https://api.openai.com/v1",
            )
            cfg.embedding_code = EmbeddingConfig(
                kind="openai_compatible",
                model="voyage-code-2",
                api_key="sk-code-must-not-leak",
                base_url="https://api.voyageai.com/v1",
            )
            cfg.save(str(cfg_path))

            yaml_text = cfg_path.read_text()
            self.assertNotIn("sk-docs-must-not-leak", yaml_text)
            self.assertNotIn("sk-code-must-not-leak", yaml_text)
            self.assertIn("keyring:embedding_docs/api_key", yaml_text)
            self.assertIn("keyring:embedding_code/api_key", yaml_text)
            self.assertEqual(self._store.get("embedding_docs/api_key"), "sk-docs-must-not-leak")
            self.assertEqual(self._store.get("embedding_code/api_key"), "sk-code-must-not-leak")


class LegacyEmbeddingMigrationTests(unittest.TestCase):
    """A pre-0.15 ``embedding:`` block migrates one-shot to both new
    fields on load. The migration is transparent: the next ``cfg.save()``
    writes only the new shape and the legacy YAML key never reappears.
    """

    def setUp(self) -> None:
        self._store = InMemorySecretStore()
        set_default_store(self._store)

    def tearDown(self) -> None:
        set_default_store(None)

    def test_legacy_plaintext_yaml_migrates_to_both_sides(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg_path.write_text(
                "embedding:\n"
                "  kind: openai_compatible\n"
                "  model: text-embedding-3-small\n"
                "  api_key: sk-plaintext-legacy\n"
                "  base_url: https://api.openai.com/v1\n"
            )
            cfg = AMXConfig.load(str(cfg_path))

            # Both new sides carry the legacy values verbatim.
            for side_attr in ("embedding_docs", "embedding_code"):
                emb = getattr(cfg, side_attr)
                self.assertEqual(emb.kind, "openai_compatible", side_attr)
                self.assertEqual(emb.model, "text-embedding-3-small", side_attr)
                self.assertEqual(emb.api_key, "sk-plaintext-legacy", side_attr)
                self.assertEqual(emb.base_url, "https://api.openai.com/v1", side_attr)

            # Next save writes only the new shape and externalises the
            # secret under the new per-side keyring paths.
            cfg.save(str(cfg_path))
            yaml_after = cfg_path.read_text()
            self.assertNotIn("sk-plaintext-legacy", yaml_after)
            # The legacy top-level ``embedding:`` block must NOT come back.
            self.assertNotIn("\nembedding:\n", "\n" + yaml_after)
            self.assertIn("embedding_docs:", yaml_after)
            self.assertIn("embedding_code:", yaml_after)
            self.assertEqual(self._store.get("embedding_docs/api_key"), "sk-plaintext-legacy")
            self.assertEqual(self._store.get("embedding_code/api_key"), "sk-plaintext-legacy")

    def test_legacy_keyring_reference_yaml_migrates_to_both_sides(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            # Seed the keyring with the legacy entry, write a YAML that
            # references it. This is what a real upgrade-from-0.14 user
            # has on disk.
            self._store.set("embedding/api_key", "sk-from-legacy-keyring")
            cfg_path.write_text(
                "embedding:\n"
                "  kind: openai_compatible\n"
                "  model: text-embedding-3-small\n"
                "  api_key: keyring:embedding/api_key\n"
                "  base_url: https://api.openai.com/v1\n"
            )

            cfg = AMXConfig.load(str(cfg_path))

            # Migration resolves the legacy reference and copies plaintext
            # into both new in-memory blocks.
            self.assertEqual(cfg.embedding_docs.api_key, "sk-from-legacy-keyring")
            self.assertEqual(cfg.embedding_code.api_key, "sk-from-legacy-keyring")

            cfg.save(str(cfg_path))

            # Next save re-externalises under the new per-side paths.
            self.assertEqual(self._store.get("embedding_docs/api_key"), "sk-from-legacy-keyring")
            self.assertEqual(self._store.get("embedding_code/api_key"), "sk-from-legacy-keyring")
            # Legacy keyring entry is best-effort deleted so it does not
            # linger as an orphan.
            self.assertIsNone(self._store.get("embedding/api_key"))

    def test_partial_new_state_drops_legacy_block(self) -> None:
        """A hand-edited YAML that already carries one of the new keys
        but still has the legacy block must end up using the new keys —
        we prefer the explicit per-side intent over the legacy mirror."""
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg_path.write_text(
                "embedding:\n"
                "  kind: openai_compatible\n"
                "  model: legacy-ignored\n"
                "embedding_docs:\n"
                "  kind: minilm\n"
                "  model: ''\n"
                "  api_key: ''\n"
                "  base_url: ''\n"
            )
            cfg = AMXConfig.load(str(cfg_path))
            self.assertEqual(cfg.embedding_docs.kind, "minilm")
            self.assertEqual(cfg.embedding_docs.model, "")
            # The code side stays on the dataclass default since neither
            # ``embedding_code`` nor ``embedding`` should reach it after
            # migration.
            self.assertEqual(cfg.embedding_code.kind, "minilm")


if __name__ == "__main__":
    unittest.main()
