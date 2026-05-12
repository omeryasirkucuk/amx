"""Sidecar manifest preserves the original filename so the doc-profile
file inventory can show ``OYK Resume 2026 old.pdf`` instead of
``704b9c354944…eadbe.pdf``.

The hash-based storage is deliberate (deduplication on re-upload), but
the user can't recognise their own files by the SHA256. The fix writes
``<root>/.amx-manifest.json`` mapping ``<stored_name> → original_name``
on every upload; the inventory layer in Studio (Settings) and CLI
(/doc-files) reads it back.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock


def test_save_uploaded_file_records_original_name_in_manifest(tmp_path, monkeypatch):
    from amx.config import AMXConfig
    from amx.docs import uploads as upload_mod

    cfg = AMXConfig()
    cfg.doc_profiles = {}
    monkeypatch.setattr(cfg, "upsert_doc_profile", MagicMock(), raising=False)
    monkeypatch.setattr(upload_mod, "_profile_uploads_root", lambda profile: tmp_path / profile)

    payload = b"%PDF-1.4 fake content"
    result = upload_mod.save_uploaded_file(cfg, "test", "My CV 2026.pdf", payload)
    assert result.original_name == "My CV 2026.pdf"
    assert result.duplicate is False

    manifest_path = tmp_path / "test" / upload_mod.MANIFEST_FILENAME
    assert manifest_path.is_file()
    data = json.loads(manifest_path.read_text())
    assert "files" in data
    stored_name = list(data["files"].keys())[0]
    assert data["files"][stored_name]["original_name"] == "My CV 2026.pdf"
    assert "uploaded_at" in data["files"][stored_name]


def test_manifest_accumulates_across_uploads(tmp_path, monkeypatch):
    from amx.config import AMXConfig
    from amx.docs import uploads as upload_mod

    cfg = AMXConfig()
    cfg.doc_profiles = {}
    monkeypatch.setattr(cfg, "upsert_doc_profile", MagicMock(), raising=False)
    monkeypatch.setattr(upload_mod, "_profile_uploads_root", lambda profile: tmp_path / profile)

    upload_mod.save_uploaded_file(cfg, "test", "first.pdf", b"%PDF-1 a")
    upload_mod.save_uploaded_file(cfg, "test", "second.pdf", b"%PDF-1 b")

    manifest = json.loads((tmp_path / "test" / upload_mod.MANIFEST_FILENAME).read_text())
    originals = sorted(entry["original_name"] for entry in manifest["files"].values())
    assert originals == ["first.pdf", "second.pdf"]


def test_read_display_names_for_root_returns_mapping(tmp_path):
    from amx.docs.uploads import MANIFEST_FILENAME, read_display_names_for_root

    (tmp_path / MANIFEST_FILENAME).write_text(
        json.dumps(
            {
                "version": 1,
                "files": {
                    "abc123.pdf": {
                        "original_name": "Report.pdf",
                        "uploaded_at": 1.0,
                    },
                },
            }
        )
    )
    mapping = read_display_names_for_root(tmp_path)
    assert mapping == {"abc123.pdf": "Report.pdf"}


def test_read_display_names_handles_missing_manifest(tmp_path):
    from amx.docs.uploads import read_display_names_for_root

    assert read_display_names_for_root(tmp_path) == {}


def test_read_display_names_handles_corrupted_manifest(tmp_path):
    from amx.docs.uploads import MANIFEST_FILENAME, read_display_names_for_root

    (tmp_path / MANIFEST_FILENAME).write_text("not json{{{")
    assert read_display_names_for_root(tmp_path) == {}


def test_dup_upload_does_not_corrupt_manifest(tmp_path, monkeypatch):
    """Re-uploading the same content (same hash) overwrites the
    manifest entry rather than losing it."""
    from amx.config import AMXConfig
    from amx.docs import uploads as upload_mod

    cfg = AMXConfig()
    cfg.doc_profiles = {}
    monkeypatch.setattr(cfg, "upsert_doc_profile", MagicMock(), raising=False)
    monkeypatch.setattr(upload_mod, "_profile_uploads_root", lambda profile: tmp_path / profile)

    upload_mod.save_uploaded_file(cfg, "test", "first.pdf", b"%PDF-1 same")
    result2 = upload_mod.save_uploaded_file(cfg, "test", "renamed.pdf", b"%PDF-1 same")
    # The file is dedup'd (same hash, same content), but the manifest
    # should still carry the latest user-supplied name.
    assert result2.duplicate is True
    manifest = json.loads((tmp_path / "test" / upload_mod.MANIFEST_FILENAME).read_text())
    names = [entry["original_name"] for entry in manifest["files"].values()]
    assert names == ["renamed.pdf"]
