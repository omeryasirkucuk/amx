"""Shared upload helpers — content-addressed write + profile wiring."""

from __future__ import annotations

import pytest

from amx.config import AMXConfig
from amx.docs.uploads import (
    MAX_BATCH_BYTES,
    UploadError,
    save_uploaded_batch,
    save_uploaded_file,
)


def test_save_writes_under_uploads_root(tmp_path) -> None:
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    res = save_uploaded_file(cfg, "design", "handbook.md", b"# Hello\n")
    assert res.duplicate is False
    assert res.bytes_written == len(b"# Hello\n")
    assert res.saved_path.endswith(".md")
    assert "uploads/design" in res.saved_path


def test_save_dedupes_by_content_hash(tmp_path) -> None:
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    payload = b"# Same content\n"
    save_uploaded_file(cfg, "design", "a.md", payload)
    res2 = save_uploaded_file(cfg, "design", "renamed.md", payload)
    # Same hash → second call sees duplicate and skips re-write.
    assert res2.duplicate is True


def test_save_registers_upload_dir_on_profile(tmp_path) -> None:
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    save_uploaded_file(cfg, "design", "h.md", b"x")
    paths = cfg.doc_profiles.get("design", []) or []
    assert any("uploads/design" in p for p in paths)
    # Calling again must not duplicate the path entry.
    save_uploaded_file(cfg, "design", "i.md", b"y")
    upload_paths = [p for p in cfg.doc_profiles["design"] if "uploads/design" in p]
    assert len(upload_paths) == 1


def test_save_rejects_unsupported_extension(tmp_path) -> None:
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    with pytest.raises(UploadError):
        save_uploaded_file(cfg, "design", "blob.exe", b"\x00\x01")


def test_save_rejects_empty_payload(tmp_path) -> None:
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    with pytest.raises(UploadError):
        save_uploaded_file(cfg, "design", "a.md", b"")


def test_save_rejects_oversized_file(tmp_path) -> None:
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    payload = b"a" * (MAX_BATCH_BYTES // 4 + 26 * 1024 * 1024)  # > 25 MB
    with pytest.raises(UploadError):
        save_uploaded_file(cfg, "design", "huge.md", payload)


def test_save_batch_rejects_when_total_too_big(tmp_path) -> None:
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    big = b"a" * (24 * 1024 * 1024)  # under per-file but five copies blow batch
    with pytest.raises(UploadError):
        save_uploaded_batch(
            cfg,
            "design",
            [(f"f{i}.md", big) for i in range(5)],
        )
