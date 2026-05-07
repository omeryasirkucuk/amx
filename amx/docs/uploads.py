"""Shared upload helpers used by the Studio drag-drop endpoint and the
CLI ``/doc-add`` command.

User-supplied files (PDFs, markdown, docx, etc.) are content-addressed
under ``~/.amx/uploads/<doc-profile>/<sha256>.<ext>``. Hashing prevents
duplicates when the user accidentally drops the same file twice; the
extension is preserved so :mod:`amx.docs.scanner` can route the file
to the right loader. The function never overwrites an existing file —
if the hash is already present the existing path is returned so the
caller can still wire it into a doc profile without re-uploading.

The doc profile is auto-extended with the upload directory so a single
``/scan`` or ``/ingest`` round picks up every dropped file. The
directory entry is added once per profile (``cfg.upsert_doc_profile``)
so re-uploading more files later doesn't duplicate the path.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from amx.config import AMXConfig

#: Per-file ceiling, mirrored on the FastAPI side. A 25 MB upload is
#: already the upper end of "design doc" — anything bigger is almost
#: certainly the wrong kind of source for the RAG agent and should
#: come in via a path/URL instead.
MAX_FILE_BYTES: int = 25 * 1024 * 1024
#: Per-batch ceiling. Keeps a runaway folder drag from filling the
#: user's home directory before the FastAPI worker has a chance to
#: respond.
MAX_BATCH_BYTES: int = 100 * 1024 * 1024

#: Extensions :mod:`amx.docs.scanner` knows how to load. Anything else
#: lands in the upload folder unread by the RAG agent.
ACCEPTED_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".md",
        ".markdown",
        ".txt",
        ".pdf",
        ".docx",
        ".doc",
        ".csv",
        ".tsv",
        ".html",
        ".htm",
        ".rst",
        ".rtf",
        ".json",
        ".yaml",
        ".yml",
    }
)


@dataclass
class UploadResult:
    """One saved upload — useful for the response payload + tests."""

    original_name: str
    saved_path: str
    bytes_written: int
    sha256: str
    duplicate: bool


class UploadError(ValueError):
    """Raised on validation failures so the caller can surface a clean
    400 / CLI error message instead of the underlying file-system
    exception text."""


def _profile_uploads_root(profile: str) -> Path:
    profile_clean = (profile or "").strip()
    if not profile_clean:
        raise UploadError("doc profile is required for uploads")
    safe = "".join(c for c in profile_clean if c.isalnum() or c in "_-.") or "default"
    return Path.home() / ".amx" / "uploads" / safe


def save_uploaded_file(
    cfg: AMXConfig,
    profile: str,
    filename: str,
    payload: bytes,
) -> UploadResult:
    """Save one file under ``~/.amx/uploads/<profile>/`` and ensure the
    directory is registered on the doc profile.

    Validation: the extension must be in :data:`ACCEPTED_EXTENSIONS`
    (case-insensitive) and the byte length cannot exceed
    :data:`MAX_FILE_BYTES`. Existing files with the same content hash
    are reported via ``duplicate=True`` so the caller can de-noise the
    success message.
    """
    name = (filename or "").strip()
    if not name:
        raise UploadError("filename is required")
    ext = os.path.splitext(name)[1].lower()
    if ext not in ACCEPTED_EXTENSIONS:
        raise UploadError(
            f"unsupported file type {ext!r}. Accepted: " + ", ".join(sorted(ACCEPTED_EXTENSIONS))
        )
    if len(payload) > MAX_FILE_BYTES:
        raise UploadError(f"file {name!r} is {len(payload)} bytes; max is {MAX_FILE_BYTES}")
    if not payload:
        raise UploadError(f"file {name!r} is empty")

    root = _profile_uploads_root(profile)
    root.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(payload).hexdigest()
    target = root / f"{digest}{ext}"
    duplicate = target.exists()
    if not duplicate:
        # Atomic-ish write: temp file + rename so a torn write doesn't
        # leave a half-loaded PDF the next ingest tries to parse.
        tmp = target.with_suffix(target.suffix + ".part")
        tmp.write_bytes(payload)
        tmp.replace(target)

    # Ensure the upload root is part of the doc profile so the next
    # ``/scan`` or ``/ingest`` pulls the new file in. Idempotent — only
    # appends when the path isn't already there.
    existing_paths = list(cfg.doc_profiles.get(profile, []) or [])
    root_str = str(root)
    if root_str not in existing_paths:
        cfg.upsert_doc_profile(profile, existing_paths + [root_str])

    return UploadResult(
        original_name=name,
        saved_path=str(target),
        bytes_written=len(payload),
        sha256=digest,
        duplicate=duplicate,
    )


def save_uploaded_batch(
    cfg: AMXConfig,
    profile: str,
    files: Iterable[tuple[str, bytes]],
) -> list[UploadResult]:
    """Save many files in one go, enforcing the batch ceiling.

    The batch is rejected as a whole when the total exceeds
    :data:`MAX_BATCH_BYTES` — partial writes followed by a failure are
    worse than rejecting up-front because the user has to chase down
    which subset landed.
    """
    items = list(files)
    total = sum(len(payload or b"") for _, payload in items)
    if total > MAX_BATCH_BYTES:
        raise UploadError(f"batch is {total} bytes; max per upload batch is {MAX_BATCH_BYTES}")
    out: list[UploadResult] = []
    for name, payload in items:
        out.append(save_uploaded_file(cfg, profile, name, payload))
    return out
