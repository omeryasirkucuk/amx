"""Large downloads emit progress so they aren't mistaken for a hang.

Document downloads streamed silently in 256KB chunks for up to the 300s
timeout — a stalled and a progressing transfer looked identical, tempting
a Ctrl-C. ``_download_to_file`` now prints a throttled byte-progress line.
"""

from __future__ import annotations

import pytest

import amx.docs.scanner as scanner
import amx.utils.console as console


class _FakeResp:
    def __init__(self, chunks: list[bytes], total: int) -> None:
        self.status_code = 200
        self.headers = {"Content-Length": str(total)} if total else {}
        self._chunks = chunks

    def iter_content(self, chunk_size: int) -> object:
        return iter(self._chunks)


def test_download_emits_throttled_progress(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    chunk = b"x" * (256 * 1024)
    n_chunks = 48  # 48 * 256KB = 12 MB → crosses the 5 MB report threshold twice
    total = n_chunks * len(chunk)
    monkeypatch.setattr(
        scanner.requests, "get", lambda *a, **k: _FakeResp([chunk] * n_chunks, total)
    )
    infos: list[str] = []
    monkeypatch.setattr(console, "info", lambda m: infos.append(m))

    dest = tmp_path / "f.bin"
    written = scanner._download_to_file("http://example/f.bin", dest)

    assert written == total
    assert dest.stat().st_size == total
    progress_lines = [i for i in infos if "Downloading" in i]
    assert len(progress_lines) >= 2  # 12 MB / 5 MB threshold
    assert any("/" in i and "MB" in i for i in progress_lines)  # shows total when known


def test_download_without_content_length_still_reports(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    chunk = b"y" * (256 * 1024)
    n_chunks = 24  # 6 MB, no Content-Length header
    monkeypatch.setattr(scanner.requests, "get", lambda *a, **k: _FakeResp([chunk] * n_chunks, 0))
    infos: list[str] = []
    monkeypatch.setattr(console, "info", lambda m: infos.append(m))

    scanner._download_to_file("http://example/f.bin", tmp_path / "g.bin")
    assert any("Downloading" in i and "MB" in i for i in infos)
