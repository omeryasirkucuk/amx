"""Tests for the eml loader used by the docs scanner and pages module."""

from __future__ import annotations

from pathlib import Path

EML_SAMPLE = b"""From: alice@example.com
To: bob@example.com
Subject: Q3 spec
Date: Wed, 01 Apr 2026 12:00:00 +0000
MIME-Version: 1.0
Content-Type: text/plain; charset=utf-8

Hello team,

The revenue calc multiplies units by net price.
"""


def test_eml_loader_extracts_headers_and_body(tmp_path: Path) -> None:
    p = tmp_path / "msg.eml"
    p.write_bytes(EML_SAMPLE)

    from amx.docs.loaders.eml_loader import load_eml

    text = load_eml(p)
    assert "Subject: Q3 spec" in text
    assert "From: alice@example.com" in text
    assert "revenue calc multiplies units by net price" in text


def test_eml_loader_lists_attachments(tmp_path: Path) -> None:
    raw = (
        b"From: a@b.com\r\nTo: c@d.com\r\nSubject: hi\r\n"
        b'MIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary="BOUND"\r\n\r\n'
        b"--BOUND\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nbody text\r\n"
        b"--BOUND\r\nContent-Type: application/octet-stream\r\n"
        b'Content-Disposition: attachment; filename="report.pdf"\r\n\r\n'
        b"dGVzdA==\r\n--BOUND--\r\n"
    )
    p = tmp_path / "attach.eml"
    p.write_bytes(raw)

    from amx.docs.loaders.eml_loader import load_eml

    text = load_eml(p)
    assert "Attachments: report.pdf" in text
    assert "body text" in text
