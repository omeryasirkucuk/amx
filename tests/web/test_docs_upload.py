"""POST /api/docs/upload — multipart drag-drop endpoint."""

from __future__ import annotations


def test_upload_creates_profile_and_saves(client, auth_headers) -> None:
    files = [("files", ("hello.md", b"# Hi\n", "text/markdown"))]
    res = client.post(
        "/api/docs/upload",
        headers=auth_headers,
        data={"profile": "design", "ingest": "false"},
        files=files,
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["profile"] == "design"
    assert body["count"] == 1
    assert body["saved"][0]["name"] == "hello.md"


def test_upload_rejects_unsupported_extension(client, auth_headers) -> None:
    files = [("files", ("blob.exe", b"\x00\x01", "application/octet-stream"))]
    res = client.post(
        "/api/docs/upload",
        headers=auth_headers,
        data={"profile": "design", "ingest": "false"},
        files=files,
    )
    assert res.status_code == 400


def test_upload_blank_profile_400(client, auth_headers) -> None:
    files = [("files", ("a.md", b"x", "text/markdown"))]
    res = client.post(
        "/api/docs/upload",
        headers=auth_headers,
        data={"profile": "  ", "ingest": "false"},
        files=files,
    )
    assert res.status_code == 400


def test_upload_marks_duplicate(client, auth_headers) -> None:
    payload = b"# Hello\n"
    for _ in range(2):
        res = client.post(
            "/api/docs/upload",
            headers=auth_headers,
            data={"profile": "design", "ingest": "false"},
            files=[("files", ("h.md", payload, "text/markdown"))],
        )
        assert res.status_code == 200
    body = res.json()  # second response
    assert body["saved"][0]["duplicate"] is True
