"""The /api/ask/context payload includes lineage_artifacts + pages counts.

These two blocks let the Studio AskChat scope picker render lineage
canvas availability and anchored-pages availability alongside the
existing doc / code profile counts. The shape contract:

* ``lineage_artifacts`` — list of ``{name, linked_db_profiles}``.
* ``anchored_pages`` — dict with at least ``count`` (int).

Both are always present (possibly empty) so the SPA can render the
panel without conditional plumbing.
"""

from __future__ import annotations


def test_ask_context_payload_includes_lineage_and_pages_blocks(client, auth_headers) -> None:
    resp = client.get("/api/ask/context", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert "lineage_artifacts" in payload
    assert "anchored_pages" in payload
    assert isinstance(payload["lineage_artifacts"], list)
    assert isinstance(payload["anchored_pages"], dict)
    assert "count" in payload["anchored_pages"]
    assert isinstance(payload["anchored_pages"]["count"], int)


def test_ask_context_existing_keys_preserved(client, auth_headers) -> None:
    """The new blocks don't displace the prior payload shape — Studio
    components still get ``scope_db_profiles`` / ``doc_profiles`` /
    ``code_profiles`` exactly as before.
    """
    resp = client.get("/api/ask/context", headers=auth_headers)
    assert resp.status_code == 200
    payload = resp.json()
    assert "scope_db_profiles" in payload
    assert "doc_profiles" in payload
    assert "code_profiles" in payload
    assert isinstance(payload["doc_profiles"], list)
    assert isinstance(payload["code_profiles"], list)
