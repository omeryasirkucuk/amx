"""Golden snapshot of ToolBox.schemas() output.

Locks the JSON-tool-schema payload that the LLM sees, so any structural
edit to the schema list (intentional or accidental) shows up as a single
diff on this file's expected hash.

If you are intentionally changing the schemas, regenerate the hash by
running pytest with ``PYTEST_SAVE_SNAPSHOT=1``; otherwise treat a
failure here as a bug to investigate.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path


def _schemas_payload() -> list[dict]:
    from amx.search.agent_tools import ToolBox

    return ToolBox.schemas()


def _digest(payload: object) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


SNAPSHOT_PATH = Path(__file__).parent / "fixtures" / "tool_schemas_snapshot.sha256"


def test_tool_schemas_snapshot_matches() -> None:
    payload = _schemas_payload()
    digest = _digest(payload)

    if os.environ.get("PYTEST_SAVE_SNAPSHOT"):
        SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
        SNAPSHOT_PATH.write_text(digest + "\n")

    assert SNAPSHOT_PATH.exists(), (
        "Snapshot file missing. Run with PYTEST_SAVE_SNAPSHOT=1 to create it."
    )

    expected = SNAPSHOT_PATH.read_text().strip()
    assert digest == expected, (
        "ToolBox.schemas() output changed. If intentional, regenerate the "
        "snapshot with PYTEST_SAVE_SNAPSHOT=1."
    )


def test_tool_schemas_shape() -> None:
    payload = _schemas_payload()
    assert isinstance(payload, list)
    assert len(payload) >= 20
    for entry in payload:
        assert entry["type"] == "function"
        fn = entry["function"]
        assert isinstance(fn["name"], str) and fn["name"]
        assert isinstance(fn["description"], str) and fn["description"]
        assert fn["parameters"]["type"] == "object"
