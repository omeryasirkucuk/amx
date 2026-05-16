# agent_tools.py — Phase 1a: extract `schemas()` data

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the 1265-line `ToolBox.schemas()` JSON-schema payload out of `amx/search/agent_tools.py` into a new `amx/search/_tool_schemas.py` module without changing any behavior, returning the exact same list-of-dicts to callers.

**Architecture:** Pure data extraction. The new module exposes a single module-level constant `TOOL_SCHEMAS: list[dict[str, Any]]` and a function `tool_schemas() -> list[dict[str, Any]]` returning a deep copy (matching today's signature, since today's `schemas()` is a `@staticmethod` returning a fresh literal each call). `ToolBox.schemas()` becomes a one-line wrapper. No imports change for callers (`tool_agent.py` keeps calling `self.box.schemas()`).

**Tech Stack:** Python 3.11, pytest. No new dependencies.

**Risk:** LOW. The output of `schemas()` is pure JSON-serializable data with no closures, no `self` access. A golden snapshot test guarantees byte-identical output.

---

## Files to Modify

| Action | Path | Purpose |
|---|---|---|
| Create | `amx/search/_tool_schemas.py` | Holds the JSON tool-schema list as a module-level constant |
| Modify | `amx/search/agent_tools.py:702-1967` | Replace 1265-line `schemas()` body with one-line wrapper |
| Create | `tests/test_tool_schemas_snapshot.py` | Golden snapshot test to prove byte-identical output |

No other files need touching. `tool_agent.py:29` and the 25 test importers continue to import `ToolBox` from `amx.search.agent_tools`; the public method `ToolBox.schemas()` keeps working.

---

## Task 1 — Baseline characterization (golden snapshot)

**Files:**
- Create: `tests/test_tool_schemas_snapshot.py`

- [ ] **Step 1: Write the snapshot test**

```python
"""Golden snapshot of ToolBox.schemas() output.

Locks the JSON-tool-schema payload that the LLM sees, so any structural
edit to the schema list (intentional or accidental) shows up as a single
diff on this file's expected hash.

If you are intentionally changing the schemas, regenerate the hash by
running pytest with PYTEST_SAVE_SNAPSHOT=1; otherwise treat a failure
here as a bug to investigate.
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
```

- [ ] **Step 2: Generate the snapshot file**

Run:

```bash
PYTEST_SAVE_SNAPSHOT=1 pytest tests/test_tool_schemas_snapshot.py -q
```

Expected: PASS (snapshot is created on first run).

- [ ] **Step 3: Run again without the env var to confirm the snapshot locks**

```bash
pytest tests/test_tool_schemas_snapshot.py -q
```

Expected: PASS (snapshot is compared, not written).

- [ ] **Step 4: Commit the baseline**

```bash
git add tests/test_tool_schemas_snapshot.py tests/fixtures/tool_schemas_snapshot.sha256
git commit -m "test(search): snapshot ToolBox.schemas() output"
```

---

## Task 2 — Create `_tool_schemas.py` with the payload

**Files:**
- Create: `amx/search/_tool_schemas.py`

- [ ] **Step 1: Capture the current `schemas()` body**

Run from the AMX repo root:

```bash
sed -n '702,1967p' amx/search/agent_tools.py > /tmp/schemas_body.py
```

Verify the head + tail look right:

```bash
head -5 /tmp/schemas_body.py && echo "---" && tail -5 /tmp/schemas_body.py
```

Expected head: starts with `    # ------------------------------------------------------------------ schemas` followed by `    @staticmethod` and `def schemas() ...`.
Expected tail: ends with the closing `]` of the schemas list.

- [ ] **Step 2: Write the new module**

Create `amx/search/_tool_schemas.py` with this skeleton:

```python
"""JSON tool schemas passed to the LLM by ``ToolBox.schemas()``.

This module holds the data definition only — names, descriptions, and
JSON-schema argument shapes for every tool the ``/ask`` agent can call.

The list is duplicated (deep-copied) on every access via ``tool_schemas()``
so callers can mutate it without poisoning the shared source. The
existing ``ToolBox.schemas()`` static method delegates here.
"""

from __future__ import annotations

import copy
from typing import Any

# The single source of truth. Defined as a module-level constant so it is
# constructed exactly once at import time; ``tool_schemas()`` returns a
# fresh deep copy on each call to preserve the historical behaviour of
# the in-class literal ``return [...]``.
_TOOL_SCHEMAS: list[dict[str, Any]] = [
    # ----- PASTE THE LIST CONTENTS HERE -----
]


def tool_schemas() -> list[dict[str, Any]]:
    """Return a fresh copy of the tool-schema list for LLM tool-calling.

    Callers must not mutate the returned list in place beyond their own
    request scope, but the deep copy ensures local edits cannot leak
    into other consumers.
    """
    return copy.deepcopy(_TOOL_SCHEMAS)
```

Then paste the list literal from `agent_tools.py:707-1967` into the `_TOOL_SCHEMAS = [ … ]` slot. The original `schemas()` body is:

```python
    @staticmethod
    def schemas() -> list[dict[str, Any]]:
        """JSON schemas for every tool — passed to the LLM as the ``tools``
        parameter. Names are stable; argument names match the Python kwargs
        of the corresponding ``ToolBox`` method."""
        return [
            {
                "type": "function",
                "function": {
                    "name": "list_schemas",
                    ...
                },
            },
            ...
        ]
```

Take only the `[ ... ]` list contents — do **not** include the `@staticmethod` decorator, the `def schemas()` line, the docstring, or the `return ` keyword in the new module. Strip one level of indentation (the list contents are indented `        ` inside the method; outside the method they need only the indentation that makes them valid Python list literals).

- [ ] **Step 3: Confirm the new module parses**

```bash
python -c "from amx.search._tool_schemas import tool_schemas; print(len(tool_schemas()))"
```

Expected: a number between 25 and 35 (the count of tool entries).

- [ ] **Step 4: Cross-check against the live `ToolBox.schemas()`**

```bash
python -c "
import json, hashlib
from amx.search.agent_tools import ToolBox
from amx.search._tool_schemas import tool_schemas
a = json.dumps(ToolBox.schemas(), sort_keys=True, separators=(',', ':'))
b = json.dumps(tool_schemas(), sort_keys=True, separators=(',', ':'))
print('match' if a == b else 'DIFFER')
print('a sha:', hashlib.sha256(a.encode()).hexdigest()[:12])
print('b sha:', hashlib.sha256(b.encode()).hexdigest()[:12])
"
```

Expected output: `match` and two equal short hashes. If they differ, the paste is corrupted — re-do step 2.

- [ ] **Step 5: Commit the new module (still unreferenced from agent_tools.py)**

```bash
git add amx/search/_tool_schemas.py
git commit -m "feat(search): add _tool_schemas module mirroring ToolBox.schemas()"
```

---

## Task 3 — Replace `ToolBox.schemas()` body with delegation

**Files:**
- Modify: `amx/search/agent_tools.py:700-1967`

- [ ] **Step 1: Add the import at the top of `agent_tools.py`**

In the existing import block (around line 29), add:

```python
from amx.search._tool_schemas import tool_schemas as _tool_schemas
```

Place it adjacent to the existing `from amx.search._agent_tools_helpers import ...` line so the search-package imports stay grouped.

- [ ] **Step 2: Replace the 1265-line method body with a one-liner**

Locate the existing `schemas()` definition in `agent_tools.py` (currently around lines 700-1967). Replace the entire block:

```python
    # ------------------------------------------------------------------ schemas
    @staticmethod
    def schemas() -> list[dict[str, Any]]:
        """JSON schemas for every tool — passed to the LLM as the ``tools``
        parameter. Names are stable; argument names match the Python kwargs
        of the corresponding ``ToolBox`` method."""
        return [
            ...
            ...     <-- 1265 lines of JSON-schema literals
            ...
        ]
```

with:

```python
    # ------------------------------------------------------------------ schemas
    @staticmethod
    def schemas() -> list[dict[str, Any]]:
        """JSON schemas for every tool — passed to the LLM as the ``tools``
        parameter. Names are stable; argument names match the Python kwargs
        of the corresponding ``ToolBox`` method. Data lives in
        ``amx/search/_tool_schemas.py``; this method delegates so callers
        keep their existing call site (``ToolBox.schemas()``)."""
        return _tool_schemas()
```

- [ ] **Step 3: Run the snapshot test**

```bash
pytest tests/test_tool_schemas_snapshot.py -q
```

Expected: PASS — byte-identical output proves the move was lossless.

- [ ] **Step 4: Run the broader search-related test set**

```bash
pytest tests/test_agent_tools_helpers.py tests/test_toolbox_cache_first_metadata.py tests/test_find_joinable_strategies.py tests/test_toolbox_multi_profile.py tests/test_ask_toolbox_cache.py -q
```

Expected: all PASS (or have the same status they had on `main` before this work — capture the pre-existing pass/fail state on the base branch first if unsure).

- [ ] **Step 5: Run full test suite**

```bash
pytest -q
```

Expected: same pass/fail count as on `main` before the branch. Investigate any new failure before continuing.

- [ ] **Step 6: Commit**

```bash
git add amx/search/agent_tools.py
git commit -m "refactor(search): delegate ToolBox.schemas() to _tool_schemas module"
```

---

## Task 4 — Final verification

- [ ] **Step 1: Confirm LOC reduction**

```bash
wc -l amx/search/agent_tools.py amx/search/_tool_schemas.py
```

Expected: `agent_tools.py` now around 5500 LOC (down from 6778); `_tool_schemas.py` around 1265 LOC.

- [ ] **Step 2: Public API check**

```bash
python -c "
from amx.search.agent_tools import (
    ToolBox,
    _ToolError,
    _name_overlap_score,
    _dtype_compat_score,
    _description_proximity,
    _safe_json,
)
print('public re-exports OK')
print('schemas count:', len(ToolBox.schemas()))
"
```

Expected: `public re-exports OK` and the same schema count as before the branch.

- [ ] **Step 3: English-only sweep on changed paths**

```bash
git diff --name-only main...HEAD | xargs -I{} grep -lE "[ğüşıöçĞÜŞİÖÇ]" {} 2>/dev/null
```

Expected: no output.

- [ ] **Step 4: "paid" + Claude attribution sweep**

```bash
git diff --name-only main...HEAD | xargs grep -niE "paid|claude|anthropic|noreply@anthropic" 2>/dev/null
```

Expected: no output (provider-level `import anthropic` references inside unchanged files are fine and won't appear in the diff).

- [ ] **Step 5: Lint + format + type-check**

```bash
make lint format type 2>/dev/null || (ruff check . && ruff format --check . && mypy amx)
```

Expected: clean. If `make` targets don't exist, the fallback `ruff` + `mypy` chain must pass.

- [ ] **Step 6: Performance smoke**

Compare `python -c "from amx.search.agent_tools import ToolBox; ToolBox.schemas()"` cold start time before and after the branch using `time`:

```bash
git stash -u && time python -c "from amx.search.agent_tools import ToolBox; ToolBox.schemas()" && git stash pop
time python -c "from amx.search.agent_tools import ToolBox; ToolBox.schemas()"
```

Expected: the post-refactor wall time is within ±10 ms of the baseline. A deep-copy on every `schemas()` call is the only added cost; for a 1265-line literal that's sub-millisecond on this machine.

- [ ] **Step 7: Final commit + branch ready for PR**

If steps 1-6 are clean, the branch is ready for a PR. (No PR opened in this phase — that happens once Phase 1a, 1b, and 1c are queued together, or as the user instructs.)

---

## Out of scope (deferred to Phase 1b+)

- Splitting the tool-implementation methods (`_tool_*`) into per-concern mixins — that is Phase 1b through 1g.
- Touching `_CacheBackedTableProfile`, `_sample_distinct_values`, `_detect_date_format` — they stay where they are this round.
- Anything in `_agent_tools_helpers.py` — already split.
- The 600-line system prompt in `tool_agent.py` — that is Phase 2.

## Rollback

If anything in Task 3 or 4 misbehaves and the snapshot test passes but a downstream test fails, the most likely cause is a copy-paste indentation slip in `_tool_schemas.py`. The safest rollback is:

```bash
git revert HEAD~1  # undoes Task 3
# investigate, fix _tool_schemas.py, re-do Task 3
```

The snapshot test (Task 1) was committed before the move, so it remains the canary for any drift.
