# Style Reference Table — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let users attach a *reference table* to an LLM profile so AMX's generated descriptions match the user's writing style (tone, language, length, structure) without ever leaking the reference table's domain content.

**Architecture:** Two-stage distillation. Stage 1 runs once per LLM profile: it reads only the reference table's column metadata (`column_name`, `data_type`, `comment`), samples up to 30 non-empty descriptions, and calls the active LLM to distill a `StyleProfile` JSON (language, tone, length stats, structural patterns, entity-masked few-shot examples). Stage 2 runs on every `/run`: a small `## Writing style` fragment is appended to each agent's system prompt; a post-generation regex guard rejects any output that contains a placeholder literal or exact substring from the redacted examples.

**Tech Stack:** Python 3.11, SQLAlchemy / sqlite3 (existing AMX storage layer), Click + AMX slash-command registry, FastAPI (Studio backend), React + TypeScript (Studio frontend), pytest.

**Reference spec:** `docs/superpowers/specs/2026-05-12-style-reference-table-design.md`

---

## File Structure

**New files:**
- `amx/llm/style/__init__.py` — package marker, re-exports the public API.
- `amx/llm/style/profile.py` — `StyleProfile` dataclass + JSON (de)serialization.
- `amx/llm/style/extractor.py` — Stage 1: metadata sampling + distillation LLM call.
- `amx/llm/style/injector.py` — Stage 2: builds the `## Writing style` prompt fragment.
- `amx/llm/style/guard.py` — post-generation leakage regex sweep.
- `amx/storage/style_store.py` — DAO around the `style_profiles` table.
- `amx/cli_support/commands/style.py` — `/style` slash-command handlers.
- `amx/web/routers/style.py` — Studio REST endpoints.
- `frontend/src/components/StyleReferenceCard.tsx` — Studio UI card.
- `tests/test_style_profile.py`
- `tests/test_style_extractor.py`
- `tests/test_style_injector.py`
- `tests/test_style_guard.py`
- `tests/test_style_store.py`
- `tests/test_style_cli.py`
- `tests/test_style_leakage_redteam.py`
- `tests/test_style_router.py`

**Modified files:**
- `amx/storage/sqlite_store.py` — add `style_profiles` table to `init()`.
- `amx/storage/sqlalchemy_store.py` — add the same table to the shared schema.
- `amx/storage/shared_schema.py` — declarative metadata for the shared store.
- `amx/cli_support/slash_commands.py` — add `/style` entry to `_LLM_COMMANDS` with `cross_namespace=True`.
- `amx/cli_support/session.py` — wire dispatch for `/style` heads (the registry derives autocomplete + help automatically).
- `amx/cli.py` — import & call `register_style_commands`.
- `amx/agents/profile_agent.py` — thread `style_profile` into `_build_system_prompt` and `_build_messages`.
- `amx/agents/rag_agent.py` — same.
- `amx/agents/code_agent.py` — same.
- `amx/web/routers/generate.py` — same on its `_build_system_prompt`.
- `amx/web/routers/__init__.py` — mount the new router.
- `frontend/src/routes/Settings.tsx` — embed `<StyleReferenceCard>` in the LLM profile detail view.

---

## Task 1: `StyleProfile` dataclass

**Files:**
- Create: `amx/llm/style/__init__.py`
- Create: `amx/llm/style/profile.py`
- Test: `tests/test_style_profile.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_style_profile.py
from amx.llm.style.profile import StyleProfile, PLACEHOLDERS


def test_round_trip_serialization():
    sp = StyleProfile(
        language="en-US",
        tone="formal, third-person",
        avg_length_words=14,
        length_range=(8, 22),
        person="impersonal",
        capitalization="sentence-case",
        ends_with_period=True,
        structural_patterns=["Definition + purpose"],
        vocabulary_register="business-technical",
        redacted_examples=["Unique identifier of the <ENTITY>."],
    )
    s = sp.to_json()
    sp2 = StyleProfile.from_json(s)
    assert sp2 == sp


def test_from_json_rejects_unknown_placeholder_in_examples():
    bad = (
        '{"language":"en","tone":"x","avg_length_words":1,'
        '"length_range":[1,1],"person":"x","capitalization":"x",'
        '"ends_with_period":true,"structural_patterns":[],'
        '"vocabulary_register":"x",'
        '"redacted_examples":["use <FOO> here"]}'
    )
    try:
        StyleProfile.from_json(bad)
    except ValueError as e:
        assert "FOO" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_placeholders_constant_is_frozen_set():
    assert PLACEHOLDERS == frozenset(
        {"<ENTITY>", "<METRIC>", "<DATE_FIELD>", "<STATUS>", "<IDENTIFIER>"}
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_style_profile.py -v`
Expected: FAIL (module does not exist).

- [ ] **Step 3: Implement `profile.py`**

```python
# amx/llm/style/__init__.py
from amx.llm.style.profile import PLACEHOLDERS, StyleProfile

__all__ = ["PLACEHOLDERS", "StyleProfile"]
```

```python
# amx/llm/style/profile.py
"""StyleProfile: a structured, content-safe summary of a user's
description writing style. Only this object — never the original
reference comments — is injected into run-time prompts."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass

PLACEHOLDERS: frozenset[str] = frozenset(
    {"<ENTITY>", "<METRIC>", "<DATE_FIELD>", "<STATUS>", "<IDENTIFIER>"}
)

_PLACEHOLDER_RE = re.compile(r"<[A-Z_]+>")


@dataclass(frozen=True)
class StyleProfile:
    language: str
    tone: str
    avg_length_words: int
    length_range: tuple[int, int]
    person: str
    capitalization: str
    ends_with_period: bool
    structural_patterns: list[str]
    vocabulary_register: str
    redacted_examples: list[str]

    def to_json(self) -> str:
        d = asdict(self)
        d["length_range"] = list(self.length_range)
        return json.dumps(d, ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_json(cls, payload: str) -> "StyleProfile":
        d = json.loads(payload)
        for ex in d.get("redacted_examples", []):
            for tag in _PLACEHOLDER_RE.findall(ex):
                if tag not in PLACEHOLDERS:
                    raise ValueError(
                        f"redacted example uses unknown placeholder {tag}"
                    )
        d["length_range"] = tuple(d["length_range"])
        return cls(**d)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_style_profile.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add amx/llm/style/__init__.py amx/llm/style/profile.py tests/test_style_profile.py
git commit -m "feat(style): add StyleProfile dataclass with placeholder validation"
```

---

## Task 2: `style_profiles` storage table + DAO

**Files:**
- Modify: `amx/storage/sqlite_store.py` (add `CREATE TABLE` inside `init()`)
- Modify: `amx/storage/shared_schema.py` (add SQLAlchemy table)
- Modify: `amx/storage/sqlalchemy_store.py` (no change unless table list is centralized — verify; if it is, add row)
- Create: `amx/storage/style_store.py`
- Test: `tests/test_style_store.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_style_store.py
import os
import tempfile

from amx.llm.style.profile import StyleProfile
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.storage.style_store import StyleStore


def _example_profile() -> StyleProfile:
    return StyleProfile(
        language="en-US",
        tone="formal",
        avg_length_words=10,
        length_range=(5, 18),
        person="impersonal",
        capitalization="sentence-case",
        ends_with_period=True,
        structural_patterns=["noun + role"],
        vocabulary_register="business",
        redacted_examples=["Unique id of the <ENTITY>."],
    )


def test_upsert_and_fetch(tmp_path):
    db = tmp_path / "amx.sqlite"
    history = SQLiteHistoryStore(str(db))
    history.init()
    store = StyleStore(str(db))

    store.upsert(
        llm_profile="default",
        source_ref="warehouse.sales.orders",
        source_db_kind="snowflake",
        profile=_example_profile(),
        sample_count=12,
    )
    row = store.get("default")
    assert row is not None
    assert row.llm_profile == "default"
    assert row.source_ref == "warehouse.sales.orders"
    assert row.profile.language == "en-US"
    assert row.enabled is True
    assert row.sample_count == 12


def test_upsert_overwrites_same_profile(tmp_path):
    db = tmp_path / "amx.sqlite"
    SQLiteHistoryStore(str(db)).init()
    store = StyleStore(str(db))
    store.upsert("default", "a.b.c", "duckdb", _example_profile(), 3)
    first = store.get("default")
    store.upsert("default", "x.y.z", "duckdb", _example_profile(), 7)
    second = store.get("default")
    assert second is not None
    assert second.source_ref == "x.y.z"
    assert second.sample_count == 7
    assert second.created_at == first.created_at  # preserved on overwrite
    assert second.updated_at >= first.updated_at


def test_set_enabled_and_clear(tmp_path):
    db = tmp_path / "amx.sqlite"
    SQLiteHistoryStore(str(db)).init()
    store = StyleStore(str(db))
    store.upsert("default", "a.b.c", "duckdb", _example_profile(), 3)
    store.set_enabled("default", False)
    assert store.get("default").enabled is False
    store.clear("default")
    assert store.get("default") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_style_store.py -v`
Expected: FAIL (`style_profiles` table missing + module missing).

- [ ] **Step 3: Add the table to `amx/storage/sqlite_store.py`**

Inside the `init(self)` method of `SQLiteHistoryStore`, alongside the other `CREATE TABLE IF NOT EXISTS` statements, add:

```python
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS style_profiles (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    llm_profile     TEXT    NOT NULL,
                    source_ref      TEXT    NOT NULL,
                    source_db_kind  TEXT    NOT NULL,
                    profile_json    TEXT    NOT NULL,
                    enabled         INTEGER NOT NULL DEFAULT 1,
                    sample_count    INTEGER NOT NULL,
                    created_at      TEXT    NOT NULL,
                    updated_at      TEXT    NOT NULL,
                    UNIQUE (llm_profile)
                )
                """
            )
```

- [ ] **Step 4: Add the matching SQLAlchemy table in `amx/storage/shared_schema.py`**

Follow the same column order and types as the SQLite version. Place it next to the most recent `Table(` definition in that file. Use `String` instead of `TEXT`, `Integer` for `INTEGER`, and `Boolean` for `enabled` (with `default=True`).

- [ ] **Step 5: Implement `amx/storage/style_store.py`**

```python
"""DAO for the `style_profiles` table.

Reads and writes happen through plain sqlite3 to keep the dependency
surface identical to the existing `SQLiteHistoryStore`. The shared
SQLAlchemy variant (history-store mode) is not yet wired here; style
profiles are local-only in v1."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass

from amx.llm.style.profile import StyleProfile


@dataclass(frozen=True)
class StoredStyleProfile:
    llm_profile: str
    source_ref: str
    source_db_kind: str
    profile: StyleProfile
    enabled: bool
    sample_count: int
    created_at: float
    updated_at: float


def _now() -> float:
    return time.time()


class StyleStore:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self._db_path)
        c.row_factory = sqlite3.Row
        return c

    def upsert(
        self,
        llm_profile: str,
        source_ref: str,
        source_db_kind: str,
        profile: StyleProfile,
        sample_count: int,
    ) -> None:
        now = _now()
        with self._conn() as c:
            existing = c.execute(
                "SELECT created_at FROM style_profiles WHERE llm_profile = ?",
                (llm_profile,),
            ).fetchone()
            created_at = float(existing["created_at"]) if existing else now
            c.execute(
                """
                INSERT INTO style_profiles
                    (llm_profile, source_ref, source_db_kind, profile_json,
                     enabled, sample_count, created_at, updated_at)
                VALUES (?, ?, ?, ?, 1, ?, ?, ?)
                ON CONFLICT(llm_profile) DO UPDATE SET
                    source_ref     = excluded.source_ref,
                    source_db_kind = excluded.source_db_kind,
                    profile_json   = excluded.profile_json,
                    sample_count   = excluded.sample_count,
                    updated_at     = excluded.updated_at
                """,
                (
                    llm_profile,
                    source_ref,
                    source_db_kind,
                    profile.to_json(),
                    sample_count,
                    str(created_at),
                    str(now),
                ),
            )

    def get(self, llm_profile: str) -> StoredStyleProfile | None:
        with self._conn() as c:
            row = c.execute(
                "SELECT * FROM style_profiles WHERE llm_profile = ?",
                (llm_profile,),
            ).fetchone()
        if not row:
            return None
        return StoredStyleProfile(
            llm_profile=row["llm_profile"],
            source_ref=row["source_ref"],
            source_db_kind=row["source_db_kind"],
            profile=StyleProfile.from_json(row["profile_json"]),
            enabled=bool(row["enabled"]),
            sample_count=int(row["sample_count"]),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
        )

    def set_enabled(self, llm_profile: str, enabled: bool) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE style_profiles SET enabled = ?, updated_at = ? "
                "WHERE llm_profile = ?",
                (1 if enabled else 0, str(_now()), llm_profile),
            )

    def clear(self, llm_profile: str) -> None:
        with self._conn() as c:
            c.execute(
                "DELETE FROM style_profiles WHERE llm_profile = ?",
                (llm_profile,),
            )
```

- [ ] **Step 6: Run test to verify it passes**

Run: `pytest tests/test_style_store.py -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add amx/storage/sqlite_store.py amx/storage/shared_schema.py \
        amx/storage/style_store.py tests/test_style_store.py
git commit -m "feat(style): style_profiles table + StyleStore DAO"
```

---

## Task 3: Style injector (Stage 2 prompt fragment)

**Files:**
- Create: `amx/llm/style/injector.py`
- Test: `tests/test_style_injector.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_style_injector.py
from amx.llm.style.injector import render_style_section
from amx.llm.style.profile import StyleProfile


def _profile() -> StyleProfile:
    return StyleProfile(
        language="en-US",
        tone="formal, third-person",
        avg_length_words=14,
        length_range=(8, 22),
        person="impersonal",
        capitalization="sentence-case",
        ends_with_period=True,
        structural_patterns=["Definition + purpose"],
        vocabulary_register="business-technical",
        redacted_examples=[
            "Unique identifier of the <ENTITY>.",
            "Creation timestamp of the <ENTITY> record.",
        ],
    )


def test_render_includes_required_fields_and_guard_text():
    out = render_style_section(_profile())
    assert "## Writing style" in out
    assert "en-US" in out
    assert "8" in out and "22" in out
    assert "formal, third-person" in out
    # examples are present
    assert "<ENTITY>" in out
    # the guard sentence is present
    assert "Never copy these placeholders" in out


def test_render_none_returns_empty_string():
    assert render_style_section(None) == ""
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_style_injector.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement `injector.py`**

```python
# amx/llm/style/injector.py
"""Renders the `## Writing style` system-prompt fragment from a
StyleProfile. The guard sentence is non-negotiable and ships every
time a profile is injected."""

from __future__ import annotations

from amx.llm.style.profile import PLACEHOLDERS, StyleProfile

_GUARD = (
    "Match this style profile for tone, language, length, and sentence "
    "structure. The placeholders {placeholders} mark domain-specific "
    "terms from another schema. Never copy these placeholders into "
    "your output, and never invent entity names from them. Always "
    "derive domain terms from the target column you are describing."
).format(placeholders=", ".join(sorted(PLACEHOLDERS)))


def render_style_section(profile: StyleProfile | None) -> str:
    if profile is None:
        return ""
    lo, hi = profile.length_range
    patterns = "\n".join(f"- {p}" for p in profile.structural_patterns)
    examples = "\n".join(f"- {ex}" for ex in profile.redacted_examples)
    return (
        "\n\n## Writing style\n"
        f"- Language: {profile.language}\n"
        f"- Tone: {profile.tone}\n"
        f"- Person: {profile.person}\n"
        f"- Capitalization: {profile.capitalization}\n"
        f"- Ends with period: {'yes' if profile.ends_with_period else 'no'}\n"
        f"- Length (words): {lo}–{hi}, target ~{profile.avg_length_words}\n"
        f"- Vocabulary register: {profile.vocabulary_register}\n"
        f"### Structural patterns\n{patterns}\n"
        f"### Examples (placeholders, do NOT copy)\n{examples}\n\n"
        f"{_GUARD}\n"
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_style_injector.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add amx/llm/style/injector.py tests/test_style_injector.py
git commit -m "feat(style): system-prompt fragment renderer with leakage guard text"
```

---

## Task 4: Post-generation leakage guard

**Files:**
- Create: `amx/llm/style/guard.py`
- Test: `tests/test_style_guard.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_style_guard.py
from amx.llm.style.guard import contains_leakage
from amx.llm.style.profile import StyleProfile


def _p(examples):
    return StyleProfile(
        language="en-US", tone="x", avg_length_words=1, length_range=(1, 1),
        person="x", capitalization="x", ends_with_period=True,
        structural_patterns=[], vocabulary_register="x",
        redacted_examples=examples,
    )


def test_flags_placeholder_literal():
    p = _p(["Unique id of the <ENTITY>."])
    assert contains_leakage("This row holds the <ENTITY> name.", p) is True


def test_flags_exact_example_match():
    p = _p(["Unique id of the <ENTITY>."])
    assert contains_leakage("Unique id of the <ENTITY>.", p) is True


def test_clean_output_passes():
    p = _p(["Unique id of the <ENTITY>."])
    assert contains_leakage("Order identifier issued at checkout.", p) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_style_guard.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement `guard.py`**

```python
# amx/llm/style/guard.py
"""Cheap post-generation regex sweep. Flags outputs that contain a
placeholder literal (the model copied the few-shot mask) or that
exactly reproduce one of the redacted examples (the model
plagiarized the example wholesale)."""

from __future__ import annotations

from amx.llm.style.profile import PLACEHOLDERS, StyleProfile


def contains_leakage(text: str, profile: StyleProfile) -> bool:
    if not text:
        return False
    for tag in PLACEHOLDERS:
        if tag in text:
            return True
    normalized = text.strip()
    for ex in profile.redacted_examples:
        if normalized == ex.strip():
            return True
    return False
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_style_guard.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add amx/llm/style/guard.py tests/test_style_guard.py
git commit -m "feat(style): post-generation leakage guard"
```

---

## Task 5: Style extractor (Stage 1)

**Files:**
- Create: `amx/llm/style/extractor.py`
- Test: `tests/test_style_extractor.py`

The extractor is a pure function: given a `dict[column_name, comment_or_None]` and an LLM callable, it returns a `StyleProfile` plus the number of samples used. Reading from the DB and selecting the LLM happens in the CLI/router layer — keeping the extractor pure makes it trivial to unit-test.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_style_extractor.py
import json

import pytest

from amx.llm.style.extractor import (
    MIN_SAMPLES,
    NoSamplesError,
    extract_style,
    sample_descriptions,
)


def test_sample_filters_empty_and_caps_at_30():
    raw = {f"col_{i}": (f"desc {i}" if i % 2 == 0 else None) for i in range(100)}
    picked = sample_descriptions(raw, cap=30)
    assert len(picked) == 30
    assert all(v for _, v in picked)


def test_sample_raises_when_under_minimum():
    raw = {"a": "x", "b": None, "c": None}
    with pytest.raises(NoSamplesError):
        sample_descriptions(raw, cap=30, min_samples=MIN_SAMPLES)


def test_extract_round_trips_through_llm_stub():
    raw = {
        "order_id": "Unique id of the order.",
        "created_at": "Creation timestamp of the order record.",
        "amount": "Sum of charged amount per transaction.",
    }
    stub_response = json.dumps({
        "language": "en-US",
        "tone": "formal",
        "avg_length_words": 6,
        "length_range": [4, 9],
        "person": "impersonal",
        "capitalization": "sentence-case",
        "ends_with_period": True,
        "structural_patterns": ["noun + role"],
        "vocabulary_register": "business",
        "redacted_examples": ["Unique id of the <ENTITY>."],
    })

    def fake_llm(system: str, user: str) -> str:
        # The extractor must NOT include cell data, only column metadata.
        assert "Unique id of the order." in user  # comments are OK
        return stub_response

    profile, n = extract_style(raw, llm_call=fake_llm)
    assert n == 3
    assert profile.language == "en-US"
    assert profile.redacted_examples == ["Unique id of the <ENTITY>."]


def test_extract_retries_on_invalid_json_then_fails():
    raw = {"a": "x", "b": "y", "c": "z"}
    calls = {"n": 0}

    def flaky_llm(system: str, user: str) -> str:
        calls["n"] += 1
        return "not json"

    with pytest.raises(ValueError):
        extract_style(raw, llm_call=flaky_llm)
    assert calls["n"] == 2  # one initial + one retry
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_style_extractor.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement `extractor.py`**

```python
# amx/llm/style/extractor.py
"""Stage 1: turn a reference table's column metadata into a
StyleProfile via one LLM call.

This module is intentionally I/O-free. The caller (slash command or
HTTP router) is responsible for reading column comments from the DB
and for selecting the LLM callable to pass in."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Iterable

from amx.llm.style.profile import StyleProfile

MIN_SAMPLES = 3
DEFAULT_CAP = 30


class NoSamplesError(Exception):
    """Raised when the reference table has too few non-empty comments."""


def _bucket(text: str) -> int:
    n = len(text.split())
    if n <= 6:
        return 0
    if n <= 14:
        return 1
    return 2


def sample_descriptions(
    raw: dict[str, str | None],
    cap: int = DEFAULT_CAP,
    min_samples: int = MIN_SAMPLES,
) -> list[tuple[str, str]]:
    non_empty = [(c, (v or "").strip()) for c, v in raw.items() if v and v.strip()]
    if len(non_empty) < min_samples:
        raise NoSamplesError(
            f"reference table has only {len(non_empty)} non-empty "
            f"descriptions; need at least {min_samples}"
        )
    if len(non_empty) <= cap:
        return non_empty
    # Stratified pick across short/medium/long buckets.
    buckets: list[list[tuple[str, str]]] = [[], [], []]
    for item in non_empty:
        buckets[_bucket(item[1])].append(item)
    quota = cap // 3
    picked: list[tuple[str, str]] = []
    for b in buckets:
        picked.extend(b[:quota])
    # Fill remainder from the largest bucket.
    if len(picked) < cap:
        leftovers: list[tuple[str, str]] = []
        for b in buckets:
            leftovers.extend(b[quota:])
        picked.extend(leftovers[: cap - len(picked)])
    return picked[:cap]


_SYSTEM = (
    "You distill a user's description-writing style from sample column "
    "comments. Reply with a single JSON object exactly matching the "
    "StyleProfile schema:\n"
    '{"language": str, "tone": str, "avg_length_words": int, '
    '"length_range": [int, int], "person": str, "capitalization": str, '
    '"ends_with_period": bool, "structural_patterns": [str], '
    '"vocabulary_register": str, "redacted_examples": [str]}\n'
    "Rules for redacted_examples (3-5 entries):\n"
    "- Replace every domain noun (entity name, metric, date field, "
    "status value, identifier) with one of these placeholders: "
    "<ENTITY>, <METRIC>, <DATE_FIELD>, <STATUS>, <IDENTIFIER>.\n"
    "- Preserve sentence structure, tone, capitalization, and punctuation.\n"
    "- Never invent placeholders outside that set.\n"
    "Return JSON only, no commentary."
)


def _user_prompt(samples: Iterable[tuple[str, str]]) -> str:
    lines = [f"- {name}: {desc}" for name, desc in samples]
    return "Samples:\n" + "\n".join(lines)


def extract_style(
    raw: dict[str, str | None],
    llm_call: Callable[[str, str], str],
    cap: int = DEFAULT_CAP,
) -> tuple[StyleProfile, int]:
    samples = sample_descriptions(raw, cap=cap)
    user = _user_prompt(samples)

    last_err: Exception | None = None
    for _ in range(2):
        raw_resp = llm_call(_SYSTEM, user)
        try:
            return StyleProfile.from_json(_extract_json(raw_resp)), len(samples)
        except (json.JSONDecodeError, ValueError) as e:
            last_err = e
            continue
    raise ValueError(
        f"distillation LLM returned invalid JSON twice: {last_err}"
    )


def _extract_json(text: str) -> str:
    """Permissive JSON locator: strip code fences if present."""
    t = text.strip()
    if t.startswith("```"):
        t = t.split("\n", 1)[1] if "\n" in t else t
        if t.endswith("```"):
            t = t.rsplit("```", 1)[0]
    return t.strip()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_style_extractor.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add amx/llm/style/extractor.py tests/test_style_extractor.py
git commit -m "feat(style): distillation extractor (Stage 1, pure)"
```

---

## Task 6: Wire injector into the four prompt builders

**Files:**
- Modify: `amx/agents/profile_agent.py:80` (`_build_system_prompt` signature)
- Modify: `amx/agents/profile_agent.py:371` (call site)
- Modify: `amx/agents/rag_agent.py:74`, `:173`
- Modify: `amx/agents/code_agent.py:72`, `:182`
- Modify: `amx/web/routers/generate.py:116`, `:196`
- Test: `tests/test_style_injector.py` (add an end-to-end test)

- [ ] **Step 1: Add an end-to-end test that the prompt fragment lands**

Append to `tests/test_style_injector.py`:

```python
def test_profile_agent_appends_style_section(monkeypatch):
    # Smoke test: importing _build_system_prompt with a profile arg
    # produces output containing the "## Writing style" section.
    from amx.agents import profile_agent
    from amx.llm.style.profile import StyleProfile

    sp = StyleProfile(
        language="en-US", tone="x", avg_length_words=1, length_range=(1, 1),
        person="x", capitalization="x", ends_with_period=True,
        structural_patterns=[], vocabulary_register="x",
        redacted_examples=["Unique id of the <ENTITY>."],
    )
    out = profile_agent._build_system_prompt(
        n_alternatives=1,
        description_verbosity="brief",
        style_profile=sp,
    )
    assert "## Writing style" in out
```

(Add the analogous test for `rag_agent`, `code_agent`, and
`generate._build_system_prompt`.)

- [ ] **Step 2: Run tests to verify the new four fail**

Run: `pytest tests/test_style_injector.py -v`
Expected: 4 new tests FAIL with `TypeError: ... unexpected keyword argument 'style_profile'`.

- [ ] **Step 3: Modify `amx/agents/profile_agent.py`**

Change the signature at line 80:

```python
def _build_system_prompt(
    n_alternatives: int,
    description_verbosity: str = "brief",
    style_profile: "StyleProfile | None" = None,
) -> str:
    ...
    # ── existing body unchanged ──
    base = "<existing return value>"
    from amx.llm.style.injector import render_style_section
    return base + render_style_section(style_profile)
```

Update the call site at line 371 to pass `style_profile=self._style_profile`
(field added in Task 8 wiring). For this task, default to `None`:

```python
        system = _build_system_prompt(
            self._n_alternatives,
            description_verbosity=getattr(self.llm.cfg, "description_verbosity", "brief"),
            style_profile=getattr(self, "_style_profile", None),
        )
```

- [ ] **Step 4: Repeat the same two edits in `rag_agent.py`, `code_agent.py`, `generate.py`**

Each file's `_build_system_prompt` gains the optional `style_profile`
parameter and ends with `+ render_style_section(style_profile)`. Each
call site passes `style_profile` (either `None` or from the local
context).

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_style_injector.py tests/test_profile_agent_*.py tests/test_rag_*.py tests/test_code_*.py -v`
Expected: PASS, no regressions in agent unit tests.

- [ ] **Step 6: Commit**

```bash
git add amx/agents/profile_agent.py amx/agents/rag_agent.py \
        amx/agents/code_agent.py amx/web/routers/generate.py \
        tests/test_style_injector.py
git commit -m "feat(style): thread style_profile through agent prompt builders"
```

---

## Task 7: Slash command registry entry

**Files:**
- Modify: `amx/cli_support/slash_commands.py:203-229` (extend `_LLM_COMMANDS`)
- Test: extend `tests/test_cli_integration.py` (or whichever test currently iterates the registry — check first).

- [ ] **Step 1: Find the existing registry test**

Run: `grep -rn "_LLM_COMMANDS\|slash_commands\|SlashCommand\b" tests/ | head`

If a test asserts `/help` is `cross_namespace`, mirror it. Otherwise
create `tests/test_style_registry.py`:

```python
# tests/test_style_registry.py
from amx.cli_support.slash_commands import _LLM_COMMANDS


def test_style_is_in_llm_namespace_and_cross_namespace():
    entry = next((c for c in _LLM_COMMANDS if c.head == "style"), None)
    assert entry is not None
    assert entry.namespace == "llm"
    assert entry.cross_namespace is True
```

- [ ] **Step 2: Run the test to verify failure**

Run: `pytest tests/test_style_registry.py -v`
Expected: FAIL.

- [ ] **Step 3: Add the entry to `_LLM_COMMANDS`**

Append inside the tuple at `amx/cli_support/slash_commands.py:203`:

```python
    SlashCommand(
        "/style",
        "llm",
        "Reference table for description style",
        long_desc=(
            "Attach a reference table to the active LLM profile so AMX "
            "matches your description style on /run. Reads metadata only; "
            "never copies entity names. "
            "Subcommands: /style [wizard] · /style set <db>.<schema>.<table> · "
            "/style show · /style clear · /style on · /style off."
        ),
        cross_namespace=True,
    ),
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/test_style_registry.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add amx/cli_support/slash_commands.py tests/test_style_registry.py
git commit -m "feat(style): register /style under llm namespace, cross-namespace"
```

---

## Task 8: `/style` command handlers + dispatch

**Files:**
- Create: `amx/cli_support/commands/style.py`
- Modify: `amx/cli_support/session.py` (dispatch `head == "style"`)
- Modify: `amx/cli.py` (import + `register_style_commands(...)`)
- Test: `tests/test_style_cli.py`

- [ ] **Step 1: Write the failing CLI test**

```python
# tests/test_style_cli.py
import json
from unittest.mock import patch

from amx.cli_support.commands.style import (
    cmd_style_clear,
    cmd_style_set,
    cmd_style_show,
)
from amx.llm.style.profile import StyleProfile
from amx.storage.style_store import StyleStore


def _profile():
    return StyleProfile(
        language="en-US", tone="formal", avg_length_words=8,
        length_range=(4, 12), person="impersonal",
        capitalization="sentence-case", ends_with_period=True,
        structural_patterns=["noun + role"],
        vocabulary_register="business",
        redacted_examples=["Unique id of the <ENTITY>."],
    )


def test_set_persists_via_store(tmp_path, capsys):
    db = tmp_path / "amx.sqlite"
    from amx.storage.sqlite_store import SQLiteHistoryStore
    SQLiteHistoryStore(str(db)).init()

    # Fake the DB read + LLM call.
    fake_comments = {f"col_{i}": f"Unique id of order {i}." for i in range(5)}
    fake_llm_resp = json.dumps({
        "language": "en-US", "tone": "formal", "avg_length_words": 5,
        "length_range": [3, 7], "person": "impersonal",
        "capitalization": "sentence-case", "ends_with_period": True,
        "structural_patterns": ["noun + role"],
        "vocabulary_register": "business",
        "redacted_examples": ["Unique id of the <ENTITY>."],
    })

    with patch(
        "amx.cli_support.commands.style._read_reference_comments",
        return_value=("snowflake", fake_comments),
    ), patch(
        "amx.cli_support.commands.style._llm_call_for_active_profile",
        return_value=lambda system, user: fake_llm_resp,
    ):
        rc = cmd_style_set(
            args=["warehouse.sales.orders"],
            llm_profile="default",
            db_path=str(db),
        )

    assert rc == 0
    stored = StyleStore(str(db)).get("default")
    assert stored is not None
    assert stored.source_ref == "warehouse.sales.orders"
    assert stored.profile.language == "en-US"


def test_show_when_missing(tmp_path, capsys):
    db = tmp_path / "amx.sqlite"
    from amx.storage.sqlite_store import SQLiteHistoryStore
    SQLiteHistoryStore(str(db)).init()
    rc = cmd_style_show(llm_profile="default", db_path=str(db))
    assert rc == 0
    out = capsys.readouterr().out
    assert "no style reference" in out.lower()


def test_clear_removes_entry(tmp_path):
    db = tmp_path / "amx.sqlite"
    from amx.storage.sqlite_store import SQLiteHistoryStore
    SQLiteHistoryStore(str(db)).init()
    StyleStore(str(db)).upsert(
        "default", "a.b.c", "duckdb", _profile(), 3,
    )
    assert cmd_style_clear(llm_profile="default", db_path=str(db)) == 0
    assert StyleStore(str(db)).get("default") is None
```

- [ ] **Step 2: Run the test to verify failure**

Run: `pytest tests/test_style_cli.py -v`
Expected: FAIL (module missing).

- [ ] **Step 3: Implement `amx/cli_support/commands/style.py`**

```python
"""/style slash-command handlers.

Public functions (used by the dispatch table in session.py):
- cmd_style_dispatch — parses subcommands, routes to cmd_style_*.
- cmd_style_set
- cmd_style_show
- cmd_style_clear
- cmd_style_on / cmd_style_off
- register_style_commands — no-op placeholder for symmetry with
  other register_* helpers in cli.py; kept so cli.py imports work.

Reading reference comments and invoking the active LLM are isolated
to two private helpers so they can be patched in tests."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from amx.config import AppConfig
from amx.llm.style.extractor import (
    DEFAULT_CAP,
    NoSamplesError,
    extract_style,
)
from amx.storage.style_store import StyleStore
from amx.utils.logging import get_logger

log = get_logger("cli.style")


def register_style_commands(*_: Any, **__: Any) -> None:
    """Kept for parity with other register_* helpers; dispatch is in
    session.py."""
    return None


def _read_reference_comments(
    cfg: AppConfig,
    db_profile: str,
    db: str,
    schema: str,
    table: str,
) -> tuple[str, dict[str, str | None]]:
    """Return (source_db_kind, column→comment) for the reference table.

    Reads metadata only; never issues SELECT against the table data."""
    from amx.db.connector import get_connector_for_profile

    conn = get_connector_for_profile(cfg, db_profile)
    # The connector normalises identifiers internally.
    conn.use(db)
    comments = conn.get_column_comments(schema, table)
    return conn.kind, dict(comments)


def _llm_call_for_active_profile(
    cfg: AppConfig, llm_profile: str
) -> Callable[[str, str], str]:
    """Return a (system, user) → str callable bound to the LLM profile.

    Uses the existing `amx.llm.provider` machinery so distillation
    inherits proxy / retry / pricing settings."""
    from amx.llm.provider import call_llm_text

    def _call(system: str, user: str) -> str:
        return call_llm_text(
            cfg=cfg,
            profile_name=llm_profile,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            response_format="text",
        )

    return _call


def _resolve_db_profile(cfg: AppConfig, llm_profile: str) -> str | None:
    """Resolve which DB profile to use for /style set.

    Returns None if the user must pick interactively."""
    pinned = getattr(cfg.llm_profiles.get(llm_profile), "pinned_db_profile", "")
    if pinned:
        return pinned
    if len(cfg.db_profiles) == 1:
        return next(iter(cfg.db_profiles))
    return None


def cmd_style_set(
    args: list[str],
    llm_profile: str,
    db_path: str,
    cfg: AppConfig | None = None,
) -> int:
    if not args:
        print(
            "Usage: /style set <db>.<schema>.<table>  "
            "(or run /style for the wizard)"
        )
        return 2

    parts = args[0].split(".")
    if len(parts) != 3:
        print("Reference must be <db>.<schema>.<table>")
        return 2
    db, schema, table = parts

    cfg = cfg or AppConfig.load()
    db_profile = _resolve_db_profile(cfg, llm_profile)
    if db_profile is None:
        print(
            "No DB profile pinned on this LLM profile and more than one "
            "exists. Pin one with /pin-db-profile, or use the /style wizard."
        )
        return 2

    try:
        kind, comments = _read_reference_comments(
            cfg, db_profile, db, schema, table,
        )
    except Exception as e:
        print(f"Could not read reference table: {e}")
        return 1

    try:
        profile, n = extract_style(
            comments,
            llm_call=_llm_call_for_active_profile(cfg, llm_profile),
            cap=DEFAULT_CAP,
        )
    except NoSamplesError as e:
        print(str(e))
        return 1
    except ValueError as e:
        print(f"Distillation failed: {e}")
        return 1

    StyleStore(db_path).upsert(
        llm_profile=llm_profile,
        source_ref=f"{db}.{schema}.{table}",
        source_db_kind=kind,
        profile=profile,
        sample_count=n,
    )
    print(
        f"Saved style profile for LLM '{llm_profile}' "
        f"(language={profile.language}, samples={n}, "
        f"examples={len(profile.redacted_examples)})."
    )
    return 0


def cmd_style_show(llm_profile: str, db_path: str) -> int:
    row = StyleStore(db_path).get(llm_profile)
    if row is None:
        print(
            f"No style reference attached to LLM profile '{llm_profile}'. "
            "Use /style set <db>.<schema>.<table>."
        )
        return 0
    payload = {
        "llm_profile": row.llm_profile,
        "source_ref": row.source_ref,
        "source_db_kind": row.source_db_kind,
        "enabled": row.enabled,
        "sample_count": row.sample_count,
        "profile": json.loads(row.profile.to_json()),
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


def cmd_style_clear(llm_profile: str, db_path: str) -> int:
    StyleStore(db_path).clear(llm_profile)
    print(f"Cleared style reference for LLM profile '{llm_profile}'.")
    return 0


def cmd_style_on(llm_profile: str, db_path: str) -> int:
    StyleStore(db_path).set_enabled(llm_profile, True)
    print(f"Style injection enabled for LLM profile '{llm_profile}'.")
    return 0


def cmd_style_off(llm_profile: str, db_path: str) -> int:
    StyleStore(db_path).set_enabled(llm_profile, False)
    print(f"Style injection disabled for LLM profile '{llm_profile}'.")
    return 0


def cmd_style_dispatch(
    args: list[str],
    llm_profile: str,
    db_path: str,
    cfg: AppConfig | None = None,
) -> int:
    if not args:
        return cmd_style_wizard(llm_profile=llm_profile, db_path=db_path, cfg=cfg)
    sub = args[0]
    rest = args[1:]
    if sub == "set":
        return cmd_style_set(rest, llm_profile, db_path, cfg)
    if sub == "show":
        return cmd_style_show(llm_profile, db_path)
    if sub == "clear":
        return cmd_style_clear(llm_profile, db_path)
    if sub == "on":
        return cmd_style_on(llm_profile, db_path)
    if sub == "off":
        return cmd_style_off(llm_profile, db_path)
    print(
        "Unknown /style subcommand. Try: set <ref>, show, clear, on, off, "
        "or /style (no args) for the wizard."
    )
    return 2


def cmd_style_wizard(
    llm_profile: str,
    db_path: str,
    cfg: AppConfig | None = None,
) -> int:
    """Interactive picker. Reuses the existing catalog_picker helpers."""
    from amx.cli_support.catalog_picker import pick_table_interactive

    cfg = cfg or AppConfig.load()
    db_profile = _resolve_db_profile(cfg, llm_profile)
    if db_profile is None:
        from amx.cli_support.commands.profiles import pick_db_profile_interactive
        db_profile = pick_db_profile_interactive(cfg)
        if not db_profile:
            return 1

    ref = pick_table_interactive(cfg, db_profile)
    if not ref:
        return 1

    return cmd_style_set(
        args=[ref], llm_profile=llm_profile, db_path=db_path, cfg=cfg,
    )
```

- [ ] **Step 4: Wire dispatch in `amx/cli_support/session.py`**

Find the dispatch ladder that maps `head` strings to namespaces and
handler calls (search for `if head ==` blocks near other LLM
commands). Add:

```python
            elif head == "style":
                from amx.cli_support.commands.style import cmd_style_dispatch
                cmd_style_dispatch(
                    args=parts[1:],
                    llm_profile=cfg.active_llm_profile,
                    db_path=cfg.db_path_for_history(),
                    cfg=cfg,
                )
                continue
```

(The exact variable names — `parts`, `cfg`, `head` — follow what
session.py already uses; mirror the nearest sibling, e.g.
`/description-verbosity`.)

- [ ] **Step 5: Import register helper from cli.py**

In `amx/cli.py`, near the other `register_*` imports (around line
14-40):

```python
from amx.cli_support.commands.style import register_style_commands
```

And in the same block where other `register_*(cli)` calls live, add:

```python
register_style_commands(cli)
```

(`register_style_commands` is a no-op kept for symmetry; the real
dispatch lives in `session.py`.)

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/test_style_cli.py tests/test_style_registry.py -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add amx/cli_support/commands/style.py amx/cli_support/session.py \
        amx/cli.py tests/test_style_cli.py
git commit -m "feat(style): /style slash command handlers and dispatch"
```

---

## Task 9: Load + inject the StyleProfile on /run

**Files:**
- Modify: `amx/agents/profile_agent.py` (load + store style on `__init__`)
- Modify: `amx/agents/rag_agent.py` (same)
- Modify: `amx/agents/code_agent.py` (same)
- Test: `tests/test_style_run_integration.py`

- [ ] **Step 1: Write the integration test**

```python
# tests/test_style_run_integration.py
from unittest.mock import patch

from amx.agents.profile_agent import ProfileAgent
from amx.llm.style.profile import StyleProfile
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.storage.style_store import StyleStore


def test_profile_agent_loads_active_style(tmp_path):
    db = tmp_path / "amx.sqlite"
    SQLiteHistoryStore(str(db)).init()
    StyleStore(str(db)).upsert(
        "default",
        "a.b.c",
        "duckdb",
        StyleProfile(
            language="en-US", tone="x", avg_length_words=1,
            length_range=(1, 1), person="x", capitalization="x",
            ends_with_period=True, structural_patterns=[],
            vocabulary_register="x",
            redacted_examples=["Unique id of the <ENTITY>."],
        ),
        sample_count=3,
    )

    with patch(
        "amx.agents.profile_agent._active_db_path", return_value=str(db)
    ), patch(
        "amx.agents.profile_agent._active_llm_profile_name",
        return_value="default",
    ):
        agent = ProfileAgent.__new__(ProfileAgent)  # bypass full init
        agent._load_style_profile()
        assert agent._style_profile is not None
        assert agent._style_profile.language == "en-US"


def test_profile_agent_skips_disabled_style(tmp_path):
    db = tmp_path / "amx.sqlite"
    SQLiteHistoryStore(str(db)).init()
    store = StyleStore(str(db))
    store.upsert(
        "default", "a.b.c", "duckdb",
        StyleProfile(
            language="en-US", tone="x", avg_length_words=1,
            length_range=(1, 1), person="x", capitalization="x",
            ends_with_period=True, structural_patterns=[],
            vocabulary_register="x", redacted_examples=[],
        ),
        sample_count=3,
    )
    store.set_enabled("default", False)

    with patch(
        "amx.agents.profile_agent._active_db_path", return_value=str(db)
    ), patch(
        "amx.agents.profile_agent._active_llm_profile_name",
        return_value="default",
    ):
        agent = ProfileAgent.__new__(ProfileAgent)
        agent._load_style_profile()
        assert agent._style_profile is None
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_style_run_integration.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement `_load_style_profile` on each agent**

In each of `profile_agent.py`, `rag_agent.py`, `code_agent.py`, add at the top of the file:

```python
def _active_db_path() -> str:
    from amx.config import AppConfig
    return AppConfig.load().db_path_for_history()


def _active_llm_profile_name() -> str:
    from amx.config import AppConfig
    return AppConfig.load().active_llm_profile
```

And inside the agent class:

```python
    def _load_style_profile(self) -> None:
        from amx.storage.style_store import StyleStore

        try:
            row = StyleStore(_active_db_path()).get(_active_llm_profile_name())
        except Exception:
            self._style_profile = None
            return
        self._style_profile = row.profile if (row and row.enabled) else None
```

Call `self._load_style_profile()` in `__init__` after the existing
LLM config wiring, and ensure `_style_profile` defaults to `None`.

- [ ] **Step 4: Run tests to verify pass**

Run: `pytest tests/test_style_run_integration.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add amx/agents/profile_agent.py amx/agents/rag_agent.py \
        amx/agents/code_agent.py tests/test_style_run_integration.py
git commit -m "feat(style): agents load and inject the active style profile"
```

---

## Task 10: Wire post-generation guard into the agent output pipeline

**Files:**
- Modify: `amx/llm/provider.py` (after `_description_value_spans` machinery; pass profile in)
- Modify: each agent's post-LLM response handling — call `contains_leakage` once per generated description; if leaked, retry once with a stronger reminder, else fall back to a no-style call.
- Test: `tests/test_style_leakage_redteam.py`

- [ ] **Step 1: Write the red-team test**

```python
# tests/test_style_leakage_redteam.py
from unittest.mock import patch

from amx.agents.profile_agent import ProfileAgent
from amx.llm.style.profile import StyleProfile

SENTINEL = "Q3-FOO-INVOICE-XYZ"


def _profile_with_sentinel():
    return StyleProfile(
        language="en-US", tone="formal", avg_length_words=8,
        length_range=(4, 12), person="impersonal",
        capitalization="sentence-case", ends_with_period=True,
        structural_patterns=["noun + role"],
        vocabulary_register="business",
        # Sentinel embedded as if it had survived redaction (red-team).
        redacted_examples=[f"Unique id of the {SENTINEL} record."],
    )


def test_guard_blocks_sentinel_passthrough():
    from amx.llm.style.guard import contains_leakage
    # Direct unit-level proof: same sentinel + placeholder regex absent
    # means the model output is clean even if it accidentally echoes
    # the sentinel.
    p = _profile_with_sentinel()
    # NOTE: the guard does NOT scan for arbitrary sentinels; that's
    # the prompt's job. The guard's promise is: no placeholder, no
    # exact example match. This test documents that contract.
    assert contains_leakage(f"Customer {SENTINEL} order.", p) is False
    assert contains_leakage(p.redacted_examples[0], p) is True
```

This test pins the guard's contract. A stronger sentinel-survival
test happens at the integration layer with a mocked LLM response.

- [ ] **Step 2: Run test, verify pass (guard already implemented)**

Run: `pytest tests/test_style_leakage_redteam.py -v`
Expected: PASS.

- [ ] **Step 3: Add guard invocation in `provider.py`**

In `amx/llm/provider.py`, find where the generated description text
is parsed and surfaced to the agent. Add an optional parameter to
the public entry point (the function the agents call) that takes the
active `StyleProfile`; after parsing the description, call:

```python
from amx.llm.style.guard import contains_leakage

if style_profile is not None and contains_leakage(description, style_profile):
    # One retry with an explicit reminder appended to the system prompt.
    description = _retry_without_style(messages, model)
```

`_retry_without_style` re-runs the original call with `style_profile=None`
to guarantee a clean second attempt. If the second attempt still
leaks, fall back to its output (we never silently produce a literal
placeholder because the guard would have stripped placeholder tags
before write — see post-processing below).

- [ ] **Step 4: Strip stray placeholder tags as final safety net**

In the description finalisation path, after the retry logic above:

```python
import re
description = re.sub(r"<(?:ENTITY|METRIC|DATE_FIELD|STATUS|IDENTIFIER)>", "", description).strip()
```

- [ ] **Step 5: Run the full style test suite**

Run: `pytest tests/test_style_*.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add amx/llm/provider.py tests/test_style_leakage_redteam.py
git commit -m "feat(style): post-generation leakage guard + placeholder scrub"
```

---

## Task 11: Studio REST endpoints

**Files:**
- Create: `amx/web/routers/style.py`
- Modify: `amx/web/routers/__init__.py` (mount the router)
- Test: `tests/test_style_router.py`

- [ ] **Step 1: Write the failing router test**

```python
# tests/test_style_router.py
from fastapi.testclient import TestClient

from amx.web import create_app


def test_get_style_returns_404_when_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("AMX_DB_PATH", str(tmp_path / "amx.sqlite"))
    app = create_app()
    client = TestClient(app)
    r = client.get("/api/llm-profiles/default/style")
    assert r.status_code == 404


def test_patch_enabled_flag(tmp_path, monkeypatch):
    from amx.llm.style.profile import StyleProfile
    from amx.storage.sqlite_store import SQLiteHistoryStore
    from amx.storage.style_store import StyleStore

    db = tmp_path / "amx.sqlite"
    monkeypatch.setenv("AMX_DB_PATH", str(db))
    SQLiteHistoryStore(str(db)).init()
    StyleStore(str(db)).upsert(
        "default", "a.b.c", "duckdb",
        StyleProfile(
            language="en-US", tone="x", avg_length_words=1,
            length_range=(1, 1), person="x", capitalization="x",
            ends_with_period=True, structural_patterns=[],
            vocabulary_register="x", redacted_examples=[],
        ),
        sample_count=3,
    )

    app = create_app()
    client = TestClient(app)
    r = client.patch(
        "/api/llm-profiles/default/style",
        json={"enabled": False},
    )
    assert r.status_code == 200
    assert StyleStore(str(db)).get("default").enabled is False
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_style_router.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement `amx/web/routers/style.py`**

```python
"""Studio endpoints for the style reference feature."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from amx.config import AppConfig
from amx.cli_support.commands.style import (
    _llm_call_for_active_profile,
    _read_reference_comments,
)
from amx.llm.style.extractor import NoSamplesError, extract_style
from amx.storage.style_store import StyleStore

router = APIRouter(prefix="/api/llm-profiles", tags=["style"])


class ExtractRequest(BaseModel):
    source_ref: str  # "db.schema.table"
    db_profile: str | None = None


class PatchStyleRequest(BaseModel):
    enabled: bool


@router.post("/{name}/style:extract")
def extract(name: str, body: ExtractRequest):
    cfg = AppConfig.load()
    parts = body.source_ref.split(".")
    if len(parts) != 3:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "source_ref must be db.schema.table")
    db, schema, table = parts
    db_profile = body.db_profile or next(iter(cfg.db_profiles), None)
    if not db_profile:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "no db_profile available")
    try:
        kind, comments = _read_reference_comments(cfg, db_profile, db, schema, table)
        profile, n = extract_style(
            comments,
            llm_call=_llm_call_for_active_profile(cfg, name),
        )
    except NoSamplesError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(e))
    except Exception as e:  # narrow in follow-up if needed
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e))

    StyleStore(cfg.db_path_for_history()).upsert(
        llm_profile=name, source_ref=body.source_ref,
        source_db_kind=kind, profile=profile, sample_count=n,
    )
    return {"ok": True, "sample_count": n}


@router.get("/{name}/style")
def get_style(name: str):
    row = StyleStore(AppConfig.load().db_path_for_history()).get(name)
    if not row:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    return {
        "llm_profile": row.llm_profile,
        "source_ref": row.source_ref,
        "source_db_kind": row.source_db_kind,
        "enabled": row.enabled,
        "sample_count": row.sample_count,
        "profile": row.profile.to_json(),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


@router.patch("/{name}/style")
def patch_style(name: str, body: PatchStyleRequest):
    store = StyleStore(AppConfig.load().db_path_for_history())
    if store.get(name) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    store.set_enabled(name, body.enabled)
    return {"ok": True}


@router.delete("/{name}/style")
def delete_style(name: str):
    StyleStore(AppConfig.load().db_path_for_history()).clear(name)
    return {"ok": True}
```

- [ ] **Step 4: Mount the router in `amx/web/routers/__init__.py`**

```python
from amx.web.routers import style as style_router
# inside the registration function:
app.include_router(style_router.router)
```

- [ ] **Step 5: Run tests to verify pass**

Run: `pytest tests/test_style_router.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add amx/web/routers/style.py amx/web/routers/__init__.py \
        tests/test_style_router.py
git commit -m "feat(style): Studio REST endpoints"
```

---

## Task 12: Studio UI card

**Files:**
- Create: `frontend/src/components/StyleReferenceCard.tsx`
- Modify: `frontend/src/routes/Settings.tsx` (embed the card under the LLM profile detail view)
- Test: Manual smoke test in browser; add a Cypress / Vitest test only if the repo already has UI tests (check `frontend/`).

- [ ] **Step 1: Confirm whether frontend tests exist**

Run: `ls frontend/src/__tests__ frontend/tests 2>/dev/null || echo "no UI test infra"`

If none exist, skip authoring UI tests; the manual smoke test in
Step 5 is the verification step.

- [ ] **Step 2: Implement `StyleReferenceCard.tsx`**

```tsx
// frontend/src/components/StyleReferenceCard.tsx
import { useEffect, useState } from "react";

type Stored = {
  source_ref: string;
  enabled: boolean;
  sample_count: number;
  profile: string; // JSON string
};

export function StyleReferenceCard({ llmProfile }: { llmProfile: string }) {
  const [state, setState] = useState<"idle" | "loading" | "extracting">("idle");
  const [data, setData] = useState<Stored | null>(null);
  const [ref, setRef] = useState("");

  async function load() {
    setState("loading");
    const r = await fetch(`/api/llm-profiles/${llmProfile}/style`);
    setData(r.ok ? await r.json() : null);
    setState("idle");
  }

  useEffect(() => { load(); }, [llmProfile]);

  async function extract() {
    setState("extracting");
    await fetch(`/api/llm-profiles/${llmProfile}/style:extract`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source_ref: ref }),
    });
    await load();
  }

  async function toggle(enabled: boolean) {
    await fetch(`/api/llm-profiles/${llmProfile}/style`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled }),
    });
    await load();
  }

  async function clear() {
    await fetch(`/api/llm-profiles/${llmProfile}/style`, { method: "DELETE" });
    await load();
  }

  if (state === "loading") return <div>Loading style reference…</div>;
  if (state === "extracting") return <div>Extracting style…</div>;

  if (!data) {
    return (
      <div className="card">
        <h3>Writing style reference</h3>
        <p>
          Attach a reference table (any database, any schema, any table) so AMX
          matches your description style on runs. AMX reads only column comments;
          row data is never sent to the LLM.
        </p>
        <input
          placeholder="db.schema.table"
          value={ref}
          onChange={(e) => setRef(e.target.value)}
        />
        <button onClick={extract} disabled={!ref}>Extract style</button>
      </div>
    );
  }

  const profile = JSON.parse(data.profile);
  return (
    <div className="card">
      <h3>Writing style reference</h3>
      <div>Source: <code>{data.source_ref}</code></div>
      <div>Language: {profile.language}</div>
      <div>Tone: {profile.tone}</div>
      <div>Length: {profile.length_range[0]}–{profile.length_range[1]} words</div>
      <div>Samples used: {data.sample_count}</div>
      <details>
        <summary>Raw profile JSON</summary>
        <pre>{JSON.stringify(profile, null, 2)}</pre>
      </details>
      <label>
        <input
          type="checkbox"
          checked={data.enabled}
          onChange={(e) => toggle(e.target.checked)}
        />{" "}
        Use this style on runs
      </label>
      <button onClick={extract} disabled={!ref}>Re-extract</button>
      <button onClick={clear}>Clear</button>
    </div>
  );
}
```

- [ ] **Step 3: Embed in `Settings.tsx`**

Inside the LLM profile detail block of `frontend/src/routes/Settings.tsx`,
import and render:

```tsx
import { StyleReferenceCard } from "../components/StyleReferenceCard";
// ...
<StyleReferenceCard llmProfile={selectedLlmProfile.name} />
```

- [ ] **Step 4: Run the frontend build**

```bash
cd frontend && npm run build && cd ..
```

Expected: build succeeds, no TypeScript errors.

- [ ] **Step 5: Manual smoke test**

```bash
make dev   # or whichever command starts AMX + Studio locally
```

In a browser:
1. Navigate to LLM Settings, pick a profile.
2. Confirm the Writing style reference card renders in *unset* state.
3. Type a `db.schema.table` from a small fixture; click *Extract style*.
4. Confirm the card flips to *configured* state with the right fields.
5. Toggle the *Use on runs* checkbox off → server reflects `enabled=false`.
6. Click *Clear* → card returns to *unset* state.

Record the result in the commit body (UI was visually verified for the cases above).

- [ ] **Step 6: Commit**

```bash
git add frontend/src/components/StyleReferenceCard.tsx \
        frontend/src/routes/Settings.tsx
git commit -m "feat(style): Studio settings card for the writing-style reference"
```

---

## Task 13: Full-suite regression + CI verification

- [ ] **Step 1: Run the entire backend test suite**

```bash
pytest -x -q
```

Expected: all green. If anything in the agents or routers regressed,
inspect the trace and fix in a focused commit.

- [ ] **Step 2: Run the frontend type check + build**

```bash
cd frontend && npm run lint && npm run build && cd ..
```

Expected: clean.

- [ ] **Step 3: Search-and-replace audit for the global house rules**

```bash
grep -rniE "p[a]id" amx frontend/src docs/superpowers/specs/2026-05-12-style-reference-table-design.md docs/superpowers/plans/2026-05-12-style-reference-table.md
grep -rn "Claude\|Co-Authored-By" amx frontend/src
```

Expected: zero hits in the files added or modified by this plan.
Pre-existing matches in unrelated files are out of scope.

- [ ] **Step 4: Push the branch and open the PR**

```bash
git push -u origin <branch-name>
gh pr create --title "feat(style): writing-style reference for descriptions" \
  --body "$(cat <<'EOF'
## Summary
- New /style slash command (LLM tab, cross-namespace) and a Studio settings card let users attach a reference table whose column comments distil into a StyleProfile.
- Stage 1 reads metadata only (no row data), samples up to 30 non-empty comments stratified by length, asks the active LLM to emit a StyleProfile JSON with entity-masked few-shot examples.
- Stage 2 appends a "## Writing style" section to every agent's system prompt; a post-generation guard rejects placeholder literals and exact example matches.
- Opt-in: no behaviour change for users who don't run /style set.

## Test plan
- [ ] pytest tests/test_style_*.py
- [ ] pytest -x -q
- [ ] frontend lint + build
- [ ] Manual Studio walkthrough: extract → toggle → clear (recorded in commit body of Task 12)
EOF
)"
```

- [ ] **Step 5: Wait for CI to go green**

Run: `gh pr checks --watch`
Expected: all checks pass. If anything fails, fix on the same branch
(new commit, no force-push) and re-watch.

---

## Self-review (already performed)

- **Spec coverage:** Sections 2-9 of the spec each map to a task above.
  Out-of-scope items (manual editing UI, non-DB sources, per-table overrides)
  are deliberately absent.
- **Placeholders:** none — every step shows the code or command to run.
- **Type consistency:** `StyleProfile`, `StoredStyleProfile`, `_style_profile`,
  `style_profile`, `_load_style_profile` reused consistently across tasks.
- **Threading model:** `extractor.extract_style` is pure; I/O sits in
  `_read_reference_comments` and `_llm_call_for_active_profile`, both
  patchable in tests.

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-12-style-reference-table.md`.**
