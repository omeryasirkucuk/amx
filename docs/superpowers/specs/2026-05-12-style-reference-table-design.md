# Style Reference Table — Design Spec

**Status:** Draft
**Date:** 2026-05-12
**Scope:** AMX CLI + AMX Studio

## 1. Motivation

Today the only way a user can influence the LLM's writing style for
generated comments / descriptions is by typing free-form input during
a manual re-run. Users have asked for a way to say, in effect,
*"this is what my team's descriptions look like — match this style"*
without having to repeat themselves on every run.

The feature must respect two hard constraints:

1. **No content leakage.** A reference table from a finance domain
   must never bleed entity names, metrics, or business terms into
   descriptions generated for an unrelated (e.g. sales) table.
2. **Scale-safe.** Users may point at tables with billions of rows
   and thousands of columns. The feature only needs the user's
   *style* — tone, language, length, structure — never their data.

The feature is fully **opt-in**. Users who do not configure a
reference table see no behavior change.

## 2. Approach: Two-Stage Style Distillation

A reference table is processed once into a structured `StyleProfile`.
At run time, only that profile (not the original reference) is
injected into the LLM prompt.

```
                   ┌─────────────────────┐
 Reference table ─►│  Stage 1: Extract   │── StyleProfile (JSON) ──┐
 (metadata only)   │  (one LLM call)     │                         │
                   └─────────────────────┘                         │
                                                                   ▼
                                                ┌─────────────────────────┐
 Target table ────────────────────────────────► │ Stage 2: Generate       │
                                                │ (existing run pipeline) │
                                                │ system prompt += style  │
                                                └─────────────────────────┘
```

### 2.1 Stage 1 — Extraction (one-time per profile)

Runs when the user picks a reference table.

**Inputs read from the source DB:**

- `column_name`, `data_type`, `comment / description` for the chosen
  table (and optionally the table-level description).
- **No row data.** Implementation uses `information_schema` (or the
  driver-specific equivalent) and never issues `SELECT … FROM <table>`.

**Sampling:**

- Filter to columns with non-empty descriptions.
- Cap at `N = 30` samples per extraction. If more exist, stratify
  by description length (short / medium / long buckets) so the
  sample covers the user's full register.
- If fewer than `MIN_SAMPLES = 3` non-empty descriptions exist,
  extraction fails with a clear message; the user is told the
  reference table has too few documented columns.

**Distillation LLM call:**

The sampled `(column_name, description)` pairs are sent to the same
LLM provider configured on the active LLM profile, with a system
prompt that demands a strict JSON response shape:

```json
{
  "language": "tr-TR",
  "tone": "formal, third-person",
  "avg_length_words": 14,
  "length_range": [8, 22],
  "person": "impersonal",
  "capitalization": "sentence-case",
  "ends_with_period": true,
  "structural_patterns": [
    "Definition + purpose",
    "Noun phrase + role clause"
  ],
  "vocabulary_register": "business-technical",
  "redacted_examples": [
    "Unique identifier of the <ENTITY>.",
    "Creation timestamp of the <ENTITY> record.",
    "Sum of <METRIC> on <DATE_FIELD>."
  ]
}
```

Critical detail: the extraction prompt instructs the LLM to **replace
all domain-specific nouns** in the examples with one of a fixed set
of placeholders (`<ENTITY>`, `<METRIC>`, `<DATE_FIELD>`, `<STATUS>`,
`<IDENTIFIER>`). Only redacted examples are stored.

### 2.2 Stage 2 — Run-time injection

Each agent that produces descriptions today
(`amx/agents/profile_agent.py`, `rag_agent.py`, `code_agent.py`, and
`amx/web/routers/generate.py`) already builds a system prompt via a
local `_build_system_prompt(...)` helper. Each of these helpers gains
an optional `style_profile: StyleProfile | None` parameter.

When a profile is present, an additional `## Writing style` section
is appended to the system prompt with:

- The structured fields (language, tone, length range, structural
  patterns, capitalization rules).
- The redacted examples, prefixed with explicit guard text:

  > "Match this style profile for tone, language, length, and
  > sentence structure. The placeholders `<ENTITY>`, `<METRIC>`,
  > `<DATE_FIELD>`, `<STATUS>`, `<IDENTIFIER>` mark domain-specific
  > terms from another schema. **Never copy these placeholders into
  > your output, and never invent entity names from them.** Always
  > derive domain terms from the target column you are describing."

Absent a profile, the prompt is unchanged.

### 2.3 Defense in depth against leakage

1. **Source isolation:** extraction reads metadata only; row data
   never enters the pipeline.
2. **Structured distillation:** the stored profile is JSON; the
   only free-text fragments are entity-masked examples.
3. **Prompt guard:** explicit "do not copy placeholders / domain
   terms" instruction in the system prompt.
4. **Post-generation check:** a cheap regex sweep rejects any
   generated description that contains a placeholder literal
   (`<ENTITY>`, etc.) or that exactly matches one of the redacted
   examples; rejected outputs trigger a single retry, then fall
   back to a no-style generation.

## 3. Storage

New table in the existing AMX storage layer
(`amx/storage/sqlalchemy_store.py`, `sqlite_store.py`, plus shared
schema in `shared_schema.py`):

```sql
CREATE TABLE style_profiles (
    id              INTEGER PRIMARY KEY,
    llm_profile_id  INTEGER NOT NULL REFERENCES llm_profiles(id)
                                ON DELETE CASCADE,
    source_ref      TEXT NOT NULL,      -- "db.schema.table"
    source_db_kind  TEXT NOT NULL,      -- "snowflake", "databricks", ...
    profile_json    TEXT NOT NULL,      -- StyleProfile serialized
    sample_count    INTEGER NOT NULL,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE (llm_profile_id)
);
```

One style profile per LLM profile. Re-extracting overwrites the
existing row (preserving `created_at`, bumping `updated_at`).

A migration is added under the existing migration mechanism in
`amx/storage/migration.py`.

## 4. CLI surface

AMX is an interactive session: the user runs `amx` to enter, then
types slash commands. There is **no** `amx style …` top-level
subcommand; only slash commands inside the session.

A single new slash command `/style` is registered with:

- `namespace = "llm"` — visually grouped under the **LLM** tab next
  to the existing `/llm-*` entries.
- `cross_namespace = True` — invocable from any tab, mirroring
  `/help`, `/doctor`, `/compare`.

Forms accepted inside the session:

```
/style                                 # interactive wizard: picks LLM profile (active by default),
                                       # opens catalog picker for the reference table, runs Stage 1.
/style set <db>.<schema>.<table>       # explicit, uses active LLM profile and the DB profile
                                       # pinned on it (else the only DB profile, else error).
/style show                            # pretty-prints the stored profile for the active LLM.
/style clear                           # deletes the stored profile for the active LLM.
/style off                             # keeps the profile but disables injection on runs.
/style on                              # re-enables injection.
```

Resolution rules for the DB profile on `set` / wizard mode (no
explicit flag because slash commands stay flag-light to match the
existing UX):

1. The DB profile pinned on the active LLM profile, if any.
2. The single DB profile in the workspace, if exactly one exists.
3. Otherwise, the wizard prompts the user to pick one; `set` form
   fails with a message telling the user to pin a DB profile first.

`/run` and `/rerun` automatically pick up the style profile attached
to the active LLM profile when injection is enabled (default after
`/style set`). Users disable per session via `/style off`; no
per-run flag is added in v1 to keep the command surface small.

Registration lives in a new
`amx/cli_support/commands/style.py`, imported from `amx/cli.py`
alongside the other `register_*` helpers, and added to the slash
command table in `amx/cli_support/slash_commands.py`.

## 5. Studio surface

Under **LLM Settings → \<profile\>**, a new collapsible card
*"Writing style reference"*:

- **State A — not configured:** explainer text + a primary
  *"Pick a reference table"* button that opens the existing catalog
  picker scoped by the user's DB profiles.
- **State B — extracting:** progress bar wired to the existing
  progress bus (`amx/web/progress_bus.py`).
- **State C — configured:** read-only summary card showing
  language, tone, avg length, and chips for each redacted example.
  Buttons: *"Re-extract"*, *"Clear"*. A *"View raw JSON"* disclosure
  reveals the full profile for power users.

A small *"Use this style on runs"* toggle (default **on** when
configured) lets the user temporarily disable injection without
deleting the profile.

New endpoints in `amx/web/routers/style.py`:

- `POST /api/llm-profiles/{id}/style:extract` (body: source_ref)
- `GET  /api/llm-profiles/{id}/style`
- `PATCH /api/llm-profiles/{id}/style` (enabled flag)
- `DELETE /api/llm-profiles/{id}/style`

## 6. Module layout

```
amx/
├── cli_support/
│   └── commands/
│       └── style.py                # /style slash command handlers
├── llm/
│   └── style/
│       ├── __init__.py
│       ├── extractor.py            # Stage 1 (metadata read + distill call)
│       ├── profile.py              # StyleProfile dataclass + JSON schema
│       ├── injector.py             # Stage 2: prompt fragment builder
│       └── guard.py                # post-generation leakage check
├── storage/
│   └── (existing files extended)
└── web/
    └── routers/
        └── style.py                # new
frontend/
└── (Studio components for the new card)
```

Each agent's `_build_system_prompt` gains the optional parameter and
calls `injector.render_style_section(profile)` when a profile is
present. No agent owns its own copy of the style logic.

## 7. Error handling and edge cases

- **DB driver lacks an `information_schema` analog** (rare; some
  custom adapters): `set` returns a clear error naming the unsupported
  driver, and recommends documenting columns manually first.
- **Reference table has < 3 non-empty descriptions:** `set` fails
  with a message naming the threshold; no profile is stored.
- **Distillation LLM returns invalid JSON:** one retry, then fail
  with the raw response surfaced so the user can switch models if
  needed.
- **Reference table description language differs from target column
  context:** the profile's `language` field dominates; this is the
  intended behavior (the user is explicitly asking for *that*
  language).

## 8. Testing strategy

- **Unit tests** for `extractor.py` (sampling logic, JSON parsing,
  placeholder enforcement on examples), `profile.py` (round-trip
  serialization), `injector.py` (system-prompt fragment shape with
  and without profile), `guard.py` (placeholder detection).
- **Integration test** that runs the full pipeline against an
  in-memory SQLite fixture with seeded comments, mocks the LLM
  provider with a deterministic response, and asserts the generated
  description contains no placeholder literal and no exact substring
  from the reference comments.
- **Leakage red-team test:** seed reference comments with a unique
  sentinel string ("Q3-FOO-INVOICE-XYZ"); assert it never appears
  in any generated output across 50 mocked runs.
- **Migration test:** fresh DB + upgrade from previous schema both
  reach the same state.

## 9. Out of scope

- Editing the StyleProfile by hand in the UI. Users re-extract
  instead. Manual editing is left for a future revision.
- Per-table or per-schema overrides. v1 binds at LLM-profile scope
  only.
- Importing a style from a non-database source (e.g. a Markdown
  glossary). Future.

## 10. Open questions

None at the time of writing. The three design choices flagged
during brainstorming (two-stage distillation, LLM-profile binding,
redacted few-shot examples) have been approved.
