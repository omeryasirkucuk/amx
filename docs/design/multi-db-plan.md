# Multi-Database Execution & Optional `database` Field — Design Plan

> Status: PROPOSAL (no code changed). Targeted version bump: **`0.11.0` (MINOR)** —
> additive, backward-compatible at the YAML / keyring / SQLite-schema level.
>
> **Branching note:** do **not** push to `main` while this plan is in flight.
> Every step in §7 lands on its own feature branch (`feat/multi-db-*`) and
> merges through PRs; the umbrella branch is `feat/multi-db-execution-and-optional-database`.

---

## 1. Problem statement (what the user asked for)

Today every connector profile in `cfg.db_profiles` is exactly **one** database
and AMX always operates against the single profile pointed to by
`cfg.active_db_profile`. That has two pain points:

1. **Forced `database` field.** A connector profile bakes in `database` /
   `catalog` / `dataset` / `project`. Some users want to register a connector
   *without* nailing down the database — they want to pick at query time, or
   ask cross-database questions where pinning a single DB is incorrect.
2. **Single-active-profile execution model.** `/ask`, `/run`, `sync` only see
   one profile per call. Cross-DB join discovery is the highest-value answer
   the search agent can give and we structurally cannot ask for it today
   without manually `/use-db` switching and losing the other side's context.

We want, in 0.11.0:

- The `database` (and `catalog` / `dataset` / `project`) field on
  `DBConfig` becomes **optional**. A connector with `database=""` is valid;
  the user picks the database interactively at command time, and the
  catalog/embeddings layer treats database as an entity attribute, not a
  partitioning key.
- `/ask`, `/run`, `/sync`, `/run-apply`, `/inspect`, `/monitor` accept a
  **set of profiles** ("scope") instead of a single active profile. The
  active profile remains the *default* scope; selecting more is a scope
  override, identical in spirit to how schemas/tables work inside a profile
  today.

---

## 2. Current architecture — the map (what already exists)

Findings from the inspection pass:

### 2.1 Configuration layer (`amx/config.py`)
- `DBConfig` is a `@dataclass(_ObservableConfig)` with backend-keyed fields.
  `database`, `catalog`, `dataset`, `project` already default to empty strings
  — but `is_configured()` *requires* `database` (PG/SF) or `catalog`-ish
  fields per backend. URL builders in `DBConfig.url` always interpolate the
  field, even when empty, sometimes producing invalid URLs.
- `cfg.db_profiles: dict[str, DBConfig]`, `cfg.active_db_profile: str`,
  mirrored to `cfg.db` for legacy callers. Two transactional helpers exist
  already: `set_active_db_profile()` and `upsert_db_profile()` — both wrap
  in `cfg.transaction()` to avoid the ghost-profile autosave race.
- Persistence is YAML + OS keyring for secrets. Schema is fully
  forward-compatible — adding optional fields is a no-op for old configs.

### 2.2 Connector layer (`amx/db/connector.py`, `amx/db/adapters/`)
- `DatabaseConnector(cfg.db)` is the universal entry point and is
  **always** instantiated from a single `DBConfig`. There are 27 call sites
  across `amx/` (see `grep "DatabaseConnector(cfg.db" amx/`).
- The connector itself never reads `cfg.active_db_profile` — it only sees
  the DBConfig handed to it. That is a useful invariant: making the call
  sites scope-aware is enough; the adapters need no change.
- `list_catalogs()` / `list_schemas()` already work without a configured
  database for the 3-level backends (Databricks Unity Catalog, BigQuery)
  — `cli_support/catalog_picker.py` runs interactively in `/run` &
  `/search sync` so we have a working pattern for "ask the user which
  database to use" already.

### 2.3 Search catalog (`amx/search/catalog.py`, `amx/search/_catalog/`)
- The SQLite store is **already keyed by `db_profile`**:
  `catalog_entities (db_profile, schema_name, table_name, column_name, entity_kind)`
  with a unique index. Joins, descriptions, settings, sync jobs, sources,
  chat sessions all carry `db_profile`.
- ChromaDB also partitions by profile (`SearchIndex._collection_for(profile)`).
- This means **multi-profile retrieval at query time is already feasible
  with a UNION over `db_profile IN (...)` — no schema migration is needed**.
  The only blocker is the agent layer hard-coding `self.db_profile = single`.

### 2.4 Search agent (`amx/search/agent.py`, `amx/search/_agent/`)
- `SearchAgent.__init__` snapshots `self.db_profile = cfg.active_db_profile`
  and every retrieval / resolution / planning call passes that single
  string into catalog methods. The fix is mechanical: replace
  `self.db_profile: str` with `self.db_profiles: list[str]` and update the
  catalog read methods to accept `Sequence[str]`.

### 2.5 Run / Sync / Ask command flow
- `/run` (`commands/analyze_flow.py`):
  - `_maybe_modify_profiles_before_run` lets users switch active DB/LLM
    profile before a run, but only one. The pattern is the right entry
    point for the multi-select expansion.
  - The actual orchestrator and per-schema loop work on the single `db`
    instance. They will need to iterate over `(profile, db, scope)`
    tuples.
- `/sync` (`commands/search.py::search_sync`): same — single connector,
  single profile passed to `catalog.start_sync_job`. It already records
  jobs per profile, so the multi-profile case is "run the loop once per
  profile".
- `/ask` (`commands/search.py::search_ask` → `SearchService` → `SearchAgent`):
  the only call that benefits from a *fused* multi-profile view rather
  than a sequential per-profile loop, because cross-DB join answers need
  catalog rows from **both** profiles in the same retrieval pass.

### 2.6 Storage / chroma layout
- SQLite catalog and Chroma collections are already multi-tenant by
  `db_profile`. No migration needed.
- Code-evidence (`catalog_usage_evidence`) and join evidence
  (`catalog_relationships`) both carry `db_profile`. Cross-profile joins
  will need a relationship row whose `from_entity_id` and `to_entity_id`
  point at *different* profiles' entities — schema already permits this
  (foreign keys are by `id`, not `db_profile`).

---

## 3. What changes — the design

### 3.1 Make `database`/`catalog`/`dataset`/`project` truly optional

`DBConfig` change:
- Drop the legacy `database: str = "SAP"` placeholder default — replace
  with `database: str = ""`. (The `"SAP"` default is a five-year-old
  artifact from a single demo dataset and is the source of "phantom
  localhost connection" reports; CHANGELOG already references this.)
- Add an explicit `is_database_pinned() -> bool` helper:
  - PG/SF: `bool(self.database)`
  - Databricks: `bool(self.catalog)`
  - BigQuery: `bool(self.dataset)` (project is still required to connect
    at all on BQ)
- Loosen `is_configured()` so a connector with no database is still
  "configured" if the connection-level fields (host/account/project) are
  present. Add a parallel `is_connection_configured()` — UI uses
  `is_configured()` to decide "do we have enough to connect", and
  `is_database_pinned()` to decide "is the user committed to a
  database".
- `DBConfig.url` builders fall back to bare engine URLs when the
  database isn't pinned (they already largely do; PG is the only one
  that needs a tweak — drop the trailing `/` when `self.database == ""`).

`/add-db-profile` flow:
- `interactive_db_block()` already has per-backend prompts. Add a single
  question after the connection fields: **"Pin a default database now? [y/N]"**.
  - If yes: today's flow.
  - If no: leave the field blank and add a banner —
    *"You can pick the database at command time with `/run --database X`
    or in the interactive scope picker."*
- `cfg.db.display_summary` returns `"<host> (no DB pinned)"` when
  unpinned so the user clearly sees the state.

### 3.2 Multi-profile execution scope (`ProfileScope`)

Introduce a small data type, NOT a config field:

```python
# amx/services/profile_scope.py
@dataclass(frozen=True)
class ProfileScope:
    profiles: tuple[str, ...]       # ordered, deduped
    default: str                    # the legacy "active" — for write-back
    @property
    def is_multi(self) -> bool: ...
    def configs(self, cfg: AMXConfig) -> list[tuple[str, DBConfig]]: ...
    def connectors(self, cfg: AMXConfig) -> Iterator[tuple[str, DBConfig, DatabaseConnector]]:
        # Yields one connector at a time; closes each before yielding next
        ...
```

This is **always built per-command** from one of three inputs (priority order):

1. CLI flag: `--db-profile name1 --db-profile name2` (multi-value Click flag).
2. Slash-arg form: `/run @prod_pg @analytics_bq …` — leading `@` denotes
   profile, mirroring how some prompts use `@table`.
3. Interactive picker: when the active profile is the only configured
   one, default scope is `[active]` silently. When ≥2 profiles exist,
   the existing `_maybe_modify_profiles_before_run()` becomes a
   **multi-select** ("Select DB profiles for this run") with the active
   pre-checked.

`cfg.active_db_profile` is unchanged — it is the default for a single-pick
scope and the "write target" when only one profile is in scope. We do
not break any existing single-profile semantics.

### 3.3 Per-command behavior

**`/sync` (search sync) — easy: per-profile loop.**
Run the existing `_sync_db_scope` once per profile in the scope. Each
catalog write already keys by `db_profile`. Status & job rows
already exist per profile.

**`/run` & `/run-apply` — per-profile loop with shared LLM session.**
- Outer loop: for each profile in scope, build a connector, run the
  scope picker (schema/asset) inside that profile, run the
  orchestrator, persist results scoped to that profile in
  `analysis_runs` / `run_results` (those tables already carry
  `db_profile`).
- Equivalence dedup pass and `/apply` write-back happen per profile —
  comments are written to the profile's actual database, so per-profile
  is the only correct semantics.
- We share the LLM provider and token tracker across all profiles in the
  scope — no need to re-test the LLM connection between iterations.
- Multi-profile mode displays a top-level progress bar
  `Profiles 1/3 · prod_pg · 4 schemas · 27 tables`.

**`/ask` (search ask) — fused retrieval, this is the killer feature.**
- `SearchAgent` accepts `db_profiles: Sequence[str]` instead of a
  single `db_profile`.
- All `catalog.*` mixin methods that take a `db_profile` parameter
  gain a sibling that accepts a `Sequence[str]` and emits
  `db_profile IN (?,?,…)`. The single-profile signature stays for
  back-compat (called from `/sync`, `/explain`, etc.).
- The plan/resolve/answer mixins compose retrieval rows from all
  profiles; each result row already carries `db_profile`,
  `database_name`, `schema_name`, `table_name`, so display rendering
  needs one extra column ("DB" or "Profile") when the scope is
  multi-profile.
- Cross-DB join inference becomes possible because
  `catalog.semantic_join_candidates` already takes two table paths
  and just needs to be allowed to span profiles. The same vector
  index call works as long as we collect candidates from each
  profile's collection and rank them together.
- Live-DB verification (`verify_live_inventory`) loops over profiles —
  the existing `_inventory_db_factory` becomes
  `_inventory_db_factory(profile_name) -> DatabaseConnector`.

**`/inspect`, `/monitor`, `/cleanup-placeholders`, `/edit`** —
the same multi-select pattern applies. These are smaller follow-ups.

### 3.4 Database-not-pinned + multi-profile interaction

When a profile is in scope but has no pinned database:

- For `/sync` and `/run`: trigger the existing catalog picker
  (`amx/cli_support/catalog_picker.py`) before the schema picker. The
  user picks **once per profile per run**; the choice is *not* persisted
  to the profile (that would re-pin it). It's a runtime override carried
  in `ScopeResult`.
- For `/ask`: when no database is pinned, retrieval queries the catalog
  across **all** databases for that profile (i.e. don't filter by
  `database_name`). This is the cross-DB superpower the user described.
  When they want narrower scope, they say so in the question
  ("…in production_warehouse…") or pin the DB.
- We will need to thread a `database_name: str | None` override
  through `_inventory_db_factory` for all 3-level backends. Today it
  already lives on the `DBConfig.catalog` field; the override path
  must temporarily set it on a `replace()`d copy before constructing
  the connector — the existing catalog picker uses exactly this
  pattern in `commands/run.py`.

### 3.5 Catalog/agent API additions (small, additive)

```python
# amx/search/_catalog/search.py (and friends)
def find_table_candidates(self, db_profiles: Sequence[str], hint: str, limit: int): ...
def name_search_columns(self, db_profiles: Sequence[str], q: str, limit: int): ...
# etc — every method that takes `db_profile: str` gets an overload accepting Sequence[str].
```

We **keep** the existing single-string signatures and dispatch to the
multi-form internally. No call site outside the agent needs to change.

### 3.6 Slash-command surface

New flags / args (minimal addition):

| Command           | New affordance                                   |
| ----------------- | ------------------------------------------------ |
| `/use-db`         | Stays single-pick (it sets the default scope)    |
| `/run`            | `--db-profile NAME` (multi); else interactive    |
| `/run-apply`      | Same                                             |
| `/sync`           | Same                                             |
| `/ask` (& `/search ask`) | `--db-profile` (multi). Default = active. |
| **NEW:** `/scope`  | Show current default scope = active profile      |
| `/db-profiles`    | Mark unpinned profiles with a `?` next to backend |

`/scope` is convenience — it just renders today's active profile and
explains how to add others. No persisted config field.

### 3.7 What we explicitly don't touch in 0.11.0

- LLM profiles, document profiles, code profiles — out of scope; the
  scope concept is DB-only for this release.
- Cross-profile write-back / `/apply` — comments are always written
  to the source profile. No "merge profile" semantics.
- Migration of legacy YAML configs that have `database: "SAP"`
  baked in — they keep working unchanged. We add a one-time
  one-line warn in `/db-profiles` if we detect the legacy
  default value, suggesting the user run `/edit` to clear it.
- Embeddings layout — Chroma collections stay per profile.

---

## 4. Risks & open questions

1. **Catalog schema lookup performance** — `db_profile IN (?,?,…)` is
   well-indexed today (`idx_catalog_entities_identity`), so two-profile
   queries should be O(log n) per profile. Worth a quick `EXPLAIN QUERY
   PLAN` smoke test on a real catalog.
2. **Vector search across profiles** — Chroma collections are
   per-profile. We have to query each collection separately and
   re-rank. The `SearchIndex` change is mechanical but means a vector
   ask in 5-profile scope does 5 round-trips. Acceptable for the first
   cut; can collapse to a single shared collection later if needed.
3. **Live-DB inventory cost** — for `/ask` with 3+ profiles the live
   inventory probe (`_live_schema_rows`) becomes expensive. We default
   `verify_live_inventory` to `false` when scope is multi-profile and
   surface that in `/search status` so the user can opt in.
4. **Catalog picker UX in multi-profile `/run`** — picking a
   database per profile per run could be tedious. Mitigation: the
   per-profile pick is sticky for the duration of the run only; if the
   profile already has a pinned DB the picker silently uses it.
5. **Cross-profile column equivalence (dedup pass)** — the equivalence
   pre-walk currently builds a flat list of columns and groups by
   dtype + name. If we feed it columns from two profiles and they
   share the same name, dedup will treat them as one class. That's
   actually desirable for cross-DB sync but surprising in some cases.
   The user-facing prompt should preview which classes span profiles
   so the user can refuse.
6. **`/run-apply` write-back ambiguity** — must error if write-back is
   attempted with a multi-profile scope today and the LLM produced
   one description for an asset that exists in both. (Iterating per
   profile sidesteps this entirely; we'll enforce the per-profile
   loop in code.)

---

## 5. Versioning & changelog

- `MAJOR.MINOR.PATCH` → `0.10.15` → **`0.11.0`**.
  - MINOR because behavior is additive: the legacy single-profile
    contract is preserved when no `--db-profile` is given.
- Changelog headline:
  > `0.11.0` — Multi-database execution and optional `database` per
  > connector. `/ask`, `/run`, `/sync` accept multiple `--db-profile`
  > values and default to the active profile when omitted.

---

## 6. Test surface

- `tests/test_db_profile_optional_database.py` — unit-test the
  loosened `is_configured()` and the new `is_database_pinned()`
  helper across all four backends.
- `tests/test_profile_scope.py` — `ProfileScope` parsing from
  CLI flags + interactive multi-select.
- `tests/test_search_catalog_multi_profile.py` — feed two profiles'
  worth of fixtures, run `find_table_candidates` / `joinable_tables`
  / `semantic_join_candidates` with `db_profiles=[…,…]`.
- `tests/test_run_multi_profile_loop.py` — orchestrator iterates
  over profiles, persists distinct `analysis_runs` rows, shares the
  same LLM session.
- `tests/test_ask_cross_db_join.py` — end-to-end RAG-less ask: two
  fixture profiles each with one table, one column-name in common;
  agent surfaces a cross-profile join candidate row.
- Pre-existing tests for `/run`, `/sync`, `/ask` must keep passing
  unchanged — the single-profile path is the default.

---

## 7. Implementation roadmap (branches)

Each phase is its own PR, merged into the umbrella branch
`feat/multi-db-execution-and-optional-database`. Nothing pushes to
`main` until the user signs off on the umbrella branch.

| Phase | Branch                                          | Scope                                                                                                       |
| ----- | ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| 0     | `feat/multi-db-execution-and-optional-database` | Umbrella branch (this plan, no code).                                                                       |
| 1     | `feat/dbconfig-optional-database`               | `DBConfig` defaults, `is_configured()` split, URL builder fix, banner in `/db-profiles` & startup summary. |
| 2     | `feat/profile-scope-type`                       | `ProfileScope` dataclass + helpers + tests, no consumers wired yet.                                         |
| 3     | `feat/search-catalog-multi-profile`             | `Sequence[str]` overloads on `_catalog/*` mixins; SQL `IN`-clauses; tests.                                  |
| 4     | `feat/search-agent-multi-profile`               | `SearchAgent.db_profiles`; retrieval/resolution/planning use the sequence; cross-DB join rendering.         |
| 5     | `feat/ask-multi-profile-cli`                    | `/search ask --db-profile NAME` (multi) + interactive multi-select + display column.                        |
| 6     | `feat/run-multi-profile-loop`                   | `/run` & `/run-apply` outer loop, equivalence dedup preview, history rows per profile.                      |
| 7     | `feat/sync-multi-profile-loop`                  | `/sync` per-profile loop with shared progress UI.                                                           |
| 8     | `feat/per-run-database-override`                | Catalog picker as a runtime override (no profile mutation); plumbed through `ProfileScope`.                 |
| 9     | `chore/bump-0.11.0`                             | Version bump, CHANGELOG, README updates, smoke-test the local `pip install -e .`.                           |

---

## 8. Confirmed decisions (locked in before implementation)

1. **`/use-db` is multi-pick.** `/use-db prod_pg analytics_bq` sets
   the *persisted* default scope. The active scope is therefore a
   list, not a single name.
   - Implementation: new persisted field `active_db_profiles: list[str]`
     on `AMXConfig`. The legacy `active_db_profile: str` becomes a
     **derived property** that returns `active_db_profiles[0] or ""`
     and a setter that wraps it in a single-element list, so all 99
     existing call sites keep working unchanged.
   - YAML migration on load: if old config has `active_db_profile: foo`
     and no `active_db_profiles`, populate `[foo]`. Save writes both
     keys for one release for forward/backward compatibility, then
     drops the legacy key in 0.12.0.
   - The "write target" for `/run-apply` is the **first** profile in
     the active scope (or the only profile in scope when running
     `--db-profile single`); described in §3.3 already.
2. **`/ask` default scope = active profiles only.** No automatic
   fan-out across all configured profiles. When `len(active_db_profiles) > 1`
   the user is *opted in* — the answer renders the `Profile` column
   and cross-DB joins surface naturally. A profile that wants
   single-DB asks just runs `/use-db prod_pg`.
   - Per-call override: `/ask --db-profile NAME …` (multi) wins over
     persisted scope.
   - One-time hint when exactly 1 active profile + ≥2 configured:
     printed once per session in the search namespace banner.
3. **Legacy `database: "SAP"` — suggest, don't mutate.** When `load()`
   detects the historical default value, emit a single-line warn
   in `/db-profiles` and the startup summary suggesting the user
   run `/edit` to clear it. We never touch their YAML.

---

## 9. Phase 2 — six new backends + extended object model (2026-05)

After 0.11 stabilised the 4-backend matrix, Phase 2 added MySQL, Oracle,
SQL Server, Redshift, ClickHouse, and DuckDB **and** extended the adapter
contract to model object types beyond tables/views.

### 9.1 Per-backend object inventory

What each backend exposes that AMX consumes. ✓ = supported, ★ = backend-distinctive.

| Object              | PG | SF | DBX | BQ | MySQL | Oracle | MSSQL | Redshift | ClickHouse | DuckDB |
|---------------------|----|----|-----|----|-------|--------|-------|----------|------------|--------|
| Tables              | ✓  | ✓  | ✓   | ✓  | ✓     | ✓      | ✓     | ✓        | ✓          | ✓      |
| Views               | ✓  | ✓  | ✓   | ✓  | ✓     | ✓      | ✓     | ✓        | ✓          | ✓      |
| Materialized views  | ✓  | ✓  | –   | ✓  | –     | ✓      | –     | ✓        | ✓          | –      |
| External tables     | –  | ✓  | ✓   | ✓  | –     | –      | –     | ✓ (Spectrum) ★ | – | ✓ (Parquet/S3) |
| Stored procedures   | ✓  | ✓  | –   | ✓  | ✓     | ✓      | ✓     | ✓        | –          | –      |
| Functions / UDFs    | ✓  | ✓  | ✓   | ✓  | ✓     | ✓      | ✓     | ✓        | ✓          | ✓      |
| Sequences           | ✓  | ✓  | –   | –  | –     | ✓      | ✓     | –        | –          | ✓      |
| Triggers            | ✓  | –  | –   | –  | ✓     | ✓      | ✓     | –        | –          | –      |
| Events / scheduled  | –  | ✓ (tasks) | – | –  | ✓ ★ (events) | – | – | – | – | – |
| Packages            | –  | –  | –   | –  | –     | ✓ ★    | –     | –        | –          | –      |
| Synonyms            | –  | –  | –   | –  | –     | ✓      | ✓     | –        | –          | –      |
| User-defined types  | ✓  | –  | –   | –  | –     | ✓      | –     | –        | –          | –      |
| Dictionaries        | –  | –  | –   | –  | –     | –      | –     | –        | ✓ ★        | –      |
| Macros              | –  | –  | –   | –  | –     | –      | –     | –        | –          | ✓ ★    |
| Volumes / stages    | –  | ✓ (stages) | ✓ ★ | – | – | – | – | – | – | – |
| Datashares          | –  | ✓  | –   | –  | –     | –      | –     | ✓        | –          | –      |
| Dist/Sort keys      | –  | –  | –   | –  | –     | –      | –     | ✓ ★      | –          | –      |
| Storage engine      | –  | –  | –   | –  | ✓ ★ (InnoDB/MyISAM) | – | – | – | ✓ ★ (MergeTree) | – |

### 9.2 Contract extension

* `BackendCapabilities` gained 13 new flags (`stored_procedures`, `functions`,
  `sequences`, `triggers`, `events`, `packages`, `synonyms`,
  `user_defined_types`, `dictionaries`, `macros`, `volumes`, `datashares`,
  `external_tables`).
* `DatabaseAdapter` gained matching `list_<object>()` methods. Each defaults
  to `[]` so existing adapters stay untouched until they opt in.
* Each method returns a list of dicts shaped
  `{name, type, definition, comment, metadata}` — uniform enough that
  search/index can treat them generically, loose enough that backend-specific
  fields (Snowflake task schedule, ClickHouse dictionary layout, Oracle
  package members) fit in `metadata`.
* `DatabaseConnector` exposes capability-gated wrappers for each method.
  Adapter exceptions degrade to `[]` with a debug-level log entry, mirroring
  the `list_databases` resilience pattern.

### 9.3 Driver packaging

Database driver dependencies migrated from `[project.dependencies]` to
`[project.optional-dependencies]` extras — one extra per backend, plus an
`all` extra for "give me everything" (~100MB+ of drivers). `get_adapter()`
catches the first-use `ImportError` and raises `MissingDriverError` with
the concrete `pip install amx[<extra>]` hint.

This is a breaking install-time change; existing users must reinstall with
the relevant extras. The migration is documented in CHANGELOG and surfaces
on first use of an unconfigured backend.
