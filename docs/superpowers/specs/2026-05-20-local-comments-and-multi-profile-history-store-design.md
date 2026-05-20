# Local Comments + Multi-Profile History-Store

## Context

Two related gaps in the AMX persistence story:

1. **Local-only column/table comments.** Today, when a user authors
   a description in AMX, the only way it survives across sessions is
   to apply it back to the database via the writeback path. Users
   who cannot (or do not want to) write to the source DB lose their
   work the moment they close the REPL or Studio. The
   `catalog_descriptions` table already carries non-applied
   descriptions during a run, but there is no surface to *create* a
   description as "mine, do not write back, but keep it visible".

2. **Multi-profile history-store inclusion.** The remote history
   store dual-write is bound to a single profile via the singular
   `history_store_profile` config field. Users running AMX against
   several profiles cannot include more than one of them in the
   shared history. The `/history-store` wizard only asks for one.

## Decisions

| Axis | Decision |
|---|---|
| Local-comment storage | Reuse `catalog_descriptions` with a new `source_kind = "user_local"`. No new table. The existing `effective_description_id` pointer on `catalog_entities` is updated so reads (Studio cards, REPL inspect) see the override. |
| Local-comment writeback semantics | Pure local: `user_local` rows never trigger `db.set_column_comment`. The existing apply flow ignores them. A separate "promote to writeback" action is out of scope for this PR. |
| Multi-profile config shape | Add `history_store_profiles: list[str]` alongside the existing singular `history_store_profile`. The effective set is the union, deduplicated. Loaders + savers carry both fields so older configs read unchanged and write back in the new shape. |
| Surfaces | Local-comment: REPL `/db comment-local` wizard + `POST /api/comments/local`. Multi-profile: REPL `/history-store profiles` wizard + `PATCH /api/history/profiles`. Studio UI updates are out of scope for this PR; the new endpoints are the API contract Studio will consume in a follow-up. |

## Architecture

### Edited modules

- **`amx/config.py`** — add `history_store_profiles: list[str] = field(default_factory=list)`; extend the `_PERSISTED_FIELDS` set, the YAML loader (`from_data`), and the YAML dumper to round-trip the list. Provide a helper `history_store_profile_set(cfg) -> set[str]` that returns the union of the singular and list fields.
- **`amx/web/routers/history.py`** — extend `GET /api/history/state` to expose `shared_profiles` (the union set). Add `PATCH /api/history/profiles` with body `{"profiles": [...]}` to set the list.
- **`amx/web/routers/comments.py`** — add `POST /api/comments/local` with body `{"profile", "schema", "table", "column" (optional), "description"}` to upsert a `user_local` description in `catalog_descriptions` and point `catalog_entities.effective_description_id` at it. No DB call.
- **`amx/cli_support/commands/history_store.py`** — add `cmd_profiles` (wizard with multi-select) and wire `/history-store profiles` into the session dispatcher.
- **`amx/cli_support/commands/db.py`** — add `cmd_comment_local` (wizard: pick profile / schema / table / column / text) and wire `/db comment-local`.
- **`amx/storage/sqlite_store.py`** — add `save_user_local_description` and `_make_user_local_effective` helpers if no existing path covers `source_kind="user_local"` inserts.

### Storage schema

No new tables. No new columns. New `source_kind` value uses an
existing TEXT column. No migration required.

## Verification

1. Unit: config round-trip for `history_store_profiles` (load -> save -> load).
2. Unit: `history_store_profile_set(cfg)` returns the union deduplicated.
3. Unit: `save_user_local_description` inserts a row in `catalog_descriptions` with `source_kind="user_local"` and updates `catalog_entities.effective_description_id`.
4. HTTP: `PATCH /api/history/profiles` accepts a list of strings and persists it.
5. HTTP: `POST /api/comments/local` creates the override; subsequent `GET /api/catalog/inventory` reflects it.
6. No-regression: existing `tests/test_db_cache_ops.py`, `tests/test_catalog_skeleton_sync.py`, and `tests/web/test_comments.py` pass.
7. Manual REPL smoke: `/history-store profiles` wizard updates the list, `/history-store status` shows the union; `/db comment-local` writes a local description, `/inspect` shows it.

## Delivery split

The two sub-features ship as separate PRs because the local-override
path requires a careful integration with the existing
`effective_description_id` / `chosen_description` / FTS reconciliation
flow in `EntityCrudMixin`, and a single PR mixing both would muddy
the review.

* **PR-1 (this PR):** Multi-profile history-store inclusion — config
  field, `PATCH /api/history/profiles`, `/history-store profiles`
  REPL wizard, tests.
* **PR-2 (follow-up):** Local-only comment override — `source_kind =
  "user_local"` write path, `POST /api/comments/local`,
  `/db comment-local` wizard, reconciliation with the existing
  description precedence, FTS re-index, tests.

## Out of scope

- Studio SPA changes (the bundled `web/static/assets/*.js` files). The new endpoints + REPL surfaces ship in this PR; the Studio Settings page and comment card UI updates are a follow-up.
- "Promote local override to writeback" action (PR-2 territory).
- Per-profile inclusion in *local* history (today every profile contributes to local SQLite; the new flag controls *remote* mirroring only).
