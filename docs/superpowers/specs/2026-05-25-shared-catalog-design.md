# Shared catalog: team-wide sharing of structural table/column metadata

## Context

A "deep sync" (`amx/search/drift.py:deep_sync_profile`) profiles every
catalogued table — running `profile_table`, which issues a `COUNT(*)`
per table — and writes the structural metadata (columns, dtypes, row
counts, key flags) into the local `catalog_entities` table inside
`~/.amx/history.db`.

That metadata is **purely local**. The shared history store
(`amx/storage/shared_schema.py`) currently shares only
`analysis_runs`, `run_results`, `documentation_pages`, and
`lineage_artifacts` — not `catalog_entities`. So every team member who
wants columns + row counts must run their own deep sync and re-pay the
`COUNT(*)` cost on every table. On a large catalog that cost is
multiplied by the size of the team.

This design shares the structural catalog so **one member's deep sync
benefits the whole team**: the expensive `COUNT(*)` pass runs once, the
results land in the shared store, and other members pull them.

## Goal

When the shared history store is enabled, a deep sync's structural
results (columns + row counts) propagate to teammates, and enabling the
store on a fresh machine pulls the team's catalog down — eliminating
redundant `COUNT(*)` passes.

## Scope

**In scope** — the structural catalog only:
`catalog_entities` rows (table + column entities) carrying
`entity_kind`, `asset_kind`, `dtype`, `nullable`, `pk_flag`, `fk_flag`,
`row_count`, and `last_synced_at`.

**Out of scope** — descriptions. Generated descriptions already flow
to the team through `analysis_runs` / `run_results`. Sharing
`catalog_descriptions` would add description-conflict complexity for no
new benefit, so it is excluded. On pull, the structural rows land with
their description link (`effective_description_id`) set to NULL; each
puller resolves descriptions from their own local state, which the
existing run-sharing path populates.

## Key decisions (confirmed)

1. **Push is gated on `history_store_enabled`.**
   - Store ON: a deep sync (and any `sync_table_profile` write) pushes
     the structural rows to the shared store immediately, best-effort.
   - Store OFF: rows stay local only; nothing is pushed.
   - Store turned ON later: the local catalog is backfilled up to the
     shared store at enable time, so results accumulated while the
     store was off are not lost.
2. **Pull happens at enable time**, in parallel with the existing
   run / lineage / pages pull: enabling the store on a machine pulls
   the shared catalog down into the local `catalog_entities`.
3. **Conflict resolution: last-write-wins by `last_synced_at`.** Row
   count is a point-in-time snapshot, so the most recently synced value
   is the most accurate. No per-member tracking.

## Architecture

The design reuses the existing local-first dual-write + backfill + pull
machinery (`dual_write.py`, `backfill.py`, `migration.py`) rather than
inventing a new transport.

```
DEEP SYNC (or any sync_table_profile write)
  ├─ write LOCAL catalog_entities         (always — independent of sharing)
  └─ if dual-write active (store ON):
        push structural rows to SHARED     (best-effort; queued in the
                                            pending outbox when offline)

ENABLE history store  (parallel to existing run/lineage/pages flows)
  ├─ BACKFILL: local catalog_entities → shared   (push what accumulated
  │                                                while the store was off)
  └─ PULL: shared catalog_entities → local        (natural-key upsert,
                                                    last-write-wins)
```

### Components / files

| File | Change |
|---|---|
| `amx/storage/shared_schema.py` | Add a `catalog_entities` table mirroring the local structural columns, plus `hostname` / `created_by` provenance and a `last_synced_at` timestamp. Natural key: `(db_profile, database_name, schema_name, table_name, column_name)`. |
| `amx/storage/schema_descriptions.py` | A non-empty description for every new column + the table (house rule: `tests/test_shared_schema_comments.py` enforces this). |
| `amx/storage/dual_write.py` | On a catalog write, when the shared store is active, enqueue/push the structural rows (mirrors the run-write path). Best-effort; never blocks the local write. |
| `amx/storage/backfill.py` | Add a `catalog` scope so enabling the store pushes existing local `catalog_entities` up. |
| `amx/storage/migration.py:pull_shared_to_local` | Pull shared `catalog_entities` into local on enable: upsert by natural key, keep the row with the greater `last_synced_at`, set `effective_description_id` to NULL. |
| Gating | All push/pull paths check that dual-write is active (store ON) before touching the shared store. |

### Natural-key upsert + last-write-wins

The shared `catalog_entities` primary/unique key is the natural tuple
`(db_profile, database_name, schema_name, table_name, column_name)`
(with `column_name = ''` for table-level rows). Upserts use
`ON CONFLICT (...) DO UPDATE ... WHERE excluded.last_synced_at >
catalog_entities.last_synced_at` so an older snapshot never clobbers a
newer one — in either direction (push or pull).

## Error handling

- **Push is best-effort and never blocks the local write.** This
  follows the decoupling already shipped for vector indexing: the
  structural catalog is written locally first, and a shared-store
  failure (network, auth, offline) queues the row in the existing
  `pending_shared_writes` outbox for a later flush. A failed push must
  not raise out of `deep_sync_profile` or `sync_table_profile`.
- **Pull failures degrade to local-only.** If the shared store is
  unreachable at enable time, the catalog pull logs a warning and the
  user keeps whatever is in their local catalog; the enable flow does
  not fail.
- **Store OFF is the silent no-op path** — no push, no pull, no error.

## Testing

- **Push gating:** a `sync_table_profile` / deep sync write with the
  shared store ON enqueues/writes the structural rows to shared; with
  the store OFF, nothing is pushed.
- **Backfill on enable:** local `catalog_entities` rows are pushed to
  the shared store when the store is enabled.
- **Pull on enable:** shared rows are upserted into local; description
  links are NULLed; the local catalog gains the team's tables/columns.
- **Last-write-wins:** pulling (or pushing) an older `last_synced_at`
  does not overwrite a newer local (or shared) row; a newer one does.
- **Best-effort push:** a shared-store exception during a catalog
  write does not raise out of the sync; the row lands locally and is
  queued in the outbox.
- **Schema comments:** `tests/test_shared_schema_comments.py` stays
  green — every new shared column has a non-empty description.

## Outcome

One member runs a deep sync (N tables × `COUNT(*)`); the structural
results land in the shared store. Every other member pulls them when
they enable the store — paying zero `COUNT(*)` cost. The team-wide
multiplication of the deep-sync cost is eliminated.
