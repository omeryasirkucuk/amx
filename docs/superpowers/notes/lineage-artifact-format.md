# Lineage artifact storage format

Pinned for the ASK lineage-evidence builder (plan
`2026-05-20-amx-lineage-pages-in-ask.md`, task 2).

## `output_path`
Absolute path to a rendered image (matplotlib `savefig` in
`amx/lineage/render.py::render_lineage_image` line 109). `format`
is `svg`, `png`, or `jpg` (`render.SUPPORTED_FORMATS`). No JSON
edge list on disk; `build_dot` is debug-only.

## Where edges live
In `catalog_relationships` joined with `catalog_entities`. Artifact
row carries `anchor_entity_id`, `depth_up`, `depth_down`,
`extractors_used` (JSON list) defining the recursive walk.

## In-process loader
`amx/lineage/store.py::list_artifact_edges(hs, *, artifact,
limit=200)` returns `{"edges":[...], "nodes":[...], "truncated":
bool}`. Edge keys: `from_id`, `to_id`, `from_path`/`to_path`,
`from_kind`/`to_kind`, `from_column`/`to_column`,
`relationship_type`, `source`, `score`. Pass artifact dict from
`store.lookup_lineage_artifact(hs, name_or_id=...)`.

## Retrieval path
Option (a) impossible (image, not JSON). Use `list_artifact_edges`;
fallback to `lineage_artifact_nodes` (lossy) on empty edges;
re-extraction rejected for ASK latency.
