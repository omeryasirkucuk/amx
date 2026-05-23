# Lineage capability map

A reference for what AMX captures, infers, and exposes as lineage,
and where the current implementation falls short of the bar set by
mainstream warehouse catalogs (Databricks Unity Catalog, Snowflake
Horizon, BigQuery Dataplex).

## Storage

| Table | Grain | Populated by |
|---|---|---|
| `catalog_relationships` | table edges and column edges; `relationship_type` is one of `lineage_fk`, `lineage_view_ddl`, `lineage_query_log`, `lineage_codebase`, `lineage_name_match`, `lineage_llm`, `lineage_manual`, `lineage_system_table`, `lineage_system_column` | The seven extractors below plus the Databricks system-tables extractor |
| `asset_lineage_edges` | `(from_kind, from_id) -> (to_kind, to_id)` with `edge_type` and an optional `direction` (`read`, `write`, `both`) | `amx/assets/lineage.py:LineageExtractor.extract_for_profile()` and the SQL-parse extractor |
| `view_definitions_cache` | View DDL plus sqlglot-parsed column lineage JSON | `amx/lineage/extractors/view_ddl.py` |
| `lineage_artifacts`, `lineage_artifact_nodes`, `lineage_artifact_edges` | Saved canvas state with positions, comments, logo nodes | Studio canvas |

`asset_lineage_edges.edge_type` covers `task_runs_notebook`,
`task_runs_query`, `task_runs_pipeline`, `task_depends_on`,
`pipeline_includes_notebook`, `pipeline_writes_table`,
`query_reads_table`, `query_writes_table`, `notebook_reads_table`,
`notebook_writes_table`.

## Extractors

The seven primary extractors live under `amx/lineage/extractors/`:

1. `fk.py` — foreign keys via adapter introspection (Postgres, Snowflake, BigQuery, MySQL, Oracle, Redshift, DuckDB)
2. `view_ddl.py` — sqlglot column-aware view DDL parse, all dialects
3. `query_log.py` — co-occurrence in the local `analysis_runs.scope_json` and `chat_turns.tables_json` tables
4. `codebase_scan.py` — Chroma RAG search then sqlglot parse
5. `name_match.py` — heuristic name overlap, weak signal
6. `llm.py` — on-demand Claude inference, triggered from the canvas
7. `manual_edges.py` — user-authored edges

Asset-side extraction lives in `amx/assets/lineage.py`. It walks
`remote_job_tasks` for task references, parses
`remote_pipelines.libraries_json` for notebook inclusion, and
resolves `remote_pipelines.target_schema` to infer
`pipeline_writes_table` edges. The SQL-parse extractor at
`amx/lineage/extractors/sql_parse.py` complements it by reading the
SQL stored in `remote_queries.sql_text` and the SQL cells of
`remote_notebooks.source_text`, then writing `query_reads_table` /
`query_writes_table` / `notebook_reads_table` /
`notebook_writes_table` edges.

The Databricks system-tables extractor at
`amx/lineage/extractors/system_tables/databricks.py` queries
`system.access.table_lineage`, `system.access.column_lineage`, and
`system.query.history`, then writes `lineage_system_table` and
`lineage_system_column` rows to `catalog_relationships` and updates
the matching `asset_lineage_edges.last_used_at` and `last_user`.

## API surface

| Endpoint | File | Purpose |
|---|---|---|
| `GET /api/lineage/{anchor_path}` | `amx/web/routers/lineage.py` | Table graph, `depth_up`/`depth_down` 0-5, capped at 200 nodes |
| `GET /api/lineage/column-trace/{anchor_path}` | `amx/web/routers/lineage.py` | Column-level BFS with operator intermediates, `max_depth` up to 200 |
| `GET /api/assets/{kind}/{asset_id}/lineage` | `amx/web/routers/assets.py:get_asset_lineage` | `{outgoing, incoming, task_dag}` for jobs and pipelines |
| `GET /api/tables/{id_or_fqn}/assets` | `amx/web/routers/assets.py` | Reverse lookup: notebooks, queries, jobs, pipelines, streams, and streamlit apps that touch the table, grouped by read/write |
| `GET /api/lineage/by-id/{artifact_id}` | `amx/web/routers/lineage.py` | Saved canvas payload |
| `GET /api/lineage/artifacts-with-table` | `amx/web/routers/lineage.py` | Canvases containing a given table |
| `GET /api/lineage/audit` | `amx/web/routers/lineage.py` | Manual-edge audit trail |

## Studio surface

- `/lineage` (ReactFlow canvas, `frontend/src/routes/lineage-canvas/Canvas.tsx`) with `TableNode`, `OperatorNode`, `QueryNode`, and `ColumnEdge`
- Column-level lineage with operator intermediates
- Inline Lineage tab on `frontend/src/routes/Table.tsx`, fed by the reverse-lookup endpoint
- `LineagePanel` in the asset detail drawer for jobs and pipelines (outgoing edges and task DAG)

## Coverage vs warehouse-native lineage

The table below describes what we capture today, measured against
the table-detail lineage tab in Databricks Unity Catalog.

| Capability | Status |
|---|---|
| Upstream table graph | Captured (depth 0-5, 200-node cap on render) |
| Downstream table graph | Captured (same caps) |
| Column-level upstream and downstream | Captured (server-side BFS, operator intermediates) |
| Notebooks that read or write a table | Captured for Databricks via system tables; captured warehouse-agnostically when the notebook contains SQL the parser can resolve |
| Queries that read or write a table | Captured via SQL parse of `remote_queries.sql_text` and, on Databricks, via `system.query.history` |
| Jobs and pipelines that write a table | Captured via `pipeline_writes_table` and the asset extractor |
| Jobs and pipelines that read a table | Captured for Databricks via system tables; otherwise reliant on SQL parsing of the asset's source |
| Dashboards consuming a table | Not captured. No dashboard asset kind exists in storage today |
| External sources (S3, GCS, Kafka, REST) | Not captured. No external-source node type exists today |
| Last-accessed timestamp per edge | Captured on edges written or refreshed by the Databricks system-tables extractor; older edges may have `NULL last_used_at` |
| Last-accessed user per edge | Same as above |
| Cross-warehouse lineage | Not captured. Extractors run per profile; there is no federation between, for example, a Postgres source and a Snowflake warehouse |

## Adapter coverage matrix

| Relationship | Postgres | Snowflake | BigQuery | Databricks | DuckDB | MySQL | Oracle | Trino |
|---|---|---|---|---|---|---|---|---|
| table to table from FKs | yes | yes | yes | no introspection | yes | yes | yes | no introspection |
| column to column from FKs | partial | partial | partial | none | partial | partial | partial | none |
| view to table from DDL parse | yes | yes | yes | yes | yes | yes | yes | yes |
| column to column from view DDL | yes | yes | yes | yes | yes | yes | yes | yes |
| query to table from SQL parse | yes | yes | yes | yes | yes | yes | yes | yes |
| notebook to table from SQL parse | yes | yes | yes | yes | yes | yes | yes | yes |
| table to table from system tables | not implemented | not implemented | not implemented | yes | not applicable | not implemented | not implemented | not implemented |
| job to notebook | via asset ingest | via asset ingest | partial | yes | none | none | none | none |
| stream to table | none | via asset ingest | none | via asset ingest | none | none | none | none |
| dashboard consumption | none | none | none | none | none | none | none | none |
| external source | none | none | none | none | none | none | none | none |

Snowflake and BigQuery still rely on view DDL parsing and the
local query-log signal for table-to-table lineage; their
`ACCOUNT_USAGE.OBJECT_DEPENDENCIES` and
`INFORMATION_SCHEMA.JOBS_BY_PROJECT` sources are not yet wired.

## Known limitations

1. **Snowflake and BigQuery system tables are unread.** The Databricks extractor is the template; the equivalent for Snowflake (`OBJECT_DEPENDENCIES`, `ACCESS_HISTORY`) and BigQuery (`INFORMATION_SCHEMA.JOBS_BY_PROJECT`, the Data Catalog Lineage API) is not implemented. Until these land, lineage on Snowflake and BigQuery profiles is limited to view DDL plus what the SQL-parse extractor finds in stored queries.
2. **Dashboards are absent from the asset model.** There is no `remote_dashboards` table; lineage cannot connect a Tableau, Power BI, Looker, or Databricks dashboard to the underlying tables.
3. **External sources are absent from the lineage graph.** S3, GCS, Kafka topics, and REST endpoints have no representation in `catalog_entities`, so the graph cuts off at the warehouse boundary.
4. **Cross-warehouse lineage is not federated.** Each extractor runs against a single profile. A pipeline that reads Postgres, lands in Databricks, and publishes to Snowflake produces three disconnected subgraphs.
5. **Streaming and CDC semantics are partial.** Streams are ingested into `remote_streams`, but stream-to-source edges only come from `pipeline_writes_table`; we do not introspect CDC sources or Kafka topics.
6. **Discovered edges have weaker user attribution than manual edges.** `catalog_relationships.audit_actor` is populated for `lineage_manual` only. The Databricks system-tables extractor backfills `last_user` on the asset-side edges it touches, but older `catalog_relationships` rows from other extractors remain anonymous.
