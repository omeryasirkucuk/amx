"""Ingested-asset search tools for :class:`ToolBox`.

``search_assets`` and ``describe_asset`` let the tool-calling /ask
agent ground answers in the user's ingested Databricks/Snowflake
notebooks, queries, jobs, pipelines, streams, and Streamlit apps.

Both tools read from the local SQLite history store (``remote_*``
tables) so they are ``freshness="cache_ok"`` and run in cache-only
mode without the live-refresh envelope.

The mixin is compose-only — it never overrides ``ToolBox.__init__``;
the host injects ``cfg`` and ``db_profiles`` as instance attributes.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from amx.config import AMXConfig


# Canonical asset-kind names as they appear in ``remote_<kind>s`` table
# names and the ``from_entity_kind`` column of ``catalog_relationships``.
_KNOWN_KINDS: tuple[str, ...] = (
    "notebook",
    "query",
    "job",
    "pipeline",
    "stream",
    "streamlit_app",
)

# Map kind → (table_name, name_field, location_field, body_field).
# Kept here (not imported from asset_evidence.py) because describe_asset
# needs richer metadata than the evidence module's 4-field selection.
_KIND_TABLE_SPEC: dict[str, dict[str, str]] = {
    "notebook": {
        "table": "remote_notebooks",
        "name": "name",
        "location": "workspace_path",
        "body": "source_text",
    },
    "query": {
        "table": "remote_queries",
        "name": "name",
        "location": "warehouse",
        "body": "sql_text",
    },
    "job": {
        "table": "remote_jobs",
        "name": "name",
        "location": "creator_user_name",
        "body": "",
    },
    "pipeline": {
        "table": "remote_pipelines",
        "name": "name",
        "location": "target_schema",
        "body": "",
    },
    "stream": {
        "table": "remote_streams",
        "name": "qualified_name",
        "location": "source_table_fqn",
        "body": "",
    },
    "streamlit_app": {
        "table": "remote_streamlit_apps",
        "name": "qualified_name",
        "location": "main_file",
        "body": "",
    },
}


def _normalise_kinds(raw: Any) -> list[str]:
    """Coerce the LLM-supplied ``kinds`` argument to a canonical list.

    Accepts None (all kinds), a single string, or a list. Filters
    unknown values silently so a stray ``notebooks`` (plural) or
    ``streamlit`` (short form) does not blow up the call. Returns an
    empty list when nothing maps — caller treats that as "skip".
    """
    if raw is None:
        return list(_KNOWN_KINDS)
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, (list, tuple)):
        return list(_KNOWN_KINDS)
    out: list[str] = []
    aliases = {
        "notebooks": "notebook",
        "queries": "query",
        "jobs": "job",
        "pipelines": "pipeline",
        "streams": "stream",
        "streamlit": "streamlit_app",
        "streamlits": "streamlit_app",
        "streamlit_apps": "streamlit_app",
    }
    for entry in raw:
        if not entry:
            continue
        key = aliases.get(str(entry).lower(), str(entry).lower())
        if key in _KNOWN_KINDS and key not in out:
            out.append(key)
    return out


def _try_hybrid_search() -> Any | None:
    """Construct an :class:`HybridAssetSearch` wrapper, or None on failure.

    The chromadb dependency is in the ``docs-extended`` optional
    install; an install without it falls back to the FTS5-only path
    inside this module rather than crashing the tool.
    """
    try:
        from amx.assets.rag import AssetRAGStore
        from amx.assets.search import HybridAssetSearch
    except Exception:
        return None
    try:
        rag_store = AssetRAGStore()
    except Exception:
        return None
    return HybridAssetSearch, rag_store


def _fts_only_search(
    conn: Any,
    *,
    query: str,
    kind: str,
    profile: str,
    limit: int,
) -> list[dict[str, Any]]:
    """FTS5-only candidate fetch when AssetRAGStore is unavailable.

    Mirrors ``HybridAssetSearch._fts_candidates`` but returns the
    minimal dict shape the tool serialises directly, without the
    semantic rerank step.
    """
    from amx.assets.search import KIND_TO_FTS_TABLE, _build_fts_match

    fts_table = KIND_TO_FTS_TABLE.get(kind)
    if not fts_table:
        return []
    match_expr = _build_fts_match(query)
    if not match_expr:
        return []
    spec = _KIND_TABLE_SPEC[kind]
    name_field = spec["name"]
    location_field = spec["location"]
    body_field = spec["body"] or name_field
    try:
        rows = conn.execute(
            f"""
            SELECT t.id, t.profile_name, t.{name_field},
                   COALESCE(t.{location_field}, '') AS location,
                   SUBSTR(COALESCE(t.{body_field}, ''), 1, 600) AS snippet,
                   bm25({fts_table}) AS score
            FROM {fts_table} f
            JOIN {spec["table"]} t ON t.id = f.rowid
            WHERE {fts_table} MATCH ?
              AND t.profile_name = ?
            ORDER BY score ASC
            LIMIT ?
            """,
            (match_expr, profile, limit),
        ).fetchall()
    except Exception:
        return []
    return [
        {
            "remote_id": int(r[0]),
            "profile": str(r[1]),
            "name": str(r[2] or ""),
            "location": str(r[3] or ""),
            "snippet": str(r[4] or ""),
            "score": float(r[5] or 0.0),
            "match_type": "keyword_strict",
        }
        for r in rows
    ]


def _job_summary(conn: Any, *, remote_id: int) -> dict[str, Any]:
    """Return tasks + recent-runs summary for a remote_jobs row.

    Jobs are the one asset kind without a body field; inline-ing the
    task list here lets ``search_assets`` answer "what does this job
    do" in a single tool call. Falls back to an empty payload when
    the job has been deleted between the FTS hit and this read
    (re-ingest racing).
    """
    try:
        tasks = conn.execute(
            """
            SELECT task_key, task_type, notebook_path, sql_query_id
            FROM remote_job_tasks
            WHERE job_id_fk = ?
            ORDER BY task_key
            """,
            (int(remote_id),),
        ).fetchall()
    except Exception:
        tasks = []
    try:
        runs = conn.execute(
            """
            SELECT state_result, start_time, execution_duration_ms
            FROM remote_job_runs
            WHERE job_id_fk = ?
            ORDER BY start_time DESC
            LIMIT 3
            """,
            (int(remote_id),),
        ).fetchall()
    except Exception:
        runs = []
    return {
        "task_count": len(tasks),
        "tasks": [
            {
                "key": str(r[0] or ""),
                "type": str(r[1] or ""),
                "notebook_path": str(r[2] or "") if r[2] else None,
                "sql_query_id": str(r[3] or "") if r[3] else None,
            }
            for r in tasks
        ],
        "recent_runs": [
            {
                "status": str(r[0] or ""),
                "started_at": str(r[1] or ""),
                "duration_ms": int(r[2]) if r[2] is not None else None,
            }
            for r in runs
        ],
    }


def _excerpt_from_raw_row(
    conn: Any,
    *,
    kind: str,
    remote_id: int,
    query: str,
    cap: int,
) -> str:
    """Best-effort body excerpt around the first query-token occurrence.

    Used when ``HybridAssetSearch._minimal_hits_from_fts`` returns
    ``text=""`` (asset row exists in FTS but its chunks were never
    embedded in Chroma). The LLM needs a concrete excerpt to cite the
    asset; without one it tends to dismiss the hit as "weak". Returns
    "" if the asset kind has no body column.
    """
    spec = _KIND_TABLE_SPEC.get(kind)
    if not spec or not spec.get("body"):
        # Jobs / pipelines / streams / streamlit don't have a body
        # column — the name + location already on the hit are enough.
        return ""
    table = spec["table"]
    body_field = spec["body"]
    try:
        row = conn.execute(
            f"SELECT COALESCE({body_field}, '') FROM {table} WHERE id = ?",
            (remote_id,),
        ).fetchone()
    except Exception:
        return ""
    if row is None:
        return ""
    body = str(row[0] or "")
    if not body:
        return ""
    needle = (query or "").strip().lower()
    if needle:
        # Find the first occurrence of any query token (longest first
        # so ``dummy_schema`` beats ``dummy``).
        tokens = sorted(
            {t for t in needle.replace("_", " ").split() if len(t) >= 2},
            key=len,
            reverse=True,
        )
        lower_body = body.lower()
        for tok in tokens:
            pos = lower_body.find(tok)
            if pos >= 0:
                start = max(0, pos - cap // 4)
                end = min(len(body), pos + cap - cap // 4)
                excerpt = body[start:end].strip()
                if start > 0:
                    excerpt = "…" + excerpt
                if end < len(body):
                    excerpt = excerpt + "…"
                return excerpt
    # No token landed — return the head of the body.
    return body[:cap].rstrip() + ("…" if len(body) > cap else "")


class _AssetsToolsMixin:
    """Ingested-asset retrieval tools for the /ask tool-calling agent."""

    # Provided by the host ``ToolBox`` instance.
    cfg: AMXConfig
    db_profiles: list[str]

    def _tool_search_assets(
        self,
        query: str,
        kinds: Any = None,
        n_results: int = 5,
    ) -> dict[str, Any]:
        """Search ingested Databricks/Snowflake assets by free-form query.

        Returns hits across the requested ``kinds`` (notebooks,
        queries, jobs, pipelines, streams, streamlit_apps), scoped to
        every profile in :attr:`db_profiles`. Hits are merged across
        kinds and profiles and ranked best-first by hybrid
        keyword + semantic score.
        """
        q = (query or "").strip()
        if not q:
            return {"hits": [], "count": 0, "reason": "empty_query"}
        n = max(1, min(int(n_results or 5), 10))
        target_kinds = _normalise_kinds(kinds)
        if not target_kinds:
            return {"hits": [], "count": 0, "reason": "no_valid_kinds"}

        from amx.storage.sqlite_store import history_store

        store = history_store()
        if store is None:
            return {"hits": [], "count": 0, "reason": "no_history_store"}

        scope = list(self.db_profiles)
        if not scope:
            return {"hits": [], "count": 0, "reason": "no_scope"}

        hybrid_pair = _try_hybrid_search()
        all_hits: list[dict[str, Any]] = []

        # ``HybridAssetSearch`` opens its own SQLite connection via
        # the store contextmanager — share one for the whole call so
        # we don't re-open per (profile, kind) pair.
        with store._connect() as conn:  # noqa: SLF001
            for profile in scope:
                for kind in target_kinds:
                    candidate_limit = max(n * 2, 5)
                    if hybrid_pair is not None:
                        HybridAssetSearch, rag_store = hybrid_pair
                        searcher = HybridAssetSearch(conn, rag_store)
                        try:
                            hits = searcher.search(
                                q,
                                kind=kind,
                                profile=profile,
                                limit=candidate_limit,
                                mode="auto",
                            )
                        except Exception:
                            hits = []
                        for h in hits:
                            text = (h.text or "").strip()
                            match_type = str(
                                h.metadata.get("match_type") or "keyword_strict"
                            )
                            # ``_minimal_hits_from_fts`` returns ``text=""``
                            # for FTS hits whose chunks are not yet embedded
                            # in Chroma. Without an excerpt the LLM cannot
                            # confirm relevance and tends to dismiss the
                            # hit ("weak match, ignoring"). Pull a small
                            # excerpt around the first occurrence of the
                            # query token from the raw source row so the
                            # LLM has something concrete to cite.
                            if not text and match_type == "keyword_strict":
                                text = _excerpt_from_raw_row(
                                    conn,
                                    kind=str(h.kind),
                                    remote_id=int(h.remote_id),
                                    query=q,
                                    cap=600,
                                )
                            if len(text) > 600:
                                text = text[:600].rstrip() + "…"
                            hit_dict: dict[str, Any] = {
                                "remote_id": int(h.remote_id),
                                "profile": str(h.profile),
                                "kind": str(h.kind),
                                "name": str(h.name or h.chunk_id),
                                "location": str(
                                    h.metadata.get("workspace_path")
                                    or h.metadata.get("warehouse")
                                    or h.metadata.get("source_table_fqn")
                                    or ""
                                ),
                                "snippet": text,
                                "score": float(getattr(h, "score", 0.0)),
                                "match_type": match_type,
                            }
                            # Jobs have no body excerpt; inline the task
                            # list + run summary so the LLM does not need
                            # a follow-up describe_asset to talk about
                            # what the job does.
                            if str(h.kind) == "job":
                                hit_dict["job_details"] = _job_summary(
                                    conn, remote_id=int(h.remote_id)
                                )
                            all_hits.append(hit_dict)
                    else:
                        # No chromadb — fall through to FTS5-only so
                        # the tool still surfaces keyword matches.
                        for hit in _fts_only_search(
                            conn,
                            query=q,
                            kind=kind,
                            profile=profile,
                            limit=candidate_limit,
                        ):
                            hit["kind"] = kind
                            all_hits.append(hit)

        if not all_hits:
            return {
                "hits": [],
                "count": 0,
                "reason": "no_matching_assets",
                "scope_dbs": scope,
                "kinds": target_kinds,
            }

        # Ranking discipline: a literal FTS5 keyword hit (``match_type=
        # "keyword_strict"``) ALWAYS ranks above a semantic-only
        # synonym hit. Without this, a notebook that matches the query
        # term verbatim but whose chunks are not yet embedded in Chroma
        # (rerank returns empty, score collapses to 0.0 via
        # ``_minimal_hits_from_fts``) gets buried under semantic hits
        # from other kinds with cosine ~0.5–0.9. That was the
        # "dummy notebook missing" regression: FTS confirmed the match
        # but the answer surfaced unrelated _amx_users SQL queries.
        # Tier 1 = keyword_strict, Tier 0 = semantic_only; inside a
        # tier we sort by the per-bucket score (cosine for rerank,
        # negated BM25 for raw FTS so bigger-is-better).
        for h in all_hits:
            mt = h.get("match_type") or "keyword_strict"
            raw = float(h.get("score") or 0.0)
            if mt == "keyword_strict":
                bucket_score = -raw if raw < 0 else raw
                h["sort_key"] = (1, bucket_score)
            else:
                h["sort_key"] = (0, raw)
        all_hits.sort(key=lambda h: h.get("sort_key", (0, 0.0)), reverse=True)
        # Dedupe across (kind, remote_id) — a notebook can appear via
        # both rerank and FTS in the same call.
        seen: set[tuple[str, int, str]] = set()
        unique: list[dict[str, Any]] = []
        for h in all_hits:
            key = (h["kind"], int(h["remote_id"]), h["profile"])
            if key in seen:
                continue
            seen.add(key)
            h.pop("sort_key", None)
            unique.append(h)
            if len(unique) >= n:
                break

        return {
            "hits": unique,
            "count": len(unique),
            "scope_dbs": scope,
            "kinds": target_kinds,
        }

    def _tool_describe_asset(
        self,
        kind: str,
        remote_id: int | None = None,
        profile: str | None = None,
        name: str | None = None,
    ) -> dict[str, Any]:
        """Return the full ingested row for a single asset.

        Pass either ``remote_id`` or ``name`` (with ``profile`` for
        disambiguation). The ``name`` fallback exists because
        re-ingest may rotate the autoincrement ``remote_id`` while the
        asset's display name is stable; an LLM that remembers an old
        id can still reach the current row via the name lookup.
        """
        kind_key = (kind or "").lower().strip()
        # Accept plural input from the LLM the same way search_assets does.
        kind_key = {
            "notebooks": "notebook",
            "queries": "query",
            "jobs": "job",
            "pipelines": "pipeline",
            "streams": "stream",
            "streamlit": "streamlit_app",
            "streamlit_apps": "streamlit_app",
        }.get(kind_key, kind_key)
        if kind_key not in _KIND_TABLE_SPEC:
            return {"error": f"Unknown asset kind: {kind!r}", "valid_kinds": list(_KNOWN_KINDS)}

        rid: int | None
        if remote_id is None or (isinstance(remote_id, str) and not str(remote_id).strip()):
            rid = None
        else:
            try:
                rid = int(remote_id)
            except (TypeError, ValueError):
                return {"error": "remote_id must be an integer"}
            if rid <= 0:
                rid = None
        clean_name = (name or "").strip()
        if rid is None and not clean_name:
            return {"error": "Pass either remote_id (int) or name (string)"}

        from amx.storage.sqlite_store import history_store

        store = history_store()
        if store is None:
            return {"error": "no_history_store"}
        spec = _KIND_TABLE_SPEC[kind_key]
        table = spec["table"]
        name_field = spec["name"]
        with store._connect() as conn:  # noqa: SLF001
            row = None
            columns: list[str] = []
            if rid is not None:
                cursor = conn.execute(f"SELECT * FROM {table} WHERE id = ?", (rid,))
                row = cursor.fetchone()
                if cursor.description:
                    columns = [d[0] for d in cursor.description]
            if row is None and clean_name:
                # Fallback: same-name lookup. When ``profile`` is
                # supplied scope to it; otherwise restrict to the
                # current scope so a stray name doesn't leak from a
                # profile the user isn't asking about. If multiple
                # rows match return the most recently ingested one
                # and report the rest as ``ambiguous_matches``.
                where = [f"{name_field} = ?"]
                params: list[Any] = [clean_name]
                if profile:
                    where.append("profile_name = ?")
                    params.append(profile)
                elif self.db_profiles:
                    placeholders = ",".join("?" for _ in self.db_profiles)
                    where.append(f"profile_name IN ({placeholders})")
                    params.extend(self.db_profiles)
                cursor = conn.execute(
                    f"SELECT * FROM {table} WHERE {' AND '.join(where)} ORDER BY ingested_at DESC",
                    tuple(params),
                )
                rows = cursor.fetchall()
                if cursor.description:
                    columns = [d[0] for d in cursor.description]
                if rows:
                    row = rows[0]
                    if len(rows) > 1:
                        # Report the ambiguity so the LLM can disambiguate.
                        ambiguous = [
                            {
                                "remote_id": int(r[columns.index("id")]),
                                "profile_name": str(r[columns.index("profile_name")] or ""),
                            }
                            for r in rows[1:6]
                        ]
                        # Re-fetch the leading row to keep the SELECT * order.
                        payload_extra: dict[str, Any] = {"ambiguous_matches": ambiguous}
                    else:
                        payload_extra = {}
                else:
                    payload_extra = {}
            else:
                payload_extra = {}

            if row is None:
                lookup = (
                    f"remote_id={rid}" if rid is not None else f"name={clean_name!r}"
                )
                return {
                    "error": f"No {kind_key} matching {lookup}",
                    "kind": kind_key,
                    "hint": (
                        "Run search_assets again — the asset may have been "
                        "re-ingested with a new id."
                    ),
                }
            payload: dict[str, Any] = dict(zip(columns, row, strict=False))
            payload.update(payload_extra)
            current_rid = int(payload.get("id") or 0)
            # For jobs, inline tasks + recent runs so describe_asset
            # returns the same shape as the enriched search_assets hit.
            if kind_key == "job" and current_rid:
                payload["job_details"] = _job_summary(conn, remote_id=current_rid)

        # Profile filter: when the caller passed ``profile`` (Studio
        # multi-profile flows do this), confirm the row matches. This
        # catches the LLM accidentally pairing the wrong remote_id
        # with another profile's scope.
        if profile and payload.get("profile_name") and str(profile) != str(payload["profile_name"]):
            return {
                "error": (
                    f"Asset {kind_key} #{current_rid} belongs to profile "
                    f"{payload['profile_name']!r}, not {profile!r}"
                ),
                "kind": kind_key,
            }

        # Bound the body text so the LLM never gets a 200KB notebook
        # blob in a single tool result. The /ask token budget would
        # blow past the input ceiling and the answer would be empty.
        body_field = spec["body"]
        if body_field and body_field in payload:
            body = str(payload.get(body_field) or "")
            if len(body) > 8000:
                payload[body_field] = (
                    body[:8000] + f"\n…[truncated; full body is {len(body)} chars]"
                )
                payload[f"{body_field}_truncated"] = True

        # JSON-serialise the few TIMESTAMP fields as ISO strings so
        # the tool result is round-trip safe.
        for key, val in list(payload.items()):
            if val is None:
                continue
            if hasattr(val, "isoformat"):
                with contextlib.suppress(Exception):
                    payload[key] = val.isoformat()

        return {
            "kind": kind_key,
            "remote_id": current_rid,
            "asset": payload,
        }


__all__ = ["_AssetsToolsMixin"]
