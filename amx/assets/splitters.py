"""Per-kind splitters for the asset-RAG ingest path.

Each splitter consumes ONE row from the matching ``remote_*`` table
and produces a list of :class:`amx.assets.types.AssetDocument`
chunks. Chunk size is tuned to MiniLM-L6-v2's 512-token window;
notebook cells over the cap fall back to character-window splitting
with overlap so a 200-line dbt SQL cell still yields useful
embeddings.

The splitters are intentionally pure functions over the raw row
data — no DB access, no embedding calls — so unit tests can feed
hand-built strings directly.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from typing import Any

from amx.assets.types import AssetDocument

# Char window for cells / statements that overflow a single chunk.
# Matches the docs RAG defaults (1000 char chunk + 200 overlap)
# tuned for MiniLM's typical token budget.
_CHUNK_CHARS = 1000
_CHUNK_OVERLAP = 200


# ── notebook ──────────────────────────────────────────────────────


def split_notebook(
    *,
    profile: str,
    remote_id: int,
    name: str,
    source_text: str,
    workspace_path: str = "",
) -> list[AssetDocument]:
    """Split an ipynb JSON blob into per-cell chunks with header path.

    Markdown cells become single chunks; markdown headers
    (``# foo`` / ``## bar``) update the running header path so
    downstream chunks for code cells inherit "Section: foo > bar"
    context in their metadata.

    Code cells over ``_CHUNK_CHARS`` are split into char-window
    sub-chunks with overlap so large statements (a 200-line dbt SQL
    block, a chained-pandas data wrangling cell) still produce
    embeddings tight enough to match relevant queries.

    Falls back to char-window over the raw blob when JSON parse fails
    so non-Jupyter source (e.g. Snowflake SHOW NOTEBOOKS returning a
    raw SQL file) still indexes.
    """
    try:
        nb = json.loads(source_text or "")
    except (json.JSONDecodeError, TypeError):
        return _char_window_chunks(
            profile=profile,
            kind="notebook",
            remote_id=remote_id,
            name=name,
            text=source_text or "",
            extra_meta={"cell_type": "raw", "workspace_path": workspace_path},
        )

    cells = nb.get("cells") if isinstance(nb, dict) else None
    if not isinstance(cells, list) or not cells:
        return []

    out: list[AssetDocument] = []
    header_path: list[str] = []  # active markdown header stack [h1, h2, h3]
    chunk_idx = 0
    for cell in cells:
        if not isinstance(cell, dict):
            continue
        ctype = cell.get("cell_type")
        src = cell.get("source", "")
        if isinstance(src, list):
            src = "".join(s for s in src if isinstance(s, str))
        if not isinstance(src, str) or not src.strip():
            continue

        if ctype == "markdown":
            _update_header_path(header_path, src)
            text = src.strip()
            metadata = {
                "cell_type": "markdown",
                "header_path": " > ".join(p for p in header_path if p),
                "workspace_path": workspace_path,
                "asset_name": name,
            }
            for piece in _maybe_split_long(text):
                out.append(
                    AssetDocument(
                        kind="notebook",
                        profile=profile,
                        remote_id=remote_id,
                        chunk_index=chunk_idx,
                        text=piece,
                        metadata=metadata,
                    )
                )
                chunk_idx += 1
        elif ctype == "code":
            text = src.strip()
            base_meta = {
                "cell_type": "code",
                "header_path": " > ".join(p for p in header_path if p),
                "workspace_path": workspace_path,
                "asset_name": name,
            }
            for piece in _maybe_split_long(text):
                out.append(
                    AssetDocument(
                        kind="notebook",
                        profile=profile,
                        remote_id=remote_id,
                        chunk_index=chunk_idx,
                        text=piece,
                        metadata=base_meta,
                    )
                )
                chunk_idx += 1
        # raw / unknown cell_type → ignore
    return out


_HEADER_RE = re.compile(r"^(#{1,3})\s+(.+?)\s*$", re.MULTILINE)


def _update_header_path(stack: list[str], markdown_src: str) -> None:
    """Mutate ``stack`` so it reflects the deepest h1/h2/h3 found.

    Matches the docs RAG markdown splitter convention. A new h1
    resets h2 + h3 in the stack; a new h2 resets h3.
    """
    for match in _HEADER_RE.finditer(markdown_src):
        level = len(match.group(1))
        title = match.group(2).strip()
        # Pad stack to 3 levels for indexing simplicity.
        while len(stack) < 3:
            stack.append("")
        if level == 1:
            stack[0] = title
            stack[1] = ""
            stack[2] = ""
        elif level == 2:
            stack[1] = title
            stack[2] = ""
        elif level == 3:
            stack[2] = title


# ── query ─────────────────────────────────────────────────────────


def split_query(
    *,
    profile: str,
    remote_id: int,
    name: str,
    sql_text: str,
    warehouse: str = "",
    kind_value: str = "saved",
) -> list[AssetDocument]:
    """Split a SQL blob into per-statement chunks.

    Splits on ``;`` boundaries first. Statements over
    ``_CHUNK_CHARS`` fall back to char-window splitting so a 5 KB
    CTE-heavy analytical query still yields several focused chunks.
    """
    text = (sql_text or "").strip()
    if not text:
        return []

    statements = [s.strip() for s in re.split(r";\s*", text) if s.strip()]
    if not statements:
        statements = [text]

    out: list[AssetDocument] = []
    chunk_idx = 0
    for statement_no, statement in enumerate(statements):
        base_meta = {
            "warehouse": warehouse,
            "asset_name": name,
            "query_kind": kind_value,
            "statement_no": statement_no,
        }
        for piece in _maybe_split_long(statement):
            out.append(
                AssetDocument(
                    kind="query",
                    profile=profile,
                    remote_id=remote_id,
                    chunk_index=chunk_idx,
                    text=piece,
                    metadata=base_meta,
                )
            )
            chunk_idx += 1
    return out


# ── pipeline ──────────────────────────────────────────────────────


def split_pipeline(
    *,
    profile: str,
    remote_id: int,
    name: str,
    target_schema: str,
    libraries_json: str,
    edition: str = "",
    continuous: bool = False,
    photon: bool = False,
    latest_update_state: str = "",
) -> list[AssetDocument]:
    """Split a pipeline metadata blob into a small fixed set of chunks.

    Pipelines hold metadata only (notebook libraries reference paths
    that AMX indexes separately as ``notebook`` rows). We emit a
    single descriptive chunk per pipeline plus one extra chunk per
    notebook library reference so a query for ``"bronze loader"``
    surfaces both the pipeline shell and the linked notebooks.
    """
    try:
        libs = json.loads(libraries_json or "[]")
    except (json.JSONDecodeError, TypeError):
        libs = []
    notebook_paths = [
        str(lib.get("notebook", {}).get("path", ""))
        for lib in libs
        if isinstance(lib, dict) and lib.get("notebook")
    ]

    header = (
        f"Pipeline {name}\n"
        f"- target schema: {target_schema or 'n/a'}\n"
        f"- edition: {edition or 'n/a'}\n"
        f"- continuous: {continuous}\n"
        f"- photon: {photon}\n"
        f"- latest update: {latest_update_state or 'n/a'}\n"
        f"- notebook libraries: {len(notebook_paths)}"
    )
    base_meta = {"asset_name": name, "target_schema": target_schema}
    out = [
        AssetDocument(
            kind="pipeline",
            profile=profile,
            remote_id=remote_id,
            chunk_index=0,
            text=header,
            metadata=base_meta,
        )
    ]
    for i, nb_path in enumerate(notebook_paths, start=1):
        out.append(
            AssetDocument(
                kind="pipeline",
                profile=profile,
                remote_id=remote_id,
                chunk_index=i,
                text=f"Notebook library: {nb_path}",
                metadata={**base_meta, "notebook_path": nb_path},
            )
        )
    return out


# ── metadata-only (stream / streamlit / job) ─────────────────────


def split_stream(
    *,
    profile: str,
    remote_id: int,
    qualified_name: str,
    source_table_fqn: str,
    mode: str,
    stale_after: str = "",
    owner: str = "",
) -> list[AssetDocument]:
    text = (
        f"Stream {qualified_name}\n"
        f"- source table: {source_table_fqn or 'n/a'}\n"
        f"- mode: {mode or 'n/a'}\n"
        f"- stale after: {stale_after or 'n/a'}\n"
        f"- owner: {owner or 'n/a'}"
    )
    return [
        AssetDocument(
            kind="stream",
            profile=profile,
            remote_id=remote_id,
            chunk_index=0,
            text=text,
            metadata={"asset_name": qualified_name, "source_table_fqn": source_table_fqn},
        )
    ]


def split_streamlit(
    *,
    profile: str,
    remote_id: int,
    qualified_name: str,
    main_file: str,
    query_warehouse: str = "",
    root_location: str = "",
    owner: str = "",
) -> list[AssetDocument]:
    text = (
        f"Streamlit app {qualified_name}\n"
        f"- main file: {main_file or 'n/a'}\n"
        f"- warehouse: {query_warehouse or 'n/a'}\n"
        f"- root location: {root_location or 'n/a'}\n"
        f"- owner: {owner or 'n/a'}"
    )
    return [
        AssetDocument(
            kind="streamlit_app",
            profile=profile,
            remote_id=remote_id,
            chunk_index=0,
            text=text,
            metadata={"asset_name": qualified_name, "main_file": main_file},
        )
    ]


def split_job(
    *,
    profile: str,
    remote_id: int,
    name: str,
    creator: str = "",
    schedule_cron: str = "",
    schedule_timezone: str = "",
    last_run_status: str = "",
    tasks: list[dict[str, Any]] | None = None,
) -> list[AssetDocument]:
    """Emit one chunk per job (header + task summary)."""
    task_lines = []
    for t in tasks or []:
        task_key = str(t.get("task_key") or "")
        task_type = str(t.get("task_type") or "")
        target = t.get("notebook_path") or t.get("sql_query_id") or t.get("pipeline_id_fk") or ""
        if task_key:
            task_lines.append(f"  - {task_key} ({task_type}) -> {target}")
    text = (
        f"Job {name}\n"
        f"- creator: {creator or 'unknown'}\n"
        f"- schedule: {schedule_cron or 'manual'} ({schedule_timezone or 'n/a'})\n"
        f"- last run status: {last_run_status or 'unknown'}\n"
        f"- tasks: {len(task_lines)}"
    )
    if task_lines:
        text += "\n" + "\n".join(task_lines)
    return [
        AssetDocument(
            kind="job",
            profile=profile,
            remote_id=remote_id,
            chunk_index=0,
            text=text,
            metadata={"asset_name": name, "schedule_cron": schedule_cron},
        )
    ]


# ── helpers ───────────────────────────────────────────────────────


def _maybe_split_long(text: str) -> Iterator[str]:
    """Emit ``text`` whole if it fits, otherwise char-window pieces."""
    if len(text) <= _CHUNK_CHARS:
        if text.strip():
            yield text
        return
    # Char-window with overlap. Matches docs RAG's
    # RecursiveCharacterTextSplitter behaviour for non-markdown content.
    step = _CHUNK_CHARS - _CHUNK_OVERLAP
    start = 0
    while start < len(text):
        end = min(start + _CHUNK_CHARS, len(text))
        piece = text[start:end].strip()
        if piece:
            yield piece
        if end >= len(text):
            break
        start += step


def _char_window_chunks(
    *,
    profile: str,
    kind: str,
    remote_id: int,
    name: str,
    text: str,
    extra_meta: dict[str, Any] | None = None,
) -> list[AssetDocument]:
    base_meta = {"asset_name": name, **(extra_meta or {})}
    out: list[AssetDocument] = []
    for idx, piece in enumerate(_maybe_split_long(text)):
        out.append(
            AssetDocument(
                kind=kind,
                profile=profile,
                remote_id=remote_id,
                chunk_index=idx,
                text=piece,
                metadata=base_meta,
            )
        )
    return out


__all__ = [
    "split_job",
    "split_notebook",
    "split_pipeline",
    "split_query",
    "split_stream",
    "split_streamlit",
]
