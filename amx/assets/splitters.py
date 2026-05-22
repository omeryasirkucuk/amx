"""Per-kind splitters for the asset-RAG ingest path.

Each splitter consumes ONE row from the matching ``remote_*`` table
and produces a list of :class:`amx.assets.types.AssetDocument`
chunks. The strategy + chunk sizes are user-tunable via
:class:`amx.assets.chunking_config.AssetChunkingConfig`; defaults
are conservative (``whole`` per asset) so a user who never touches
the knob gets coarse but predictable embeddings.

The splitters are intentionally pure functions over the raw row
data — no DB access, no embedding calls — so unit tests can feed
hand-built strings directly.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from typing import Any

from amx.assets.chunking_config import (
    DEFAULT_CHUNK_CHARS,
    DEFAULT_CHUNK_OVERLAP,
    NotebookChunkingConfig,
    PipelineChunkingConfig,
    QueryChunkingConfig,
)
from amx.assets.types import AssetDocument

# ── notebook ──────────────────────────────────────────────────────


def split_notebook(
    *,
    profile: str,
    remote_id: int,
    name: str,
    source_text: str,
    workspace_path: str = "",
    config: NotebookChunkingConfig | None = None,
) -> list[AssetDocument]:
    """Split a notebook source blob per the active chunking strategy.

    * ``strategy='whole'`` (default) — one chunk per notebook, no
      parsing. Predictable but coarse for long notebooks.
    * ``strategy='cell'`` — parse ipynb JSON, emit one chunk per
      markdown / code cell, preserve header path in metadata,
      char-window-split cells longer than ``chunk_chars``.
      Falls back to char-window when JSON parse fails so non-Jupyter
      source (e.g. raw SQL files) still indexes.
    * ``strategy='char_window'`` — ignore cell boundaries; pure
      char-window over the raw source text.
    """
    cfg = config or NotebookChunkingConfig()
    chunk_chars = max(int(cfg.chunk_chars or DEFAULT_CHUNK_CHARS), 200)
    chunk_overlap = max(int(cfg.chunk_overlap or DEFAULT_CHUNK_OVERLAP), 0)

    if cfg.strategy == "whole":
        text = (source_text or "").strip()
        if not text:
            return []
        return [
            AssetDocument(
                kind="notebook",
                profile=profile,
                remote_id=remote_id,
                chunk_index=0,
                text=text,
                metadata={
                    "cell_type": "whole",
                    "workspace_path": workspace_path,
                    "asset_name": name,
                },
            )
        ]

    if cfg.strategy == "char_window":
        return _char_window_chunks(
            profile=profile,
            kind="notebook",
            remote_id=remote_id,
            name=name,
            text=source_text or "",
            chunk_chars=chunk_chars,
            chunk_overlap=chunk_overlap,
            extra_meta={"cell_type": "char_window", "workspace_path": workspace_path},
        )

    # strategy == "cell"
    try:
        nb = json.loads(source_text or "")
    except (json.JSONDecodeError, TypeError):
        return _char_window_chunks(
            profile=profile,
            kind="notebook",
            remote_id=remote_id,
            name=name,
            text=source_text or "",
            chunk_chars=chunk_chars,
            chunk_overlap=chunk_overlap,
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
            for piece in _maybe_split_long(text, chunk_chars, chunk_overlap):
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
            for piece in _maybe_split_long(text, chunk_chars, chunk_overlap):
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
    config: QueryChunkingConfig | None = None,
) -> list[AssetDocument]:
    """Split a SQL blob per the active chunking strategy.

    * ``strategy='whole'`` (default) — full SQL embedded as one chunk.
    * ``strategy='statement'`` — split on ``;`` boundaries; statements
      over ``chunk_chars`` get char-window sub-split.
    * ``strategy='char_window'`` — ignore boundaries; pure
      char-window.
    """
    cfg = config or QueryChunkingConfig()
    chunk_chars = max(int(cfg.chunk_chars or DEFAULT_CHUNK_CHARS), 200)
    chunk_overlap = max(int(cfg.chunk_overlap or DEFAULT_CHUNK_OVERLAP), 0)

    text = (sql_text or "").strip()
    if not text:
        return []

    base_meta_root = {
        "warehouse": warehouse,
        "asset_name": name,
        "query_kind": kind_value,
    }

    if cfg.strategy == "whole":
        return [
            AssetDocument(
                kind="query",
                profile=profile,
                remote_id=remote_id,
                chunk_index=0,
                text=text,
                metadata={**base_meta_root, "statement_no": -1},
            )
        ]

    if cfg.strategy == "char_window":
        out: list[AssetDocument] = []
        for idx, piece in enumerate(_maybe_split_long(text, chunk_chars, chunk_overlap)):
            out.append(
                AssetDocument(
                    kind="query",
                    profile=profile,
                    remote_id=remote_id,
                    chunk_index=idx,
                    text=piece,
                    metadata={**base_meta_root, "statement_no": -1},
                )
            )
        return out

    # strategy == "statement"
    statements = [s.strip() for s in re.split(r";\s*", text) if s.strip()]
    if not statements:
        statements = [text]

    out_stmt: list[AssetDocument] = []
    chunk_idx = 0
    for statement_no, statement in enumerate(statements):
        meta = {**base_meta_root, "statement_no": statement_no}
        for piece in _maybe_split_long(statement, chunk_chars, chunk_overlap):
            out_stmt.append(
                AssetDocument(
                    kind="query",
                    profile=profile,
                    remote_id=remote_id,
                    chunk_index=chunk_idx,
                    text=piece,
                    metadata=meta,
                )
            )
            chunk_idx += 1
    return out_stmt


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
    config: PipelineChunkingConfig | None = None,
) -> list[AssetDocument]:
    """Split a pipeline metadata blob per the active strategy.

    * ``strategy='metadata'`` (default) — one chunk for the
      pipeline header plus one per notebook library reference, so a
      query for ``"bronze loader"`` surfaces both the pipeline
      shell and the linked notebooks.
    * ``strategy='whole'`` — one chunk with everything inlined.
    """
    cfg = config or PipelineChunkingConfig()
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

    if cfg.strategy == "whole":
        nb_block = "\n".join(f"  - {p}" for p in notebook_paths) if notebook_paths else "  (none)"
        text = header + "\nNotebook libraries:\n" + nb_block
        return [
            AssetDocument(
                kind="pipeline",
                profile=profile,
                remote_id=remote_id,
                chunk_index=0,
                text=text,
                metadata=base_meta,
            )
        ]

    # strategy == "metadata"
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


def _maybe_split_long(
    text: str,
    chunk_chars: int = DEFAULT_CHUNK_CHARS,
    chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
) -> Iterator[str]:
    """Emit ``text`` whole if it fits ``chunk_chars``, otherwise yield
    char-window pieces of ``chunk_chars`` with ``chunk_overlap``."""
    if len(text) <= chunk_chars:
        if text.strip():
            yield text
        return
    step = max(chunk_chars - chunk_overlap, 1)
    start = 0
    while start < len(text):
        end = min(start + chunk_chars, len(text))
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
    chunk_chars: int = DEFAULT_CHUNK_CHARS,
    chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
    extra_meta: dict[str, Any] | None = None,
) -> list[AssetDocument]:
    base_meta = {"asset_name": name, **(extra_meta or {})}
    out: list[AssetDocument] = []
    for idx, piece in enumerate(_maybe_split_long(text, chunk_chars, chunk_overlap)):
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
