"""Production :class:`amx.pages.context.Resolver` implementation.

Wires the pages context-gathering layer to the live AMX subsystems:

* DB asset refs resolve through :class:`amx.db.connector.DatabaseConnector`.
* Doc profile refs resolve through :class:`amx.docs.rag.RAGStore`.
* Lineage refs resolve through :mod:`amx.lineage.store` against the
  active history store.
* Source refs resolve through the docs loader registry.

Every method is defensive: failures degrade to a one-line stub so a
single bad asset never breaks page generation.
"""

from __future__ import annotations

from pathlib import Path

from amx.config import AMXConfig
from amx.pages.types import SourceRef
from amx.utils.logging import get_logger

log = get_logger("pages.resolver")

#: Per-source byte cap so a single large attachment cannot blow the
#: composer's context budget on its own.
MAX_SOURCE_BYTES = 8 * 1024

_TEXT_EXTENSIONS = frozenset(
    {
        ".md",
        ".markdown",
        ".txt",
        ".csv",
        ".tsv",
        ".json",
        ".yaml",
        ".yml",
        ".rst",
        ".py",
        ".html",
        ".htm",
    }
)


class AMXResolver:
    """Concrete resolver that adapts the running AMX runtime."""

    def __init__(self, cfg: AMXConfig) -> None:
        self.cfg = cfg

    def resolve_db_asset(self, ref: str) -> str:
        try:
            return self._resolve_db_asset(ref)
        except Exception as exc:  # noqa: BLE001
            log.debug("resolve_db_asset(%s) failed: %s", ref, exc)
            return f"asset {ref} not found"

    def resolve_doc_profile(self, ref: str, intent: str, k: int = 5) -> list[str]:
        try:
            return self._resolve_doc_profile(ref, intent, k)
        except Exception as exc:  # noqa: BLE001
            log.debug("resolve_doc_profile(%s) failed: %s", ref, exc)
            return []

    def resolve_lineage(self, ref: str) -> str:
        try:
            return self._resolve_lineage(ref)
        except Exception as exc:  # noqa: BLE001
            log.debug("resolve_lineage(%s) failed: %s", ref, exc)
            return f"lineage {ref} not found"

    def resolve_source(self, src: SourceRef) -> str:
        try:
            return self._resolve_source(src)
        except Exception as exc:  # noqa: BLE001
            log.debug("resolve_source(%s) failed: %s", src.path, exc)
            return f"source {src.original_name} unavailable"

    # ------------------------------------------------------------------
    # Internal helpers - uncaught exceptions bubble to the public wrappers.
    # ------------------------------------------------------------------

    def _resolve_db_asset(self, ref: str) -> str:
        parts = [p for p in ref.split("/") if p]
        if not parts:
            return f"asset {ref} not found"
        profile_name = parts[0]
        profile = self.cfg.db_profiles.get(profile_name)
        if profile is None:
            return f"asset {ref} not found"
        if len(parts) == 1:
            return f"## DB profile `{profile_name}`\n\nbackend: {profile.backend}"

        database = parts[1] if len(parts) >= 2 else ""
        schema = parts[2] if len(parts) >= 3 else ""
        table_spec = parts[3] if len(parts) >= 4 else ""
        table = table_spec
        column = ""
        if "." in table_spec:
            table, column = table_spec.split(".", 1)

        from amx.db.connector import DatabaseConnector

        connector = DatabaseConnector(profile, profile_name=profile_name)
        if not schema:
            schemas = connector.list_schemas()
            head = ", ".join(schemas[:10])
            return f"## `{profile_name}/{database}`\n\nschemas: {head}"
        if not table:
            tables = connector.list_tables(schema)
            head = ", ".join(tables[:25])
            return f"## `{profile_name}/{database}/{schema}`\n\ntables: {head}"

        cols = connector.list_column_profiles(schema, table)
        comments = connector.get_column_comments(schema, table) or {}
        title = f"## `{profile_name}/{database}/{schema}/{table}`"
        if column:
            match = next((c for c in cols if c.name == column), None)
            if match is None:
                return f"{title}\n\ncolumn `{column}` not found"
            desc = comments.get(column) or ""
            body = f"column `{column}` ({getattr(match, 'data_type', '')})"
            if desc:
                body = f"{body}\ndescription: {desc}"
            return f"{title}\n\n{body}"

        lines = [title, "", "| column | type | description |", "| --- | --- | --- |"]
        for c in cols[:40]:
            desc = comments.get(c.name) or ""
            lines.append(f"| {c.name} | {getattr(c, 'data_type', '')} | {desc} |")
        return "\n".join(lines)

    def _resolve_doc_profile(self, ref: str, intent: str, k: int) -> list[str]:
        profile = ref.removeprefix("doc:").strip()
        if not profile:
            return []
        paths = self.cfg.doc_profiles.get(profile)
        if not paths:
            return []

        from amx.docs.rag import RAGStore

        store = RAGStore(source_filters=list(paths), cfg=self.cfg)
        hits = store.query(intent or profile, n_results=int(k))
        return [str(h.get("text", "")) for h in hits if h.get("text")]

    def _resolve_lineage(self, ref: str) -> str:
        artifact_ref = ref.removeprefix("lineage:").strip()
        if not artifact_ref:
            return f"lineage {ref} not found"

        from amx.lineage.store import lookup_lineage_artifact
        from amx.storage.sqlite_store import history_store

        hs = history_store()
        if hs is None:
            return f"lineage {ref} not found"
        artifact = lookup_lineage_artifact(hs, name_or_id=artifact_ref)
        if artifact is None:
            return f"lineage {ref} not found"

        name = artifact.get("name") or artifact_ref
        node_count = artifact.get("node_count", 0)
        edge_count = artifact.get("edge_count", 0)
        output_path = artifact.get("output_path") or ""
        return (
            f"## Lineage `{name}`\n\n"
            f"- nodes: {node_count}\n"
            f"- edges: {edge_count}\n"
            f"- output: {output_path}"
        )

    def _resolve_source(self, src: SourceRef) -> str:
        path = Path(src.path)
        if not path.is_file():
            return f"source {src.original_name} unavailable"
        ext = path.suffix.lower()
        text = self._load_text(path, ext)
        if not text:
            return f"source {src.original_name} (empty)"
        if len(text) > MAX_SOURCE_BYTES:
            text = text[: MAX_SOURCE_BYTES - 1] + "..."
        return f"## Source `{src.original_name}`\n\n{text}"

    @staticmethod
    def _load_text(path: Path, ext: str) -> str:
        if ext == ".xlsx":
            from amx.docs.loaders.xlsx_loader import load_xlsx

            return load_xlsx(path)
        if ext == ".eml":
            from amx.docs.loaders.eml_loader import load_eml

            return load_eml(path)
        if ext in _TEXT_EXTENSIONS:
            return path.read_text(encoding="utf-8", errors="replace")
        # Fall back to the langchain-backed loader map for binary
        # formats (pdf / docx / pptx). Each loader returns a list of
        # documents; concatenate their ``page_content`` so the LLM
        # sees the entire file.
        try:
            from amx.docs.rag import _build_loader_map

            loader_map = _build_loader_map()
            loader_cls = loader_map.get(ext)
            if loader_cls is None:
                return ""
            docs = loader_cls(str(path)).load()
        except Exception as exc:  # noqa: BLE001
            log.debug("loader for %s failed: %s", ext, exc)
            return ""
        return "\n\n".join(getattr(d, "page_content", "") for d in docs)
