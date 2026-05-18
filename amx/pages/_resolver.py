"""Production :class:`amx.pages.context.Resolver` implementation.

Wires the pages context-gathering layer to the local AMX subsystems:

* DB asset refs resolve through :class:`amx.search.catalog.SearchCatalog`
  — every drill is a local SQLite read; no live DB round-trip.
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
        # Reads exclusively from the persistent catalog cache via
        # SearchCatalog (same surface the Pages picker uses). No
        # DatabaseConnector instantiation, no live round-trip — a
        # 5,000-table Databricks workspace turns into a single local
        # SQLite query per drill level. Cold cache returns a concise
        # stub so the LLM sees a clear "not in cache" marker instead
        # of fabricating columns.
        parts = [p for p in ref.split("/") if p]
        if not parts:
            return f"asset {ref} not found"
        profile_name = parts[0]
        if profile_name not in self.cfg.db_profiles:
            return f"asset {ref} not found"
        backend = self.cfg.db_profiles[profile_name].backend
        if len(parts) == 1:
            return f"## DB profile `{profile_name}`\n\nbackend: {backend}"

        database = parts[1] if len(parts) >= 2 else ""
        schema = parts[2] if len(parts) >= 3 else ""
        table_spec = parts[3] if len(parts) >= 4 else ""
        table = table_spec
        column = ""
        if "." in table_spec:
            table, column = table_spec.split(".", 1)

        from amx.search.catalog import SearchCatalog

        catalog = SearchCatalog.from_history_store()
        if catalog is None:
            return f"asset {ref} not in cache"

        if not schema:
            schemas = catalog.fetch_distinct_schemas(profile_name, database_name=database or None)
            if not schemas:
                return f"## `{profile_name}/{database}`\n\nnot in cache"
            head = ", ".join(s["name"] for s in schemas[:10])
            return f"## `{profile_name}/{database}`\n\nschemas: {head}"

        if not table:
            tables = catalog.fetch_distinct_tables_in_schema(
                profile_name,
                schema_name=schema,
                database_name=database or None,
            )
            if not tables:
                return f"## `{profile_name}/{database}/{schema}`\n\nnot in cache"
            head = ", ".join(t["name"] for t in tables[:25])
            return f"## `{profile_name}/{database}/{schema}`\n\ntables: {head}"

        cols = catalog.fetch_columns_for_table(
            profile_name,
            schema_name=schema,
            table_name=table,
            database_name=database or None,
        )
        title = f"## `{profile_name}/{database}/{schema}/{table}`"
        if not cols:
            return f"{title}\n\nnot in cache"

        if column:
            match = next((c for c in cols if c.get("name") == column), None)
            if match is None:
                return f"{title}\n\ncolumn `{column}` not found"
            body = f"column `{column}` ({match.get('dtype', '')})"
            return f"{title}\n\n{body}"

        lines = [title, "", "| column | type |", "| --- | --- |"]
        for c in cols[:40]:
            lines.append(f"| {c.get('name', '')} | {c.get('dtype', '')} |")
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
