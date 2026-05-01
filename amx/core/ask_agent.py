"""Tool-based ask agent primitives for headless AMX usage."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any

from amx.agents.tools import SchemaExplorer
from amx.config import AMXConfig
from amx.db.connector import DatabaseConnector, ProfilingError
from amx.search.catalog import SearchCatalog


@dataclass(frozen=True)
class ToolResult:
    tool: str
    query: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    error: str = ""


@dataclass(frozen=True)
class ReasoningTraceStep:
    step: int
    action: str
    observation: str


@dataclass(frozen=True)
class ToolAskResponse:
    question: str
    answer: str
    trace: list[ReasoningTraceStep]
    tool_results: list[ToolResult]
    strategy: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "question": self.question,
            "answer": self.answer,
            "strategy": self.strategy,
            "trace": [asdict(item) for item in self.trace],
            "tool_results": [asdict(item) for item in self.tool_results],
        }


class AskToolbox:
    """Bounded metadata tools available to loop-based ask agents."""

    def __init__(
        self,
        cfg: AMXConfig,
        catalog: SearchCatalog,
        *,
        db_factory: Callable[[], DatabaseConnector] | None = None,
        doc_query: Callable[[str, int], list[dict[str, Any]]] | None = None,
    ) -> None:
        self.cfg = cfg
        self.catalog = catalog
        self.db_profile = cfg.active_db_profile or "default"
        self._db_factory = db_factory or (lambda: DatabaseConnector(cfg.db))
        self._doc_query = doc_query

    def metadata_query(self, query: str, *, limit: int = 8) -> ToolResult:
        try:
            rows = self.catalog.search_columns(self.db_profile, query, limit=limit)
            if not rows:
                rows = self.catalog.search_tables(self.db_profile, query, limit=limit)
            return ToolResult("metadata_query", query, rows=[dict(row) for row in rows])
        except Exception as exc:
            return ToolResult("metadata_query", query, error=str(exc))

    def db_schema_lookup(self, query: str, *, limit: int = 8) -> ToolResult:
        return self.metadata_query(query, limit=limit)

    def semantic_search(self, query: str, *, limit: int = 8) -> ToolResult:
        try:
            rows = []
            for hit in self.catalog.index.query(query, db_profile=self.db_profile, n_results=limit):
                rows.append(
                    {
                        "text": hit.get("text", ""),
                        "metadata": hit.get("metadata", {}),
                        "distance": hit.get("distance"),
                    }
                )
            return ToolResult("semantic_search", query, rows=rows)
        except Exception as exc:
            return ToolResult("semantic_search", query, error=str(exc))

    def vector_rag_lookup(self, query: str, *, limit: int = 8) -> ToolResult:
        return self.semantic_search(query, limit=limit)

    def doc_rag_query(self, query: str, *, limit: int = 5) -> ToolResult:
        if self._doc_query is None:
            return ToolResult("doc_rag_query", query, error="No document RAG store is configured.")
        try:
            return ToolResult("doc_rag_query", query, rows=self._doc_query(query, limit))
        except Exception as exc:
            return ToolResult("doc_rag_query", query, error=str(exc))

    def sample_data_query(self, schema: str, table: str, *, sample_size: int = 3) -> ToolResult:
        try:
            profile = self._db_factory().profile_table(schema, table, sample_size=sample_size)
            rows = [
                {
                    "column": column.name,
                    "dtype": column.dtype,
                    "nullable": column.nullable,
                    "samples": list(column.samples),
                    "null_count": column.null_count,
                    "distinct_count": column.distinct_count,
                }
                for column in profile.columns
            ]
            return ToolResult("sample_data_query", f"{schema}.{table}", rows=rows)
        except ProfilingError as exc:
            return ToolResult("sample_data_query", f"{schema}.{table}", error=str(exc))
        except Exception as exc:
            return ToolResult("sample_data_query", f"{schema}.{table}", error=str(exc))

    def table_sample_query(self, schema: str, table: str, *, sample_size: int = 3) -> ToolResult:
        return self.sample_data_query(schema, table, sample_size=sample_size)

    def schema_explorer(self, *, schema_name: str | None = None, limit: int = 500) -> ToolResult:
        try:
            result = SchemaExplorer(self.cfg, self.catalog, db_factory=self._db_factory).explore(
                schema_name=schema_name,
                limit=limit,
            )
            summary = dict(result.get("summary") or {})
            rows = [dict(row) for row in result.get("rows", [])]
            return ToolResult(
                "SchemaExplorer",
                str(result.get("scope") or {}),
                rows=[{"row_type": "schema_explorer_summary", **summary}, *rows],
            )
        except Exception as exc:
            return ToolResult("SchemaExplorer", schema_name or "", error=str(exc))


class LoopBasedAskAgent:
    """Deterministic tool-loop ask agent — DEPRECATED.

    .. deprecated:: 0.3.0

       The canonical ``/search ask`` path is :class:`amx.search.agent.SearchAgent`,
       which performs full multi-stage interpretation, retrieval, live
       probes, verification, and synthesis. ``LoopBasedAskAgent`` was an
       earlier deterministic alternative used by ``AMXApplication.ask_with_tools``;
       it predates the unified search pipeline and is kept for one
       release cycle to avoid breaking any library users that imported
       it directly.

       New code must use ``AMXApplication.ask()`` (or the underlying
       ``SearchService``); ``ask_with_tools()`` will be removed in
       0.4.0.
    """

    _deprecation_warned = False

    def __init__(self, toolbox: AskToolbox) -> None:
        self.toolbox = toolbox
        self._emit_deprecation_warning()

    @classmethod
    def _emit_deprecation_warning(cls) -> None:
        if cls._deprecation_warned:
            return
        cls._deprecation_warned = True
        import warnings

        warnings.warn(
            "LoopBasedAskAgent is deprecated and will be removed in 0.4.0; "
            "use AMXApplication.ask() / SearchService instead.",
            DeprecationWarning,
            stacklevel=3,
        )

    def answer(self, question: str) -> ToolAskResponse:
        trace: list[ReasoningTraceStep] = []
        tool_results: list[ToolResult] = []
        strategy = self._strategy(question)

        if strategy == "inventory":
            inventory = self.toolbox.schema_explorer()
            tool_results.append(inventory)
            table_rows = [
                row for row in inventory.rows if row.get("row_type") == "schema_explorer_table"
            ]
            summary = next(
                (row for row in inventory.rows if row.get("row_type") == "schema_explorer_summary"),
                {},
            )
            trace.append(
                ReasoningTraceStep(
                    1,
                    "SchemaExplorer",
                    f"Inventory strategy selected; retrieved {len(table_rows)} table(s) and {int(summary.get('total_columns') or 0)} column(s).",
                )
            )
            if table_rows:
                return ToolAskResponse(
                    question,
                    self._inventory_answer(table_rows, summary),
                    trace,
                    tool_results,
                    strategy,
                )
            return ToolAskResponse(
                question,
                "SchemaExplorer did not find table inventory for the active namespace. Sync the catalog or provide a schema scope.",
                trace,
                tool_results,
                strategy,
            )

        metadata = self.toolbox.metadata_query(question)
        tool_results.append(metadata)
        trace.append(
            ReasoningTraceStep(
                1,
                "metadata_query",
                f"Retrieved {len(metadata.rows)} catalog row(s) for the technical metadata question.",
            )
        )

        semantic = self.toolbox.semantic_search(question)
        tool_results.append(semantic)
        trace.append(
            ReasoningTraceStep(
                2,
                "semantic_search",
                f"Retrieved {len(semantic.rows)} semantic index hit(s) for meaning-oriented evidence.",
            )
        )

        sample_result: ToolResult | None = None
        first = metadata.rows[0] if metadata.rows else {}
        schema = str(first.get("schema_name") or "")
        table = str(first.get("table_name") or "")
        if schema and table:
            sample_result = self.toolbox.sample_data_query(schema, table)
            tool_results.append(sample_result)
            trace.append(
                ReasoningTraceStep(
                    3,
                    "sample_data_query",
                    "Checked real sample/statistical signals for the top catalog candidate."
                    if not sample_result.error
                    else f"Sample check could not run: {sample_result.error}",
                )
            )

        if metadata.rows:
            top = metadata.rows[0]
            path = ".".join(
                str(top.get(part) or "")
                for part in ("schema_name", "table_name", "column_name")
                if str(top.get(part) or "")
            )
            desc = str(top.get("effective_description") or "").strip()
            answer = f"Best grounded match: `{path}`"
            if desc:
                answer += f" - {desc}"
            evidence = ["metadata_query"]
            if semantic.rows:
                evidence.append("semantic_search")
            if sample_result is not None and not sample_result.error:
                evidence.append("sample_data_query")
            answer += f" Evidence used: {', '.join(evidence[:3])}"
            return ToolAskResponse(question, answer + ".", trace, tool_results, strategy)

        return ToolAskResponse(
            question,
            "I could not find a grounded catalog match. Sync or enrich the search catalog, then retry.",
            trace,
            tool_results,
            strategy,
        )

    def _strategy(self, question: str) -> str:
        text = (question or "").lower()
        asks_inventory = any(
            token in text
            for token in (
                "how many",
                "count",
                "list",
                "show",
                "all",
                "kaç",
                "kac",
                "hangi",
                "tum",
                "tüm",
            )
        )
        asks_columns = any(
            token in text for token in ("column", "columns", "field", "fields", "kolon", "kolonlar")
        )
        asks_tables = any(token in text for token in ("table", "tables", "tablo", "tablolar"))
        if asks_inventory and (asks_tables or asks_columns):
            return "inventory"
        if any(
            token in text
            for token in ("join", "link", "relationship", "relate", "connect", "bağ", "bag")
        ):
            return "relationship"
        if any(token in text for token in ("detail", "deep", "full", "all columns", "detay")):
            return "deep_dive"
        return "definition"

    def _inventory_answer(self, rows: list[dict[str, Any]], summary: dict[str, Any]) -> str:
        table_count = int(summary.get("table_count") or len(rows))
        total_columns = int(
            summary.get("total_columns") or sum(int(row.get("column_count") or 0) for row in rows)
        )
        lines = [
            f"SchemaExplorer found **{table_count}** tables and **{total_columns}** total columns.",
            "",
            "| Schema | Table | Columns | Rows | Cluster |",
            "|---|---:|---:|---:|---|",
        ]
        for row in rows[:50]:
            lines.append(
                "| {schema} | {table} | {columns} | {rows_count} | {cluster} |".format(
                    schema=str(row.get("schema_name") or ""),
                    table=str(row.get("table_name") or ""),
                    columns=int(row.get("column_count") or 0),
                    rows_count=int(row.get("row_count") or 0),
                    cluster=str(row.get("semantic_cluster") or "Unclustered"),
                )
            )
        if len(rows) > 50:
            lines.append(f"| ... | {len(rows) - 50} more tables |  |  |  |")
        return "\n".join(lines)
