"""Cold-path latency guards for the /ask doc/code RAG plumbing.

The PR-1..4 series wired ``search_docs`` and ``search_code`` into the
LLM tool list. The contract was:

  * No doc/code profile in scope → tool short-circuits with
    ``reason: no_*_for_scope`` and never opens a Chroma client.
  * The scope resolver is pure-Python over a couple of dicts, well
    under a millisecond.
  * The static ``ToolBox.schemas()`` listing is constant work — the
    LLM tool registry building should not balloon per question.

These benchmarks pin those guarantees. They run under
``pytest tests/perf -m perf`` and are intentionally cheap so the
nightly perf job can keep them under a second total.

Hot-path /ask benchmarks that need a live LLM live in their own
file — these here assert the cold path stays cold.
"""

from __future__ import annotations

import pytest

from amx.config import AMXConfig, DBConfig
from amx.search._agent.scope import (
    resolve_code_profiles_for_scope,
    resolve_doc_profiles_for_scope,
)
from amx.search.agent_tools import ToolBox
from amx.search.catalog import SearchCatalog


@pytest.fixture
def cold_toolbox(tmp_path):
    """ToolBox with one DB profile and no docs/code — the no-context path."""
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    cfg.db_profiles["prod_pg"] = DBConfig(backend="postgresql", host="x")
    cfg.active_db_profile = "prod_pg"
    cfg.active_db_profiles = ["prod_pg"]
    catalog = SearchCatalog(db_path=tmp_path / "search.db")
    box = ToolBox(cfg, catalog, db_profiles=["prod_pg"])
    yield box
    box.close()


@pytest.mark.perf
def test_toolbox_schemas_static(benchmark):
    """``ToolBox.schemas()`` is a constant LLM tool registry. Should be
    free — guarding against a careless caller adding a per-call DB
    lookup or schema introspection inside the static."""
    schemas = benchmark(ToolBox.schemas)
    names = {s["function"]["name"] for s in schemas}
    assert "search_docs" in names
    assert "search_code" in names


@pytest.mark.perf
def test_search_docs_no_context_short_circuit(benchmark, cold_toolbox):
    """No doc profile → must not open Chroma; payload reports the
    skip reason. Guard against an accidental eager-init regression."""
    payload = benchmark(lambda: cold_toolbox._tool_search_docs(query="hello"))
    assert payload["count"] == 0
    assert payload["reason"] == "no_docs_for_scope"


@pytest.mark.perf
def test_search_code_no_context_short_circuit(benchmark, cold_toolbox):
    """Same guarantee for ``search_code`` — empty scope, fast return."""
    payload = benchmark(lambda: cold_toolbox._tool_search_code(query="hello"))
    assert payload["count"] == 0
    assert payload["reason"] == "no_code_for_scope"


@pytest.mark.perf
def test_scope_resolver_pure_python(benchmark, tmp_path):
    """Resolver is dict comprehension over small lists. Should be in
    the microseconds; guards against someone reaching for I/O later."""
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    cfg.db_profiles["prod_pg"] = DBConfig(backend="postgresql", host="x")
    cfg.db_profiles["analytics_bq"] = DBConfig(backend="bigquery", host="bq")
    cfg.doc_profiles["contracts"] = ["/abs/contracts"]
    cfg.doc_profiles["handbook"] = ["/abs/handbook"]
    cfg.code_profiles["etl"] = "/abs/etl"
    cfg.set_doc_profile_linked_dbs("contracts", ["prod_pg"])

    def _resolve_both() -> tuple[list[str], list[str]]:
        return (
            resolve_doc_profiles_for_scope(cfg, ["prod_pg"]),
            resolve_code_profiles_for_scope(cfg, ["prod_pg"]),
        )

    docs, code = benchmark(_resolve_both)
    assert "contracts" in docs
    assert code == ["etl"]
