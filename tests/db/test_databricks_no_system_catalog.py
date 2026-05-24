"""Bulk-metadata SQL must target ``<catalog>.information_schema``, never
``system.information_schema``.

Background: ``system.information_schema`` is the global aggregator and
needs an explicit grant the typical workspace user does not hold. Each
Unity Catalog catalog also exposes its own ``information_schema`` view
which is reachable by any principal that holds ``USE CATALOG`` on that
catalog. This test pins the adapter to the per-catalog form so a future
edit cannot silently regress and require ``system`` grants again.
"""

from __future__ import annotations

import re
from pathlib import Path

ADAPTER_SRC = Path(__file__).resolve().parents[2] / "amx" / "db" / "adapters" / "databricks.py"


def test_no_executable_system_information_schema_sql() -> None:
    src = ADAPTER_SRC.read_text(encoding="utf-8")
    # Strip comment lines and docstrings so historical references in
    # prose don't trip the guard. Anything left that matches the regex
    # is executable SQL — and that is exactly what must not exist.
    src_no_comments = re.sub(r"#[^\n]*", "", src)
    src_no_docstrings = re.sub(r'"""[\s\S]*?"""', "", src_no_comments)
    offenders = re.findall(
        r"system\.information_schema\.[a-z_]+",
        src_no_docstrings,
    )
    assert not offenders, (
        "Found executable references to system.information_schema "
        "(should target <catalog>.information_schema instead): "
        f"{offenders}"
    )


def test_info_schema_helper_quotes_catalog() -> None:
    from types import SimpleNamespace

    from amx.db.adapters.databricks import DatabricksAdapter

    assert DatabricksAdapter._info_schema("main", "tables") == "`main`.information_schema.tables"
    # backticks inside the catalog name must be doubled
    assert (
        DatabricksAdapter._info_schema("weird`name", "views")
        == "`weird``name`.information_schema.views"
    )
    # works as instance method too
    a = DatabricksAdapter.__new__(DatabricksAdapter)
    a.cfg = SimpleNamespace(catalog="my-catalog")  # type: ignore[attr-defined]
    assert a._info_schema("my-catalog", "columns") == "`my-catalog`.information_schema.columns"
