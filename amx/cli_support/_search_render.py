"""Search results renderer for ``amx search`` and ``amx ask``.

Extracted from :mod:`amx.cli_support.commands.search` so the 180-LOC
``_render_search_rows`` (which shapes ranked-list answers, the schema
explorer fallback, and inventory rows into Rich tables with optional
debug columns) lives in one focused module.

``search.py`` re-exports the function so existing test imports
(``tests/test_cli_integration.py:648-741``) keep working unchanged.
"""

from __future__ import annotations

from typing import Any

from rich import box
from rich.table import Table
from rich.text import Text

from amx.search.confidence import band as _confidence_band
from amx.search.confidence import band_style as _confidence_band_style
from amx.utils.console import console, info, render_table
from amx.utils.terminal_theme import accent_color, info_color


def _render_search_rows(
    rows: list[dict[str, Any]],
    *,
    answer_shape: str = "",
    debug: bool = False,
) -> None:
    if not rows:
        info("No results.")
        return
    first = rows[0]
    if first.get("row_type") == "joinable_table" or "target_table_name" in first:
        render_table(
            "Joinable tables",
            [
                "Base table",
                "Target schema",
                "Target table",
                "Base columns",
                "Target columns",
                "Type",
                "Band",
                "Score",
                "Source",
            ],
            [
                [
                    f"{row.get('schema_name', '')}.{row.get('table_name', '')}",
                    row.get("target_schema_name", ""),
                    row.get("target_table_name", ""),
                    row.get("left_column", ""),
                    row.get("right_column", ""),
                    row.get("relationship_type", ""),
                    row.get("confidence_band", ""),
                    f"{float(row.get('score') or 0):.2f}",
                    row.get("source", ""),
                ]
                for row in rows
            ],
        )
        return
    if "left_column" in first:
        render_table(
            "Join candidates",
            ["Left column", "Right column", "Type", "Band", "Score", "Source"],
            [
                [
                    row.get("left_column", ""),
                    row.get("right_column", ""),
                    row.get("relationship_type", ""),
                    row.get("confidence_band", ""),
                    f"{float(row.get('score') or 0):.2f}",
                    row.get("source", ""),
                ]
                for row in rows
            ],
        )
        return
    if first.get("row_type") == "schema_explorer_table":
        # Inventory rows have no useful score; render as a focused inventory table
        # rather than the generic Search matches grid.
        inventory = Table(title="Inventory", show_lines=True, box=box.SIMPLE_HEAVY)
        inventory.add_column("Schema", style=info_color(), no_wrap=True)
        inventory.add_column("Table", style=info_color(), no_wrap=True)
        inventory.add_column("Columns", style=info_color(), no_wrap=True, justify="right")
        inventory.add_column("Rows", style=info_color(), no_wrap=True, justify="right")
        inventory.add_column("Cluster", style="white", overflow="fold", max_width=40)
        for row in rows:
            inventory.add_row(
                str(row.get("schema_name", "") or ""),
                str(row.get("table_name", "") or ""),
                str(int(row.get("column_count") or 0)),
                str(int(row.get("row_count") or 0)),
                str(row.get("semantic_cluster") or "Unclustered"),
            )
        console.print(inventory)
        return
    if answer_shape == "table_summary":
        # Focused key-columns view for "what is table X" answers. The retrieval
        # path puts a single row_type="table" header at the top followed by
        # row_type="column" entries; we drop the header row (it has no
        # column_name and used to render as a noisy "-" line) and lift the
        # schema/table into the panel title so columns are easier to read.
        column_rows = [row for row in rows if str(row.get("row_type") or "") != "table"]
        if not column_rows:
            column_rows = rows
        title = "Key columns"
        for row in rows:
            schema_name = str(row.get("schema_name") or "")
            table_name = str(row.get("table_name") or "")
            if schema_name and table_name:
                title = f"Key columns — {schema_name}.{table_name}"
                break
        summary_table = Table(title=title, show_lines=False, box=box.SIMPLE_HEAVY)
        summary_table.add_column("Column", style=info_color(), no_wrap=True)
        summary_table.add_column("Type", style="white", no_wrap=True)
        summary_table.add_column("Description", style="white", overflow="fold", max_width=72)
        rendered = 0
        for row in column_rows[:12]:
            column_name = str(row.get("column_name") or "")
            if not column_name:
                continue
            data_type = str(row.get("data_type") or row.get("dtype") or "")
            description = str(row.get("effective_description") or "")
            summary_table.add_row(column_name, data_type, Text(description))
            rendered += 1
        if rendered:
            console.print(summary_table)
        return
    # Default: ranked Search matches. Drop rows whose score is exactly 0.00 —
    # those are inventory/diagnostic rows that leaked into the result set and
    # only add noise (every line saying "0.00" with no description).
    visible: list[dict[str, Any]] = []
    for row in rows:
        score = float(row.get("rank_score") or row.get("score") or 0)
        if score > 0.0:
            visible.append(row)
    if not visible:
        return
    # 0.11.0: when results span multiple DB profiles, surface the
    # originating profile so the user can tell at a glance which DB
    # each row came from. The rows have ``db_profile`` stamped by the
    # catalog read methods.
    profiles_in_view = {
        str(row.get("db_profile") or "") for row in visible if row.get("db_profile")
    }
    show_profile_col = len(profiles_in_view) > 1
    table = Table(title="Search matches", show_lines=True, box=box.SIMPLE_HEAVY)
    if show_profile_col:
        table.add_column("Profile", style=accent_color(), no_wrap=True)
    table.add_column("Schema.Table", style=info_color(), no_wrap=True)
    table.add_column("Match", no_wrap=True)
    table.add_column("Why", style="white", overflow="fold", max_width=32)
    table.add_column("Rows", style=info_color(), no_wrap=True, justify="right")
    table.add_column("Cols", style=info_color(), no_wrap=True, justify="right")
    table.add_column("Description", style="white", overflow="fold", max_width=60)
    if debug:
        table.add_column("Score", style=info_color(), no_wrap=True, justify="right")
        table.add_column("Source", style=info_color(), no_wrap=True)
        table.add_column("Conf", style=info_color(), no_wrap=True)
    for row in visible:
        score = float(row.get("rank_score") or row.get("score") or 0)
        match_label = _confidence_band(score)
        match_text = Text(match_label, style=_confidence_band_style(match_label))
        matched_cols = row.get("matched_columns") or []
        if isinstance(matched_cols, list) and matched_cols:
            why = ", ".join(str(c) for c in matched_cols if c)
        else:
            why = str(row.get("column_name") or "—")
        schema_name = str(row.get("schema_name", "") or "")
        table_name = str(row.get("table_name", "") or "")
        st = f"{schema_name}.{table_name}".strip(".") or "—"
        rows_value = row.get("row_count")
        cols_value = row.get("column_count")
        rows_str = (
            str(int(rows_value)) if isinstance(rows_value, (int, float)) and rows_value else "—"
        )
        cols_str = (
            str(int(cols_value)) if isinstance(cols_value, (int, float)) and cols_value else "—"
        )
        desc = str(row.get("effective_description", "") or "")
        cells: list[Any] = []
        if show_profile_col:
            cells.append(str(row.get("db_profile") or "—"))
        cells += [
            st,
            match_text,
            Text(why),
            rows_str,
            cols_str,
            Text(desc),
        ]
        if debug:
            cells.extend(
                [
                    f"{score:.2f}",
                    str(row.get("effective_source_kind", "") or ""),
                    str(row.get("current_confidence", "") or ""),
                ]
            )
        table.add_row(*cells)
    console.print(table)
