"""Suggestions must only be saved under columns that exist in the table.

Models sometimes echo the profile line as the column name
(``COLUMN: ad_nhl | type=TEXT | nulls=0/7761``). AMX stored that text as
a new "column" row next to the real one, so result tables carried rows
for columns the table does not have.
"""

from __future__ import annotations

from amx.agents.base import AgentContext, Confidence, MetadataSuggestion


def _s(column: str | None, text: str = "desc") -> MetadataSuggestion:
    return MetadataSuggestion(
        schema="s",
        table="t",
        column=column,
        suggestions=[text],
        confidence=Confidence.MEDIUM,
        reasoning="",
        source="db_profile",
    )


def _ctx(*names: str) -> AgentContext:
    return AgentContext(schema="s", table="t", db_profile={"columns": [{"name": n} for n in names]})


def test_echoed_profile_line_maps_back_to_the_real_column() -> None:
    from amx.agents._orchestrator.column_guard import keep_known_columns

    out = keep_known_columns([_s("ad_nhl | type=TEXT | nulls=0/7761")], _ctx("ad_nhl", "ad_wha"))
    assert [s.column for s in out] == ["ad_nhl"]


def test_case_and_markup_differences_map_to_the_real_column() -> None:
    from amx.agents._orchestrator.column_guard import keep_known_columns

    out = keep_known_columns([_s("`KUND_NR`"), _s("**ort**")], _ctx("kund_nr", "ORT"))
    assert [s.column for s in out] == ["kund_nr", "ORT"]


def test_unknown_columns_are_dropped_and_table_level_is_kept() -> None:
    from amx.agents._orchestrator.column_guard import keep_known_columns

    out = keep_known_columns([_s("not_a_column"), _s(None, "table")], _ctx("id"))
    assert [s.column for s in out] == [None]


def test_exact_match_wins_over_a_recovered_duplicate() -> None:
    from amx.agents._orchestrator.column_guard import keep_known_columns

    out = keep_known_columns([_s("id | type=INT", "echoed"), _s("id", "exact")], _ctx("id"))
    assert [(s.column, s.suggestions[0]) for s in out] == [("id", "exact")]


def test_without_a_profile_nothing_is_filtered() -> None:
    from amx.agents._orchestrator.column_guard import keep_known_columns

    items = [_s("anything"), _s(None)]
    assert keep_known_columns(items, AgentContext(schema="s", table="t")) == items


def test_merge_drops_rows_for_columns_the_table_does_not_have() -> None:
    from amx.agents._orchestrator.merge import merge_suggestions

    merged = merge_suggestions(
        None,
        [_s("ad_wha | type=TEXT | nulls=0/7761"), _s("ad_nhl"), _s("ghost")],
        _ctx("ad_nhl", "ad_wha"),
    )
    assert sorted(s.column for s in merged) == ["ad_nhl", "ad_wha"]
