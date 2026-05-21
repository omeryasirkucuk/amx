def test_extract_table_refs_finds_three_part_names_in_python():
    from amx.codebase.analyzer import extract_table_refs
    src = """
    df = spark.read.table("analytics.gold.kpi_daily")
    cur.execute("SELECT * FROM raw.public.orders WHERE id = 1")
    """
    refs = extract_table_refs(src, language="python")
    lowered = {r.lower() for r in refs}
    assert "analytics.gold.kpi_daily" in lowered
    assert "raw.public.orders" in lowered


def test_extract_table_refs_from_sql():
    from amx.codebase.analyzer import extract_table_refs
    refs = extract_table_refs(
        "SELECT * FROM marts.gold.dashboards JOIN raw.public.orders USING (id)",
        language="sql",
    )
    lowered = {r.lower() for r in refs}
    assert "marts.gold.dashboards" in lowered
    assert "raw.public.orders" in lowered


def test_extract_table_refs_handles_two_part_names_sql():
    from amx.codebase.analyzer import extract_table_refs
    refs = extract_table_refs("SELECT * FROM public.users", language="sql")
    assert any(r.lower() == "public.users" for r in refs)


def test_extract_table_refs_dedups():
    from amx.codebase.analyzer import extract_table_refs
    refs = extract_table_refs(
        "SELECT * FROM a.b.c UNION ALL SELECT * FROM a.b.c",
        language="sql",
    )
    lowered = [r.lower() for r in refs]
    assert lowered.count("a.b.c") == 1


def test_extract_table_refs_empty_source_returns_empty_list():
    from amx.codebase.analyzer import extract_table_refs
    assert extract_table_refs("", language="sql") == []
    assert extract_table_refs("", language="python") == []


def test_extract_table_refs_unknown_language_falls_back_to_regex():
    from amx.codebase.analyzer import extract_table_refs
    # Even if we don't know the language, the regex fallback should still
    # find dotted identifiers that look like table refs.
    refs = extract_table_refs("# comment\nselect from raw.public.orders;", language="r")
    lowered = {r.lower() for r in refs}
    assert "raw.public.orders" in lowered
