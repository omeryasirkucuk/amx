import json


def test_databricks_command_format_to_ipynb():
    from amx.codebase.notebook_normalize import normalize_databricks_source

    source = (
        "# Databricks notebook source\n"
        "# MAGIC %md\n# MAGIC ## Header\n"
        "\n# COMMAND ----------\n\n"
        "print('hello')\n"
        "\n# COMMAND ----------\n\n"
        "# MAGIC %sql\n# MAGIC SELECT 1\n"
    )
    nb = json.loads(normalize_databricks_source(source, default_language="python"))
    cells = nb["cells"]
    assert len(cells) == 3
    assert cells[0]["cell_type"] == "markdown"
    assert "Header" in "".join(cells[0]["source"])
    assert cells[1]["cell_type"] == "code"
    assert "print('hello')" in "".join(cells[1]["source"])
    assert cells[2]["cell_type"] == "code"  # SQL cell stored as code with cell metadata
    assert cells[2].get("metadata", {}).get("language") == "sql"


def test_passthrough_when_already_ipynb():
    from amx.codebase.notebook_normalize import normalize_source

    src = json.dumps(
        {
            "cells": [
                {
                    "cell_type": "code",
                    "source": ["print(1)"],
                    "metadata": {},
                    "outputs": [],
                    "execution_count": None,
                }
            ],
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {},
        }
    )
    out = normalize_source(src, hint="ipynb")
    assert json.loads(out)["cells"][0]["source"] == ["print(1)"]


def test_normalize_source_dispatches_on_hint():
    from amx.codebase.notebook_normalize import normalize_source

    src = "# Databricks notebook source\n# COMMAND ----------\nprint('x')\n"
    out = normalize_source(src, hint="databricks_source", default_language="python")
    assert json.loads(out)["cells"]


def test_invalid_ipynb_falls_back_to_single_cell():
    from amx.codebase.notebook_normalize import normalize_source

    out = normalize_source("not json at all", hint="ipynb")
    nb = json.loads(out)
    assert len(nb["cells"]) == 1
    assert nb["cells"][0]["cell_type"] == "raw"
