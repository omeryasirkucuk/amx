"""Cell-aware ``.ipynb`` chunker for the code RAG (PR beta).

Pins the behaviour of :func:`amx.codebase.code_rag._iter_ipynb_chunks`
and its integration with :func:`index_codebase_tree`:

* Code cells become ``ipynb_code`` chunks; markdown cells become
  ``ipynb_md`` chunks.
* Cell outputs are dropped on the floor.
* Malformed JSON falls back to the generic splitter instead of
  raising.
"""

from __future__ import annotations

import json
from pathlib import Path

import chromadb

from amx.codebase.code_rag import (
    COLLECTION,
    _iter_ipynb_chunks,
    index_codebase_tree,
)


def _make_notebook(cells: list[dict]) -> str:
    return json.dumps(
        {
            "cells": cells,
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 5,
        }
    )


def test_ipynb_code_cells_become_chunks(tmp_path: Path) -> None:
    nb = _make_notebook(
        [
            {
                "cell_type": "code",
                "source": ["import pandas as pd\n", "df = pd.DataFrame()\n"],
                "outputs": [{"output_type": "stream", "text": "ignored"}],
            },
        ]
    )
    chunks = _iter_ipynb_chunks("nb.ipynb", nb)
    assert len(chunks) == 1
    cid, text, kind = chunks[0]
    assert kind == "ipynb_code"
    assert "pandas" in text
    assert "ignored" not in text
    assert cid == "cell0"


def test_ipynb_markdown_cells_become_chunks(tmp_path: Path) -> None:
    nb = _make_notebook(
        [
            {"cell_type": "markdown", "source": "# Heading\n\nSome prose."},
            {"cell_type": "code", "source": "x = 1\n"},
        ]
    )
    chunks = _iter_ipynb_chunks("nb.ipynb", nb)
    assert len(chunks) == 2
    kinds = {c[2] for c in chunks}
    assert kinds == {"ipynb_md", "ipynb_code"}


def test_ipynb_outputs_are_dropped(tmp_path: Path) -> None:
    nb = _make_notebook(
        [
            {
                "cell_type": "code",
                "source": "print('hello')\n",
                "outputs": [
                    {"output_type": "stream", "name": "stdout", "text": "OUTPUT_LEAK"},
                    {
                        "output_type": "display_data",
                        "data": {"image/png": "BASE64_LEAK_PAYLOAD"},
                    },
                ],
            }
        ]
    )
    chunks = _iter_ipynb_chunks("nb.ipynb", nb)
    blob = "\n".join(c[1] for c in chunks)
    assert "OUTPUT_LEAK" not in blob
    assert "BASE64_LEAK_PAYLOAD" not in blob


def test_ipynb_empty_cells_are_skipped() -> None:
    nb = _make_notebook(
        [
            {"cell_type": "code", "source": "   \n\t\n"},
            {"cell_type": "markdown", "source": ""},
            {"cell_type": "code", "source": "real = 1\n"},
        ]
    )
    chunks = _iter_ipynb_chunks("nb.ipynb", nb)
    assert len(chunks) == 1
    assert chunks[0][2] == "ipynb_code"


def test_malformed_ipynb_falls_back_gracefully(tmp_path: Path) -> None:
    chunks = _iter_ipynb_chunks("broken.ipynb", "{ this is not json")
    # Returning the empty list signals "use the generic splitter".
    assert chunks == []


def test_index_codebase_tree_indexes_ipynb_cells(tmp_path: Path) -> None:
    persist = tmp_path / "chroma"
    repo = tmp_path / "repo"
    repo.mkdir()
    nb_path = repo / "demo.ipynb"
    nb_path.write_text(
        _make_notebook(
            [
                {"cell_type": "markdown", "source": "# Demo\n"},
                {
                    "cell_type": "code",
                    "source": "import pandas\n",
                    "outputs": [{"output_type": "stream", "text": "drop"}],
                },
            ]
        ),
        encoding="utf-8",
    )

    n = index_codebase_tree(repo, persist_dir=str(persist), source_root=str(repo))
    assert n >= 2

    client = chromadb.PersistentClient(path=str(persist))
    coll = client.get_collection(COLLECTION)
    rows = coll.get(where={"rel_path": "demo.ipynb"}, include=["metadatas", "documents"])
    kinds = {(m or {}).get("kind") for m in (rows.get("metadatas") or [])}
    assert kinds == {"ipynb_md", "ipynb_code"}
    docs = rows.get("documents") or []
    assert all("drop" not in d for d in docs)
