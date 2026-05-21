"""Normalize platform-native notebook formats into ``.ipynb`` JSON.

Supported inputs:
  * Already-valid ``.ipynb`` JSON (passthrough; coerced to a minimal nb4 shell on parse error).
  * Databricks SOURCE format (``# Databricks notebook source`` header, ``# COMMAND ----------`` cell separators, ``# MAGIC`` line prefixes).
  * Snowflake stage-stored notebooks (``.ipynb`` shape; passthrough).

Single entry point: :func:`normalize_source`.
"""

from __future__ import annotations

import json
import logging
from typing import Any

log = logging.getLogger(__name__)

_DATABRICKS_HEADER = "# Databricks notebook source"
_CELL_SEPARATOR = "# COMMAND ----------"
_MAGIC_PREFIX = "# MAGIC "
_MAGIC_BARE = "# MAGIC"


def normalize_source(
    source: str,
    *,
    hint: str,
    default_language: str = "python",
) -> str:
    """Return ``.ipynb`` JSON for ``source``.

    ``hint`` is one of ``"ipynb"`` | ``"databricks_source"``. Unknown hints
    are treated as ``"ipynb"``.
    """
    if hint == "databricks_source":
        return normalize_databricks_source(source, default_language=default_language)
    return _ensure_ipynb_shell(source)


def normalize_databricks_source(source: str, *, default_language: str = "python") -> str:
    """Convert Databricks SOURCE-format text into an ``.ipynb`` JSON string."""
    text = source.lstrip("﻿")
    if text.lstrip().startswith(_DATABRICKS_HEADER):
        # strip the header line
        first_nl = text.find("\n")
        text = text[first_nl + 1 :] if first_nl >= 0 else ""
    raw_cells = [seg.strip("\n") for seg in text.split(_CELL_SEPARATOR)]
    cells: list[dict[str, Any]] = []
    for raw in raw_cells:
        raw = raw.strip("\n")
        if not raw:
            continue
        cell = _classify_cell(raw, default_language=default_language)
        if cell is not None:
            cells.append(cell)
    return json.dumps(
        {
            "cells": cells,
            "metadata": {"language_info": {"name": default_language}},
            "nbformat": 4,
            "nbformat_minor": 5,
        }
    )


def _classify_cell(raw: str, *, default_language: str) -> dict[str, Any] | None:
    lines = raw.splitlines()
    magic_lines = [_strip_magic(line) for line in lines if line.startswith(_MAGIC_BARE)]
    if magic_lines and len(magic_lines) == len(lines):
        first = magic_lines[0].strip()
        if first.startswith("%md"):
            content = "\n".join(magic_lines[1:]) if len(magic_lines) > 1 else first[3:].strip()
            return {
                "cell_type": "markdown",
                "metadata": {"language": "markdown"},
                "source": _split_source(content),
            }
        if first.startswith("%sql"):
            body = "\n".join(magic_lines[1:]) if len(magic_lines) > 1 else first[4:].strip()
            return {
                "cell_type": "code",
                "metadata": {"language": "sql"},
                "execution_count": None,
                "outputs": [],
                "source": _split_source(body),
            }
        if first.startswith("%scala"):
            body = "\n".join(magic_lines[1:])
            return _code_cell(body, language="scala")
        if first.startswith("%r"):
            body = "\n".join(magic_lines[1:])
            return _code_cell(body, language="r")
        if first.startswith("%python"):
            body = "\n".join(magic_lines[1:])
            return _code_cell(body, language="python")
    # plain code cell
    return _code_cell(raw, language=default_language)


def _code_cell(body: str, *, language: str) -> dict[str, Any]:
    return {
        "cell_type": "code",
        "metadata": {"language": language},
        "execution_count": None,
        "outputs": [],
        "source": _split_source(body),
    }


def _strip_magic(line: str) -> str:
    if line.startswith(_MAGIC_PREFIX):
        return line[len(_MAGIC_PREFIX) :]
    if line == _MAGIC_BARE:
        return ""
    return line


def _split_source(text: str) -> list[str]:
    if not text:
        return []
    lines = text.splitlines(keepends=True)
    return lines if lines else [text]


def _ensure_ipynb_shell(source: str) -> str:
    """Either return ``source`` as-is (when it parses as nb4) or wrap in a minimal raw cell."""
    try:
        parsed = json.loads(source)
        if isinstance(parsed, dict) and "cells" in parsed:
            parsed.setdefault("nbformat", 4)
            parsed.setdefault("nbformat_minor", 5)
            parsed.setdefault("metadata", {})
            return json.dumps(parsed)
    except (json.JSONDecodeError, ValueError):
        log.debug("Falling back to raw-cell wrapper for non-ipynb source")
    return json.dumps(
        {
            "cells": [
                {
                    "cell_type": "raw",
                    "metadata": {},
                    "source": _split_source(source),
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 5,
        }
    )
