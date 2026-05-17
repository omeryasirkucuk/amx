"""AMX `/lineage` — column-level lineage extraction, caching, and rendering.

The package is intentionally split:

* :mod:`amx.lineage.types` — value objects (``Edge``, ``Scope``, ``ExtractResult``)
  and the ``LineageExtractor`` protocol. No I/O.
* :mod:`amx.lineage.store` — SQLite reads/writes for ``view_definitions_cache``
  and ``lineage_artifacts``. Pure cache layer; no DB or rendering.
* :mod:`amx.lineage.extractors` — three pluggable extractors (FK, view-DDL,
  name-match heuristic). Each is cache-first; live DB calls are opt-in.
* :mod:`amx.lineage.render` — DOT generation + ``dot`` subprocess. Cross-platform.
* :mod:`amx.lineage.service` — orchestration. Fans out extractors, prompts
  before any DB round-trip, and gates render on scale guardrails.

The CLI lives in :mod:`amx.cli_support.commands.lineage` and imports only
:mod:`amx.lineage.service`. The Studio Lineage View (when added) will import
the same service module.
"""

from __future__ import annotations

__all__: list[str] = []
