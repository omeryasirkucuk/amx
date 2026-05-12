"""Single source of truth for document file extensions AMX can ingest.

Historically the upload validator (``amx/docs/uploads.py``) and the
local/remote scanner (``amx/docs/scanner.py``) each maintained their
own whitelist. The two drifted: ``.markdown`` and ``.tsv`` were
accepted on upload but silently dropped at scan time; ``.rtf`` was
scan-supported but had no deterministic loader so ingest dropped it
with a warning.

Consolidating both callers behind one frozenset (the intersection of
"we accept it on upload" AND "we can actually parse it via the loader
map") closes those gaps once and for all. New supported extensions
are added here exactly once and become visible to every layer
automatically.
"""

from __future__ import annotations

#: Extensions for which AMX has a real loader and which the upload UI
#: accepts. Both ``ACCEPTED_EXTENSIONS`` (uploads) and
#: ``SUPPORTED_EXTENSIONS`` (scanner) re-export this set; the rag
#: pipeline's ``LOADER_MAP`` must carry an entry for every extension
#: in here.
SUPPORTED_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".md",
        ".markdown",
        ".txt",
        ".pdf",
        ".docx",
        ".doc",
        ".html",
        ".htm",
        ".csv",
        ".tsv",
        ".json",
        ".yaml",
        ".yml",
        ".rst",
        ".py",
        ".xlsx",
        ".xls",
        ".pptx",
    }
)
