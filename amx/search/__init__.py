"""Search catalog and search-agent helpers for AMX.

This package's ``__init__.py`` deliberately re-exports nothing. The
``SearchAgent``/``SearchCatalog``/``SearchService`` symbols are
imported by full path (``from amx.search.catalog import …``) at the
boot-cold call sites that actually need them. Eagerly re-exporting
them here used to drag chromadb (~400 ms) onto every CLI launch via
``amx.search.session_store`` → ``amx.search`` → ``__init__`` →
``catalog`` → ``index`` → chromadb.
"""
