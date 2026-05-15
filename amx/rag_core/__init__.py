"""Shared helpers for the retrieval pipelines.

The Document RAG (``amx.docs.rag``), Code RAG (``amx.codebase.code_rag``),
and Catalog Search (``amx.search.index``) pipelines re-implement
variations of the same Chroma-backed retrieval loop. This package
collects the genuinely shared bits so a single fix can cover all three.

For now the only resident is :mod:`amx.rag_core.collection_identity`,
which standardises the metadata recorded on every collection and the
mismatch-vs-active-config check that runs at reopen.
"""
