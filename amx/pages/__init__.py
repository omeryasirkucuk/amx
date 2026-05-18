"""AMX documentation-pages module.

Orchestrates asset selection -> context gathering -> LLM composition
-> persistence -> markdown / PDF export. Sits between transport
(FastAPI router + CLI commands) and storage (the four
documentation_pages* tables) so neither transport layer reaches past
:mod:`amx.pages.service`.
"""
