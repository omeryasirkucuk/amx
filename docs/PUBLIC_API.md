# AMX public Python API

This document defines the **stable Python API contract** for AMX. Anything documented here will follow [Semantic Versioning](https://semver.org/) starting from `1.0.0`:

- **Patch** (`1.0.x`) — bug fixes, no public-API changes.
- **Minor** (`1.x.0`) — additive only (new names, new optional kwargs, new optional fields on returned dataclasses). Existing code keeps working.
- **Major** (`x.0.0`) — breaking changes only here, with a `DEPRECATED` notice in a prior minor release whenever practical.

Anything **not** listed in this document is **internal**. Importing it works today but the symbol, signature, location, or behaviour can change in any release without notice. If something internal is genuinely useful to you, open an issue — we'll consider promoting it.

---

## What's public

### Top-level convenience surface

`amx`:

| Symbol | Kind | Stability |
|---|---|---|
| `amx.__version__` | `str` | Stable |
| `amx.init(config_path: str \| None = None) -> AMXApplication` | function | Stable |
| `amx.AMXApplication` | class (lazy re-export) | Stable |
| `amx.AbstractEntity` | class (lazy re-export) | Stable |
| `amx.UniversalMetadataAdapter` | class (lazy re-export) | Stable |

These are re-exports of names defined in `amx.core` for one-line scripts. Library code should prefer importing from `amx.core` directly.

### `amx.core` — library-first API

Every name listed in `amx/core/__init__.py.__all__` is part of the public contract:

| Symbol | Kind | What it does |
|---|---|---|
| `amx.core.AMXApplication` | dataclass | Composable runtime that owns a config, a history store, and the active agents. Built via `AMXApplication.load(config_path)` for the typical case. |
| `amx.core.AskToolbox` | dataclass | Tool registry the loop-based ask agent calls into (schema explorer, catalog, etc.). |
| `amx.core.LoopBasedAskAgent` | class | Headless re-implementation of `/ask` for scripts and notebooks. |
| `amx.core.ToolAskResponse` | dataclass | Structured response from the loop-based ask agent (answer, intent, tool trace, confidence). |
| `amx.core.AbstractEntity` | dataclass | Backend-neutral entity abstraction used by the Universal Metadata Interface. |
| `amx.core.UniversalMetadataAdapter` | class | Maps backend-specific column / table profiles into `AbstractEntity`. |
| `amx.core.StateManager` | class | Write-through persistence for config + SQLite-backed state across sessions. |
| `amx.core.infer_table_metadata(cfg, schema, table, *, include_rag, include_codebase)` | function | One-call programmatic metadata inference. Returns a list of suggestion dicts ready for review. |

### CLI

The `amx` console script (defined under `[project.scripts]` in `pyproject.toml`) is part of the public API:

- The set of slash commands documented in `README.md` is the contract.
- Slash command **flags** (`--db-profile`, `--last`, `--diff`, `--csv`, …) are stable within a major version.
- Output is **rendered for humans** — no contract on column order, terminal styling, or table widths. Scripts that need to consume AMX output should use the export flags (`--csv`, `--md`, `--json`) where available.

### On-disk formats

These are part of the public contract because users depend on them across upgrades:

- `~/.amx/config.yml` — schema is versioned (`schema_version: N` field, see `amx.config.CONFIG_SCHEMA_VERSION`). When AMX bumps the schema, an older binary refuses to load a newer config rather than silently mangling it (raises `ConfigSchemaTooNewError`).
- `~/.amx/history.db` — SQLite tables (`analysis_runs`, `run_results`, `app_events`, etc.) accept additive migrations within a major version. Column types and the meaning of existing columns are stable.
- `--json` export shape (see `tests/eval/README.md`) — the keys `schema_version`, `run_summary`, `per_column`, `aggregate_metrics` are stable.

---

## What's internal

Everything else. Highlights:

| Module | Why it's internal |
|---|---|
| `amx.cli`, `amx.cli_support.*` | CLI plumbing, refactored frequently |
| `amx.cli_*` (top-level shims like `amx.cli_db`, `amx.cli_run`) | Backwards-compat shims that re-export from `amx.cli_support.commands.*`; will be removed in a future major release. Use the underlying modules only at your own risk; prefer `amx.core.*` for programmatic access. |
| `amx.agents.*` | Profile / RAG / Code agent internals. The orchestrator decides what gets called and how — directly instantiating these from user code couples you to the agent contract. |
| `amx.search._agent.*`, `amx.search._catalog.*` | Already underscore-prefixed. Do not import. |
| `amx.search.agent`, `amx.search.catalog`, `amx.search.service` | Public-shaped names but not part of the contract — use `amx.core.AMXApplication` to get a configured `SearchService`. |
| `amx.db.*`, `amx.llm.*`, `amx.docs.*`, `amx.codebase.*` | Backend adapters; tightly coupled to the active config. |
| `amx.storage.*` | History store implementation. `amx.core.AMXApplication` exposes the configured store via `app.history_store`. |
| `amx.utils.*` | Internal helpers (Rich console wrappers, logging, token counting). |
| `amx.config.AMXConfig` | Used internally; configure programmatically by passing a path to `amx.init(...)` or by editing `~/.amx/config.yml`. The dataclass shape is **not** stable. |

---

## How to write code that survives upgrades

```python
# Good — uses only public surface.
from amx.core import AMXApplication, infer_table_metadata

app = AMXApplication.load("~/.amx/config.yml")
suggestions = infer_table_metadata(
    app.config, "sales", "orders", include_rag=True, include_codebase=False
)
```

```python
# Risky — imports an internal symbol whose location may move.
from amx.search.service import SearchService   # internal
from amx.agents.orchestrator import Orchestrator   # internal

# The replacement when this breaks:
from amx.core import AMXApplication
app = AMXApplication.load(...)
service = app.search_service          # configured for you
```

---

## Pre-1.0 caveat

While AMX is at `0.x`, the contract above is **best-effort**. We will avoid breaking the listed symbols whenever possible and will flag any necessary breakage in `CHANGELOG.md` under `BREAKING CHANGE`. The hard guarantees kick in at `1.0.0`.

If a stability guarantee here matters to you for production use, open an issue — we'd rather hear it now than break you later.
