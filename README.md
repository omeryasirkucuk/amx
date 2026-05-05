<p align="center">
  <img src="https://raw.githubusercontent.com/omeryasirkucuk/amx/main/docs/assets/amx-banner.png" alt="AMX — Agentic Metadata Extractor" width="760">
</p>

<p align="center">
  <strong>Stop staring at <code>transactions.posting NUMBER(8)</code> wondering what it means.</strong>
</p>

<p align="center">
  AI-powered guidance and reference for data analysts, data engineers,<br>
  and catalog owners working with undocumented database schemas.
</p>

<p align="center">
  <a href="https://omeryasirkucuk.github.io/amx-docs/"><strong>Documentation</strong></a>
  ·
  <a href="https://omeryasirkucuk.github.io/amx-docs/getting-started/quickstart/">Quickstart</a>
  ·
  <a href="https://github.com/omeryasirkucuk/amx/blob/main/CHANGELOG.md">Changelog</a>
  ·
  <a href="https://github.com/omeryasirkucuk/amx/issues">Issues</a>
</p>

---

AMX walks your database, reads your documentation and codebase, then **drafts a description for every table, view, and column** — with confidence scores and a human review before anything lands in the live database. Three independent sub-agents (Profile, RAG, Code) gather evidence, an orchestrator merges and ranks them, you accept / edit / skip, and AMX writes approved descriptions back as native `COMMENT` statements on the engine.

Five minutes from `pip install` to your first reviewed description. **Ten supported database backends, seven LLM providers.**

## Install

```bash
pip install amx-cli
```

The PyPI distribution is `amx-cli`; the import package is `amx` (`import amx`). Requires Python 3.10+. See the [installation guide](https://omeryasirkucuk.github.io/amx-docs/getting-started/installation/) for prerequisites, source builds, and where AMX writes config / history / logs.

## Quick start

```bash
amx                       # open the interactive session (the AMX REPL)
/setup                    # one-time wizard: DB profile + LLM profile
/connect                  # sanity-check the active connection
/run core.transactions          # generate suggestions, review, accept
/apply                    # write approved descriptions back to the database
```

`/run` without an argument opens a scope picker (Database / Schema / Asset). `/run-apply` short-circuits review-and-apply when you already trust the model. If anything misbehaves, `amx doctor` runs from any shell — even when AMX itself can't start — and prints actionable hints next to each ✗.

The full guided walkthrough is at the [5-minute quickstart](https://omeryasirkucuk.github.io/amx-docs/getting-started/quickstart/) and [first-run walkthrough](https://omeryasirkucuk.github.io/amx-docs/getting-started/first-run/).

## `/studio` — same workflow on a local web UI

`amx /studio` boots **AMX Studio**, a token-protected web UI on `127.0.0.1`, and opens your browser. Same review-and-apply workflow as the REPL — runs, results, the pending queue, the `/ask` chat, and full DB / LLM / Docs / Code profile management — but on a denser surface. **Browse** any database / schema / table to inline-edit comments or hit per-asset **Generate** to draft just one comment through the same human-in-the-loop queue. The SPA bundle ships inside `amx-cli`; no Node toolchain or extra install needed.

<p align="center">
  <img src="https://raw.githubusercontent.com/omeryasirkucuk/amx/main/docs/assets/studio-overview.png" alt="AMX Studio Overview page — sidebar with active SAP profile, four stat cards (active backend, LLM model, total runs, success rate), and a Recent runs feed" width="900">
</p>

Full walkthrough at [`/studio`](https://omeryasirkucuk.github.io/amx-docs/cli/studio/).

## What you get

Cryptic identifier in:

```
core.transactions.posting   NUMBER(8) NULL
```

Reviewed description out:

```
Posting date. The accounting period this transaction lands in, encoded
as YYYYMMDD. Distinct from the system-level effective date (eff_dt)
that records when the row physically arrived in the warehouse.

  confidence: high · logprob: 0.91 · sources: code (3 refs), docs, db profile
```

Every column gets up to N ranked alternatives, every suggestion is grounded in evidence (db profile, code references, doc snippets), and every approval is recorded in local run history that you can re-evaluate later with `/history review`.

## Supported database backends

PostgreSQL · Snowflake · Databricks (Unity Catalog) · BigQuery · MySQL / MariaDB · Oracle · SQL Server · Redshift · ClickHouse · DuckDB

Per-backend setup, connection details, and the capability matrix live in the [Backends section](https://omeryasirkucuk.github.io/amx-docs/backends/).

## Supported LLM providers

OpenAI · Anthropic · Google Gemini · DeepSeek · OpenRouter · Ollama · vLLM / LM Studio · any OpenAI-compatible endpoint

Provider-specific guides (including OpenAI / Anthropic Batch mode and local-model setups) live in the [LLM providers section](https://omeryasirkucuk.github.io/amx-docs/llm-providers/).

## Documentation

Full user, operator, and contributor docs live at **[omeryasirkucuk.github.io/amx-docs](https://omeryasirkucuk.github.io/amx-docs/)** — concepts, the slash-command map, configuration, data sources, collaboration, troubleshooting, and the [Python API](https://omeryasirkucuk.github.io/amx-docs/api/reference/) for headless use. Release notes are in [`CHANGELOG.md`](https://github.com/omeryasirkucuk/amx/blob/main/CHANGELOG.md) and on the [GitHub Releases page](https://github.com/omeryasirkucuk/amx/releases).

## Contributing & support

- [Contributing guide](https://github.com/omeryasirkucuk/amx/blob/main/CONTRIBUTING.md) — development setup, branching, commit format, release process
- [Security policy](https://github.com/omeryasirkucuk/amx/blob/main/SECURITY.md) — how to report a vulnerability
- [Open an issue](https://github.com/omeryasirkucuk/amx/issues) — bugs, questions, feature requests

## License

Apache-2.0 — see [`LICENSE`](https://github.com/omeryasirkucuk/amx/blob/main/LICENSE).
