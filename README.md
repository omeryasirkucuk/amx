<p align="center">
  <img src="https://raw.githubusercontent.com/omeryasirkucuk/amx/main/docs/assets/amx-readme.gif" alt="AMX in action" width="760">
</p>

<p align="center">
  <strong>Cryptic columns, reviewed comments.</strong>
</p>

<p align="center">
  AMX walks your database, reads your codebase and docs, then drafts a
  <code>COMMENT</code> for every table, view, and column — with confidence
  scores and a human review before anything lands in the live database.
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

_AMX is in early development — expect breaking changes between releases. Stay current with `pip install --upgrade amx-cli`._

Three independent sub-agents — **Profile** (schema, stats, sample values), **RAG** (ingested docs), and **Code** (references mined from your repos) — gather evidence in parallel. An orchestrator merges and ranks them, you accept / edit / skip each suggestion, and AMX writes the approved text back as native `COMMENT` statements on the engine.

Five minutes from `pip install` to your first reviewed comment. **Ten database engines, seven LLM providers.**

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
/run sales.orders         # generate suggestions, review, accept
/apply                    # write approved descriptions back to the database
```

`/run` without an argument opens a scope picker (Database / Schema / Asset). `/run-apply` short-circuits review-and-apply when you already trust the model. If anything misbehaves, `amx doctor` runs from any shell — even when AMX itself can't start — and prints actionable hints next to each ✗.

The full guided walkthrough is at the [5-minute quickstart](https://omeryasirkucuk.github.io/amx-docs/getting-started/quickstart/) and [first-run walkthrough](https://omeryasirkucuk.github.io/amx-docs/getting-started/first-run/).

## AMX CLI

A typical session — running `/run`, inspecting ranked suggestions inline, accepting or editing each one before it lands in the database.

<p align="center">
  <img src="https://raw.githubusercontent.com/omeryasirkucuk/amx/main/docs/assets/amx-banner.png" alt="AMX REPL — interactive session showing /run output with ranked suggestions, confidence scores, and review prompts" width="900">
</p>

## AMX Studio (`/studio`) — same workflow on a local web UI

From inside the REPL, `/studio` boots **AMX Studio**, a token-protected web UI on `127.0.0.1`, and opens your browser. Same review-and-apply workflow as the REPL — runs, results, the pending queue, the `/ask` chat, and full DB / LLM / Docs / Code profile management — but on a denser surface. **Browse** any database / schema / table to inline-edit comments or hit per-asset **Generate** to draft just one comment through the same human-in-the-loop queue. The SPA bundle ships inside `amx-cli`; no Node toolchain or extra install needed.

<p align="center">
  <img src="https://raw.githubusercontent.com/omeryasirkucuk/amx/main/docs/assets/studio-overview.png" alt="AMX Studio Catalog landing page — sidebar with Catalog selected, four quick-action cards (Browse, New run, Ask, Audit), and a Recent activity feed" width="900">
</p>

Full walkthrough at [`/studio`](https://omeryasirkucuk.github.io/amx-docs/cli/studio/).

## How it works

<p align="center">
  <img src="https://amxcli.com/assets/amx-flow.png" alt="AMX flow — Profile, RAG, and Code agents feed evidence to an orchestrator that ranks suggestions; you review accept/edit/skip and AMX writes the approved text back as COMMENT ON COLUMN native DDL" width="900">
</p>

Each agent runs independently, surfaces its own evidence, and assigns its own confidence. The orchestrator picks the narrower, defensible description and ranks up to N alternatives. You never apply anything you didn't approve.

### A column, before and after

A typical column in `orders`, the kind every analytics codebase has:

```
orders.shipped_dt   DATE   NULL   -- (no comment)
```

What AMX produces, after you approve:

```
COMMENT ON COLUMN orders.shipped_dt IS
  'Date the carrier scanned the package at origin. Distinct from
   order_dt (when the customer placed the order) and delivered_dt
   (when the carrier recorded proof of delivery).';

confidence: high · logprob: 0.91
sources: code (3 refs) · docs · db profile
```

Every approval is recorded in local run history — re-evaluate later with `/history review` to see how the same column scores under a different model, prompt detail, or evidence mix.

## Supported database backends

PostgreSQL · Snowflake · Databricks (Unity Catalog) · BigQuery · MySQL / MariaDB · Oracle · SQL Server · Redshift · ClickHouse · DuckDB

Per-backend setup, connection details, and the capability matrix live in the [Backends section](https://omeryasirkucuk.github.io/amx-docs/backends/).

## Supported LLM providers

OpenAI · Anthropic · Google Gemini · DeepSeek · OpenRouter · Ollama · vLLM / LM Studio · any OpenAI-compatible endpoint

Provider-specific guides (including OpenAI / Anthropic Batch mode and local-model setups) live in the [LLM providers section](https://omeryasirkucuk.github.io/amx-docs/llm-providers/).

## Documentation

Full user, operator, and contributor docs live at **[omeryasirkucuk.github.io/amx-docs](https://omeryasirkucuk.github.io/amx-docs/)** — concepts, the slash-command map, configuration, data sources, collaboration, troubleshooting, and the [Python API](https://omeryasirkucuk.github.io/amx-docs/api/reference/) for headless use. Release notes are in [`CHANGELOG.md`](https://github.com/omeryasirkucuk/amx/blob/main/CHANGELOG.md) and on the [GitHub Releases page](https://github.com/omeryasirkucuk/amx/releases).

Recent: Studio Lineage & Variations, Confidence Scoring, and explicit cache controls — see the [changelog](https://omeryasirkucuk.github.io/amx-docs/changelog/).

## Contributing & support

- [Contributing guide](https://github.com/omeryasirkucuk/amx/blob/main/CONTRIBUTING.md) — development setup, branching, commit format, release process
- [Security policy](https://github.com/omeryasirkucuk/amx/blob/main/SECURITY.md) — how to report a vulnerability
- [Open an issue](https://github.com/omeryasirkucuk/amx/issues) — bugs, questions, feature requests

## License

Apache-2.0 — see [`LICENSE`](https://github.com/omeryasirkucuk/amx/blob/main/LICENSE).

---

_This project is developed with the help of multiple AI coding agents._
