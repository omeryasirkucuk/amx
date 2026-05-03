<p align="center">
  <img src="docs/assets/amx-banner.png" alt="AMX — Agentic Metadata Extractor" width="760">
</p>

<p align="center"><strong>AI-inferred database descriptions, with a human in the loop.</strong></p>

---

AMX walks your database, reads your documentation and codebase, and proposes a description for every table, view, and column. Three sub-agents — Profile, RAG, and Code — merge their evidence into ranked suggestions; you review and approve before anything lands as a `COMMENT` on the live database.

## Install

```bash
pip install amx
```

Pick the database backends you actually use, or grab them all at once:

```bash
pip install "amx[postgresql]"
pip install "amx[postgresql,snowflake,bigquery]"
pip install "amx[all]"
```

## Quick start

```bash
amx              # open the interactive session
/setup           # configure DB + LLM profiles
/run             # pick a scope, run the agents, review, apply
amx doctor       # diagnose install / config / connectivity
```

## Example

Cryptic identifier in:

```
sap_s6p.t001.audat   NUMBER(8) NULL
```

Reviewed description out:

```
Document date. The calendar date the source business event was recorded,
distinct from posting date (BUDAT) which controls the accounting period
the transaction lands in.

  confidence: high · logprob: 0.91 · sources: code (3 refs), docs, db profile
```

## Supported database backends

PostgreSQL · Snowflake · Databricks (Unity Catalog) · BigQuery · MySQL / MariaDB · Oracle · SQL Server · Redshift · ClickHouse · DuckDB

## Supported LLM providers

OpenAI · Anthropic · Google Gemini · DeepSeek · OpenRouter · Ollama · vLLM / LM Studio · any OpenAI-compatible endpoint

## Programmatic use

```python
import amx
from amx.core import infer_table_metadata

app = amx.init()
results = infer_table_metadata(app.config, schema="sap_test", table="adr6")
```

The stable public surface is documented in [`docs/PUBLIC_API.md`](./docs/PUBLIC_API.md).

## Documentation

Full user and operator documentation is published separately. Release notes live in [`CHANGELOG.md`](./CHANGELOG.md) and on the [GitHub Releases page](https://github.com/omeryasirkucuk/amx/releases).

## Contributing & support

- [Contributing guide](./CONTRIBUTING.md)
- [Security policy](./SECURITY.md)
- [Open an issue](https://github.com/omeryasirkucuk/amx/issues)

## License

Apache-2.0 — see [`LICENSE`](./LICENSE).
