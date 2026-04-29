# Contributing to AMX

Thanks for considering a contribution. AMX (`amx`) is an open-source CLI for AI-driven database metadata extraction. This guide walks you through the development workflow.

## Development setup

```bash
git clone https://github.com/omeryasirkucuk/amx.git
cd amx
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,code-intel]"
pre-commit install
```

A local PostgreSQL is convenient for end-to-end testing:

```bash
docker compose up -d
```

This starts a Postgres on `localhost:5432`. Credentials are dev-only and live in `docker-compose.yml`; do not reuse them anywhere.

## Branching and pull requests

- `main` is the release branch and is protected. **Direct pushes are not allowed.**
- Create a short-lived feature branch off `main`: `feat/setup-wizard`, `fix/snowflake-comment-fetch`, etc.
- Open a pull request when ready. CI must pass and at least one approving review is required (you can self-approve when working solo, but the review still gets recorded).

## Commit message format

We use [Conventional Commits](https://www.conventionalcommits.org/) so that releases and the changelog are generated automatically. The PR title becomes the merge commit subject and drives the version bump:

| Prefix     | Effect on version | Example                                     |
| ---------- | ----------------- | ------------------------------------------- |
| `feat:`    | minor             | `feat(search): add OpenAI embedding provider` |
| `fix:`     | patch             | `fix(db): surface snowflake auth error`       |
| `perf:`    | patch             | `perf(rag): batch chunk encoding`             |
| `refactor:`| no bump           | `refactor(cli): split db commands`            |
| `docs:`    | no bump           | `docs: add embeddings guide`                  |
| `test:`    | no bump           | `test: cover bigquery adapter`                |
| `chore:`   | no bump           | `chore: bump pre-commit pin`                  |
| `ci:`      | no bump           | `ci: cache pip wheels`                        |
| `build:`   | no bump           | `build: drop python 3.9 support`              |

A `BREAKING CHANGE:` footer (or `feat!:` / `fix!:`) triggers a major bump.

## Tests

```bash
pytest -ra                    # run everything
pytest -m "not slow"          # skip slow tests
pytest tests/test_search_catalog.py -k embedding   # narrow scope
```

The suite uses `unittest.mock`; LLM and DB calls are mocked. Real-endpoint tests live behind the `integration` and `live` markers and are not run by default.

## Lint, format, type check

```bash
ruff check amx tests
ruff format amx tests
mypy amx
```

Pre-commit runs the lint and format hooks automatically on `git commit`. If you skipped install, run `pre-commit run --all-files` before pushing.

## Adding dependencies

Add runtime deps to `[project.dependencies]` in `pyproject.toml`. Put dev-only tooling under `[project.optional-dependencies].dev`. Pin to a sensible lower bound (`>=X.Y`); avoid upper caps unless we hit a known incompatibility.

## Releasing

Releases are automated. Cutting a release is a single tag push from `main`:

```bash
git tag v0.3.0
git push origin v0.3.0
```

The `release.yml` workflow then builds the wheel, publishes to PyPI via OIDC Trusted Publisher (no API token in the repo), and creates a GitHub Release. Version numbers are derived from Conventional Commits via `python-semantic-release`.

## Reporting bugs

Open an issue at https://github.com/omeryasirkucuk/amx/issues. Please include:

- AMX version (`amx --version`)
- Python version and OS
- Steps to reproduce
- Relevant excerpts from `~/.amx/logs/amx.log` (redact secrets)
- For DB connector bugs: backend, auth method, and the actionable error AMX printed (not the full traceback unless asked)

## Security

Do **not** open public issues for security problems. See `SECURITY.md` for the disclosure process.
