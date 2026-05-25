# Unified embedding-model management

## Context

AMX embeds text into vectors for retrieval in several subsystems, each
with its own Chroma collection and its own config field:

| Side | Config | Resolver | Collection prefix |
|---|---|---|---|
| Docs **+ catalog search** (same side) | `embedding_docs` | `_resolve_docs_embedding` | `amx_search*` |
| Code | `embedding_code` | `_resolve_code_embedding` | `amx_code` |
| Assets | `embedding_assets` | `_resolve_assets_embedding` | `amx_assets` |

Shared machinery: `amx/rag_core/collection_identity.py`
(`CollectionIdentity`, `reconcile_identity`, `CollectionIdentityMismatch`)
and `GET /api/profiles/embedding/status`.

Changing an embedding model is hard to manage and easy to get wrong.
Five root flaws surfaced while debugging a gte-small ↔ minilm mismatch:

1. **Silent fallback everywhere.** Each resolver, when the configured
   model can't be built (e.g. `sentence-transformers` not installed),
   silently returns minilm with no user-visible signal. The user
   believes their configured model is in use when it is not.
2. **No single source of truth.** Three independent resolvers
   (docs/code/assets) each read config and fall back on their own. A
   model change must be reasoned about per side.
3. **Environment-dependent resolution.** The same config resolves
   differently across Python environments: the Studio venv has
   `sentence-transformers` (gte-small loads), a bare shell does not
   (falls back to minilm). The collection is stamped with the resolved
   identity, so a collection built in one env looks "stale" in another
   — a confusing ghost mismatch.
4. **Model-change workflow is manual, per-side, and undiscoverable.**
   Rebuild lives only in Settings → Embeddings and is gated behind a
   staleness check that itself depends on the environment.
5. **Empty-collection false positives.** A leftover empty collection
   with an old identity trips the stale detector even though it holds
   no vectors.

## Goal

Make embedding-model changes easy to manage across every side, with
honest visibility into what is actually running (never a silent
substitution), a single resolution authority, and a one-stop health +
rebuild surface.

## Confirmed decisions

1. **Visible fallback + honest stamp.** When a configured model can't
   load, the side still falls back to minilm so the feature keeps
   working, BUT the fallback is surfaced (a `fell_back` flag + reason)
   and the collection is stamped with the *actually used* identity
   (minilm), not the configured one. No silent substitution; no
   within-environment ghost mismatch.
2. **Single resolution authority** — one `resolve_embedding(side, cfg)`
   that all three sides delegate to.
3. **Unified "Embeddings health" panel** — one surface listing every
   side: configured vs actually-running model, dependency availability,
   collection counts, stale state, and per-side / all rebuild. The
   degraded state is surfaced as a CTA on the pages where it bites
   (catalog cache, /ask, code, assets).

## Architecture

### 1. Unified resolver — `amx/rag_core/embedding_resolver.py` (new)

```python
@dataclass(frozen=True)
class ResolvedEmbedding:
    side: str                      # "docs" | "code" | "assets"
    configured_provider: str       # what config asked for
    configured_model: str
    active_provider: str           # what is actually used (post-fallback)
    active_model: str
    embedding_function: EmbeddingFunction | None
    fell_back: bool                # configured != active
    fallback_reason: str | None    # e.g. "sentence-transformers not installed"
    dependency_available: bool

def resolve_embedding(side: str, cfg) -> ResolvedEmbedding: ...
```

The three existing `_resolve_*_embedding` functions become thin
wrappers that call `resolve_embedding(side, cfg)` and return the legacy
`(provider, model, embedding_function)` tuple, so existing callers are
unchanged. The fallback policy and reason-capture live in one place.

The collection identity continues to be stamped from the **active**
(post-fallback) identity — which the current code already does — so a
fell-back collection is honestly stamped minilm.

### 2. Status enrichment — `_collection_status_for_side`

Extend the per-side status payload to:

```
{
  configured: {provider, model},
  active: {provider, model},
  fell_back: bool,
  fallback_reason: str | None,
  dependency_available: bool,
  collections: [{name, count, embedding_provider, embedding_model, stale}],
  stale: bool,            # any populated collection whose identity != active
  needs_rebuild: bool,    # stale OR fell_back-with-old-vectors
}
```

Staleness uses the **resolved active** identity (honest) and ignores
empty (count 0) collections (fixes flaw 5).

### 3. Embeddings health panel — Studio (extends Settings → Embeddings)

One card per side showing: configured model, **actually running**
model (red when they differ, e.g. "configured gte-small but running
minilm — sentence-transformers not installed"), dependency status,
collection count, a stale badge, and per-side **Rebuild** + a
**Rebuild all** action. A compact "embeddings degraded" banner with a
Rebuild CTA is shown on the catalog-cache, /ask, code, and assets
surfaces where the degradation is felt (reusing the same status query).

### 4. Unified rebuild

`POST /api/profiles/embedding/{side}/rebuild` already exists per side;
add `side=all` to fan out to docs/search (`rebuild_profile`), code
(reset + reindex), and assets (reset + reindex). Each rebuild
re-stamps the collection with the current active identity.

### 5. CLI parity

* `/embeddings status` — print the health table (configured / running /
  dependency / stale per side).
* `/embeddings rebuild [side|all]` — unified rebuild.
* `/embeddings {side} <provider> <model>` — set (already exists).

## Error handling

* A model that can't be built never silently masquerades: the side
  falls back to minilm AND `fell_back`/`fallback_reason` are set so
  every surface (status, panel, CLI, logs) shows the truth.
* Rebuild is best-effort per side: one side failing does not abort the
  others; failures are reported in the response.
* Empty collections are never flagged stale.

## Testing

* **Resolver:** configured model loads → `active == configured`,
  `fell_back == False`; build fails → `active == minilm`,
  `fell_back == True` with a reason; dependency_available reflects the
  real import check.
* **Status:** configured vs active reported; stale uses active identity;
  count-0 collections not flagged; `needs_rebuild` correct.
* **Rebuild all:** dispatches to every side; a per-side failure is
  isolated.
* **Honest stamp:** a fell-back collection is stamped minilm, not the
  configured model.
* **Back-compat:** the three legacy `_resolve_*_embedding` wrappers
  return the same tuple shape as before.

## Implementation plan (shippable PRs)

1. **PR1 — unified resolver.** New `embedding_resolver.py`; the three
   resolvers delegate to it; capture `fell_back`/reason/dependency.
   Behaviour-preserving + adds the signal. Backend only.
2. **PR2 — status enrichment + empty-collection guard.** Extend
   `_collection_status_for_side` to report configured vs active +
   fell_back + needs_rebuild; ignore count-0 in staleness.
3. **PR3 — health panel + rebuild-all + cross-page CTA.** Studio
   Embeddings panel shows configured-vs-running per side; `side=all`
   rebuild endpoint; a degraded banner with a Rebuild CTA on the
   catalog-cache / ask / code / assets surfaces.
4. **PR4 — CLI parity.** `/embeddings status` health table +
   `/embeddings rebuild [side|all]`.

## Outcome

Changing an embedding model — on any side — is a one-screen operation:
the panel shows what you configured, what is actually running, whether
the dependency is present, and whether a rebuild is needed, with a
one-click rebuild. The system never silently runs a different model
than the one configured.
