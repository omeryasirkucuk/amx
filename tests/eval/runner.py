"""End-to-end retrieval-eval runner for the docs RAG pipeline.

Loads the gold set from ``tests/eval/fixtures/docs_gold.jsonl``, ingests
the companion fixture documents under ``tests/eval/fixtures/docs/``
into a fresh :class:`amx.docs.rag.RAGStore` (Chroma persistent client
rooted at a caller-supplied path, MiniLM bundled embedder so the run is
offline), executes each gold question against the live retrieval +
heuristic-rerank surface, and returns a structured metrics dict.

The runner is deliberately stateless: every call builds a new store in
the supplied directory so CI baselines do not drift on Chroma state
left over from a prior run. The expensive piece is the first
embedding-model load (~1-2 seconds for MiniLM via Chroma); subsequent
in-process calls reuse it.

This module is imported by ``tests/eval/test_baselines.py`` (the CI
gate) and by ad-hoc scripts. It contains no test logic of its own.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from amx.docs.rag import RAGStore
from amx.docs.scanner import DocInfo
from tests.eval.metrics import (
    hit_at_k,
    mean_reciprocal_rank,
    ndcg_at_k,
    precision_at_k,
    reciprocal_rank,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
DOCS_FIXTURE_DIR = FIXTURES_DIR / "docs"
DOCS_GOLD_PATH = FIXTURES_DIR / "docs_gold.jsonl"


@dataclass(frozen=True)
class GoldQuery:
    """One gold-set row: question + the documents that should be retrieved."""

    id: str
    question: str
    expected_sources: frozenset[str]
    expected_answer_contains: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()


@dataclass
class QueryResult:
    """Per-question evaluation outcome."""

    id: str
    question: str
    retrieved_sources: list[str]
    hit_at_1: float
    hit_at_3: float
    hit_at_5: float
    reciprocal_rank: float
    precision_at_5: float
    ndcg_at_5: float
    keyword_recall: float


@dataclass
class DocsEvalReport:
    """Aggregate report across the gold set."""

    n_queries: int
    hit_at_1: float
    hit_at_3: float
    hit_at_5: float
    mrr: float
    precision_at_5: float
    ndcg_at_5: float
    keyword_recall: float
    per_query: list[QueryResult] = field(default_factory=list)

    def to_baseline_dict(self) -> dict[str, float | int]:
        """Subset that the CI baseline compares against. Per-query
        details are excluded — they're useful for human debugging but
        too noisy to gate on."""
        return {
            "n_queries": self.n_queries,
            "hit_at_1": round(self.hit_at_1, 4),
            "hit_at_3": round(self.hit_at_3, 4),
            "hit_at_5": round(self.hit_at_5, 4),
            "mrr": round(self.mrr, 4),
            "precision_at_5": round(self.precision_at_5, 4),
            "ndcg_at_5": round(self.ndcg_at_5, 4),
            "keyword_recall": round(self.keyword_recall, 4),
        }


def load_gold_set(path: Path = DOCS_GOLD_PATH) -> list[GoldQuery]:
    """Parse the JSONL gold set. Each line must be a single object with
    ``id``, ``question``, ``expected_sources`` (list of filenames
    relative to the fixture dir). Other fields are optional."""
    rows: list[GoldQuery] = []
    with path.open("r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            try:
                obj = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{lineno}: invalid JSON: {exc}") from exc
            rows.append(
                GoldQuery(
                    id=str(obj["id"]),
                    question=str(obj["question"]),
                    expected_sources=frozenset(obj.get("expected_sources") or ()),
                    expected_answer_contains=tuple(obj.get("expected_answer_contains") or ()),
                    tags=tuple(obj.get("tags") or ()),
                )
            )
    if not rows:
        raise ValueError(f"{path}: gold set is empty")
    return rows


def _scan_fixture_docs(fixture_dir: Path) -> list[DocInfo]:
    """Build :class:`DocInfo` records for every plain-text fixture file.

    We use ``.txt`` rather than ``.md`` on purpose: ``.md`` routes
    ingest through ``UnstructuredMarkdownLoader``, which depends on
    the ``markdown`` PyPI package — installed as a transitive in some
    environments but **not** in CI's ``pip install -e ".[all,code-intel]"``
    matrix. ``.txt`` lands on ``TextLoader``, which is dependency-free.
    The fixture content is still Markdown-formatted prose; only the
    file extension changes.

    We deliberately do not invoke the real ``amx.docs.scanner.scan_docs``
    — the scanner walks gitignore, runs binary-sniffers, and consults
    configuration the eval shouldn't have to mock. A flat directory of
    text files is enough.
    """
    docs: list[DocInfo] = []
    for path in sorted(fixture_dir.glob("*.txt")):
        stat = path.stat()
        docs.append(
            DocInfo(
                path=str(path),
                size_bytes=stat.st_size,
                extension=".txt",
                source_type="local",
                source_root=str(fixture_dir),
            )
        )
    return docs


def _retrieved_source_names(hits: Sequence[dict]) -> list[str]:
    """Project each Chroma hit down to its source filename, deduped
    while preserving rank order.

    The retrieval surface returns chunks; the gold set names *files*.
    Without dedup, five chunks from the same file count five times
    against the source-level metrics — inflating DCG above IDCG and
    making precision/recall meaningless at the source level. Keeping
    the first occurrence preserves the "which document showed up
    first" signal that source-level eval actually cares about.

    Filenames (not absolute paths) so fixture moves don't invalidate
    the baseline.
    """
    seen: set[str] = set()
    names: list[str] = []
    for hit in hits:
        meta = hit.get("metadata") or {}
        source = str(meta.get("source") or "")
        if not source:
            continue
        name = Path(source).name
        if name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


def _keyword_recall(hits: Sequence[dict], expected_terms: Iterable[str]) -> float:
    """Fraction of ``expected_answer_contains`` terms present anywhere in
    the concatenated retrieved text. Cheap proxy for "did the right
    information land in the context window" — sharper than hit@k when
    the gold set expects content rather than a specific file."""
    terms = [t for t in (expected_terms or ()) if t]
    if not terms:
        return 1.0  # nothing to recall = trivially recalled
    joined = " ".join(str(hit.get("text") or "") for hit in hits).lower()
    matched = sum(1 for t in terms if t.lower() in joined)
    return matched / float(len(terms))


def run_docs_eval(
    persist_dir: Path,
    *,
    fixture_dir: Path = DOCS_FIXTURE_DIR,
    gold_path: Path = DOCS_GOLD_PATH,
    top_k: int = 5,
) -> DocsEvalReport:
    """Build a fresh RAGStore at ``persist_dir``, ingest the fixture
    docs, score the gold set, return the aggregate report.

    ``persist_dir`` must not contain an existing AMX Chroma collection.
    The CI test invokes this against ``tmp_path`` so each test run gets
    a clean directory; ad-hoc callers should do the same.
    """
    persist_dir.mkdir(parents=True, exist_ok=True)
    store = RAGStore(
        persist_dir=str(persist_dir),
        embedding_function=None,  # bundled MiniLM — offline, deterministic
        embedding_provider="minilm",
        embedding_model="minilm-l6-v2",
    )
    docs = _scan_fixture_docs(fixture_dir)
    summary = store.ingest(docs)
    if summary.failed:
        raise RuntimeError(f"fixture ingest failed: {summary.failed}")
    if summary.chunk_count == 0:
        raise RuntimeError("fixture ingest produced zero chunks")

    gold = load_gold_set(gold_path)
    per_query: list[QueryResult] = []
    mrr_inputs: list[tuple[Sequence[str], frozenset[str]]] = []

    for q in gold:
        hits = store.query(q.question, n_results=top_k)
        retrieved = _retrieved_source_names(hits)
        relevant = q.expected_sources

        result = QueryResult(
            id=q.id,
            question=q.question,
            retrieved_sources=retrieved,
            hit_at_1=hit_at_k(retrieved, relevant, k=1),
            hit_at_3=hit_at_k(retrieved, relevant, k=min(3, top_k)),
            hit_at_5=hit_at_k(retrieved, relevant, k=top_k),
            reciprocal_rank=reciprocal_rank(retrieved, relevant),
            precision_at_5=precision_at_k(retrieved, relevant, k=top_k),
            ndcg_at_5=ndcg_at_k(retrieved, relevant, k=top_k),
            keyword_recall=_keyword_recall(hits, q.expected_answer_contains),
        )
        per_query.append(result)
        mrr_inputs.append((retrieved, relevant))

    n = len(per_query)

    def _avg(attr: str) -> float:
        if not per_query:
            return 0.0
        return sum(getattr(r, attr) for r in per_query) / float(n)

    return DocsEvalReport(
        n_queries=n,
        hit_at_1=_avg("hit_at_1"),
        hit_at_3=_avg("hit_at_3"),
        hit_at_5=_avg("hit_at_5"),
        mrr=mean_reciprocal_rank(mrr_inputs),
        precision_at_5=_avg("precision_at_5"),
        ndcg_at_5=_avg("ndcg_at_5"),
        keyword_recall=_avg("keyword_recall"),
        per_query=per_query,
    )
