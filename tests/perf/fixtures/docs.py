"""Deterministic synthetic doc set for embedding benchmarks.

Generates ``n_chunks`` reproducible text chunks (seeded RNG) so RAG
benchmarks compare like-for-like across runs. No external deps —
keeps the ``[perf]`` extra small.
"""

from __future__ import annotations

import random
import string
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DocChunk:
    doc_id: str
    chunk_id: str
    text: str


def make_chunks(n_chunks: int, *, seed: int = 42, words_per_chunk: int = 80) -> list[DocChunk]:
    rng = random.Random(seed)
    vocabulary = [
        "transaction",
        "account",
        "ledger",
        "posting",
        "settlement",
        "currency",
        "amount",
        "tenant",
        "schema",
        "column",
        "metadata",
        "lineage",
        "audit",
        "entity",
        "customer",
        "invoice",
        "payment",
        "exchange",
        "rate",
        "balance",
    ]
    chunks: list[DocChunk] = []
    for i in range(n_chunks):
        words = rng.choices(vocabulary, k=words_per_chunk)
        chunks.append(
            DocChunk(
                doc_id=f"doc-{i // 50:04d}",
                chunk_id=f"chunk-{i:06d}",
                text=" ".join(words),
            )
        )
    return chunks


def write_chunks_to_dir(chunks: list[DocChunk], out_dir: Path) -> Path:
    """Materialise chunks as one ``.md`` per doc in ``out_dir``.

    Useful when a benchmark needs an on-disk repo for the scanner. The
    layout matches what AMX's docs ingestion expects (flat text files,
    relative paths).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    by_doc: dict[str, list[DocChunk]] = {}
    for c in chunks:
        by_doc.setdefault(c.doc_id, []).append(c)
    for doc_id, doc_chunks in by_doc.items():
        body = "\n\n".join(f"## {c.chunk_id}\n\n{c.text}" for c in doc_chunks)
        (out_dir / f"{doc_id}.md").write_text(body, encoding="utf-8")
    return out_dir


def random_token(length: int = 8, *, seed: int | None = None) -> str:
    rng = random.Random(seed)
    return "".join(rng.choices(string.ascii_lowercase, k=length))
