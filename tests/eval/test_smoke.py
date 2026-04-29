"""Smoke test for the retrieval evaluation harness.

Verifies that the metrics-plus-fake-retriever loop works end-to-end so
contributors can drop a real fixture file into ``tests/eval/fixtures/``
and re-use the same machinery without fighting the plumbing.

A real eval against the live ``SearchIndex`` belongs in a separate
suite — it requires a populated catalog and a chosen embedding
provider. This smoke test runs in milliseconds against an in-memory
fake retriever so CI does not depend on Chroma state.
"""

from __future__ import annotations

import unittest
from collections.abc import Sequence

from tests.eval.metrics import (
    hit_at_k,
    mean_reciprocal_rank,
    ndcg_at_k,
    precision_at_k,
)


class FakeRetriever:
    """Returns the answer ranking encoded in *answers* for each question.

    Stand-in for SearchIndex.query — real evals pass a wrapper that
    calls the live retriever and projects the result down to identifier
    strings (e.g. ``f"{schema}.{table}"``).
    """

    def __init__(self, answers: dict[str, list[str]]) -> None:
        self._answers = answers

    def query(self, question: str, *, k: int) -> Sequence[str]:
        return list(self._answers.get(question, []))[:k]


class EvalHarnessSmokeTests(unittest.TestCase):
    """Tiny end-to-end exercise of the harness using a fake retriever.

    Real evals follow the same shape: load a fixture mapping each
    question to its set of relevant entity ids, run the retriever on
    each question, score the ranked output, aggregate.
    """

    FIXTURE: dict[str, set[str]] = {
        # question -> relevant entity ids (any of these in the top-k counts)
        "what is the orders table?": {"public.orders"},
        "show me customers": {"public.customers", "crm.customers"},
        "where is shipment data?": {"public.shipments"},
    }

    def test_perfect_retriever_scores_one(self) -> None:
        retriever = FakeRetriever(
            {
                "what is the orders table?": ["public.orders", "x", "y"],
                # The fixture marks BOTH public.customers and crm.customers
                # as relevant; ranking both at positions 1-2 gives nDCG=1.
                "show me customers": ["public.customers", "crm.customers", "x"],
                "where is shipment data?": ["public.shipments", "x"],
            }
        )

        per_query_pairs = [
            (retriever.query(q, k=5), self.FIXTURE[q]) for q in self.FIXTURE
        ]

        self.assertEqual(mean_reciprocal_rank(per_query_pairs), 1.0)
        for ranked, relevant in per_query_pairs:
            self.assertEqual(hit_at_k(ranked, relevant, k=1), 1.0)
            self.assertAlmostEqual(ndcg_at_k(ranked, relevant, k=3), 1.0, places=2)

    def test_partial_retriever_scores_below_one(self) -> None:
        # Best result drops to position 2 for one query, position 3 for another.
        retriever = FakeRetriever(
            {
                "what is the orders table?": ["x", "public.orders"],
                "show me customers": ["x", "y", "public.customers"],
                "where is shipment data?": ["x", "y", "public.shipments"],
            }
        )

        per_query_pairs = [
            (retriever.query(q, k=5), self.FIXTURE[q]) for q in self.FIXTURE
        ]

        mrr = mean_reciprocal_rank(per_query_pairs)
        self.assertLess(mrr, 1.0)
        self.assertGreater(mrr, 0.0)

        # Hit@1 must be 0 for all three since the top result is wrong.
        for ranked, relevant in per_query_pairs:
            self.assertEqual(hit_at_k(ranked, relevant, k=1), 0.0)
            # But hit@3 catches at least the first two queries.
            if "orders" in next(iter(relevant)):
                self.assertEqual(hit_at_k(ranked, relevant, k=2), 1.0)

    def test_empty_results_score_zero(self) -> None:
        # Retriever returns nothing — every metric must be 0.0 without
        # raising. Real eval scripts treat this as a hard failure.
        retriever = FakeRetriever({})
        per_query_pairs = [
            (retriever.query(q, k=5), self.FIXTURE[q]) for q in self.FIXTURE
        ]
        self.assertEqual(mean_reciprocal_rank(per_query_pairs), 0.0)
        for ranked, relevant in per_query_pairs:
            self.assertEqual(hit_at_k(ranked, relevant, k=5), 0.0)
            self.assertEqual(precision_at_k(ranked, relevant, k=5), 0.0)


if __name__ == "__main__":
    unittest.main()
