"""Unit tests for the retrieval evaluation metrics."""

from __future__ import annotations

import unittest

from tests.eval.metrics import (
    hit_at_k,
    mean_reciprocal_rank,
    ndcg_at_k,
    precision_at_k,
    reciprocal_rank,
)


class HitAtKTests(unittest.TestCase):
    def test_hit_in_top_one(self) -> None:
        self.assertEqual(hit_at_k(["orders", "users"], {"orders"}, k=1), 1.0)

    def test_hit_outside_window(self) -> None:
        self.assertEqual(hit_at_k(["users", "events", "orders"], {"orders"}, k=2), 0.0)

    def test_no_relevant_returns_zero(self) -> None:
        self.assertEqual(hit_at_k(["a", "b"], set(), k=2), 0.0)

    def test_invalid_k_raises(self) -> None:
        with self.assertRaises(ValueError):
            hit_at_k(["a"], {"a"}, k=0)
        with self.assertRaises(ValueError):
            hit_at_k(["a"], {"a"}, k=-1)


class ReciprocalRankTests(unittest.TestCase):
    def test_first_position_is_one(self) -> None:
        self.assertEqual(reciprocal_rank(["orders", "x", "y"], {"orders"}), 1.0)

    def test_third_position_is_one_third(self) -> None:
        self.assertAlmostEqual(reciprocal_rank(["x", "y", "orders"], {"orders"}), 1.0 / 3)

    def test_no_match_is_zero(self) -> None:
        self.assertEqual(reciprocal_rank(["x", "y"], {"orders"}), 0.0)

    def test_returns_first_match_only(self) -> None:
        # Even though "orders" appears at position 2 and 3, RR uses the
        # first occurrence.
        self.assertAlmostEqual(
            reciprocal_rank(["x", "orders", "orders"], {"orders"}),
            0.5,
        )


class MeanReciprocalRankTests(unittest.TestCase):
    def test_average_across_queries(self) -> None:
        queries = [
            (["users", "orders"], {"orders"}),  # RR = 0.5
            (["events", "audit"], {"audit"}),  # RR = 0.5
            (["x", "y", "z"], {"missing"}),  # RR = 0.0
        ]
        # (0.5 + 0.5 + 0.0) / 3 = 1/3
        self.assertAlmostEqual(mean_reciprocal_rank(queries), 1.0 / 3.0)

    def test_empty_iterable_returns_zero(self) -> None:
        self.assertEqual(mean_reciprocal_rank([]), 0.0)


class NdcgAtKTests(unittest.TestCase):
    def test_perfect_ranking_returns_one(self) -> None:
        # Single relevant at top → DCG = IDCG = 1/log2(2) = 1.0
        self.assertAlmostEqual(ndcg_at_k(["a", "b", "c"], {"a"}, k=3), 1.0)

    def test_relevant_at_position_two(self) -> None:
        # DCG = 1/log2(3); IDCG = 1/log2(2) = 1.0
        self.assertAlmostEqual(
            ndcg_at_k(["x", "a", "b"], {"a"}, k=3),
            1.0 / 1.5849625007211562,  # log2(3)
            places=4,
        )

    def test_no_relevant_in_window(self) -> None:
        self.assertEqual(ndcg_at_k(["x", "y"], {"a"}, k=2), 0.0)

    def test_multiple_relevant_normalises_against_ideal(self) -> None:
        # Two relevant items in a 3-window: actual order has them at
        # positions 1 and 3 (DCG = 1/log2(2) + 1/log2(4) = 1 + 0.5).
        # Ideal would have them at 1 and 2 (IDCG = 1 + 1/log2(3)).
        score = ndcg_at_k(["a", "x", "b"], {"a", "b"}, k=3)
        self.assertGreater(score, 0.0)
        self.assertLess(score, 1.0)


class PrecisionAtKTests(unittest.TestCase):
    def test_all_top_k_relevant(self) -> None:
        self.assertEqual(precision_at_k(["a", "b", "c"], {"a", "b"}, k=2), 1.0)

    def test_half_top_k_relevant(self) -> None:
        self.assertEqual(precision_at_k(["a", "x"], {"a"}, k=2), 0.5)

    def test_none_relevant(self) -> None:
        self.assertEqual(precision_at_k(["x", "y"], {"a"}, k=2), 0.0)


if __name__ == "__main__":
    unittest.main()
