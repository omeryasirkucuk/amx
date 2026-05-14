"""Signal D: LLM-as-judge second-pass ranking.

When ``cfg.confidence_signal == "judge"`` the orchestrator issues a
second LLM call after the alternatives are produced, asking the model
to rank them best-to-worst. The rank position is normalised to a
``[0, 1]`` score (rank 1 → ``1.0``, rank N → ``0.0``) and used directly
as the per-alternative confidence score.

To mitigate position bias the alternatives are shuffled before being
shown to the judge; the shuffle is seeded for reproducible tests but
defaults to a fresh seed at runtime so consecutive runs of the same
input still get a fair re-shuffle.

Cost: roughly doubles the per-suggestion token spend on the active LLM,
which is why ``"judge"`` is an opt-in signal — users pick it explicitly
on the profile dropdown instead of getting it by default.
"""

from __future__ import annotations

import random
import re
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("confidence.judge")

#: Match ``RANKING:`` followed by a comma-separated list of 1-based ints.
#: Tolerates whitespace, trailing commas, and surrounding bullets / numbering.
_RANKING_LINE = re.compile(
    r"^RANKING\s*:\s*([0-9,\s]+)",
    re.IGNORECASE | re.MULTILINE,
)


def _build_prompt(shuffled: list[str]) -> str:
    """Format the judge prompt with one alternative per numbered line."""
    lines = [
        "You are evaluating candidate descriptions for one database column.",
        "Rank the alternatives below from MOST to LEAST accurate / informative.",
        "Respond with a single ``RANKING:`` line listing the indices (1-based)",
        "in best-to-worst order, separated by commas. Example for 3 inputs:",
        "    RANKING: 2, 1, 3",
        "Add a short ``REASONING:`` line if useful, but the RANKING is required.",
        "",
        "Alternatives:",
    ]
    for idx, text in enumerate(shuffled, start=1):
        lines.append(f"{idx}. {text}")
    return "\n".join(lines)


def _parse_ranking(response_text: str, n: int) -> list[int]:
    """Return the 1-based ranking parsed out of the response, or ``[]``."""
    match = _RANKING_LINE.search(response_text or "")
    if not match:
        return []
    raw = match.group(1)
    out: list[int] = []
    for piece in raw.split(","):
        piece = piece.strip()
        if not piece:
            continue
        try:
            idx = int(piece)
        except ValueError:
            continue
        if 1 <= idx <= n and idx not in out:
            out.append(idx)
    return out


def score_per_alternative(
    alternatives: list[str],
    llm: Any,
    *,
    seed: int | None = None,
    shuffle: bool = True,
) -> list[float | None]:
    """Return one judge-derived score per alternative, aligned by index.

    ``llm`` is an object with a ``.chat(messages, ...)`` method matching
    :class:`amx.llm.provider.LLMProvider`'s contract; the judge call
    uses a single user message containing the ranking prompt.

    ``shuffle`` is on by default to mitigate position bias from the
    judge; pass ``False`` in tests where the assertion needs the
    identity mapping between ``RANKING:`` indices and ``alternatives``.

    A failure inside the LLM call yields ``[None, …]`` of length N so
    the run is never aborted by a judge regression; the ensemble
    downstream degrades gracefully to the remaining signals.
    """
    n = len(alternatives)
    if n == 0:
        return []
    if n == 1:
        return [1.0]

    shuffled_indices = list(range(n))
    if shuffle:
        rng = random.Random(seed)
        rng.shuffle(shuffled_indices)
    shuffled = [alternatives[i] for i in shuffled_indices]
    prompt = _build_prompt(shuffled)

    try:
        result = llm.chat([{"role": "user", "content": prompt}])
        response_text = getattr(result, "content", "") or ""
    except Exception as exc:  # pragma: no cover — environmental
        log.warning("Judge LLM call failed: %s", exc)
        return [None] * n

    ranking_in_shuffled = _parse_ranking(response_text, n)
    if not ranking_in_shuffled:
        return [None] * n

    # ``ranking_in_shuffled`` ranks the *shuffled* positions; map each
    # back to the original alternative index.
    out: list[float | None] = [None] * n
    denom = max(1, n - 1)
    for rank, shuffled_idx in enumerate(ranking_in_shuffled):
        original_idx = shuffled_indices[shuffled_idx - 1]
        out[original_idx] = (n - 1 - rank) / denom
    return out


__all__ = ["score_per_alternative"]
