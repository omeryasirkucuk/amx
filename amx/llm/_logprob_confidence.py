"""Logprob-based confidence scoring for LLM-generated descriptions.

Extracted from :mod:`amx.llm.provider`. The provider exposes raw
token-logprob arrays from the underlying chat completion; this module
turns them into a confidence number we can render in the run summary
and use to gate auto-apply.

All scoring functions are pure (only stdlib + the LiteLLM-shaped
``logprobs_content`` list as input). They share a notion of
"description value spans" — substrings of the model output that
correspond to AMX-relevant descriptions (table comment, column
description, alternatives) — so the weighted score reflects only the
description text, not surrounding JSON / response scaffolding.

``provider.py`` re-exports the public names so historical imports
(``from amx.llm.provider import logprob_confidence_score`` and
``confidence_from_logprobs``) continue to work unchanged.
"""

from __future__ import annotations

import math
import re

from amx.utils.logging import get_logger

log = get_logger("llm.provider.logprob")


_BOILERPLATE_TOKENS: frozenset[str] = frozenset(
    {
        "COLUMN",
        "DESCRIPTION",
        "DESCRIPTION_1",
        "DESCRIPTION_2",
        "DESCRIPTION_3",
        "DESCRIPTION_4",
        "DESCRIPTION_5",
        "TABLE_DESCRIPTION",
        "TABLE_DESCRIPTION_1",
        "TABLE_DESCRIPTION_2",
        "TABLE_DESCRIPTION_3",
        "TABLE_DESCRIPTION_4",
        "TABLE_DESCRIPTION_5",
        "CONFIDENCE",
        "REASONING",
        "BEST_DESCRIPTION",
    }
)


def _lp_token_text(token_obj: object) -> str:
    if isinstance(token_obj, dict):
        return str(token_obj.get("token", "") or "")
    return str(getattr(token_obj, "token", "") or "")


def _lp_token_logprob(token_obj: object) -> float | None:
    raw = (
        token_obj.get("logprob")
        if isinstance(token_obj, dict)
        else getattr(token_obj, "logprob", None)
    )
    if raw is None:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _is_value_token(token_text: str) -> bool:
    t = (token_text or "").strip()
    if not t:
        return False
    upper = t.upper().strip(":")
    if upper in _BOILERPLATE_TOKENS:
        return False
    if t in {"{", "}", "[", "]", ":", ",", '"', "```"}:
        return False
    return not all(ch in "-_=*#`|:;,.()[]{} " for ch in t)


def _description_value_spans(text: str) -> list[tuple[int, int]]:
    """Return spans for generated description/comment values only."""
    if not text:
        return []
    spans: list[tuple[int, int]] = []
    json_pattern = re.compile(
        r'"(?:description|comment|best_description|table_description)(?:_\d+)?"\s*:\s*"((?:\\.|[^"\\])*)"',
        re.IGNORECASE,
    )
    for match in json_pattern.finditer(text):
        spans.append((match.start(1), match.end(1)))
    label_pattern = re.compile(
        r"(?im)^(?:DESCRIPTION(?:_\d+)?|TABLE_DESCRIPTION(?:_\d+)?|BEST_DESCRIPTION|COMMENT)\s*:\s*(.+)$"
    )
    for match in label_pattern.finditer(text):
        value = match.group(1).strip()
        if not value:
            continue
        offset = match.group(1).find(value)
        start = match.start(1) + max(0, offset)
        spans.append((start, start + len(value)))
    spans.sort()
    merged: list[tuple[int, int]] = []
    for start, end in spans:
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return merged


def _weighted_score_for_spans(
    logprobs_content: list | None, spans: list[tuple[int, int]] | None = None
) -> float | None:
    if not logprobs_content:
        return None
    weighted_logprob_sum = 0.0
    total_weight = 0.0
    token_spans = _logprob_token_spans(logprobs_content)
    for tok_start, tok_end, token_obj in token_spans:
        if spans and not any(
            tok_end > span_start and tok_start < span_end for span_start, span_end in spans
        ):
            continue
        token_text = _lp_token_text(token_obj)
        if not _is_value_token(token_text):
            continue
        lp = _lp_token_logprob(token_obj)
        if lp is None:
            continue
        weight = max(1.0, float(len(token_text.strip())))
        weighted_logprob_sum += lp * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    avg_lp = weighted_logprob_sum / total_weight
    return math.exp(avg_lp)


def logprob_confidence_score(logprobs_content: list | None) -> float | None:
    """Weighted geometric-mean confidence from generated description/comment text."""
    if not logprobs_content:
        return None
    generated_text = "".join(_lp_token_text(token_obj) for token_obj in logprobs_content)
    spans = _description_value_spans(generated_text)
    return _weighted_score_for_spans(logprobs_content, spans or None)


def _logprob_token_spans(logprobs_content: list | None) -> list[tuple[int, int, object]]:
    """Best-effort offsets for completion tokens reconstructed from token text."""
    if not logprobs_content:
        return []
    spans: list[tuple[int, int, object]] = []
    pos = 0
    for token_obj in logprobs_content:
        token_text = _lp_token_text(token_obj)
        start = pos
        pos += len(token_text)
        spans.append((start, pos, token_obj))
    return spans


def logprob_confidence_score_for_text(
    logprobs_content: list | None,
    generated_text: str,
    target_text: str,
) -> float | None:
    """Score the logprob confidence for one generated text fragment.

    Provider APIs return logprobs for completion tokens, not for parsed AMX
    suggestions. AMX reconstructs token offsets and scores only the tokens
    overlapping the selected description text, falling back to the full
    response score when the fragment cannot be located exactly.
    """
    if not logprobs_content or not generated_text or not target_text:
        return None

    start = generated_text.find(target_text)
    if start < 0:
        return None
    end = start + len(target_text)

    return _weighted_score_for_spans(logprobs_content, [(start, end)])


def confidence_from_logprobs(
    logprobs_content: list | None,
    high_threshold: float = 0.85,
    medium_threshold: float = 0.50,
) -> str | None:
    """Map weighted geometric-mean token probability to HIGH/MEDIUM/LOW."""
    score = logprob_confidence_score(logprobs_content)
    if score is None:
        return None
    log.debug("Weighted logprob confidence score=%.6f", score)
    if score >= high_threshold:
        return "HIGH"
    if score >= medium_threshold:
        return "MEDIUM"
    return "LOW"
