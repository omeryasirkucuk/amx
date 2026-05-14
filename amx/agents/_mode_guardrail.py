"""Post-generation guardrail: flag suspected ``alternatives_mode`` inversions.

Per Definition 1 (NLP standard), the self-consistency scorer is a
semantic-similarity metric (mean cosine over sentence-transformer
embeddings). So we expect:

* ``semantic`` mode → alternatives are paraphrases → embeddings cluster
  → **mean SC should be HIGH** across the N alternatives of one asset.
* ``lexical`` mode → alternatives share vocabulary but drift in meaning
  → embeddings spread → **mean SC should be LOWER** across the N
  alternatives of one asset.

When a run violates that expectation we don't fail it — we LOG a
diagnostic. The orchestrator surfaces these in its
``consume_diagnostics`` buffer so the CLI / Studio can show them to the
reviewer. The thresholds are calibrated against the worked examples
from the user's Definition 1 spec and tuned to be permissive (avoid
crying wolf on a single edge-case asset).

Reading the log:

* ``alternatives_mode_inversion_suspect`` — semantic produced low mean
  SC OR lexical produced very high mean SC. Either the LLM ignored the
  directive, the prompt is too weak for this domain, or the labels are
  about to be re-inverted by accident.
* The log line carries the asset key, the mode, the mean SC, and the
  per-alternative scores so a reviewer can drill in.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("agents.mode_guardrail")

#: Floors / ceilings for the per-asset mean self-consistency score
#: under each mode. Calibrated empirically:
#: * semantic paraphrases cluster tightly — expect mean SC well above
#:   0.65; below that suggests the LLM produced meaning-shifted alts
#:   despite the semantic directive (possible inversion).
#: * lexical meaning-shifted alts spread — expect mean SC below 0.85;
#:   above that suggests the LLM collapsed into paraphrases despite the
#:   lexical directive (possible inversion).
SEMANTIC_MIN_MEAN_SC = 0.65
LEXICAL_MAX_MEAN_SC = 0.85


def _coerce_score(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _mean_sc(scores: Sequence[Any]) -> float | None:
    numeric = [s for s in (_coerce_score(x) for x in scores) if s is not None]
    if not numeric:
        return None
    return sum(numeric) / len(numeric)


def check_mode_consistency(
    *,
    asset_label: str,
    mode: str | None,
    confidence_signal: str | None,
    sc_scores: Sequence[Any],
) -> str | None:
    """Return a diagnostic message when the mode/SC pair looks inverted.

    Returns ``None`` when consistency cannot be assessed (mode missing,
    scorer not self-consistency, fewer than two scores) or when stats
    match the mode's expectation.

    The caller — orchestrator's per-asset bookkeeping — appends the
    returned string to its diagnostics buffer and logs it. The function
    deliberately does NOT raise: a flaky signal on one asset should
    never stop the whole run.
    """
    if mode not in ("semantic", "lexical"):
        return None
    # Only the self-consistency signal is interpretable as semantic
    # similarity by Definition 1; other signals (logprob span, judge,
    # self-decl) measure different things and cannot drive an
    # inversion verdict from this guardrail.
    if confidence_signal != "self_consistency":
        return None
    if len(sc_scores) < 2:
        return None

    mean = _mean_sc(sc_scores)
    if mean is None:
        return None

    if mode == "semantic" and mean < SEMANTIC_MIN_MEAN_SC:
        diag = (
            f"alternatives_mode_inversion_suspect: asset={asset_label!r} "
            f"mode=semantic mean_sc={mean:.3f} "
            f"(< floor {SEMANTIC_MIN_MEAN_SC:.2f}); the model produced "
            "meaning-shifted alternates despite the semantic (paraphrase) "
            "directive — likely weak prompt steering or label inversion. "
            f"per_alt_sc={[round(_coerce_score(s) or 0.0, 3) for s in sc_scores]}"
        )
        log.warning(diag)
        return diag

    if mode == "lexical" and mean > LEXICAL_MAX_MEAN_SC:
        diag = (
            f"alternatives_mode_inversion_suspect: asset={asset_label!r} "
            f"mode=lexical mean_sc={mean:.3f} "
            f"(> ceiling {LEXICAL_MAX_MEAN_SC:.2f}); the model produced "
            "near-paraphrases despite the lexical (shared-vocab / drifted-"
            "meaning) directive — likely weak prompt steering or label "
            "inversion. "
            f"per_alt_sc={[round(_coerce_score(s) or 0.0, 3) for s in sc_scores]}"
        )
        log.warning(diag)
        return diag

    return None


__all__ = [
    "SEMANTIC_MIN_MEAN_SC",
    "LEXICAL_MAX_MEAN_SC",
    "check_mode_consistency",
]
