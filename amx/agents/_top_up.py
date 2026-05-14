"""Top-up retry helper — hard guarantee that an LLM call returns
exactly ``n_alternatives`` for every asset.

User contract: "if I set ``n_alternatives=3``, the persisted row
carries 3. Not 2, not 1." Profile agents (and the variations
seed-filter step) can leave a suggestion short of N when the model
under-produces or echoes the seed. This helper makes one
continuation LLM call asking for the missing slots; callers pad
with the existing FALLBACK string pattern if the retry also
under-produces, so every row that lands in storage carries N
entries regardless of model behaviour.

Wire points:

* :func:`amx.agents.profile_agent.ProfileAgent._run_single_batch` —
  after ``apply_confidence_signals``; covers /run, /rerun, and the
  base Variations path (which calls rerun → ProfileAgent).
* :func:`amx.agents._orchestrator.variations._update_variation_columns`
  — after the seed-filter step that drops the seed entry from the
  variations alternatives list. The variations path uses this
  helper specifically when the seed was echoed and one slot was
  legitimately removed.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from amx.utils.logging import get_logger

if TYPE_CHECKING:
    from amx.llm.provider import LLMProvider

log = get_logger("agents.top_up")


_DESCRIPTION_LINE = re.compile(
    r"^\s*DESCRIPTION(?:_\d+)?\s*:\s*(.+?)\s*$",
    re.IGNORECASE | re.MULTILINE,
)


def _build_top_up_prompt(
    *,
    existing_alts: list[str],
    n_needed: int,
    asset_label: str,
    mode: str | None,
    seed_text: str | None,
) -> str:
    """Build the continuation prompt. Names the asset, echoes the
    existing alternatives the LLM already produced, and asks for
    exactly ``n_needed`` MORE alternatives that are not paraphrases
    of the existing set."""
    lines: list[str] = [
        f"You previously generated alternatives for: {asset_label}.",
    ]
    if seed_text:
        lines.append(f'\nSEED_DESCRIPTION:\n  "{seed_text}"')
    if existing_alts:
        lines.append("\nExisting alternatives (do NOT repeat or paraphrase these):")
        for i, alt in enumerate(existing_alts, start=1):
            lines.append(f'  EXISTING_{i}: "{alt}"')
    else:
        lines.append("\n(No alternatives have been produced yet.)")

    mode_lower = (mode or "").strip().lower()
    if mode_lower == "lexical":
        diversity_block = (
            "Generate alternatives in LEXICAL mode: re-use the key tokens of the "
            "existing set / seed, but each new DESCRIPTION_i must propose a "
            "DISTINCT CANDIDATE MEANING (different interpretation of what the "
            "column actually refers to), not just a rephrasing."
        )
    elif mode_lower == "semantic":
        diversity_block = (
            "Generate alternatives in SEMANTIC mode: each new DESCRIPTION_i must "
            "be a PARAPHRASE — same factual content, different surface form. Do "
            "NOT introduce new attributes, scope changes, or nuances."
        )
    else:
        diversity_block = (
            "Each new DESCRIPTION_i must be a genuinely distinct alternative — "
            "neither a verbatim repeat nor a paraphrase of any existing entry."
        )

    lines.append(f"\n{diversity_block}")
    lines.append(
        f"\nReturn exactly {n_needed} NEW description(s) in the same format "
        "as before, using DESCRIPTION_1, DESCRIPTION_2, … labels (numbering "
        "restarts from 1 for this continuation — the caller will merge them "
        "into the existing set)."
    )
    return "\n".join(lines)


def _parse_top_up_response(text: str, *, n_needed: int) -> list[str]:
    """Pull ``DESCRIPTION_*: ...`` lines out of the continuation
    response. Tolerant — accepts un-numbered ``DESCRIPTION:`` as
    well as numbered variants. Returns at most ``n_needed`` entries
    so a chatty model doesn't blow past the requested slot count."""
    if not text:
        return []
    out: list[str] = []
    for match in _DESCRIPTION_LINE.finditer(text):
        candidate = (match.group(1) or "").strip()
        # Strip surrounding quotes the model occasionally adds.
        if len(candidate) >= 2 and candidate[0] in ('"', "'") and candidate[-1] == candidate[0]:
            candidate = candidate[1:-1].strip()
        if candidate:
            out.append(candidate)
            if len(out) >= n_needed:
                break
    return out


def top_up_alternatives(
    *,
    llm: LLMProvider,
    existing_alts: list[str],
    n_needed: int,
    asset_label: str,
    mode: str | None,
    seed_text: str | None = None,
) -> list[str]:
    """Make ONE continuation LLM call asking for ``n_needed`` MORE
    distinct alternatives. Returns the parsed list. Empty list on
    parse failure or LLM error — the caller pads with fallback
    strings to maintain the N-entries guarantee.

    Hard-capped at one call. Token usage flows through the standard
    ``token_tracker`` because ``LLMProvider.chat`` records usage
    internally. Logged at INFO so cost surfaces in studio.log.
    """
    if n_needed <= 0:
        return []
    prompt = _build_top_up_prompt(
        existing_alts=existing_alts,
        n_needed=n_needed,
        asset_label=asset_label,
        mode=mode,
        seed_text=seed_text,
    )
    log.info(
        "top_up: asking for %d more alternatives for %s (existing=%d, mode=%s, seed=%s)",
        n_needed,
        asset_label,
        len(existing_alts),
        mode or "—",
        "yes" if seed_text else "no",
    )
    try:
        result = llm.chat(
            [
                {"role": "system", "content": "You produce database column descriptions."},
                {"role": "user", "content": prompt},
            ]
        )
    except Exception as exc:  # noqa: BLE001 — caller pads on failure
        log.warning("top_up LLM call failed for %s: %s", asset_label, exc)
        return []

    parsed = _parse_top_up_response(getattr(result, "content", "") or "", n_needed=n_needed)

    # Drop any entry that exactly matches an existing one (case +
    # whitespace insensitive) — the model occasionally echoes
    # despite the explicit "do not repeat" instruction.
    def _norm(s: str) -> str:
        return " ".join((s or "").strip().lower().split())

    existing_norms = {_norm(a) for a in existing_alts}
    if seed_text:
        existing_norms.add(_norm(seed_text))
    deduped = [p for p in parsed if _norm(p) not in existing_norms]
    log.info(
        "top_up: %s -- parsed=%d, deduped=%d (target=%d)",
        asset_label,
        len(parsed),
        len(deduped),
        n_needed,
    )
    return deduped[:n_needed]


__all__ = ["top_up_alternatives"]
