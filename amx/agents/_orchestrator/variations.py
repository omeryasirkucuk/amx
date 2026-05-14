"""Variations executor: generate seeded alternatives from one chosen description.

The Variations feature lets a user click ✨ on a specific alternative
of an existing run_results row (e.g. "Geographic reference table
storing coordinates") and ask the LLM to generate fresh alternatives
that are biased toward that seed — paraphrases of it in ``semantic``
mode, or shared-vocabulary candidate meanings in ``lexical`` mode.

Where Re-Run regenerates the alternatives **from scratch** with no
seed, Variations anchors generation on the user-chosen text so the
output stays in the same conceptual neighbourhood. The seed is
encoded in the prompt's ``user_instructions`` block (we layer it on
top of the existing Re-Run plumbing rather than fork the agents, so a
future prompt evolution lands in one place). The new ``run_results``
row carries a full audit trail:

* ``seed_alternative_id`` — ``"{parent_result_id}:{alt_index}"``;
* ``seed_alternative_text`` — verbatim text of the seed;
* ``parent_run_id`` — the seed's owning ``analysis_runs.id``;
* ``model`` / ``provider`` — effective LLM identity post-override.

The seed text is filtered out of the new alternatives list before
persistence (the user already sees it on the original row).
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from amx.agents._orchestrator.rerun import RerunContextError, RerunOutcome, rerun_items
from amx.config import AMXConfig
from amx.storage.sqlite_store import history_store
from amx.utils.logging import get_logger

log = get_logger("agents._orchestrator.variations")


def _seed_directive(*, seed_text: str, mode: str, user_addendum: str | None) -> str:
    """Build the SEED_DESCRIPTION + diversity directive injected as the
    ``user_instructions`` block for a Variations run.

    The agents already render any non-empty ``user_instructions`` as a
    suffix to the prompt (see :func:`amx.agents.base._user_instructions_block`).
    We piggy-back on that channel rather than threading a new
    ``seed_description`` field through every agent's prompt builder —
    one less place for the contract to drift.
    """
    mode_lower = (mode or "").strip().lower()
    if mode_lower == "lexical":
        diversity_block = (
            "ALTERNATIVES DIVERSITY (lexical mode — shared vocabulary, allow "
            "meaning to drift):\n"
            "  Each DESCRIPTION_i MUST re-use the key tokens of the "
            "SEED_DESCRIPTION above (the same nouns / verbs / domain "
            "vocabulary) while introducing ONE new conceptual nuance that "
            "shifts what the description claims. A reviewer should be able "
            "to articulate the difference in a single sentence per "
            "alternative. Do NOT echo the seed verbatim."
        )
    else:
        diversity_block = (
            "ALTERNATIVES DIVERSITY (semantic mode — paraphrase only):\n"
            "  Each DESCRIPTION_i MUST be a paraphrase of the SEED_DESCRIPTION "
            "above. Same factual content, different surface form (synonyms, "
            "restructured phrasing, alternative word choices). Do NOT "
            "introduce new attributes, scope changes, or nuances. Do NOT "
            "echo the seed verbatim — return genuinely new wordings."
        )
    addendum = (user_addendum or "").strip()
    suffix = f"\n\nAdditional guidance from the user:\n{addendum}" if addendum else ""
    return (
        f"SEED_DESCRIPTION (the user picked this alternative; generate "
        f'variations of it):\n  "{seed_text}"\n\n'
        f"{diversity_block}"
        f"{suffix}"
    )


def _filter_seed_out(alternatives: list[str], seed_text: str) -> list[str]:
    """Remove any alternative that exactly matches the seed text.

    The LLM occasionally returns the seed as the first variation; the
    user already has it on the original row, so showing it again as a
    "new variation" wastes a slot. Case-insensitive whitespace-tolerant
    comparison.
    """

    def _norm(s: str) -> str:
        return " ".join((s or "").strip().lower().split())

    seed_norm = _norm(seed_text)
    return [alt for alt in alternatives if _norm(alt) != seed_norm]


def variations_one_item(
    cfg: AMXConfig,
    *,
    original_run_id: int,
    result_id: int,
    alternative_index: int,
    seed_text: str,
    mode: str = "semantic",
    user_instructions: str | None = None,
    llm_overrides: dict[str, Any] | None = None,
    job_id: str | None = None,
    cancel_token: threading.Event | None = None,
    on_event: Callable[[str, dict[str, Any]], None] | None = None,
    on_run_created: Callable[[int], None] | None = None,
) -> tuple[int, RerunOutcome]:
    """Generate seeded variations from one row's chosen alternative.

    Returns ``(new_run_id, outcome)``. Raises :class:`RerunContextError`
    on lookup failures (missing row, missing seed text).

    Implementation: piggybacks on :func:`rerun_items` with a
    seed-aware ``user_instructions`` block + a forced
    ``alternatives_mode`` override (the user's pick at the modal's
    top-level radio is the source of truth — wins over both the
    saved profile and any nested override). After the inner re-run
    returns we ``UPDATE`` the new row with the Variations audit
    columns (seed_alternative_id, parent_run_id, etc.).
    """
    seed_text = (seed_text or "").strip()
    if not seed_text:
        raise RerunContextError("seed_text is required for a Variations run.")

    overrides = dict(llm_overrides or {})
    # The user's top-level mode pick wins over any in-flight nested
    # ``llm_overrides.alternatives_mode`` and over the saved profile.
    overrides["alternatives_mode"] = mode

    seed_block = _seed_directive(seed_text=seed_text, mode=mode, user_addendum=user_instructions)

    log.info(
        "variations_one_item: starting (result_id=%s alt=%s mode=%s profile_override=%s)",
        result_id,
        alternative_index,
        mode,
        overrides.get("profile") or "—",
    )
    new_run_id, outcomes = rerun_items(
        cfg,
        target_result_ids=[int(result_id)],
        user_instructions=seed_block,
        llm_overrides=overrides,
        job_id=job_id,
        cancel_token=cancel_token,
        on_event=on_event,
        on_run_created=on_run_created,
    )
    if not outcomes:
        raise RerunContextError("Variations executor returned no outcomes.")
    outcome = outcomes[0]

    # Filter the seed back out of the alternatives, then write the
    # Variations audit columns onto the new run_results row.
    filtered = _filter_seed_out(outcome.alternatives, seed_text)
    hs = history_store()
    if hs is not None and outcome.new_result_id:
        try:
            _update_variation_columns(
                hs,
                new_result_id=int(outcome.new_result_id),
                seed_alternative_id=f"{result_id}:{alternative_index}",
                seed_alternative_text=seed_text,
                parent_run_id=int(original_run_id),
                alternatives_filtered=filtered,
            )
        except Exception as exc:  # noqa: BLE001 — audit-only post-step
            log.warning(
                "Variations audit column write failed for run_result %s: %s",
                outcome.new_result_id,
                exc,
            )

    outcome.alternatives = filtered
    return int(new_run_id), outcome


def _update_variation_columns(
    hs: Any,
    *,
    new_result_id: int,
    seed_alternative_id: str,
    seed_alternative_text: str,
    parent_run_id: int,
    alternatives_filtered: list[str],
) -> None:
    """Patch the new row with Variations audit columns.

    We can't go through the public ``save_run_results`` API because the
    row is already inserted by :func:`rerun_items`. Use the store's
    private connection helper directly; held under the store's lock
    so we don't race a concurrent write.
    """
    import json as _json

    with hs._lock, hs._connect() as conn:  # noqa: SLF001 — internal patch path
        conn.execute(
            """
            UPDATE run_results
            SET seed_alternative_id = ?,
                seed_alternative_text = ?,
                parent_run_id = ?,
                alternatives_json = ?
            WHERE id = ?
            """,
            (
                seed_alternative_id,
                seed_alternative_text,
                int(parent_run_id),
                _json.dumps(alternatives_filtered, ensure_ascii=True),
                int(new_result_id),
            ),
        )


__all__ = ["variations_one_item"]
