"""Equivalence-class deduplication LLM pass.

The dedup pass runs BEFORE the per-table ProfileAgent loop in
``/run`` / ``/run-apply``. For every column equivalence class with two
or more members, it makes a single LLM call with all member tables
listed in the prompt and applies the resulting description to every
member. Singleton classes are left untouched and flow through the
normal per-table profile path.

The dedup pass is opt-in (the user is asked at the start of the run)
and idempotent: applying it twice on the same scope yields the same
descriptions and skip-set.

This module owns the prompt format, the LLM call, the catalog/history
write-back, and the live-DB COMMENT ON ... write-back when the run is
in apply mode. The orchestrator only needs the resulting
:class:`DedupOutcome` so it can filter out columns that were already
handled.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from amx.agents.equivalence import EquivalenceClass
from amx.utils.console import info, step_spinner, success, warn
from amx.utils.logging import get_logger
from amx.utils.token_tracker import tracker

log = get_logger("agents.equivalence_agent")


# --------------------------------------------------------------------------
# Prompt building
# --------------------------------------------------------------------------

_DIVERGES_SENTINEL = "DIVERGES"

_SYSTEM_PROMPT = (
    "You are an expert database documentation writer. You help generate "
    "concise, technically accurate descriptions for database columns that "
    "appear repeatedly across many tables.\n\n"
    "When the same column name + dtype recurs across tables, the column "
    "very often plays the same role (think 'mandt', 'created_at', "
    "'tenant_id'). Your job is to write ONE description that fits ALL of "
    "the listed tables.\n\n"
    "If you have strong evidence that the meaning DIVERGES across the "
    f"member tables (e.g. one is a country code, another a currency "
    f"code), respond with exactly the token '{_DIVERGES_SENTINEL}' as the "
    "first word of your message. Do not write a description — AMX will "
    "fall back to per-table profiling for that class. Use this sparingly: "
    "DIVERGES only when you can name two member tables where the meaning "
    "clearly differs.\n\n"
    "Otherwise: respond with a single description, 1–3 sentences, no "
    "bullet points, no markdown, do not repeat the column name or table "
    "list. Plain prose only."
)


def _format_member_tables(klass: EquivalenceClass, *, table_limit: int = 25) -> str:
    """Render a compact 'tables this column appears in' list.

    Hard-caps at ``table_limit`` entries to keep prompts cheap on huge
    classes; the prompt explicitly tells the model "+ N more" so it
    knows about the long tail without paying for it.
    """
    members = klass.members
    visible = members[:table_limit]
    lines: list[str] = []
    for m in visible:
        existing = (m.existing_comment or "").strip()
        if existing:
            lines.append(f"  - {m.schema}.{m.table}: {existing[:200]}")
        else:
            lines.append(f"  - {m.schema}.{m.table}: (no existing comment)")
    if len(members) > table_limit:
        lines.append(f"  - … and {len(members) - table_limit} more table(s)")
    return "\n".join(lines)


def _build_user_prompt(klass: EquivalenceClass) -> str:
    sample_dtype = klass.members[0].dtype if klass.members else ""
    body = _format_member_tables(klass)
    return (
        f"Column name: {klass.name}\n"
        f"Dtype family: {klass.family}\n"
        f"Example raw dtype (from a representative member): {sample_dtype}\n"
        f"Members ({klass.size} total across {len(klass.schemas())} schema(s)):\n"
        f"{body}\n\n"
        "TASK: Write ONE generalized description for this column. Use the "
        "existing comments above as evidence when present; ignore them when "
        "they look auto-generated or wrong.\n"
        "Output: plain prose, 1–3 sentences, no bullet points. If the "
        f"meaning materially differs across member tables, respond with "
        f"'{_DIVERGES_SENTINEL}' instead."
    )


# --------------------------------------------------------------------------
# Outcome data structures
# --------------------------------------------------------------------------


@dataclass
class ClassDecision:
    """The decision made for one equivalence class.

    ``description`` is set when the LLM produced a usable description;
    ``diverged`` is set when the LLM responded with DIVERGES (or we
    couldn't get a useable answer) — in that case the class falls back
    to normal per-table profiling.
    """

    klass: EquivalenceClass
    description: str = ""
    diverged: bool = False
    error: str = ""

    @property
    def applied(self) -> bool:
        return bool(self.description) and not self.diverged and not self.error


@dataclass
class DedupOutcome:
    """Result of a full dedup pass over many classes.

    ``skip_set`` is what the orchestrator consults to know which
    (schema, table, column) tuples were already handled — those columns
    are filtered out of the per-table ProfileAgent batch.

    ``descriptions`` maps the same (schema, table, column) tuple to the
    description that was applied; the orchestrator wraps these into
    synthetic ReviewResults so apply / catalog-sync still happens for
    them at the end-of-table boundary.
    """

    decisions: list[ClassDecision] = field(default_factory=list)
    skip_set: set[tuple[str, str, str]] = field(default_factory=set)
    descriptions: dict[tuple[str, str, str], str] = field(default_factory=dict)

    @property
    def classes_processed(self) -> int:
        return sum(1 for d in self.decisions if d.applied)

    @property
    def classes_diverged(self) -> int:
        return sum(1 for d in self.decisions if d.diverged)

    @property
    def classes_failed(self) -> int:
        return sum(1 for d in self.decisions if d.error)

    @property
    def members_skipped(self) -> int:
        return len(self.skip_set)


# --------------------------------------------------------------------------
# Main pass
# --------------------------------------------------------------------------


def run_equivalence_pass(
    classes: list[EquivalenceClass],
    *,
    llm: Any,
    db: Any | None = None,
    apply_to_db: bool = False,
    run_id: int | None = None,
    db_profile: str | None = None,
    db_backend: str = "",
    asset_kind: str = "table",
    on_class_done: Any = None,
) -> DedupOutcome:
    """Run a dedup LLM pass over the given equivalence classes.

    Only multi-member classes are passed in — singletons are left to
    the normal per-table flow. The caller (analyze_flow) is responsible
    for filtering before calling this function.

    Each class becomes one LLM call. On success, the description is:
    1) appended to ``descriptions`` for every member,
    2) recorded in the catalog as an "accepted" suggestion for every
       member, linked to ``run_id`` so /history shows it,
    3) (if ``apply_to_db`` and ``db`` provided) written to the live
       database via ``COMMENT ON COLUMN``.

    Members of diverged or failed classes are NOT added to the skip
    set; the normal per-table flow handles them.
    """
    outcome = DedupOutcome()
    if not classes:
        return outcome

    # Filter: only multi-member classes are worth deduping.
    multi = [c for c in classes if not c.is_singleton]
    if not multi:
        return outcome

    info(
        f"Equivalence pass: {len(multi)} multi-member class(es) "
        f"({sum(c.size for c in multi)} member columns)."
    )

    # Lazy import so test environments without LLM creds can still
    # import this module for unit testing the data structures.
    from amx.search.catalog import SearchCatalog

    catalog = SearchCatalog.from_history_store()

    for idx, klass in enumerate(multi, start=1):
        decision = ClassDecision(klass=klass)
        outcome.decisions.append(decision)

        prompt_user = _build_user_prompt(klass)
        messages = [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": prompt_user},
        ]
        label = (
            f"Equivalence agent: '{klass.name}' [{klass.family}] "
            f"({klass.size} members) ({idx}/{len(multi)})"
        )

        try:
            with step_spinner(label):
                result = llm.chat(messages, max_tokens=400, use_logprobs=False)
        except Exception as exc:
            decision.error = str(exc)
            warn(
                f"Equivalence pass: LLM call failed for class '{klass.name}' "
                f"[{klass.family}]: {exc}. Falling back to per-table flow."
            )
            log.warning("Equivalence agent LLM call failed: %s", exc)
            continue

        try:
            tracker.record("equivalence_agent", 0, result.usage)
        except Exception:
            pass

        raw = (result.content or "").strip()
        if not raw:
            decision.error = "empty LLM response"
            warn(
                f"Equivalence pass: empty response for '{klass.name}'. "
                "Falling back to per-table flow."
            )
            continue

        # Strip a leading 'DIVERGES' marker — case-insensitive, with or
        # without trailing punctuation. We're conservative: any leading
        # token that matches counts.
        first_token = raw.split(maxsplit=1)[0].strip(".,:;").upper()
        if first_token == _DIVERGES_SENTINEL:
            decision.diverged = True
            info(
                f"Equivalence pass: '{klass.name}' diverges across members "
                "— falling back to per-table flow."
            )
            continue

        # Strip any leading/trailing quote noise + collapse blank lines
        # the model sometimes inserts. Keep paragraph internal newlines.
        description = raw.strip().strip('"').strip("'").strip()
        if not description:
            decision.error = "blank description after cleanup"
            continue

        decision.description = description

        # Apply to every member: catalog write, optional live-DB write.
        applied_count = 0
        failed_writes: list[tuple[str, str]] = []
        for member in klass.members:
            key = (member.schema, member.table, member.column)
            outcome.descriptions[key] = description
            outcome.skip_set.add(key)

            # Catalog persistence: every member becomes a row in
            # run_results so /history can count them, and the catalog
            # gets the description for /ask.
            if catalog is not None and run_id is not None:
                try:
                    catalog.record_dedup_decision(
                        db_profile=db_profile or "default",
                        db_backend=db_backend,
                        run_id=run_id,
                        schema_name=member.schema,
                        table_name=member.table,
                        column_name=member.column,
                        description=description,
                        equivalence_key=f"{klass.name}::{klass.family}",
                        member_count=klass.size,
                    )
                except Exception as exc:
                    # Soft-fail; live DB write still happens if requested.
                    log.debug(
                        "Equivalence pass: catalog write failed for %s.%s.%s: %s",
                        member.schema, member.table, member.column, exc,
                    )

            if apply_to_db and db is not None:
                try:
                    db.set_column_comment(
                        member.schema, member.table, member.column, description,
                    )
                    applied_count += 1
                except Exception as exc:
                    failed_writes.append((f"{member.schema}.{member.table}.{member.column}", str(exc)))

        if apply_to_db and db is not None:
            success(
                f"  Applied '{klass.name}' description to {applied_count}/"
                f"{klass.size} live DB members."
            )
            if failed_writes:
                warn(f"  {len(failed_writes)} write(s) failed:")
                for label, msg in failed_writes[:3]:
                    warn(f"    {label}: {msg[:120]}")

        if on_class_done is not None:
            try:
                on_class_done(decision)
            except Exception:
                pass

    info(
        f"Equivalence pass complete: {outcome.classes_processed} class(es) "
        f"applied, {outcome.classes_diverged} diverged, "
        f"{outcome.classes_failed} failed; "
        f"{outcome.members_skipped} member columns will skip per-table LLM."
    )
    return outcome


__all__ = [
    "ClassDecision",
    "DedupOutcome",
    "run_equivalence_pass",
]
