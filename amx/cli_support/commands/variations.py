"""``amx variations`` — generate seeded variations from one chosen alternative.

Mirrors the Studio Variations modal flow on the command line. The user
points at a ``run_results.id`` + alternative letter (or zero-based
index) and the executor generates fresh alternatives anchored on that
seed text.

* ``amx variations <result_id> B`` — alt B of row <result_id>, mode
  defaults to the parent run's ``alternatives_mode``.
* ``amx variations 12345 B --mode lexical --instructions "..."`` —
  override mode + addendum.

The actual work lives in
:func:`amx.agents._orchestrator.variations.variations_one_item` so the
CLI stays a thin wrapper. The CLI version runs synchronously (no SSE).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import click

from amx.agents.rerun_context import RerunContextError
from amx.config import AMXConfig
from amx.storage.sqlite_store import history_store
from amx.utils.console import error, heading, info, render_table, success, warn
from amx.utils.logging import get_logger

LogEvent = Callable[..., None]
log = get_logger("cli.variations")


def _resolve_alt_index(letter_or_index: str) -> int:
    """Accept either a zero-based index ('0', '1', …) or a letter
    ('A', 'b', …) and return the integer index.
    """
    raw = (letter_or_index or "").strip()
    if not raw:
        raise click.ClickException("Empty alternative argument.")
    if raw.isdigit():
        return int(raw)
    if len(raw) == 1 and raw.isalpha():
        idx = ord(raw.upper()) - ord("A")
        if idx < 0 or idx > 25:
            raise click.ClickException(f"Letter {raw!r} is out of range A–Z.")
        return idx
    raise click.ClickException(
        f"Cannot parse {letter_or_index!r}; pass a letter (A–Z) or 0-based index."
    )


def _seed_from_row(row: dict[str, Any], idx: int) -> str:
    """Read the seed text from a row's ``alternatives_json``."""
    alts = row.get("alternatives_json")
    if not isinstance(alts, list):
        raise click.ClickException(
            f"row {row.get('id')}: alternatives_json is not a list "
            f"(value: {alts!r}); cannot resolve seed."
        )
    if idx < 0 or idx >= len(alts):
        raise click.ClickException(
            f"Alternative index {idx} out of range — row has {len(alts)} alternative(s)."
        )
    entry = alts[idx]
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict):
        text = entry.get("text") or entry.get("description") or ""
        if isinstance(text, str):
            return text
    raise click.ClickException(
        f"Cannot extract seed text from alternatives_json[{idx}]: {entry!r}."
    )


def _parent_run_mode(parent_run: dict[str, Any] | None) -> str:
    """Read the parent run's effective alternatives_mode from settings_json."""
    if not parent_run:
        return "semantic"
    settings = parent_run.get("settings_json") or parent_run.get("settings") or {}
    if isinstance(settings, str):
        import json as _json

        try:
            settings = _json.loads(settings) or {}
        except Exception:
            settings = {}
    if isinstance(settings, dict):
        mode = settings.get("alternatives_mode")
        if isinstance(mode, str) and mode in ("semantic", "lexical"):
            return mode
    return "semantic"


def register_variations_command(
    main: click.Group,
    *,
    pass_config: Callable[[Callable[..., Any]], Callable[..., Any]],
    log_event: LogEvent,
) -> None:
    """Attach ``amx variations`` (and ``/variations`` inside the session)."""

    @main.command("variations")
    @click.argument("result_id", type=int)
    @click.argument("alternative", type=str)
    @click.option(
        "--instructions",
        "-i",
        "user_instructions",
        type=str,
        default=None,
        help="Optional free-text addendum appended to the seed directive.",
    )
    @click.option(
        "--mode",
        type=click.Choice(["semantic", "lexical"]),
        default=None,
        help=(
            "Variation diversity mode. Defaults to the parent run's "
            "alternatives_mode so a follow-up Variations stays in the "
            "same exploration."
        ),
    )
    @click.option(
        "--temperature",
        "temperature_override",
        type=float,
        default=None,
        help="Override LLM temperature for this variations run (0.0–1.0).",
    )
    @pass_config
    def variations(
        cfg: AMXConfig,
        result_id: int,
        alternative: str,
        user_instructions: str | None,
        mode: str | None,
        temperature_override: float | None,
    ) -> None:
        """Generate seeded variations of one alternative from a previous run."""
        hs = history_store()
        if hs is None:
            raise click.ClickException(
                "History store is not initialised. Run /history-store enable first."
            )
        row = hs.get_run_result(int(result_id))
        if row is None:
            raise click.ClickException(f"run_results row {result_id} not found.")
        alt_idx = _resolve_alt_index(alternative)
        seed_text = _seed_from_row(row, alt_idx)
        parent_run_id = int(row.get("run_id") or 0)
        if not parent_run_id:
            raise click.ClickException(
                f"row {result_id} has no parent run_id — cannot generate variations."
            )

        if not mode:
            parent_run = hs.get_run(parent_run_id) or {}
            mode = _parent_run_mode(parent_run)

        info(
            f"Generating variations from result {result_id}, alt {alternative.upper()} "
            f"({mode} mode)…"
        )
        info(f'  seed: "{seed_text[:80]}{"…" if len(seed_text) > 80 else ""}"')

        llm_overrides: dict[str, Any] = {}
        if temperature_override is not None:
            llm_overrides["temperature"] = max(0.0, min(1.0, float(temperature_override)))

        from amx.agents._orchestrator.variations import variations_one_item

        try:
            new_run_id, outcome = variations_one_item(
                cfg,
                original_run_id=parent_run_id,
                result_id=int(result_id),
                alternative_index=alt_idx,
                seed_text=seed_text,
                mode=mode,
                user_instructions=user_instructions,
                llm_overrides=llm_overrides or None,
            )
        except RerunContextError as exc:
            error(str(exc))
            log_event(
                event_type="variations",
                status="failed",
                command="variations",
                details={"reason": str(exc), "result_id": int(result_id), "alt": alternative},
            )
            raise click.ClickException(str(exc)) from exc

        heading(
            f"Variations produced {len(outcome.alternatives)} new row(s) under run {new_run_id}"
        )
        if not outcome.alternatives:
            warn(outcome.error or "LLM returned no parseable variation.")
        else:
            rendered: list[list[str]] = []
            for i, alt_text in enumerate(outcome.alternatives):
                trunc = alt_text[:80] + ("…" if len(alt_text) > 80 else "")
                rendered.append([f"{alternative.upper()}{i + 1}", trunc])
            render_table(
                f"Variations under run #{new_run_id} (mode={mode})",
                ["label", "alternative"],
                rendered,
            )
            success(
                f"{len(outcome.alternatives)} variation(s) saved. seed_alternative_id="
                f"{result_id}:{alt_idx}, parent_run_id={parent_run_id}."
            )

        log_event(
            event_type="variations",
            status="success" if not outcome.error else "partial",
            command="variations",
            details={
                "new_run_id": int(new_run_id),
                "result_id": int(result_id),
                "alternative_index": alt_idx,
                "mode": mode,
                "had_instructions": bool((user_instructions or "").strip()),
                "temperature_override": temperature_override,
            },
        )


__all__ = ["register_variations_command"]
