"""Interactive row picker + bulk action helpers for the bulk-review UX.

PR B introduces:

* ``parse_row_spec`` — turn a user string like ``"1,3,5-8"`` or ``"all"`` into
  zero-indexed row positions. Used by the numbered fallback prompt and by the
  ``--pick`` flag's offline tests.
* ``pick_rows`` — pick a subset of rows via ``fzf`` when available, else via a
  numbered ``input()`` prompt. Pure CLI; no Click / Rich dependency so the
  helper stays trivial to unit-test under monkeypatch.
* ``paginate_rows`` — yield slices of a row list with a press-space-or-q
  prompt between pages. Caller decides what to render for each slice.
* ``bulk_confirm`` — render the count-confirmation prompt used by the
  ``--accept-filtered`` / ``--skip-filtered`` / ``--apply-filtered`` flags
  before any side-effect fires.

The module is intentionally minimal — every helper is a pure function or a
thin wrapper around ``input`` / ``subprocess``. The CLI command site composes
these into the user-facing flow.
"""

from __future__ import annotations

import shutil
import subprocess
from collections.abc import Callable, Iterable, Iterator
from typing import Any

# ── Row-spec parsing ───────────────────────────────────────────────────


def parse_row_spec(spec: str, max_n: int) -> list[int]:
    """Convert a row-spec string to zero-indexed positions.

    Accepts comma-separated tokens; each token may be:

    * a single 1-indexed integer (``"3"`` → ``[2]``)
    * a hyphen range (``"5-8"`` → ``[4, 5, 6, 7]``)
    * the literal ``"all"`` (case-insensitive) → ``list(range(max_n))``

    Raises ``ValueError`` with a helpful message on invalid input. Out-of-range
    indices (``> max_n`` or ``< 1``) raise too — silently dropping them would
    surprise a user typing ``1-100`` against a 50-row run.
    """
    raw = (spec or "").strip()
    if not raw:
        raise ValueError("Empty row spec. Type indices like '1,3,5-8' or 'all'.")
    if raw.lower() == "all":
        return list(range(max_n))

    out: list[int] = []
    seen: set[int] = set()
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            lo_s, _, hi_s = token.partition("-")
            try:
                lo = int(lo_s.strip())
                hi = int(hi_s.strip())
            except ValueError as exc:
                raise ValueError(
                    f"Invalid range {token!r}. Expected ``A-B`` with integer A, B."
                ) from exc
            if lo < 1 or hi < 1 or lo > max_n or hi > max_n:
                raise ValueError(f"Range {token!r} out of bounds (valid: 1..{max_n}).")
            if lo > hi:
                lo, hi = hi, lo
            for i in range(lo - 1, hi):
                if i not in seen:
                    seen.add(i)
                    out.append(i)
        else:
            try:
                n = int(token)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid token {token!r}. Use integers, ranges, or 'all'."
                ) from exc
            if n < 1 or n > max_n:
                raise ValueError(f"Index {n} out of bounds (valid: 1..{max_n}).")
            i = n - 1
            if i not in seen:
                seen.add(i)
                out.append(i)
    return out


# ── fzf availability + picker ──────────────────────────────────────────


def fzf_available() -> bool:
    """Return True when an ``fzf`` binary is on ``PATH``."""
    return shutil.which("fzf") is not None


def _fzf_pick(labels: list[str]) -> list[int]:
    """Run fzf with the labels and return zero-indexed selected positions.

    ``labels`` must include a leading ``f"{i + 1}\\t"`` index column so we can
    parse the selection back to a row position. fzf is run with ``--multi``
    so TAB toggles, ENTER confirms.
    """
    if not labels:
        return []
    payload = "\n".join(labels).encode("utf-8")
    try:
        result = subprocess.run(
            ["fzf", "--multi", "--with-nth=2..", "--delimiter=\t"],
            input=payload,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError:
        return []
    if result.returncode not in (0, 1):
        # 130 is fzf's "user cancelled with ESC / Ctrl-C". Treat as empty.
        return []
    picked: list[int] = []
    for raw_line in result.stdout.decode("utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        head, _, _ = line.partition("\t")
        try:
            n = int(head)
        except ValueError:
            continue
        if n >= 1:
            picked.append(n - 1)
    return picked


def _numbered_prompt(
    labels: list[str],
    *,
    prompt: str = "Pick rows (e.g. 1,3,5-8 or 'all'): ",
    print_fn: Callable[[str], None] | None = None,
    input_fn: Callable[[str], str] | None = None,
) -> list[int]:
    """Show a numbered list and parse the user's row spec.

    Splits print and input so tests can monkeypatch both without touching
    sys.stdin. Re-prompts on ``ValueError`` from ``parse_row_spec``.
    """
    pr = print_fn or print
    inp = input_fn or input
    for i, label in enumerate(labels, start=1):
        pr(f"{i}) {label}")
    while True:
        raw = inp(prompt)
        try:
            return parse_row_spec(raw, max_n=len(labels))
        except ValueError as exc:
            pr(f"  ✗ {exc}")


def pick_rows(
    labels: list[str],
    *,
    prefer_fzf: bool = True,
    print_fn: Callable[[str], None] | None = None,
    input_fn: Callable[[str], str] | None = None,
) -> list[int]:
    """Pick zero-indexed positions from ``labels``.

    Routes to ``fzf`` when available (and ``prefer_fzf`` is on); otherwise
    falls back to the numbered Python prompt. The fallback path is the
    contract tested under CI, where fzf is not installed.
    """
    if not labels:
        return []
    if prefer_fzf and fzf_available():
        indexed = [f"{i + 1}\t{lab}" for i, lab in enumerate(labels)]
        picked = _fzf_pick(indexed)
        if picked:
            return picked
        # fzf returning empty (cancelled / no matches) means the user
        # wants no rows — preserve the empty list.
        return []
    return _numbered_prompt(labels, print_fn=print_fn, input_fn=input_fn)


# ── Pagination ─────────────────────────────────────────────────────────


def paginate_rows(
    rows: list[Any],
    *,
    page_size: int,
) -> Iterator[tuple[int, int, list[Any]]]:
    """Yield ``(page_index, total_pages, slice)`` for each page.

    ``page_size`` ≤ 0 yields a single page with the full list (matches the
    spec: ``--paginate 0`` means "no pagination").
    """
    if page_size <= 0 or not rows:
        yield (1, 1, list(rows))
        return
    total = (len(rows) + page_size - 1) // page_size
    for i in range(total):
        yield (i + 1, total, rows[i * page_size : (i + 1) * page_size])


def paginate_with_prompt(
    rows: list[Any],
    *,
    page_size: int,
    render_page: Callable[[int, int, list[Any]], None],
    input_fn: Callable[[str], str] | None = None,
    print_fn: Callable[[str], None] | None = None,
) -> None:
    """Render rows in pages with a between-page space/q prompt.

    Calls ``render_page(page_idx, total_pages, slice)`` for each slice. Between
    slices, waits for the user — empty / space → next page, ``q`` → stop. The
    final page does not prompt.
    """
    pr = print_fn or print
    inp = input_fn or input
    pages = list(paginate_rows(rows, page_size=page_size))
    for i, (page_idx, total, slice_) in enumerate(pages):
        render_page(page_idx, total, slice_)
        if i == len(pages) - 1:
            return
        answer = inp(f"  Page {page_idx}/{total} · [space] next · [q] quit: ").strip().lower()
        if answer == "q":
            pr(f"  Stopped at page {page_idx}/{total}.")
            return


# ── Bulk-action confirmation ───────────────────────────────────────────


def bulk_confirm(
    *,
    action: str,
    count: int,
    sample: Iterable[str] | None = None,
    extra_warning: str | None = None,
    input_fn: Callable[[str], str] | None = None,
    print_fn: Callable[[str], None] | None = None,
) -> bool:
    """Render the ``Will <action> N rows: …  [yes/no]:`` prompt.

    Returns True only on a literal ``yes`` (case-insensitive). Anything else —
    ``no``, ``""``, ``y`` (we want explicit yes), Ctrl-D — returns False so a
    misclick can't silently mutate the database.

    ``extra_warning`` is rendered before the prompt and is used by
    ``--apply-filtered`` to surface the live-DB risk in a second pass.
    """
    pr = print_fn or print
    inp = input_fn or input
    samples = list(sample or [])
    head_preview = ", ".join(samples[:3])
    if len(samples) > 3:
        head_preview = f"{head_preview}, … (+{len(samples) - 3} more)"
    pr(
        f"Will {action} {count} row{'s' if count != 1 else ''}{(': ' + head_preview) if head_preview else ''}."
    )
    if extra_warning:
        pr(extra_warning)
    try:
        answer = inp("Continue? [yes/no]: ").strip().lower()
    except EOFError:
        return False
    return answer == "yes"


__all__ = [
    "parse_row_spec",
    "fzf_available",
    "pick_rows",
    "paginate_rows",
    "paginate_with_prompt",
    "bulk_confirm",
]
