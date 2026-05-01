"""Per-command DB profile scope (0.11.0 multi-DB execution).

A ``ProfileScope`` captures *which* DB profile(s) a single ``/run``,
``/sync`` or ``/ask`` invocation operates on. It is built from one of
three inputs (priority order):

1. CLI flag — ``--db-profile NAME`` (multi). Wins over everything.
2. Slash-arg form — leading ``@`` token in the user's prompt
   (e.g. ``/run @prod_pg @analytics_bq``). Same precedence as the
   CLI flag. (Implementation lives in the command site; the scope
   class only consumes the resolved list.)
3. Persisted default — ``cfg.active_db_profiles``.

The persisted default itself comes from ``/use-db prod_pg
analytics_bq`` (the multi-pick decision the user opted into in the
0.11.0 design). Single-profile setups (the common case) get a
single-element scope and the previous single-active-profile semantics
are preserved.

Construction is cheap; the scope holds *names*, not connectors. Use
``connectors(cfg)`` to iterate ``(name, DBConfig, DatabaseConnector)``
tuples — each connector is opened lazily and closed before the next
one yields, so even large multi-profile runs do not exhaust the OS
file-descriptor budget.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover — type-only import
    from amx.config import AMXConfig, DBConfig
    from amx.db.connector import DatabaseConnector


@dataclass(frozen=True)
class ProfileScope:
    """Ordered, deduped set of DB profile names for a single command call.

    ``profiles`` is the actual scope. ``default`` is the legacy
    "active" pointer — the profile to which write-back operations
    (``/run-apply``, comment write-through) and "where do I go when no
    other context exists" decisions resolve. By convention ``default``
    is ``profiles[0]``.

    The class is frozen so a scope can be passed around and stored on
    a long-lived object (e.g. ``SearchAgent``) without worrying about
    accidental mutation.
    """

    profiles: tuple[str, ...]
    default: str

    # ── Predicates ────────────────────────────────────────────────────

    @property
    def is_empty(self) -> bool:
        return not self.profiles

    @property
    def is_single(self) -> bool:
        return len(self.profiles) == 1

    @property
    def is_multi(self) -> bool:
        return len(self.profiles) > 1

    def __iter__(self) -> Iterator[str]:
        return iter(self.profiles)

    def __len__(self) -> int:
        return len(self.profiles)

    def __contains__(self, name: object) -> bool:
        return name in self.profiles

    # ── Builders ──────────────────────────────────────────────────────

    @classmethod
    def from_names(
        cls,
        names: Sequence[str],
        *,
        default: str | None = None,
    ) -> ProfileScope:
        """Build from an explicit (typically CLI-derived) list of names.

        Dedupes while preserving user-specified order. Falls through
        to ``ProfileScope.empty()`` when the input is empty.
        """
        seen: set[str] = set()
        ordered: list[str] = []
        for raw in names:
            n = (raw or "").strip()
            if not n or n in seen:
                continue
            seen.add(n)
            ordered.append(n)
        if not ordered:
            return cls.empty()
        chosen_default = (default or "").strip() or ordered[0]
        if chosen_default not in seen:
            chosen_default = ordered[0]
        return cls(profiles=tuple(ordered), default=chosen_default)

    @classmethod
    def from_config(cls, cfg: AMXConfig) -> ProfileScope:
        """Build the *persisted* default scope from an AMXConfig.

        Reads ``cfg.active_db_profiles`` first (0.11.0+ source of truth),
        falling back to the single-active scalar when the YAML predates
        this release.
        """
        names = cfg.effective_db_profiles()
        if not names:
            return cls.empty()
        default = cfg.active_db_profile or names[0]
        if default not in names:
            default = names[0]
        return cls(profiles=tuple(names), default=default)

    @classmethod
    def empty(cls) -> ProfileScope:
        return cls(profiles=(), default="")

    # ── Resolution helpers ────────────────────────────────────────────

    def configs(self, cfg: AMXConfig) -> list[tuple[str, DBConfig]]:
        """Resolve scope names to the currently-saved ``DBConfig`` objects.

        Skips names that no longer exist in ``cfg.db_profiles`` (could
        happen after ``/remove-db-profile``); the caller can compare
        the returned length to ``len(self)`` to detect drift.
        """
        out: list[tuple[str, DBConfig]] = []
        for name in self.profiles:
            cfgrow = cfg.db_profiles.get(name)
            if cfgrow is None:
                continue
            out.append((name, cfgrow))
        return out

    def connectors(self, cfg: AMXConfig) -> Iterator[tuple[str, DBConfig, DatabaseConnector]]:
        """Yield ``(name, DBConfig, connector)`` one profile at a time.

        Each connector is opened just before the yield and disposed
        immediately after the consumer resumes the generator — the
        guarantee is that at most one connector is alive in this
        loop at any time, which matches the FD budget the existing
        SearchService uses for single-profile asks.
        """
        from amx.db.connector import DatabaseConnector

        for name, db_cfg in self.configs(cfg):
            conn = DatabaseConnector(db_cfg)
            try:
                yield name, db_cfg, conn
            finally:
                try:
                    conn.close()
                except Exception:
                    # Best-effort dispose; an adapter that mis-implements
                    # close() must not break the per-profile loop.
                    pass

    def with_default(self, name: str) -> ProfileScope:
        """Return a copy with a different ``default`` profile.

        Useful when a write-back command needs to retarget the "primary"
        profile without re-ordering the scope. Raises if ``name`` is
        not in the scope.
        """
        if name not in self.profiles:
            raise ValueError(f"Profile {name!r} is not in this scope ({list(self.profiles)})")
        return ProfileScope(profiles=self.profiles, default=name)

    def __str__(self) -> str:
        if not self.profiles:
            return "(empty scope)"
        if self.is_single:
            return self.profiles[0]
        return ", ".join(self.profiles) + f" (default={self.default})"


__all__ = ["ProfileScope"]
