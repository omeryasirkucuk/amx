"""Equivalence-class deduplication for `/analyze` runs.

Wide schemas (think SAP) repeat the same column hundreds of times across
hundreds of tables — ``mandt``, ``client``, ``created_at``, ``updated_at``,
``customer_id`` — and the per-column LLM cost adds up. Two columns are
``equivalent`` when they share both a (case-insensitive) name AND a coarse
dtype family (so ``varchar(50)`` and ``varchar(100)`` collapse but ``int``
and ``varchar`` stay separate). One LLM call per equivalence class is
typically enough — we send ALL member tables as context (option E in the
release notes) and the model writes one generalised description that we
fan out to every member.

This module is pure data + logic. The LLM call, the orchestrator hook,
and the user-facing prompt all live elsewhere; ``equivalence`` only
defines:

* :func:`dtype_family` — collapse raw dtype strings into family buckets.
* :class:`ColumnMember` — the (schema, table, column, dtype) record we
  feed in.
* :class:`EquivalenceClass` — name + family + members; the unit a single
  LLM call describes.
* :func:`compute_column_equivalence_classes` — bucket members by class.
* :func:`summarize_classes` — counts for the user-facing prompt.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# --------------------------------------------------------------------------
# Dtype family normalization
# --------------------------------------------------------------------------
# Goal: collapse the long tail of backend-specific dtype names into a
# handful of coarse families. We keep this conservative: when in doubt,
# we'd rather under-collapse (treat two columns as different) than
# over-collapse (apply one description to columns that aren't really the
# same shape). The primary call sites are PostgreSQL and Snowflake — the
# table below covers their type names plus the SQLAlchemy-rendered names
# we've seen in profiles.

_DTYPE_FAMILY_MAP: dict[str, str] = {
    # ── integers ─────────────────────────────────────────────────────────
    "int": "integer",
    "int2": "integer",
    "int4": "integer",
    "int8": "integer",
    "smallint": "integer",
    "integer": "integer",
    "bigint": "integer",
    "tinyint": "integer",
    "mediumint": "integer",
    "serial": "integer",
    "bigserial": "integer",
    # ── strings ──────────────────────────────────────────────────────────
    "char": "string",
    "varchar": "string",
    "text": "string",
    "string": "string",
    "nvarchar": "string",
    "nchar": "string",
    "clob": "string",
    "character": "string",
    "bpchar": "string",
    # ── numeric / floating ───────────────────────────────────────────────
    "numeric": "numeric",
    "decimal": "numeric",
    "double": "numeric",
    "real": "numeric",
    "float": "numeric",
    "float4": "numeric",
    "float8": "numeric",
    "money": "numeric",
    "number": "numeric",
    # ── date/time ────────────────────────────────────────────────────────
    "date": "date",
    "time": "time",
    "timetz": "time",
    "timestamp": "timestamp",
    "timestamptz": "timestamp",
    "datetime": "timestamp",
    "datetime2": "timestamp",
    "smalldatetime": "timestamp",
    # ── booleans ─────────────────────────────────────────────────────────
    "bool": "boolean",
    "boolean": "boolean",
    "bit": "boolean",
    # ── structured ───────────────────────────────────────────────────────
    "json": "json",
    "jsonb": "json",
    "xml": "xml",
    "array": "array",
    # ── identifiers ──────────────────────────────────────────────────────
    "uuid": "uuid",
    # ── binary ───────────────────────────────────────────────────────────
    "bytea": "binary",
    "blob": "binary",
    "varbinary": "binary",
    "binary": "binary",
}


def dtype_family(dtype: str | None) -> str:
    """Map a raw dtype string to a coarse family bucket.

    Examples:
        >>> dtype_family('VARCHAR(50)')
        'string'
        >>> dtype_family('numeric(10,2)')
        'numeric'
        >>> dtype_family('timestamp without time zone')
        'timestamp'
        >>> dtype_family('TEXT[]')
        'string'
        >>> dtype_family('exotic_type')
        'exotic_type'
        >>> dtype_family(None)
        'unknown'

    The unknown-dtype escape value is the dtype string itself (lowered),
    so two columns whose dtype isn't in the map will still be grouped
    together if they happen to use the exact same exotic type — we
    don't want to merge unrelated dtypes silently, so we fall through
    to the literal as a self-bucket.
    """
    if not dtype:
        return "unknown"
    raw = str(dtype).strip().lower()
    if not raw:
        return "unknown"
    # Strip array suffix ("text[]" → "text") so arrays of the same
    # element type collapse to the family of that element.
    if raw.endswith("[]"):
        raw = raw[:-2].strip()
    # Drop length / precision: "varchar(50)" → "varchar".
    base = raw.split("(", 1)[0].strip()
    # Drop multiword tails: "timestamp without time zone" → "timestamp".
    base = base.split()[0] if base else raw
    return _DTYPE_FAMILY_MAP.get(base, base)


# --------------------------------------------------------------------------
# Equivalence members + classes
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ColumnMember:
    """One concrete column in the run scope.

    Frozen so it's hashable — we use ColumnMember objects as map keys
    when building the dedup cache that the orchestrator consults during
    ``process_table``.
    """

    schema: str
    table: str
    column: str
    dtype: str
    existing_comment: str = ""

    @property
    def fqcn(self) -> str:
        """Fully-qualified column name, e.g. ``sap.bseg.mandt``."""
        return f"{self.schema}.{self.table}.{self.column}"


@dataclass
class EquivalenceClass:
    """A bucket of columns that share name + dtype family.

    The ``key`` tuple is what makes two members "the same"; everything
    else (member list, ordering of representative) is bookkeeping.
    """

    name: str
    family: str
    members: list[ColumnMember] = field(default_factory=list)

    @property
    def key(self) -> tuple[str, str]:
        return (self.name.lower(), self.family)

    @property
    def size(self) -> int:
        return len(self.members)

    @property
    def is_singleton(self) -> bool:
        return len(self.members) <= 1

    def representative(self) -> ColumnMember:
        """Pick a member whose existing_comment is non-empty if possible.

        Falls back to the first inserted member otherwise. Having a
        commented exemplar in the prompt biases the LLM toward an
        already-validated phrasing — useful when ONLY ONE of N members
        is human-curated and the rest are blank.
        """
        for m in self.members:
            if (m.existing_comment or "").strip():
                return m
        return self.members[0]

    def schemas(self) -> list[str]:
        """Distinct schemas the class touches, in stable order."""
        seen: dict[str, None] = {}
        for m in self.members:
            seen.setdefault(m.schema, None)
        return list(seen.keys())

    def tables(self, limit: int | None = None) -> list[str]:
        """Distinct ``schema.table`` strings the class touches.

        ``limit`` clamps the list — handy for prompt construction where
        we want at most N member tables before we compress to "and X
        more".
        """
        seen: dict[str, None] = {}
        for m in self.members:
            seen.setdefault(f"{m.schema}.{m.table}", None)
        out = list(seen.keys())
        if limit is not None and len(out) > limit:
            return out[:limit]
        return out


def compute_column_equivalence_classes(
    members: list[ColumnMember],
) -> dict[tuple[str, str], EquivalenceClass]:
    """Bucket ColumnMembers into equivalence classes.

    The bucketing key is ``(name.lower(), dtype_family(dtype))``. The
    insertion order of members is preserved within each class so the
    representative is deterministic — important for reproducible runs.
    """
    classes: dict[tuple[str, str], EquivalenceClass] = {}
    for member in members:
        key = (member.column.lower(), dtype_family(member.dtype))
        bucket = classes.get(key)
        if bucket is None:
            bucket = EquivalenceClass(
                name=member.column.lower(),
                family=key[1],
                members=[],
            )
            classes[key] = bucket
        bucket.members.append(member)
    return classes


@dataclass
class EquivalenceSummary:
    """Numbers a caller needs to render the user-facing dedup prompt."""

    total_members: int
    total_classes: int
    multi_member_classes: int
    singleton_classes: int
    largest_class_size: int
    largest_class_name: str

    @property
    def llm_call_savings_pct(self) -> float:
        """Approximate token saving as a percentage of total members.

        Each multi-member class collapses to one LLM call, each singleton
        is one call as before. So the new total is
        (multi_member_classes + singleton_classes) = total_classes.
        """
        if self.total_members == 0:
            return 0.0
        saved = self.total_members - self.total_classes
        return (saved / self.total_members) * 100.0


def summarize_classes(
    classes: dict[tuple[str, str], EquivalenceClass],
) -> EquivalenceSummary:
    """Compress a class map into the headline numbers shown to the user."""
    total_members = sum(c.size for c in classes.values())
    total_classes = len(classes)
    multi = sum(1 for c in classes.values() if c.size > 1)
    singletons = total_classes - multi
    largest_size = 0
    largest_name = ""
    for c in classes.values():
        if c.size > largest_size:
            largest_size = c.size
            largest_name = c.name
    return EquivalenceSummary(
        total_members=total_members,
        total_classes=total_classes,
        multi_member_classes=multi,
        singleton_classes=singletons,
        largest_class_size=largest_size,
        largest_class_name=largest_name,
    )


__all__ = [
    "ColumnMember",
    "EquivalenceClass",
    "EquivalenceSummary",
    "compute_column_equivalence_classes",
    "dtype_family",
    "summarize_classes",
]
