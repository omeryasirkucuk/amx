/**
 * PR C — pinned-cells store.
 *
 * Persists the user's "pin for comparison" picks in localStorage so a
 * pin survives page reloads and tab switches. Keyed by ``db_profile``
 * so two profiles don't cross-pollute each other's pinned set. When
 * the active profile is unknown we fall through to a ``__global``
 * bucket so the affordance still works during onboarding.
 */
export interface PinnedCell {
  run_id: number;
  db?: string | null;
  schema: string;
  table: string;
  /** ``null`` for table-level cells; the cell type is inferred from
   *  whether this field is present. */
  column: string | null;
}

const STORAGE_PREFIX = "amx.compare.pinnedCells.";

function storageKey(profile: string | null | undefined): string {
  const slug = (profile || "").trim() || "__global";
  return `${STORAGE_PREFIX}${slug}`;
}

function safeParse(raw: string | null): PinnedCell[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(
      (entry): entry is PinnedCell =>
        entry &&
        typeof entry === "object" &&
        typeof entry.run_id === "number" &&
        typeof entry.schema === "string" &&
        typeof entry.table === "string",
    );
  } catch {
    return [];
  }
}

export function readPinnedCells(profile: string | null | undefined): PinnedCell[] {
  if (typeof window === "undefined") return [];
  return safeParse(window.localStorage.getItem(storageKey(profile)));
}

function writePinnedCells(
  profile: string | null | undefined,
  cells: PinnedCell[],
): void {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(storageKey(profile), JSON.stringify(cells));
}

function sameCell(a: PinnedCell, b: PinnedCell): boolean {
  return (
    a.run_id === b.run_id &&
    a.schema === b.schema &&
    a.table === b.table &&
    (a.column ?? null) === (b.column ?? null)
  );
}

/** Add ``cell`` to the pinned set (no-op when already pinned). */
export function pinCell(
  profile: string | null | undefined,
  cell: PinnedCell,
): PinnedCell[] {
  const current = readPinnedCells(profile);
  if (current.some((c) => sameCell(c, cell))) return current;
  const next = [...current, cell];
  writePinnedCells(profile, next);
  return next;
}

/** Remove ``cell`` from the pinned set. */
export function unpinCell(
  profile: string | null | undefined,
  cell: PinnedCell,
): PinnedCell[] {
  const current = readPinnedCells(profile);
  const next = current.filter((c) => !sameCell(c, cell));
  writePinnedCells(profile, next);
  return next;
}

export function isPinned(
  profile: string | null | undefined,
  cell: PinnedCell,
): boolean {
  return readPinnedCells(profile).some((c) => sameCell(c, cell));
}

export function clearPinnedCells(profile: string | null | undefined): void {
  writePinnedCells(profile, []);
}

/** Render a pinned cell as the colon-separated wire format used by the
 *  ``RunsCompare`` ``?cells=`` query param: ``schema.table.col:run_id``
 *  or ``schema.table:run_id`` for table-level cells. */
export function pinnedCellToToken(cell: PinnedCell): string {
  const parts = [cell.schema, cell.table];
  if (cell.column) parts.push(cell.column);
  return `${parts.join(".")}:${cell.run_id}`;
}

export function pinnedCellFromToken(token: string): PinnedCell | null {
  const [path, ridRaw] = token.split(":");
  if (!path || !ridRaw) return null;
  const rid = Number(ridRaw);
  if (!Number.isFinite(rid)) return null;
  const parts = path.split(".");
  if (parts.length < 2 || parts.length > 3) return null;
  return {
    run_id: rid,
    schema: parts[0],
    table: parts[1],
    column: parts[2] ?? null,
  };
}
