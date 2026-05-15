/**
 * Shared helpers for the Catalog-Freshness recurrence picker.
 *
 * Two callers consume this module:
 *
 * 1. ``ScheduleCacheRefreshDialog`` — turns the user's recurrence
 *    pick (and optional custom cron) into the wire format the
 *    backend stores, and reverse-maps an existing schedule's cron
 *    back to a dropdown value when the dialog opens in edit mode.
 * 2. ``CatalogFreshnessBadge`` + ``CatalogRefreshSchedules`` —
 *    render a human-readable label for the schedule's cron in lists
 *    and detail panels.
 *
 * Centralising the mapping keeps the preset cron strings in one
 * place; without this the dropdown labels in the dialog could
 * disagree with the labels in the management list (the prior inline
 * implementations already drifted by one entry).
 */

export type Recurrence =
  | "none"
  | "1h"
  | "6h"
  | "12h"
  | "1d"
  | "1w"
  | "custom";

export interface RecurrenceOption {
  value: Recurrence;
  label: string;
  /**
   * Cron expression rendered when this option is picked. ``null``
   * means "no cron — one-shot schedule fires once and stays at
   * completed". ``custom`` uses whatever the user types into the
   * cron input directly, so its preset is also ``null`` here.
   */
  cron: string | null;
}

export const RECURRENCE_OPTIONS: RecurrenceOption[] = [
  { value: "none", label: "One-shot (fire once)", cron: null },
  { value: "1h", label: "Every 1 hour", cron: "0 * * * *" },
  { value: "6h", label: "Every 6 hours", cron: "0 */6 * * *" },
  { value: "12h", label: "Every 12 hours", cron: "0 */12 * * *" },
  { value: "1d", label: "Every day at 03:00", cron: "0 3 * * *" },
  { value: "1w", label: "Every Sunday at 03:00", cron: "0 3 * * 0" },
  { value: "custom", label: "Custom cron (advanced)", cron: null },
];

/**
 * Convert a dropdown pick (+ optional custom text) into the cron
 * string the backend persists. Returns ``null`` for one-shot
 * schedules and for custom picks with an empty text input — the
 * caller should surface a validation error when ``custom`` resolves
 * to ``null``.
 */
export function intervalToCron(
  value: Recurrence,
  customCron: string,
): string | null {
  if (value === "none") return null;
  if (value === "custom") {
    const trimmed = customCron.trim();
    return trimmed || null;
  }
  const opt = RECURRENCE_OPTIONS.find((o) => o.value === value);
  return opt?.cron ?? null;
}

/**
 * Inverse of :func:`intervalToCron`. Used when the edit dialog
 * opens against an existing schedule — the cron string flows back
 * to the dropdown so the user sees the friendly label instead of a
 * raw expression they have to recognise.
 *
 * Unknown expressions land on ``"custom"`` so the cron input
 * surfaces and the user can edit it directly.
 */
export function cronToRecurrence(
  expr: string | null | undefined,
): Recurrence {
  if (!expr) return "none";
  const trimmed = expr.trim();
  if (!trimmed) return "none";
  const match = RECURRENCE_OPTIONS.find((o) => o.cron === trimmed);
  return match?.value ?? "custom";
}

/**
 * Compact label rendered in schedule lists and the freshness pill
 * dropdown. ``"One-shot"`` for NULL, the friendly recurrence label
 * for known presets, and ``"Cron: <expr>"`` for custom expressions
 * so a power user can still read the raw cron at a glance.
 */
export function recurrenceLabel(
  expr: string | null | undefined,
): string {
  if (!expr) return "One-shot";
  const trimmed = expr.trim();
  if (!trimmed) return "One-shot";
  const match = RECURRENCE_OPTIONS.find((o) => o.cron === trimmed);
  if (match) return match.label;
  return `Cron: ${trimmed}`;
}
