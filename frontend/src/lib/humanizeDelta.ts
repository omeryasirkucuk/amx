/**
 * Lightweight relative-time formatter. Returns strings like
 * ``"3 hours ago"``, ``"yesterday"``, ``"just now"``. The Studio
 * doesn't pull in ``date-fns`` so this stays a 30-line helper used by
 * the doc-profile health line in Settings.
 *
 * Accepts seconds since the epoch (Python ``time.time()``) so the
 * caller doesn't have to remember whether the backend serialised in
 * seconds or milliseconds — the doc-profile-health endpoint emits
 * seconds via ``cfg.doc_profiles_last_ingested_at``.
 */
export function humanizeDelta(epochSeconds: number | null | undefined): string {
  if (!epochSeconds || !Number.isFinite(epochSeconds)) return "never";
  const now = Date.now() / 1000;
  const diff = Math.max(0, now - epochSeconds);
  if (diff < 30) return "just now";
  if (diff < 60) return "less than a minute ago";
  const minutes = Math.floor(diff / 60);
  if (minutes < 60) return `${minutes} minute${minutes === 1 ? "" : "s"} ago`;
  const hours = Math.floor(diff / 3600);
  if (hours < 24) return `${hours} hour${hours === 1 ? "" : "s"} ago`;
  const days = Math.floor(diff / 86400);
  if (days === 1) return "yesterday";
  if (days < 30) return `${days} days ago`;
  const months = Math.floor(diff / (86400 * 30));
  if (months < 12) return `${months} month${months === 1 ? "" : "s"} ago`;
  const years = Math.floor(diff / (86400 * 365));
  return `${years} year${years === 1 ? "" : "s"} ago`;
}
