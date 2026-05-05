// Per-page browse scope: profile + (database|catalog) + optional
// schema/table. Studio's multi-profile sidebar lets the user expand
// any number of profiles simultaneously; every page that fetches
// live-DB data reads its scope from the URL via `useScope()` so two
// tabs on different profiles never bleed state into each other.
//
// 2-level backends use `/db/:profile/:database/...`; 3-level
// backends use `/cat/:profile/:catalog/...`. The kind comes straight
// from the route prefix — no follow-up fetch to discover whether
// `:scope` is a database or a catalog.

import { useParams, useLocation } from "react-router-dom";

export type ScopeKind = "database" | "catalog";

export interface Scope {
  /** Required for every browse page. */
  profile: string;
  /** Database name (2-level backends). */
  database?: string;
  /** Catalog name (3-level backends — Databricks, BigQuery). */
  catalog?: string;
  /** Convenience: derived from which of database/catalog is set. */
  kind: ScopeKind;
  /** Schema page and below. */
  schema?: string;
  /** Table page only. */
  table?: string;
}

export interface MaybeScope {
  scope: Scope | null;
  /** Friendly explanation when scope is null — e.g. "no profile in URL". */
  reason: string | null;
}

/**
 * Read the current browse scope from the URL.
 *
 * Returns `{scope: null, reason: …}` if the user is on a page that
 * doesn't carry a profile (e.g. `/runs`, `/settings`). Pages that
 * REQUIRE a scope should narrow the result with a guard at the top
 * of their render.
 */
export function useScope(): MaybeScope {
  const params = useParams();
  const location = useLocation();
  const path = location.pathname;

  // 3-level: /cat/:profile/:catalog/...
  if (path.startsWith("/cat/")) {
    const profile = params.profile;
    const catalog = params.catalog;
    if (!profile) return { scope: null, reason: "missing profile in /cat URL" };
    if (!catalog) return { scope: null, reason: "missing catalog in /cat URL" };
    return {
      scope: {
        profile,
        catalog,
        kind: "catalog",
        schema: params.schema,
        table: params.table,
      },
      reason: null,
    };
  }

  // 2-level: /db/:profile/:database/...
  if (path.startsWith("/db/")) {
    const profile = params.profile;
    const database = params.database;
    if (!profile) return { scope: null, reason: "missing profile in /db URL" };
    if (!database) return { scope: null, reason: "missing database in /db URL" };
    return {
      scope: {
        profile,
        database,
        kind: "database",
        schema: params.schema,
        table: params.table,
      },
      reason: null,
    };
  }

  return { scope: null, reason: "not a browse page" };
}

/**
 * Build the query-string fragment that scopes a `live-db` /
 * `comments` / `generate` API call to a profile + database/catalog.
 *
 * Returns `""` when no fields apply (caller can prepend `?` only
 * when this is non-empty). Callers append additional params with
 * `&` after this fragment.
 */
export function scopeQuery(scope: Scope): string {
  const parts: string[] = [];
  parts.push(`profile=${encodeURIComponent(scope.profile)}`);
  if (scope.database) parts.push(`database=${encodeURIComponent(scope.database)}`);
  if (scope.catalog) parts.push(`catalog=${encodeURIComponent(scope.catalog)}`);
  return parts.join("&");
}

/**
 * Build the in-app URL for a scope. Used by the sidebar tree and
 * any "navigate to this table" link in the SPA.
 */
export function scopePath(
  scope: { profile: string; database?: string; catalog?: string },
  schema?: string,
  table?: string,
): string {
  const segs: string[] = [];
  if (scope.catalog) {
    segs.push("cat", encodeURIComponent(scope.profile), encodeURIComponent(scope.catalog));
  } else if (scope.database) {
    segs.push("db", encodeURIComponent(scope.profile), encodeURIComponent(scope.database));
  } else {
    // No scope segment yet — the profile is selected but no DB/catalog
    // chosen. Caller should generally avoid this; we still produce a
    // safe URL that lands on the profile's first row.
    segs.push("db", encodeURIComponent(scope.profile));
  }
  if (schema) segs.push(encodeURIComponent(schema));
  if (table) segs.push(encodeURIComponent(table));
  return "/" + segs.join("/");
}
