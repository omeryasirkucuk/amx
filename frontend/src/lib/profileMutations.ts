// Single source of truth for the React Query invalidations that must
// fire after ANY mutation against ``/api/profiles/db/*`` (create,
// update, delete). Before this helper landed, ``Settings.tsx`` only
// invalidated ``["profiles", "db"]`` — which made the Settings list
// re-render but left every other surface that caches profile-derived
// data stale:
//
// - Sidebar's catalog / database / schema / asset trees
// - Topbar's profile picker context
// - RunNew's scope dropdowns
// - Any view that consumes the ``/api/context`` snapshot
//
// The result was the user reports we're now fixing:
//   - "Delete pretends to work" (Settings row disappears, Sidebar
//     still shows the profile because its query keys weren't busted).
//   - "Profile update needs a page refresh" (Sidebar's
//     ``live-catalogs`` query is keyed by profile and didn't refetch).
//
// Pattern mirrors ``topbar/ProfilePicker.tsx`` which already does
// this correctly for the profile-activation flow.

import type { QueryClient } from "@tanstack/react-query";

/**
 * Invalidate every cached query that may depend on the state of a
 * DB profile. Safe to call after create / update / delete — TanStack
 * Query's partial-key matching means we hit every variant of these
 * keys regardless of which profile they were keyed against.
 */
export function invalidateAfterDbProfileMutation(qc: QueryClient): void {
  // Profile listings (Settings page, sidebar profile rail, system page).
  qc.invalidateQueries({ queryKey: ["profiles", "db"] });
  qc.invalidateQueries({ queryKey: ["db-profiles", "list"] });
  // Context — the active-profile envelope every route reads.
  qc.invalidateQueries({ queryKey: ["context"] });
  // Live metadata trees that key off ``["...", profile, ...]``.
  qc.invalidateQueries({ queryKey: ["live-catalogs"] });
  qc.invalidateQueries({ queryKey: ["live-databases"] });
  qc.invalidateQueries({ queryKey: ["live-schemas"] });
  qc.invalidateQueries({ queryKey: ["live-assets"] });
  // Recent runs surface the profile name in each row, so a renamed
  // or deleted profile would otherwise show the old label.
  qc.invalidateQueries({ queryKey: ["recent-runs"] });
}
