import { useQuery } from "@tanstack/react-query";
import { Link, useLocation, useParams } from "react-router-dom";
import { Database as DatabaseIcon, HardDrive, Layers } from "lucide-react";

import { api } from "../lib/api";
import { cn } from "../lib/cn";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import EmptyState from "../components/EmptyState";
import { Skeleton } from "../components/ui";

/**
 * Profile-level browse page. Sits between Landing (sidebar tree only)
 * and Database (the per-database view): when the user clicks the
 * profile crumb in the top-bar breadcrumb, they land here and see
 * every catalog or database reachable under the profile as a list
 * they can drill into.
 *
 * The URL prefix decides which sub-resource we render:
 *   - ``/db/:profile`` → list databases (2-level backends)
 *   - ``/cat/:profile`` → list catalogs (3-level backends — Databricks
 *     Unity Catalog, BigQuery)
 *
 * Both fetches are cheap and already cached by the sidebar tree, so
 * landing here after expanding the profile in the sidebar is usually
 * an instant render off the React Query cache.
 */
export default function Profile() {
  const params = useParams();
  const location = useLocation();
  const profile = params.profile ?? "";
  // The route prefix tells us which sub-resource the profile uses
  // (no probe needed). 3-level backends route through ``/cat/…``,
  // 2-level through ``/db/…`` — the sidebar already split them, so
  // the breadcrumb link landing here just preserves whichever prefix
  // the user was already browsing under.
  const isCatalogRoute = location.pathname.startsWith("/cat/");

  const catalogs = useQuery({
    queryKey: ["live-catalogs", profile],
    queryFn: () => api.liveCatalogs({ profile }),
    enabled: !!profile && isCatalogRoute,
    retry: false,
  });
  const databases = useQuery({
    queryKey: ["live-databases", profile],
    queryFn: () => api.liveDatabases({ profile }),
    enabled: !!profile && !isCatalogRoute,
    retry: false,
  });

  if (!profile) {
    return (
      <EmptyState
        icon={DatabaseIcon}
        title="Pick a profile from the sidebar"
        description="Expand a DB profile in the left tree to start browsing."
      />
    );
  }

  const active = isCatalogRoute ? catalogs : databases;
  const names: string[] = isCatalogRoute
    ? (catalogs.data?.catalogs ?? [])
    : (databases.data?.databases ?? []);
  const activeName: string | null = isCatalogRoute
    ? (catalogs.data?.active_catalog ?? catalogs.data?.active_project ?? null)
    : (databases.data?.active_database ?? null);

  const routePrefix = isCatalogRoute ? "/cat" : "/db";
  const Icon = isCatalogRoute ? Layers : HardDrive;
  const childLabel = isCatalogRoute ? "Catalogs" : "Databases";
  const childSingular = isCatalogRoute ? "catalog" : "database";

  return (
    <>
      <PageHeader
        title={profile}
        breadcrumbs={[
          { label: "Browse", to: "/" },
          { label: profile },
        ]}
        description={
          <span className="block text-[11px] uppercase tracking-wider text-ink-dim">
            Profile · pick a {childSingular} to drill into
          </span>
        }
      />

      <Card>
        <CardHeader
          title={childLabel}
          description={
            active.data
              ? `${names.length} ${childSingular}${names.length === 1 ? "" : "s"} reachable.`
              : undefined
          }
        />
        <CardBody className="p-0">
          {active.isLoading ? (
            <ul className="divide-y divide-border">
              {Array.from({ length: 3 }).map((_, i) => (
                <li key={i} className="flex items-center gap-3 px-5 py-3">
                  <Skeleton shape="circle" className="h-3.5 w-3.5" />
                  <Skeleton className="h-3 w-1/3" />
                </li>
              ))}
            </ul>
          ) : active.error ? (
            <div className="px-5 py-6 text-sm text-critical">
              {(active.error as Error).message}
            </div>
          ) : names.length ? (
            <ul className="divide-y divide-border">
              {names.map((name) => (
                <li
                  key={name}
                  className="group flex items-stretch text-sm transition-colors duration-fast hover:bg-surface-subtle/50"
                >
                  <Link
                    to={`${routePrefix}/${encodeURIComponent(profile)}/${encodeURIComponent(name)}`}
                    className="flex flex-1 items-center gap-3 px-5 py-2.5"
                  >
                    <Icon size={14} className="text-accent" />
                    <span className="min-w-0 flex-1 truncate font-mono text-ink">
                      {name}
                    </span>
                    {activeName === name && (
                      <span
                        className={cn(
                          "shrink-0 rounded bg-accent-soft px-1.5 py-px text-[10px] font-semibold uppercase tracking-wide text-accent-ink",
                        )}
                        title={`Profile is pinned to ${name}`}
                      >
                        active
                      </span>
                    )}
                  </Link>
                </li>
              ))}
            </ul>
          ) : (
            <div className="px-5 py-5">
              <EmptyState
                icon={DatabaseIcon}
                title={`No ${childLabel.toLowerCase()} reachable`}
                description={`The profile's connector returned no ${childSingular}s. Check credentials or the profile configuration under Settings.`}
                compact
              />
            </div>
          )}
        </CardBody>
      </Card>
    </>
  );
}
