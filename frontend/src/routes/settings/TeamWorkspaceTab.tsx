/**
 * TeamWorkspaceTab — "Team workspace" panel inside Settings.
 *
 * Layout:
 *   - HistoryStoreCard (enable/disable team history store)
 *   - Role banner: shows the current user's role + active admins to ask
 *   - Horizontal sub-nav: Members / Activity / Audit / Settings
 *     - All tabs visible to everyone (read-only)
 *     - Action controls inside each tab self-gate on role (admin-only writes)
 *
 * The inner sub-tab is synced to the `inner` query param so refreshes
 * preserve the selected sub-tab alongside the outer `tab=team` param.
 */

import { useSearchParams } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { ShieldCheck, Eye, Crown } from "lucide-react";
import { Tabs, TabsList, Tab, TabPanel, Badge, useToast } from "../../components/ui";
import HistoryStoreCard from "../../components/HistoryStoreCard";
import { useCurrentUserRole } from "../../hooks/useCurrentUserRole";
import { apiFetch } from "../../lib/api";
import AdminMembers from "../admin/Members";
import AdminActivity from "../admin/Activity";
import AdminAudit from "../admin/Audit";
import AdminSettings from "../admin/Settings";

type InnerTab = "members" | "activity" | "audit" | "settings";

const INNER_TABS: { id: InnerTab; label: string }[] = [
  { id: "members", label: "Members" },
  { id: "activity", label: "Activity" },
  { id: "audit", label: "Audit log" },
  { id: "settings", label: "Settings" },
];

const INNER_TAB_IDS: readonly InnerTab[] = ["members", "activity", "audit", "settings"];

function isInnerTab(value: string | null): value is InnerTab {
  return value !== null && (INNER_TAB_IDS as readonly string[]).includes(value);
}

interface MemberLite {
  username: string;
  hostname: string;
  role: string;
  revoked_at: string | null;
}

function RoleBanner() {
  const qc = useQueryClient();
  const toast = useToast();
  const { role, username, hostname } = useCurrentUserRole();
  const members = useQuery<{ members: MemberLite[] }>({
    queryKey: ["admin-members"],
    queryFn: () => apiFetch<{ members: MemberLite[] }>("/api/admin/members"),
    staleTime: 60_000,
  });

  const admins = (members.data?.members ?? []).filter(
    (m) => m.role === "admin" && !m.revoked_at,
  );

  const isAdmin = role === "admin";
  const noActiveAdmin = !members.isLoading && admins.length === 0;

  const claim = useMutation({
    mutationFn: () => apiFetch("/api/admin/claim", { method: "POST" }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin-me"] });
      qc.invalidateQueries({ queryKey: ["admin-members"] });
      toast.push({ title: "You are now the workspace admin", tone: "success" });
    },
    onError: (err: Error) => {
      toast.push({ title: "Claim failed", description: err.message, tone: "error" });
    },
  });

  return (
    <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border bg-surface-subtle/40 px-4 py-3 text-sm">
      <div className="flex items-center gap-3">
        {isAdmin ? (
          <ShieldCheck size={18} className="text-accent" />
        ) : (
          <Eye size={18} className="text-ink-muted" />
        )}
        <div>
          <div className="font-medium text-ink">
            {username}
            <span className="ml-1.5 font-mono text-xs text-ink-dim">
              @{hostname}
            </span>
          </div>
          <div className="text-xs text-ink-muted">
            Your role:{" "}
            <Badge tone={isAdmin ? "info" : "neutral"}>{role}</Badge>
          </div>
        </div>
      </div>
      {!isAdmin && noActiveAdmin && (
        <button
          type="button"
          disabled={claim.isPending}
          onClick={() => claim.mutate()}
          className="inline-flex items-center gap-1.5 rounded-md border border-accent bg-accent-soft px-3 py-1.5 text-xs font-medium text-accent-ink hover:bg-accent hover:text-white disabled:opacity-50"
        >
          <Crown size={13} />
          {claim.isPending ? "Claiming…" : "Claim admin role"}
        </button>
      )}
      {!isAdmin && admins.length > 0 && (
        <div className="text-xs text-ink-muted">
          To request admin access, contact:{" "}
          <span className="font-mono text-ink">
            {admins.map((a) => a.username).join(", ")}
          </span>
        </div>
      )}
    </div>
  );
}

export default function TeamWorkspaceTab() {
  const [searchParams, setSearchParams] = useSearchParams();

  const innerParam = searchParams.get("inner");
  const inner: InnerTab = isInnerTab(innerParam) ? innerParam : "members";

  function setInner(next: InnerTab) {
    setSearchParams(
      (prev) => {
        const sp = new URLSearchParams(prev);
        sp.set("inner", next);
        return sp;
      },
      { replace: true },
    );
  }

  return (
    <div className="space-y-6">
      <HistoryStoreCard />
      <RoleBanner />
      <Tabs value={inner} onValueChange={(v) => setInner(v as InnerTab)}>
        <TabsList className="flex-wrap gap-1 sm:flex-nowrap overflow-x-auto">
          {INNER_TABS.map((t) => (
            <Tab key={t.id} value={t.id}>
              {t.label}
            </Tab>
          ))}
        </TabsList>

        <TabPanel value="members">
          <AdminMembers />
        </TabPanel>
        <TabPanel value="activity">
          <AdminActivity />
        </TabPanel>
        <TabPanel value="audit">
          <AdminAudit />
        </TabPanel>
        <TabPanel value="settings">
          <AdminSettings />
        </TabPanel>
      </Tabs>
    </div>
  );
}
