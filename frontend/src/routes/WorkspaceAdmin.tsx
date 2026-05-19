/**
 * Workspace Admin — four-tab admin panel shown only to admin-role users.
 *
 * Tabs:
 *   Members  — member list with promote/demote/revoke actions
 *   Activity — live session event feed (polled every 30 s)
 *   Audit    — chronological admin audit log with filters
 *   Settings — read-only shared store configuration
 *
 * Visibility gate: the parent route checks useCurrentUserRole() and
 * renders this page only when role === "admin". The sidebar entry
 * is similarly gated.
 */

import { useSearchParams } from "react-router-dom";
import PageHeader from "../components/PageHeader";
import { Tabs, TabsList, Tab, TabPanel } from "../components/ui";
import AdminMembers from "./admin/Members";
import AdminActivity from "./admin/Activity";
import AdminAudit from "./admin/Audit";
import AdminSettings from "./admin/Settings";

type AdminTab = "members" | "activity" | "audit" | "settings";

const TABS: { id: AdminTab; label: string }[] = [
  { id: "members", label: "Members" },
  { id: "activity", label: "Activity" },
  { id: "audit", label: "Audit log" },
  { id: "settings", label: "Settings" },
];

export default function WorkspaceAdmin() {
  const [params, setParams] = useSearchParams();
  const tab = (params.get("tab") as AdminTab) || "members";

  function setTab(next: AdminTab) {
    setParams({ tab: next }, { replace: true });
  }

  return (
    <div className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6">
      <PageHeader
        title="Workspace Admin"
        description="Manage team members, review activity, and inspect workspace configuration."
      />
      <Tabs
        value={tab}
        onValueChange={(v) => setTab(v as AdminTab)}
        className="mt-4"
      >
        <TabsList className="mb-6 flex flex-wrap gap-1 sm:flex-nowrap">
          {TABS.map((t) => (
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
