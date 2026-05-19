/**
 * TeamWorkspaceTab — "Team workspace" panel inside Settings.
 *
 * Layout:
 *   - Always visible: HistoryStoreCard (enable/disable team history store)
 *   - Admin-only: horizontal sub-nav with Members / Activity / Audit / Settings
 *
 * The inner sub-tab is synced to the `inner` query param so refreshes
 * preserve the selected sub-tab alongside the outer `tab=team` param.
 */

import { useSearchParams } from "react-router-dom";
import { Tabs, TabsList, Tab, TabPanel } from "../../components/ui";
import HistoryStoreCard from "../../components/HistoryStoreCard";
import { useCurrentUserRole } from "../../hooks/useCurrentUserRole";
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

export default function TeamWorkspaceTab() {
  const { role } = useCurrentUserRole();
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

      {role === "admin" && (
        <div>
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
      )}
    </div>
  );
}
