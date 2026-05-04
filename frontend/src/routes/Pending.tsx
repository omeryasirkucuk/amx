import { Inbox } from "lucide-react";

import PageHeader from "../components/PageHeader";
import EmptyState from "../components/EmptyState";

export default function Pending() {
  return (
    <>
      <PageHeader
        eyebrow="Review"
        title="Pending review"
        description="Approved suggestions that haven't been written back to the live database yet."
      />
      <EmptyState
        icon={Inbox}
        title="Pending queue lands in PR-E"
        description="Approve / edit / reject in bulk and trigger /apply with live progress straight from this page."
      />
    </>
  );
}
