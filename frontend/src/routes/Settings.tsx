import { Settings as SettingsIcon } from "lucide-react";

import PageHeader from "../components/PageHeader";
import EmptyState from "../components/EmptyState";

export default function Settings() {
  return (
    <>
      <PageHeader
        eyebrow="Configuration"
        title="Settings"
        description="DB, LLM, doc and code profile management — same configuration surface as the CLI wizards."
      />
      <EmptyState
        icon={SettingsIcon}
        title="Profile editor lands in PR-E"
        description="Add, edit, activate, and connection-test DB and LLM profiles directly from the browser. Also where you'll wire up codebases and document RAG sources."
      />
    </>
  );
}
