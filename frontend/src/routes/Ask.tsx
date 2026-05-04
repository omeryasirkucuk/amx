import { Sparkles } from "lucide-react";

import PageHeader from "../components/PageHeader";
import EmptyState from "../components/EmptyState";

export default function Ask() {
  return (
    <>
      <PageHeader
        eyebrow="Conversational"
        title="Ask"
        description="Chat with the AMX search agent over your live database, catalog, and run history."
      />
      <EmptyState
        icon={Sparkles}
        title="Ask lands in PR-D"
        description="Streaming reasoning + tool calls + final answer will render in this panel as soon as the SSE chat backend ships."
      />
    </>
  );
}
