import { useQuery } from "@tanstack/react-query";
import { Sparkles } from "lucide-react";

import PageHeader from "../components/PageHeader";
import AskChat from "../components/AskChat";
import { Card, CardBody, CardHeader } from "../components/Card";
import { apiFetch } from "../lib/api";
import { cn } from "../lib/cn";

interface SessionRow {
  id: number;
  title: string | null;
  first_question: string | null;
  started_at: number | null;
  last_active_at: number | null;
  ended_at: number | null;
}

interface SessionsResponse {
  sessions: SessionRow[];
  count: number;
}

export default function Ask() {
  const sessions = useQuery({
    queryKey: ["ask-sessions"],
    queryFn: () => apiFetch<SessionsResponse>("/api/ask/sessions"),
    retry: false,
    staleTime: 0,
  });

  return (
    <>
      <PageHeader
        eyebrow="Conversational"
        title="Ask"
        description="Chat with the AMX search agent over your live database, catalog, and run history."
      />

      <div className="grid gap-4 md:grid-cols-[18rem_1fr]">
        <Card>
          <CardHeader title="Sessions" description="Recent /ask threads — CLI and visualizer share the same SQLite-backed history." />
          <CardBody className="max-h-[60vh] overflow-y-auto p-0">
            {sessions.isLoading ? (
              <div className="px-5 py-6 text-sm text-ink-dim">Loading…</div>
            ) : sessions.data?.sessions?.length ? (
              <ul className="divide-y divide-surface-border">
                {sessions.data.sessions.map((session) => (
                  <li
                    key={session.id}
                    className={cn(
                      "px-4 py-3 text-sm transition hover:bg-surface-subtle/50",
                      session.ended_at == null && "border-l-2 border-positive",
                    )}
                  >
                    <div className="flex items-center justify-between">
                      <span className="truncate font-medium">
                        {session.title || `Session #${session.id}`}
                      </span>
                      <span className="font-mono text-[10px] text-ink-dim">#{session.id}</span>
                    </div>
                    {session.first_question && (
                      <p className="mt-1 line-clamp-2 text-xs text-ink-muted">
                        {session.first_question}
                      </p>
                    )}
                  </li>
                ))}
              </ul>
            ) : (
              <div className="flex flex-col items-center px-5 py-10 text-center text-ink-dim">
                <Sparkles size={20} className="mb-2 opacity-60" />
                <p className="text-sm">No sessions yet.</p>
                <p className="mt-1 text-xs text-ink-dim">
                  Ask a question to start your first one.
                </p>
              </div>
            )}
          </CardBody>
        </Card>

        <AskChat />
      </div>
    </>
  );
}
