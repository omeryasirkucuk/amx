import { useParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";

import { apiFetch } from "../lib/api";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";

interface RunDetailPayload {
  id: number;
  command: string;
  status: string;
  scope?: Record<string, string[]>;
  metrics?: Record<string, unknown>;
  settings?: Record<string, unknown>;
  llm_model?: string | null;
  duration_sec?: number | null;
}

export default function RunDetail() {
  const params = useParams();
  const runId = Number(params.runId);
  const run = useQuery({
    queryKey: ["run", runId],
    queryFn: () => apiFetch<RunDetailPayload>(`/api/history/runs/${runId}`),
    enabled: Number.isFinite(runId),
  });

  if (!Number.isFinite(runId)) return null;

  return (
    <>
      <PageHeader
        eyebrow={`Run #${runId}`}
        title={run.data?.command ?? "Loading…"}
        description={
          run.data
            ? `${run.data.status.toUpperCase()} · ${
                run.data.duration_sec != null ? `${run.data.duration_sec.toFixed(1)}s` : "—"
              } · ${run.data.llm_model ?? "—"}`
            : ""
        }
      />
      <div className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader title="Scope" />
          <CardBody>
            <pre className="overflow-x-auto whitespace-pre-wrap rounded-md bg-surface-subtle p-3 font-mono text-xs">
              {JSON.stringify(run.data?.scope ?? {}, null, 2)}
            </pre>
          </CardBody>
        </Card>
        <Card>
          <CardHeader title="Metrics" />
          <CardBody>
            <pre className="overflow-x-auto whitespace-pre-wrap rounded-md bg-surface-subtle p-3 font-mono text-xs">
              {JSON.stringify(run.data?.metrics ?? {}, null, 2)}
            </pre>
          </CardBody>
        </Card>
        <Card className="md:col-span-2">
          <CardHeader title="Settings snapshot" description="Captured the moment the run started." />
          <CardBody>
            <pre className="overflow-x-auto whitespace-pre-wrap rounded-md bg-surface-subtle p-3 font-mono text-xs">
              {JSON.stringify(run.data?.settings ?? {}, null, 2)}
            </pre>
          </CardBody>
        </Card>
      </div>
    </>
  );
}
