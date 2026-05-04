import { useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { Loader2, PlayCircle } from "lucide-react";

import { ApiError, api } from "../lib/api";
import { cn } from "../lib/cn";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";

interface SchemaPickState {
  schema: string;
  tables: string[]; // empty = "every reachable table"
}

export default function RunNew() {
  const navigate = useNavigate();
  const [picked, setPicked] = useState<SchemaPickState[]>([]);
  const [missingOnly, setMissingOnly] = useState(true);
  const [autoApply, setAutoApply] = useState(false);

  const schemas = useQuery({
    queryKey: ["live-schemas"],
    queryFn: () => api.liveSchemas(),
    retry: false,
  });

  // 412 here means the user hasn't selected a catalog/database yet —
  // we can't pick a scope without it, so steer them up to the topbar.
  const scopeUnavailable =
    schemas.error instanceof ApiError &&
    schemas.error.status === 412 &&
    (schemas.error.hint === "select-catalog" || schemas.error.hint === "select-database");

  const submit = useMutation({
    mutationFn: () =>
      api.submitRun({
        scope: Object.fromEntries(picked.map((p) => [p.schema, p.tables])),
        apply: autoApply,
        missing_only: missingOnly,
      }),
    onSuccess: (result) => {
      // Pass the job id to RunDetail so it can subscribe to the SSE
      // stream until the run.created event lands; once we have a real
      // run_id the URL is rewritten in place by RunDetail itself.
      navigate(`/runs/new-${result.job_id}`);
    },
  });

  const totalAssets = useMemo(
    () =>
      picked.reduce(
        (acc, p) => acc + (p.tables.length === 0 ? 1 : p.tables.length),
        0,
      ),
    [picked],
  );

  function toggleSchema(name: string) {
    setPicked((curr) => {
      const idx = curr.findIndex((p) => p.schema === name);
      if (idx >= 0) {
        return [...curr.slice(0, idx), ...curr.slice(idx + 1)];
      }
      return [...curr, { schema: name, tables: [] }];
    });
  }

  function isPicked(name: string) {
    return picked.some((p) => p.schema === name);
  }

  return (
    <>
      <PageHeader
        eyebrow="Run"
        title="Start a /run"
        description="Pick the schemas (and optionally tables) AMX should describe. The agents stream alternatives back into the run detail page; nothing is written to the live database until you Apply."
      />

      {scopeUnavailable ? (
        <Card>
          <CardBody className="px-6 py-8 text-sm">
            <p className="font-medium text-warning">
              {schemas.error instanceof ApiError &&
              schemas.error.hint === "select-catalog"
                ? "No catalog selected."
                : "No database selected."}
            </p>
            <p className="mt-2 text-ink-muted">
              Pick one from the top bar (or the sidebar tree) before starting a
              run — without an active catalog/database the agents can't see any
              schemas.
            </p>
          </CardBody>
        </Card>
      ) : (
        <div className="grid gap-4 lg:grid-cols-[2fr_1fr]">
          <Card>
            <CardHeader
              title="Scope"
              description="Click a schema to include it. Leaving its table list empty means 'every table in that schema'."
            />
            <CardBody className="p-0">
              {schemas.isLoading ? (
                <div className="px-5 py-6 text-sm text-ink-dim">Loading schemas…</div>
              ) : schemas.error ? (
                <div className="px-5 py-6 text-sm text-critical">
                  {(schemas.error as Error).message}
                </div>
              ) : !schemas.data?.schemas?.length ? (
                <div className="px-5 py-6 text-sm text-ink-dim">No schemas reachable.</div>
              ) : (
                <ul className="divide-y divide-surface-border">
                  {schemas.data.schemas.map((name) => (
                    <li key={name}>
                      <button
                        type="button"
                        onClick={() => toggleSchema(name)}
                        className={cn(
                          "flex w-full items-center justify-between px-5 py-3 text-left text-sm transition hover:bg-surface-subtle/40",
                          isPicked(name) && "bg-accent-soft/40",
                        )}
                      >
                        <span className="font-medium">{name}</span>
                        <span className="text-[11px] uppercase tracking-wider text-ink-dim">
                          {isPicked(name) ? "selected" : "—"}
                        </span>
                      </button>
                      {isPicked(name) && (
                        <SchemaTablePicker
                          schema={name}
                          selected={
                            picked.find((p) => p.schema === name)?.tables ?? []
                          }
                          onChange={(tables) =>
                            setPicked((curr) =>
                              curr.map((p) =>
                                p.schema === name ? { ...p, tables } : p,
                              ),
                            )
                          }
                        />
                      )}
                    </li>
                  ))}
                </ul>
              )}
            </CardBody>
          </Card>

          <Card>
            <CardHeader title="Settings" description="Tune the run before launch." />
            <CardBody className="space-y-4 text-sm">
              <Toggle
                label="Missing-only"
                description="Skip tables and columns that already have a comment. Recommended for incremental runs."
                checked={missingOnly}
                onChange={setMissingOnly}
              />
              <Toggle
                label="Auto-apply on success"
                description="When the run finishes cleanly, write approved descriptions to the live DB without a separate Apply step. Equivalent to /run-apply."
                checked={autoApply}
                onChange={setAutoApply}
              />
              <hr className="border-surface-border" />
              <div className="text-xs text-ink-muted">
                <div>
                  <span className="text-ink-dim">Schemas:</span>{" "}
                  <span className="font-mono">{picked.length}</span>
                </div>
                <div>
                  <span className="text-ink-dim">Asset slots:</span>{" "}
                  <span className="font-mono">{totalAssets}</span>
                </div>
              </div>
              {submit.isError && (
                <div className="rounded-md border border-critical/30 bg-critical/5 px-3 py-2 text-xs text-critical">
                  {submit.error instanceof Error
                    ? submit.error.message
                    : "Submit failed."}
                </div>
              )}
              <button
                type="button"
                onClick={() => submit.mutate()}
                disabled={picked.length === 0 || submit.isPending}
                className="inline-flex w-full items-center justify-center gap-2 rounded-md bg-accent px-4 py-2 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-40"
              >
                {submit.isPending ? (
                  <>
                    <Loader2 size={14} className="animate-spin" />
                    Starting…
                  </>
                ) : (
                  <>
                    <PlayCircle size={14} />
                    Start run
                  </>
                )}
              </button>
              <p className="text-[11px] text-ink-dim">
                Live progress streams on the next page.
              </p>
            </CardBody>
          </Card>
        </div>
      )}
    </>
  );
}

function SchemaTablePicker({
  schema,
  selected,
  onChange,
}: {
  schema: string;
  selected: string[];
  onChange: (tables: string[]) => void;
}) {
  const assets = useQuery({
    queryKey: ["live-assets", schema],
    queryFn: () => api.liveAssets(schema),
  });
  if (assets.isLoading) {
    return <div className="px-8 pb-3 text-xs text-ink-dim">Loading tables…</div>;
  }
  if (assets.error) {
    return (
      <div className="px-8 pb-3 text-xs text-critical">
        {(assets.error as Error).message}
      </div>
    );
  }
  if (!assets.data?.assets?.length) {
    return <div className="px-8 pb-3 text-xs text-ink-dim">(empty)</div>;
  }

  function toggle(name: string) {
    if (selected.includes(name)) {
      onChange(selected.filter((t) => t !== name));
    } else {
      onChange([...selected, name]);
    }
  }
  function selectAll() {
    onChange([]);
  }

  return (
    <div className="space-y-2 px-8 pb-3">
      <div className="flex items-center gap-2 text-[11px] text-ink-dim">
        <button
          type="button"
          onClick={selectAll}
          className="rounded border border-surface-border px-1.5 py-0.5 hover:bg-surface-subtle"
        >
          all tables
        </button>
        <span>
          {selected.length === 0
            ? `every table (${assets.data.assets.length})`
            : `${selected.length} of ${assets.data.assets.length} selected`}
        </span>
      </div>
      <div className="flex flex-wrap gap-1.5">
        {assets.data.assets.map((asset) => {
          const on = selected.length === 0 || selected.includes(asset.name);
          return (
            <button
              key={`${schema}.${asset.name}`}
              type="button"
              onClick={() => toggle(asset.name)}
              className={cn(
                "rounded-md border px-2 py-0.5 font-mono text-[11px]",
                on
                  ? "border-accent/40 bg-accent-soft/30 text-ink"
                  : "border-surface-border text-ink-dim hover:border-accent/30 hover:text-ink",
              )}
            >
              {asset.name}
            </button>
          );
        })}
      </div>
    </div>
  );
}

function Toggle({
  label,
  description,
  checked,
  onChange,
}: {
  label: string;
  description: string;
  checked: boolean;
  onChange: (v: boolean) => void;
}) {
  return (
    <label className="flex cursor-pointer items-start gap-3">
      <input
        type="checkbox"
        checked={checked}
        onChange={(e) => onChange(e.target.checked)}
        className="mt-0.5 h-4 w-4 cursor-pointer accent-current"
      />
      <span>
        <span className="block font-medium text-ink">{label}</span>
        <span className="block text-xs text-ink-muted">{description}</span>
      </span>
    </label>
  );
}
