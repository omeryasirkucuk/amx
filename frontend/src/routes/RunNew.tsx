import { useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { PlayCircle } from "lucide-react";

import { ApiError, api } from "../lib/api";
import { cn } from "../lib/cn";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import { Button, Skeleton, Switch, useToast } from "../components/ui";

interface SchemaPickState {
  schema: string;
  tables: string[]; // empty = "every reachable table"
}

export default function RunNew() {
  const navigate = useNavigate();
  const toast = useToast();
  const [picked, setPicked] = useState<SchemaPickState[]>([]);
  const [missingOnly, setMissingOnly] = useState(true);
  const [autoApply, setAutoApply] = useState(false);

  const schemas = useQuery({
    queryKey: ["live-schemas"],
    queryFn: () => api.liveSchemas(),
    retry: false,
  });

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
      toast.push({
        title: "Run started",
        description: `${picked.length} ${picked.length === 1 ? "schema" : "schemas"} queued.`,
        tone: "success",
        duration: 2200,
      });
      navigate(`/runs/new-${result.job_id}`);
    },
    onError: (err: Error) => {
      toast.push({
        title: "Could not start run",
        description: err.message,
        tone: "error",
      });
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
        title="New run"
        breadcrumbs={[{ label: "Runs", to: "/runs" }, { label: "New" }]}
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
              Pick one from the top bar or sidebar before starting a run — without
              an active catalog/database the agents can&apos;t see any schemas.
            </p>
          </CardBody>
        </Card>
      ) : (
        <div className="grid gap-4 lg:grid-cols-[2fr_1fr]">
          <Card>
            <CardHeader title="Scope" />
            <CardBody className="p-0">
              {schemas.isLoading ? (
                <ul className="divide-y divide-border">
                  {Array.from({ length: 5 }).map((_, i) => (
                    <li key={i} className="px-5 py-3">
                      <Skeleton className="h-3 w-1/3" />
                    </li>
                  ))}
                </ul>
              ) : schemas.error ? (
                <div className="px-5 py-6 text-sm text-critical">
                  {(schemas.error as Error).message}
                </div>
              ) : !schemas.data?.schemas?.length ? (
                <div className="px-5 py-6 text-sm text-ink-dim">
                  No schemas reachable.
                </div>
              ) : (
                <ul className="divide-y divide-border">
                  {schemas.data.schemas.map((name) => (
                    <li key={name}>
                      <button
                        type="button"
                        onClick={() => toggleSchema(name)}
                        className={cn(
                          "flex w-full items-center justify-between px-5 py-2.5 text-left text-sm transition-colors duration-fast hover:bg-surface-subtle/50",
                          isPicked(name) && "bg-accent-soft/40",
                        )}
                      >
                        <span className="font-medium text-ink">{name}</span>
                        <span
                          className={cn(
                            "text-[10.5px] uppercase tracking-wider",
                            isPicked(name) ? "text-accent-ink" : "text-ink-dim",
                          )}
                        >
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
            <CardHeader title="Options" />
            <CardBody className="space-y-4 text-sm">
              <Switch
                checked={missingOnly}
                onChange={(e) => setMissingOnly(e.target.checked)}
                label="Missing only"
                description="Skip tables and columns that already have a comment."
              />
              <Switch
                checked={autoApply}
                onChange={(e) => setAutoApply(e.target.checked)}
                label="Auto-apply on success"
                description="Write approved descriptions to the live DB without a separate Apply step."
              />
              <hr className="border-border" />
              <dl className="grid grid-cols-2 gap-y-1.5 text-xs">
                <dt className="text-ink-dim">Schemas</dt>
                <dd className="text-right font-mono tabular-nums text-ink">
                  {picked.length}
                </dd>
                <dt className="text-ink-dim">Asset slots</dt>
                <dd className="text-right font-mono tabular-nums text-ink">
                  {totalAssets}
                </dd>
              </dl>
              <Button
                type="button"
                onClick={() => submit.mutate()}
                disabled={picked.length === 0}
                loading={submit.isPending}
                variant="primary"
                size="lg"
                fullWidth
                leadingIcon={<PlayCircle size={14} />}
              >
                {submit.isPending ? "Starting…" : "Start run"}
              </Button>
              {picked.length === 0 && (
                <p className="text-[11px] text-ink-dim">
                  Pick at least one schema to enable the start button.
                </p>
              )}
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
    return (
      <div className="space-y-1.5 px-8 pb-3">
        <Skeleton className="h-3 w-1/4" />
        <div className="flex flex-wrap gap-1.5">
          {Array.from({ length: 6 }).map((_, i) => (
            <Skeleton key={i} className="h-5 w-20" />
          ))}
        </div>
      </div>
    );
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
          className="rounded border border-border px-1.5 py-0.5 hover:bg-surface-subtle"
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
                "rounded-md border px-2 py-0.5 font-mono text-[11px] transition-colors duration-fast",
                on
                  ? "border-accent/40 bg-accent-soft/40 text-ink"
                  : "border-border text-ink-dim hover:border-accent/40 hover:text-ink",
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
