/**
 * Schedules — list + create + lifecycle controls for one-shot scheduled
 * metadata runs.
 *
 * Uses AMX Studio's design system (PageHeader, DataTable, Field, Input,
 * Select, Button, Badge) so the visual rhythm matches Runs / Settings.
 */

import { useMemo, useState } from "react";
import {
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import {
  CalendarPlus,
  PauseCircle,
  PlayCircle,
  RefreshCw,
  Trash2,
} from "lucide-react";

import PageHeader from "../components/PageHeader";
import {
  Badge,
  type BadgeTone,
  Button,
  DataTable,
  type DataTableColumn,
  Dialog,
  Field,
  IconButton,
  Input,
  Select,
  useToast,
} from "../components/ui";
import { api, apiFetch, type ScheduleCreatePayload, type ScheduleRow } from "../lib/api";

interface DbProfileSummary {
  name: string;
  backend: string;
  database: string;
}

interface LlmProfileSummary {
  name: string;
  provider: string;
  model: string;
}

interface SchemaItem {
  name: string;
}

const STATUS_TONE: Record<string, BadgeTone> = {
  pending: "neutral",
  paused: "warning",
  running: "info",
  completed: "positive",
  failed: "critical",
  missed: "warning",
  cancelled: "neutral",
};

function StatusChip({ status }: { status: string }) {
  return <Badge tone={STATUS_TONE[status] ?? "neutral"}>{status}</Badge>;
}

function defaultLocalDatetime() {
  const t = new Date(Date.now() + 60 * 60 * 1000);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${t.getFullYear()}-${pad(t.getMonth() + 1)}-${pad(t.getDate())}T${pad(t.getHours())}:${pad(t.getMinutes())}`;
}

function browserTz() {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  } catch {
    return "UTC";
  }
}

interface NewScheduleDialogProps {
  open: boolean;
  onClose: () => void;
  onCreated: () => void;
}

function NewScheduleDialog({ open, onClose, onCreated }: NewScheduleDialogProps) {
  const toast = useToast();
  const [name, setName] = useState("");
  const [fireAtLocal, setFireAtLocal] = useState(defaultLocalDatetime());
  const [fireAtTz, setFireAtTz] = useState(browserTz());
  const [dbProfile, setDbProfile] = useState("");
  const [scopeMode, setScopeMode] = useState<"all" | "schemas" | "tables">(
    "all",
  );
  const [selectedSchemas, setSelectedSchemas] = useState<string[]>([]);
  const [tablesText, setTablesText] = useState("");
  const [llmProfile, setLlmProfile] = useState("");
  const [reviewStrategy, setReviewStrategy] = useState<"auto" | "manual">(
    "auto",
  );
  const [error, setError] = useState<string | null>(null);

  const dbProfilesQ = useQuery({
    queryKey: ["profiles", "db"],
    queryFn: () =>
      apiFetch<{ profiles: DbProfileSummary[] }>("/api/profiles/db"),
    enabled: open,
  });
  const llmProfilesQ = useQuery({
    queryKey: ["profiles", "llm"],
    queryFn: () =>
      apiFetch<{ profiles: LlmProfileSummary[] }>("/api/profiles/llm"),
    enabled: open,
  });
  const schemasQ = useQuery({
    queryKey: ["live-schemas", dbProfile],
    queryFn: () =>
      apiFetch<{ schemas: SchemaItem[] }>(
        `/api/live/schemas?profile=${encodeURIComponent(dbProfile)}`,
      ),
    enabled: open && Boolean(dbProfile) && scopeMode !== "all",
  });

  // Auto-pick the first DB / LLM when the dialog opens (no profile chosen
  // yet) so the user almost never has to touch these fields.
  if (open && !dbProfile && dbProfilesQ.data?.profiles?.length) {
    setDbProfile(dbProfilesQ.data.profiles[0].name);
  }
  if (open && !llmProfile && llmProfilesQ.data?.profiles?.length) {
    setLlmProfile(llmProfilesQ.data.profiles[0].name);
  }

  const mutation = useMutation({
    mutationFn: (body: ScheduleCreatePayload) => api.createSchedule(body),
    onSuccess: () => {
      toast.push({ tone: "success", title: "Schedule created" });
      setName("");
      setScopeMode("all");
      setSelectedSchemas([]);
      setTablesText("");
      setError(null);
      onCreated();
      onClose();
    },
    onError: (err) =>
      setError(err instanceof Error ? err.message : String(err)),
  });

  function buildScope(): Record<string, unknown> {
    if (scopeMode === "all") return { mode: "all" };
    if (scopeMode === "schemas") {
      if (!selectedSchemas.length)
        throw new Error("Pick at least one schema");
      return { mode: "schemas", schemas: selectedSchemas };
    }
    const tables = tablesText
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
      .map((piece) => {
        const [schema, table] = piece.split(".");
        if (!schema || !table)
          throw new Error(`Table entry "${piece}" must be schema.table`);
        return { schema, table };
      });
    if (!tables.length) throw new Error("List ≥1 schema.table pair");
    return { mode: "tables", tables };
  }

  function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    try {
      mutation.mutate({
        name: name.trim(),
        fire_at_local: fireAtLocal,
        fire_at_tz: fireAtTz,
        db_profile: dbProfile,
        scope: buildScope(),
        llm_profile: llmProfile,
        review_strategy: reviewStrategy,
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="New scheduled run"
      size="lg"
    >
      <form onSubmit={onSubmit} className="space-y-4">
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          <Field label="Name" required>
            <Input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Quarterly meta refresh"
              required
            />
          </Field>
          <Field label="DB profile" required>
            <Select
              value={dbProfile}
              onChange={(e) => {
                setDbProfile(e.target.value);
                setSelectedSchemas([]);
              }}
              required
              disabled={dbProfilesQ.isLoading}
            >
              {dbProfilesQ.isLoading && <option value="">Loading…</option>}
              {!dbProfilesQ.isLoading &&
                !dbProfilesQ.data?.profiles?.length && (
                  <option value="">(no profiles configured)</option>
                )}
              {dbProfilesQ.data?.profiles?.map((p) => (
                <option key={p.name} value={p.name}>
                  {p.name} · {p.backend}
                </option>
              ))}
            </Select>
          </Field>
          <Field label="Fire at (local)" required>
            <Input
              type="datetime-local"
              value={fireAtLocal}
              onChange={(e) => setFireAtLocal(e.target.value)}
              required
            />
          </Field>
          <Field label="Timezone" hint="IANA, e.g. Europe/Istanbul">
            <Input
              value={fireAtTz}
              onChange={(e) => setFireAtTz(e.target.value)}
              placeholder="Europe/Istanbul"
              required
            />
          </Field>
          <Field label="LLM profile" required>
            <Select
              value={llmProfile}
              onChange={(e) => setLlmProfile(e.target.value)}
              required
              disabled={llmProfilesQ.isLoading}
            >
              {llmProfilesQ.isLoading && <option value="">Loading…</option>}
              {!llmProfilesQ.isLoading &&
                !llmProfilesQ.data?.profiles?.length && (
                  <option value="">(no profiles configured)</option>
                )}
              {llmProfilesQ.data?.profiles?.map((p) => (
                <option key={p.name} value={p.name}>
                  {p.name} · {p.provider}/{p.model}
                </option>
              ))}
            </Select>
          </Field>
          <Field label="Review strategy">
            <Select
              value={reviewStrategy}
              onChange={(e) =>
                setReviewStrategy(e.target.value as "auto" | "manual")
              }
            >
              <option value="auto">auto</option>
              <option value="manual">manual</option>
            </Select>
          </Field>
          <Field
            label="Scope mode"
            className="md:col-span-2"
          >
            <Select
              value={scopeMode}
              onChange={(e) =>
                setScopeMode(e.target.value as "all" | "schemas" | "tables")
              }
            >
              <option value="all">All schemas in this DB</option>
              <option value="schemas">Specific schemas</option>
              <option value="tables">Specific tables</option>
            </Select>
          </Field>
          {scopeMode === "schemas" && (
            <Field
              label="Schemas"
              hint="Tick the schemas to include"
              className="md:col-span-2"
            >
              <div className="max-h-44 overflow-auto rounded-md border border-border bg-surface-raised p-2 text-sm">
                {schemasQ.isLoading && (
                  <p className="text-ink-dim">Loading schemas…</p>
                )}
                {schemasQ.isError && (
                  <p className="text-critical">
                    Could not load schemas for "{dbProfile}".
                  </p>
                )}
                {schemasQ.data?.schemas?.length === 0 && (
                  <p className="text-ink-dim">
                    No schemas visible on "{dbProfile}".
                  </p>
                )}
                <div className="grid grid-cols-2 gap-1">
                  {schemasQ.data?.schemas?.map((s) => {
                    const checked = selectedSchemas.includes(s.name);
                    return (
                      <label
                        key={s.name}
                        className="flex items-center gap-2 rounded px-1 py-0.5 hover:bg-surface-muted"
                      >
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={(e) => {
                            if (e.target.checked)
                              setSelectedSchemas([
                                ...selectedSchemas,
                                s.name,
                              ]);
                            else
                              setSelectedSchemas(
                                selectedSchemas.filter((n) => n !== s.name),
                              );
                          }}
                        />
                        <span className="font-mono text-xs">{s.name}</span>
                      </label>
                    );
                  })}
                </div>
              </div>
            </Field>
          )}
          {scopeMode === "tables" && (
            <Field
              label="Tables"
              hint="Comma-separated schema.table pairs (e.g. public.users, sales.orders)"
              className="md:col-span-2"
            >
              <Input
                value={tablesText}
                onChange={(e) => setTablesText(e.target.value)}
                placeholder="public.users, public.orders"
                required
              />
            </Field>
          )}
        </div>
        {error && (
          <p className="rounded-md border border-critical/40 bg-critical/10 px-3 py-2 text-sm text-critical">
            {error}
          </p>
        )}
        <p className="rounded-md border border-border bg-surface-muted px-3 py-2 text-xs text-ink-dim">
          Heads-up — AMX is invocation-based. For this schedule to fire on
          time, keep AMX/Studio open at fire time OR enable the background
          daemon: <code className="font-mono text-ink">amx scheduler install-daemon</code>.
        </p>
        <div className="flex items-center justify-end gap-2 pt-1">
          <Button variant="secondary" size="md" onClick={onClose} type="button">
            Cancel
          </Button>
          <Button
            type="submit"
            variant="primary"
            size="md"
            disabled={mutation.isPending}
          >
            {mutation.isPending ? "Creating…" : "Create schedule"}
          </Button>
        </div>
      </form>
    </Dialog>
  );
}

export default function Schedules() {
  const qc = useQueryClient();
  const toast = useToast();
  const [statusFilter, setStatusFilter] = useState<string>("active");
  const [dialogOpen, setDialogOpen] = useState(false);

  const apiStatus =
    statusFilter === "active"
      ? "pending,paused,missed,running"
      : statusFilter === "past"
        ? "completed,failed,cancelled"
        : undefined;

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ["schedules", statusFilter],
    queryFn: () => api.listSchedules({ status: apiStatus }),
  });

  const statusQ = useQuery({
    queryKey: ["scheduler-status"],
    queryFn: () => api.schedulerStatus(),
    refetchInterval: 15_000,
  });
  const bootstrap = useQuery({
    queryKey: ["scheduler-bootstrap"],
    queryFn: () => api.schedulerBootstrapReport(),
  });

  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ["schedules"] });
    qc.invalidateQueries({ queryKey: ["scheduler-status"] });
  };

  const pauseMut = useMutation({
    mutationFn: (id: number) => api.pauseSchedule(id),
    onSuccess: invalidate,
    onError: (e) =>
      toast.push({
        tone: "error",
        title: e instanceof Error ? e.message : "Pause failed",
      }),
  });
  const resumeMut = useMutation({
    mutationFn: (id: number) => api.resumeSchedule(id),
    onSuccess: invalidate,
    onError: (e) =>
      toast.push({
        tone: "error",
        title: e instanceof Error ? e.message : "Resume failed",
      }),
  });
  const runNowMut = useMutation({
    mutationFn: (id: number) => api.runScheduleNow(id),
    onSuccess: () => {
      toast.push({ tone: "info", title: "Schedule fired" });
      invalidate();
    },
    onError: (e) =>
      toast.push({
        tone: "error",
        title: e instanceof Error ? e.message : "Fire failed",
      }),
  });
  const deleteMut = useMutation({
    mutationFn: (id: number) => api.deleteSchedule(id),
    onSuccess: () => {
      toast.push({ tone: "success", title: "Schedule deleted" });
      invalidate();
    },
  });

  const rows = data?.schedules ?? [];

  const columns = useMemo<DataTableColumn<ScheduleRow>[]>(
    () => [
      {
        id: "id",
        header: "#",
        sortValue: (row) => row.id,
        cell: (row) => (
          <span className="font-mono text-xs text-ink-dim">{row.id}</span>
        ),
        width: "w-12",
      },
      {
        id: "name",
        header: "Name",
        sortValue: (row) => row.name,
        cell: (row) => (
          <span className="font-medium text-ink">{row.name}</span>
        ),
      },
      {
        id: "when",
        header: "When (local)",
        sortValue: (row) => row.fire_at_local,
        cell: (row) => (
          <span className="text-ink">{row.fire_at_local}</span>
        ),
      },
      {
        id: "tz",
        header: "Tz",
        sortValue: (row) => row.fire_at_tz,
        cell: (row) => (
          <span className="font-mono text-xs text-ink-dim">
            {row.fire_at_tz}
          </span>
        ),
      },
      {
        id: "status",
        header: "Status",
        sortValue: (row) => row.status,
        cell: (row) => <StatusChip status={row.status} />,
      },
      {
        id: "db",
        header: "DB",
        sortValue: (row) => row.db_profile,
        cell: (row) => (
          <span className="text-ink-dim">{row.db_profile}</span>
        ),
      },
      {
        id: "llm",
        header: "LLM",
        sortValue: (row) => row.llm_profile,
        cell: (row) => (
          <span className="text-ink-dim">{row.llm_profile}</span>
        ),
      },
      {
        id: "actions",
        header: "",
        cell: (row) => (
          <div className="flex items-center justify-end gap-1">
            {row.status === "pending" && (
              <IconButton
                size="sm"
                icon={<PauseCircle size={16} />}
                label="Pause"
                title="Pause"
                onClick={() => pauseMut.mutate(row.id)}
              />
            )}
            {row.status === "paused" && (
              <IconButton
                size="sm"
                icon={<PlayCircle size={16} />}
                label="Resume"
                title="Resume"
                onClick={() => resumeMut.mutate(row.id)}
              />
            )}
            {(row.status === "pending" ||
              row.status === "paused" ||
              row.status === "missed") && (
              <IconButton
                size="sm"
                icon={<PlayCircle size={16} />}
                label="Run now"
                title="Run now"
                onClick={() => runNowMut.mutate(row.id)}
              />
            )}
            <IconButton
              size="sm"
              icon={<Trash2 size={16} />}
              label="Delete"
              title="Delete"
              onClick={() => {
                if (
                  window.confirm(`Delete schedule "${row.name}"?`)
                ) {
                  deleteMut.mutate(row.id);
                }
              }}
            />
          </div>
        ),
        width: "w-36",
        align: "right",
      },
    ],
    [pauseMut, resumeMut, runNowMut, deleteMut],
  );

  const banner =
    bootstrap.data &&
    (bootstrap.data.missed_for_review.length > 0 ||
      bootstrap.data.stale_recovered.length > 0) ? (
      <div className="mb-4 rounded-md border border-warning/40 bg-warning/10 px-3 py-2 text-sm text-warning">
        {bootstrap.data.stale_recovered.length > 0 && (
          <span>
            {bootstrap.data.stale_recovered.length} interrupted run
            {bootstrap.data.stale_recovered.length === 1 ? "" : "s"} recovered.{" "}
          </span>
        )}
        {bootstrap.data.missed_for_review.length > 0 && (
          <span>
            {bootstrap.data.missed_for_review.length} schedule
            {bootstrap.data.missed_for_review.length === 1 ? "" : "s"} missed
            while AMX was closed.
          </span>
        )}
      </div>
    ) : null;

  const daemonChip = statusQ.data ? (
    <Badge tone={statusQ.data.daemon.installed ? "positive" : "warning"}>
      Daemon {statusQ.data.daemon.installed ? "installed" : "not installed"}
    </Badge>
  ) : null;

  return (
    <>
      <PageHeader
        title="Schedules"
        breadcrumbs={[{ label: "Schedules" }]}
        description="One-shot scheduled metadata runs. Catch-up surfaces on next open when AMX was closed at fire time."
        actions={
          <div className="flex items-center gap-2">
            {daemonChip}
            <Button
              variant="secondary"
              size="md"
              leadingIcon={<RefreshCw size={14} />}
              onClick={() => refetch()}
            >
              Refresh
            </Button>
            <Button
              variant="primary"
              size="md"
              leadingIcon={<CalendarPlus size={14} />}
              onClick={() => setDialogOpen(true)}
            >
              New schedule
            </Button>
          </div>
        }
      />

      <div className="px-6 py-4">
        {banner}

        <div className="mb-3 flex items-center gap-2">
          <span className="text-sm text-ink-dim">Show:</span>
          <Select
            className="max-w-[160px]"
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
          >
            <option value="active">Active</option>
            <option value="past">Past</option>
            <option value="all">All</option>
          </Select>
        </div>

        {error && (
          <p className="rounded-md border border-critical/40 bg-critical/10 px-3 py-2 text-sm text-critical">
            {error instanceof Error ? error.message : String(error)}
          </p>
        )}

        <DataTable<ScheduleRow>
          columns={columns}
          rows={rows}
          rowKey={(row) => String(row.id)}
          isLoading={isLoading}
          emptyState={
            <div className="py-6 text-center text-sm text-ink-dim">
              No schedules. Create one with the button up top, or with{" "}
              <code className="font-mono">amx schedule add</code> in the
              terminal.
            </div>
          }
        />
      </div>

      <NewScheduleDialog
        open={dialogOpen}
        onClose={() => setDialogOpen(false)}
        onCreated={invalidate}
      />
    </>
  );
}
