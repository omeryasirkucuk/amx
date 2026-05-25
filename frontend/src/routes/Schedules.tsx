/**
 * Schedules — list + create + lifecycle controls for one-shot scheduled
 * metadata runs.
 *
 * Uses AMX Studio's design system (PageHeader, DataTable, Field, Input,
 * Select, Button, Badge) so the visual rhythm matches Runs / Settings.
 */

import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import {
  CalendarPlus,
  Download,
  ExternalLink,
  Pencil,
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
import ScopeTree, {
  picksToScopeJson,
  type SchemaPick,
} from "../components/ScopeTree";

interface DbProfileSummary {
  name: string;
  backend: string;
  database: string;
  catalog?: string;
}

interface LlmProfileSummary {
  name: string;
  provider: string;
  model: string;
}

const CATALOG_BACKENDS = new Set(["databricks", "bigquery"]);

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

function formatLocalDatetime(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function defaultLocalDatetime() {
  // Default to one hour from now, so the picker isn't already-invalid
  // by the time the user lands on the dialog.
  return formatLocalDatetime(new Date(Date.now() + 60 * 60 * 1000));
}

function nowLocalDatetime() {
  return formatLocalDatetime(new Date());
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
  /** When supplied, the dialog operates in **edit** mode: title and
   * verbs read "Edit schedule" / "Save changes" and the form submits
   * a PATCH instead of a POST. ``null`` = create mode. */
  editing?: ScheduleRow | null;
}

function NewScheduleDialog({
  open,
  onClose,
  onCreated,
  editing,
}: NewScheduleDialogProps) {
  const isEdit = Boolean(editing);
  const toast = useToast();
  // Trigger type: a 'time' schedule fires at fire_at_local; a 'change'
  // watcher has no fire time and auto-runs when a new asset appears under
  // the watched scope.
  const [trigger, setTrigger] = useState<"time" | "change">(
    editing?.trigger === "change" ? "change" : "time",
  );
  const isChange = trigger === "change";
  const [name, setName] = useState(editing?.name ?? "");
  const [fireAtLocal, setFireAtLocal] = useState(
    editing?.fire_at_local ?? defaultLocalDatetime(),
  );
  const [fireAtTz, setFireAtTz] = useState(editing?.fire_at_tz ?? browserTz());
  const [dbProfile, setDbProfile] = useState(editing?.db_profile ?? "");
  // Hydrate ``database`` from the loaded row when editing -- mirrors
  // the picker's ``database`` axis. Falling back to ``catalog`` keeps
  // catalog-backend schedules (Unity Catalog etc.) round-trippable
  // through the same dropdown.
  const [database, setDatabase] = useState(
    editing?.database ?? editing?.catalog ?? "",
  );
  const [scopePicks, setScopePicks] = useState<SchemaPick[]>([]);
  const [llmProfile, setLlmProfile] = useState(editing?.llm_profile ?? "");
  const [reviewStrategy, setReviewStrategy] = useState<"auto" | "manual">(
    (editing?.review_strategy as "auto" | "manual" | undefined) ?? "auto",
  );
  // Auto-generation knobs (stored in scope_json). missing_only →
  // describe only columns lacking a description; deep_first → deep-sync
  // the catalog before generating so new columns are discovered.
  const [missingOnly, setMissingOnly] = useState<boolean>(
    Boolean((editing?.scope_json as Record<string, unknown> | null | undefined)?.missing_only),
  );
  const [deepFirst, setDeepFirst] = useState<boolean>(
    Boolean((editing?.scope_json as Record<string, unknown> | null | undefined)?.deep_first),
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

  const selectedProfile = dbProfilesQ.data?.profiles.find(
    (p) => p.name === dbProfile,
  );
  const isCatalogBackend = Boolean(
    selectedProfile && CATALOG_BACKENDS.has(selectedProfile.backend),
  );

  // Profile's own database / catalog list — drives the database dropdown.
  const databasesQ = useQuery({
    queryKey: ["scope-tree-dbs", dbProfile, isCatalogBackend],
    queryFn: () => {
      const path = isCatalogBackend
        ? `/api/live/catalogs?profile=${encodeURIComponent(dbProfile)}`
        : `/api/live/databases?profile=${encodeURIComponent(dbProfile)}`;
      return apiFetch<{
        databases?: string[];
        catalogs?: string[];
      }>(path);
    },
    enabled: open && Boolean(dbProfile),
  });
  const databaseOptions = isCatalogBackend
    ? databasesQ.data?.catalogs ?? []
    : databasesQ.data?.databases ?? [];

  // Auto-pick the first available database/catalog when the profile changes.
  if (open && dbProfile && !database && databaseOptions.length) {
    setDatabase(databaseOptions[0]);
  }

  // Auto-pick the first DB / LLM when the dialog opens (no profile chosen
  // yet) so the user almost never has to touch these fields.
  if (open && !dbProfile && dbProfilesQ.data?.profiles?.length) {
    setDbProfile(dbProfilesQ.data.profiles[0].name);
  }
  if (open && !llmProfile && llmProfilesQ.data?.profiles?.length) {
    setLlmProfile(llmProfilesQ.data.profiles[0].name);
  }

  const mutation = useMutation({
    mutationFn: (body: ScheduleCreatePayload) =>
      isEdit
        ? api.patchSchedule(editing!.id, body)
        : api.createSchedule(body),
    onSuccess: () => {
      toast.push({
        tone: "success",
        title: isEdit ? "Schedule updated" : "Schedule created",
      });
      setName("");
      setScopePicks([]);
      setDatabase("");
      setError(null);
      onCreated();
      onClose();
    },
    onError: (err) =>
      setError(err instanceof Error ? err.message : String(err)),
  });

  function buildScope(): Record<string, unknown> {
    return {
      ...picksToScopeJson(scopePicks),
      // A change watcher only ever generates for the new (missing) assets,
      // so missing_only is implicit; for time schedules it's the checkbox.
      missing_only: isChange ? true : missingOnly,
      deep_first: deepFirst,
    };
  }

  function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    try {
      // Past-time guard. The ``min=`` on the datetime-local input
      // covers the common case but is trivially bypassed (DevTools,
      // pasting an ISO string). Re-check on submit so a user can't
      // create a schedule that is already overdue. We allow edit
      // mode to keep the existing fire time even if it has slipped
      // into the past while the user was reading the dialog;
      // they'll explicitly pick a new time if they want to re-arm.
      // A change watcher has no fire time, so the past-time guard only
      // applies to time-based schedules.
      if (!isEdit && !isChange) {
        const picked = new Date(fireAtLocal).getTime();
        if (Number.isFinite(picked) && picked <= Date.now()) {
          setError(
            "Fire time must be in the future. Pick a date / time that " +
              "hasn't passed yet.",
          );
          return;
        }
      }
      mutation.mutate({
        name: name.trim(),
        // Change watchers send no fire time; the server stores a
        // placeholder and never uses it.
        fire_at_local: isChange ? "" : fireAtLocal,
        fire_at_tz: fireAtTz,
        db_profile: dbProfile,
        // Persist the picker's ``database`` axis so the scheduler
        // rebuilds the same connection at fire time. Catalog-backend
        // profiles funnel the same dropdown value into ``catalog`` so
        // a Unity Catalog schedule round-trips correctly too.
        database: isCatalogBackend ? null : database || null,
        catalog: isCatalogBackend ? database || null : null,
        scope: buildScope(),
        llm_profile: llmProfile,
        review_strategy: reviewStrategy,
        trigger,
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={isEdit ? `Edit schedule #${editing!.id}` : "New scheduled run"}
      size="lg"
    >
      <form onSubmit={onSubmit} className="space-y-4">
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          <Field
            label="Trigger"
            className="md:col-span-2"
            hint={
              isChange
                ? "Runs automatically when a new asset appears under the watched scope — detected after a sync. No fire time."
                : "Fires once (or on a recurrence) at the date/time you pick."
            }
          >
            <div className="inline-flex rounded-md border border-border p-0.5">
              <button
                type="button"
                onClick={() => setTrigger("time")}
                className={
                  "rounded px-3 py-1 text-sm transition " +
                  (!isChange ? "bg-accent text-white" : "text-ink-dim hover:text-ink")
                }
              >
                On a schedule
              </button>
              <button
                type="button"
                onClick={() => setTrigger("change")}
                className={
                  "rounded px-3 py-1 text-sm transition " +
                  (isChange ? "bg-accent text-white" : "text-ink-dim hover:text-ink")
                }
              >
                When new assets appear
              </button>
            </div>
          </Field>
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
                setDatabase("");
                setScopePicks([]);
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
          <Field
            label={isCatalogBackend ? "Catalog" : "Database"}
            required
          >
            <Select
              value={database}
              onChange={(e) => {
                setDatabase(e.target.value);
                setScopePicks([]);
              }}
              required
              disabled={!dbProfile || databasesQ.isLoading}
            >
              {!dbProfile && <option value="">Pick a DB profile first</option>}
              {dbProfile && databasesQ.isLoading && (
                <option value="">Loading…</option>
              )}
              {dbProfile &&
                !databasesQ.isLoading &&
                !databaseOptions.length && (
                  <option value="">(none visible)</option>
                )}
              {databaseOptions.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </Select>
          </Field>
          {!isChange && (
            <>
              <Field
                label="Fire at (local)"
                required
                hint={isEdit ? undefined : "Must be in the future"}
              >
                <Input
                  type="datetime-local"
                  value={fireAtLocal}
                  onChange={(e) => setFireAtLocal(e.target.value)}
                  min={isEdit ? undefined : nowLocalDatetime()}
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
            </>
          )}
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
            label="Scope"
            hint="Untick everything for the whole database. Tick schemas to limit, expand to drill into tables and columns."
            className="md:col-span-2"
          >
            <ScopeTree
              dbProfile={dbProfile}
              database={database}
              isCatalogBackend={isCatalogBackend}
              picks={scopePicks}
              onChange={setScopePicks}
            />
          </Field>
          <div className="md:col-span-2 space-y-2">
            {!isChange && (
              <label className="flex items-start gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={missingOnly}
                  onChange={(e) => setMissingOnly(e.target.checked)}
                  className="mt-0.5"
                />
                <span className="text-sm">
                  <span className="font-medium">Only missing descriptions</span>
                  <span className="block text-xs text-ink-dim">
                    Describe just the columns that lack a description. With
                    review strategy “auto”, this auto-generates descriptions
                    for new/undocumented columns each run.
                  </span>
                </span>
              </label>
            )}
            <label className="flex items-start gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={deepFirst}
                onChange={(e) => setDeepFirst(e.target.checked)}
                className="mt-0.5"
              />
              <span className="text-sm">
                <span className="font-medium">Refresh catalog first (deep)</span>
                <span className="block text-xs text-ink-dim">
                  Deep-sync columns + row counts before generating so
                  newly added columns are discovered and described in the
                  same run.
                </span>
              </span>
            </label>
          </div>
        </div>
        {error && (
          <p className="rounded-md border border-critical/40 bg-critical/10 px-3 py-2 text-sm text-critical">
            {error}
          </p>
        )}
        {isChange ? (
          <p className="rounded-md border border-border bg-surface-muted px-3 py-2 text-xs text-ink-dim">
            This watcher has no fire time. It checks for new assets whenever
            a sync runs for this profile (manual Sync, a cache-refresh
            schedule, or the background drift probe) and auto-runs only over
            what newly appeared. Timeliness follows how often you sync.
          </p>
        ) : (
          <p className="rounded-md border border-border bg-surface-muted px-3 py-2 text-xs text-ink-dim">
            Heads-up — AMX is invocation-based. For this schedule to fire on
            time, keep AMX/Studio open at fire time OR enable the background
            daemon. At the AMX REPL prompt, run:{" "}
            <code className="font-mono text-ink">/analyze schedule install-daemon</code>
            .
          </p>
        )}
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
            {mutation.isPending
              ? isEdit
                ? "Saving…"
                : "Creating…"
              : isEdit
                ? "Save changes"
                : "Create schedule"}
          </Button>
        </div>
      </form>
    </Dialog>
  );
}

interface ScheduleDetailDialogProps {
  open: boolean;
  row: ScheduleRow | null;
  onClose: () => void;
  onNavigate: (path: string) => void;
}

function ScheduleDetailDialog({
  open,
  row,
  onClose,
  onNavigate,
}: ScheduleDetailDialogProps) {
  if (!row) return null;

  let scopePretty: string;
  try {
    scopePretty = JSON.stringify(JSON.parse(row.scope_json), null, 2);
  } catch {
    scopePretty = row.scope_json || "(empty)";
  }
  const firedAt =
    row.fired_at != null
      ? new Date(row.fired_at * 1000).toLocaleString()
      : "—";

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={`Schedule #${row.id} — ${row.name}`}
      size="lg"
    >
      <div className="space-y-4">
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          <DetailItem label="Status">
            <StatusChip status={row.status} />
          </DetailItem>
          <DetailItem label="Review strategy">
            <span className="font-mono text-xs text-ink">
              {row.review_strategy}
            </span>
          </DetailItem>
          <DetailItem label="Fires at">
            <span className="text-ink">
              {row.fire_at_local}
              <span className="ml-2 text-xs text-ink-dim">{row.fire_at_tz}</span>
            </span>
          </DetailItem>
          <DetailItem label="Fired at">
            <span className="text-ink">{firedAt}</span>
          </DetailItem>
          <DetailItem label="DB profile">
            <span className="font-mono text-xs text-ink">{row.db_profile}</span>
          </DetailItem>
          <DetailItem label={row.catalog ? "Catalog" : "Database"}>
            <span className="font-mono text-xs text-ink">
              {row.catalog ?? row.database ?? "—"}
            </span>
          </DetailItem>
          <DetailItem label="LLM profile" className="md:col-span-2">
            <span className="font-mono text-xs text-ink">{row.llm_profile}</span>
          </DetailItem>
        </div>

        <DetailItem label="Scope">
          <pre className="max-h-60 overflow-auto rounded-md border border-border bg-surface-muted px-3 py-2 text-xs text-ink">
            {scopePretty}
          </pre>
        </DetailItem>

        {row.last_error && (
          <DetailItem label="Last error">
            <pre className="max-h-40 overflow-auto whitespace-pre-wrap rounded-md border border-critical/40 bg-critical/10 px-3 py-2 text-xs text-critical">
              {row.last_error}
            </pre>
          </DetailItem>
        )}

        <div className="flex flex-wrap items-center justify-end gap-2 pt-2">
          {row.triggered_run_id != null && (
            <Button
              variant="primary"
              size="md"
              leadingIcon={<ExternalLink size={14} />}
              onClick={() => {
                onNavigate(`/runs/${row.triggered_run_id}`);
                onClose();
              }}
            >
              View Results
            </Button>
          )}
          <Button variant="secondary" size="md" onClick={onClose}>
            Close
          </Button>
        </div>
      </div>
    </Dialog>
  );
}

function DetailItem({
  label,
  children,
  className,
}: {
  label: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={className}>
      <div className="mb-1 text-xs uppercase tracking-wide text-ink-dim">
        {label}
      </div>
      <div>{children}</div>
    </div>
  );
}

export default function Schedules() {
  const qc = useQueryClient();
  const toast = useToast();
  const navigate = useNavigate();
  const [statusFilter, setStatusFilter] = useState<string>("all");
  const [dialogOpen, setDialogOpen] = useState(false);
  // When non-null the dialog opens in edit mode pre-filled with this
  // schedule's current fields. Set by the row-level Edit IconButton.
  const [editing, setEditing] = useState<ScheduleRow | null>(null);
  // When non-null a read-only detail dialog opens with the clicked row.
  const [detailRow, setDetailRow] = useState<ScheduleRow | null>(null);

  const apiStatus =
    statusFilter === "active"
      ? "pending,paused,missed,running"
      : statusFilter === "past"
        ? "completed,failed,cancelled"
        : undefined;

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ["schedules", statusFilter],
    queryFn: () => api.listSchedules({ status: apiStatus, kind: "analyze" }),
    // Poll the list so the page reflects daemon-driven fires
    // without forcing the user to refresh: 5s while at least one
    // schedule is mid-fire (status='running'), 15s baseline so
    // newly-created or just-fired entries surface quickly even
    // when nothing is currently in flight.
    refetchInterval: (query) => {
      const rows =
        (query.state.data as { schedules?: { status: string }[] } | undefined)
          ?.schedules ?? [];
      const anyRunning = rows.some((r) => r.status === "running");
      return anyRunning ? 5000 : 15000;
    },
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
  const installDaemonMut = useMutation({
    mutationFn: () => api.installDaemon(),
    onSuccess: (result) => {
      toast.push({
        tone: "success",
        title: "Daemon installed",
        description: result.message,
      });
      qc.invalidateQueries({ queryKey: ["scheduler-status"] });
    },
    onError: (e) =>
      toast.push({
        tone: "error",
        title: e instanceof Error ? e.message : "Install failed",
      }),
  });
  const uninstallDaemonMut = useMutation({
    mutationFn: () => api.uninstallDaemon(),
    onSuccess: (result) => {
      toast.push({
        tone: "success",
        title: "Daemon removed",
        description: result.message,
      });
      qc.invalidateQueries({ queryKey: ["scheduler-status"] });
    },
    onError: (e) =>
      toast.push({
        tone: "error",
        title: e instanceof Error ? e.message : "Uninstall failed",
      }),
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
        sortValue: (row) => (row.trigger === "change" ? "" : row.fire_at_local),
        cell: (row) =>
          row.trigger === "change" ? (
            <span className="inline-flex items-center gap-1 text-ink-dim">
              <Badge tone="info">Watching</Badge>
              {row.fired_at && (
                <span className="text-xs">
                  last fired {new Date(row.fired_at * 1000).toLocaleString()}
                </span>
              )}
            </span>
          ) : (
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
        hideOnMobile: true,
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
        sortValue: (row) => `${row.db_profile}/${row.database ?? row.catalog ?? ""}`,
        cell: (row) => {
          // Surface the (profile, database) pair so the user can see
          // which DB a schedule targets at a glance. A schedule with
          // ``database=null`` is a legacy row created before the
          // overlay was persisted; flag it so the user knows the
          // next fire will use the profile default (and almost
          // certainly miss the picker's tables).
          const overlay = row.database ?? row.catalog;
          if (overlay) {
            return (
              <span className="text-ink-dim">
                {row.db_profile}
                <span className="text-ink-muted"> · </span>
                <span className="font-mono text-xs text-ink">{overlay}</span>
              </span>
            );
          }
          return (
            <span className="text-ink-dim">
              {row.db_profile}
              <span
                className="ml-2 rounded border border-warning/40 bg-warning/10 px-1.5 py-0.5 text-[10px] font-medium text-warning"
                title="No database picked. Edit the schedule and pick one — otherwise the next fire connects to the profile default and may not find the scheduled tables."
              >
                no db
              </span>
            </span>
          );
        },
        hideOnMobile: true,
      },
      {
        id: "llm",
        header: "LLM",
        sortValue: (row) => row.llm_profile,
        cell: (row) => (
          <span className="text-ink-dim">{row.llm_profile}</span>
        ),
        hideOnMobile: true,
      },
      {
        id: "actions",
        header: "",
        cell: (row) => (
          // stopPropagation so action-button clicks don't ALSO trigger
          // the row's onRowClick (which would open the detail dialog
          // on top of the action that just fired).
          <div
            className="flex items-center justify-end gap-1"
            onClick={(e) => e.stopPropagation()}
          >
            {(row.status === "pending" || row.status === "paused") && (
              <IconButton
                size="sm"
                icon={<Pencil size={16} />}
                label="Edit"
                title="Edit"
                onClick={() => {
                  setEditing(row);
                  setDialogOpen(true);
                }}
              />
            )}
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

  // Live count of currently-missed schedules, decoupled from the
  // page's active filter (the banner is global; it must reflect
  // reality regardless of which Show: option the user picked).
  const missedQ = useQuery({
    queryKey: ["schedules", "missed-banner"],
    queryFn: () => api.listSchedules({ status: "missed", kind: "analyze" }),
    refetchInterval: 30_000,
  });
  const liveMissedCount = missedQ.data?.schedules?.length ?? 0;
  const bootstrapStale = bootstrap.data?.stale_recovered ?? [];

  const banner =
    liveMissedCount > 0 || bootstrapStale.length > 0 ? (
      <div className="mb-4 rounded-md border border-warning/40 bg-warning/10 px-3 py-2 text-sm text-warning">
        {bootstrapStale.length > 0 && (
          <span>
            {bootstrapStale.length} interrupted run
            {bootstrapStale.length === 1 ? "" : "s"} recovered.{" "}
          </span>
        )}
        {liveMissedCount > 0 && (
          <span>
            {liveMissedCount} schedule
            {liveMissedCount === 1 ? "" : "s"} missed while AMX was
            closed.
          </span>
        )}
      </div>
    ) : null;

  // Daemon control: chip + actionable button. Three states now:
  //   * not installed → orange "not installed" + Install button.
  //   * installed + not loaded → orange "needs reload" + Reload button.
  //     This is the case the modern launchctl path used to hide:
  //     the plist sat on disk but launchd never bootstrapped it,
  //     so schedules silently stopped firing.
  //   * installed + loaded → green "running" + Uninstall button.
  const daemonInstalled = !!statusQ.data?.daemon.installed;
  const daemonLoaded = !!statusQ.data?.daemon.loaded;
  const daemonRunning = daemonInstalled && daemonLoaded;
  let daemonBadgeLabel: string;
  if (!daemonInstalled) {
    daemonBadgeLabel = "Daemon: not installed";
  } else if (!daemonLoaded) {
    daemonBadgeLabel = "Daemon: installed but not loaded";
  } else {
    daemonBadgeLabel = "Daemon: running";
  }
  const daemonControls = statusQ.data ? (
    <div className="hidden items-center gap-1 sm:inline-flex">
      <Badge tone={daemonRunning ? "positive" : "warning"}>
        {daemonBadgeLabel}
      </Badge>
      {daemonRunning ? (
        <Button
          variant="secondary"
          size="sm"
          onClick={() => {
            if (
              window.confirm(
                "Uninstall the scheduler daemon? Schedules will only fire while AMX or Studio is open.",
              )
            ) {
              uninstallDaemonMut.mutate();
            }
          }}
          disabled={uninstallDaemonMut.isPending}
        >
          Uninstall
        </Button>
      ) : (
        <Button
          variant="secondary"
          size="sm"
          leadingIcon={<Download size={14} />}
          onClick={() => installDaemonMut.mutate()}
          disabled={installDaemonMut.isPending}
        >
          {installDaemonMut.isPending
            ? "Installing…"
            : daemonInstalled
              ? "Reload"
              : "Install"}
        </Button>
      )}
    </div>
  ) : null;

  return (
    <>
      <PageHeader
        title="Analyze schedules"
        breadcrumbs={[{ label: "Runs", to: "/runs" }, { label: "Schedules" }]}
        description="Scheduled analysis runs (LLM-driven metadata generation). For Catalog Freshness cache refreshes, see Runs → Catalog refreshes. Both kinds share the same OS daemon — there is only one com.amx.scheduler service, not two."
        actions={
          <div className="flex flex-wrap items-center gap-2">
            {daemonControls}
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

      <div className="px-4 py-4 sm:px-6">
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
          onRowClick={(row) => setDetailRow(row)}
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
        // Remount the dialog whenever the user switches between
        // create and edit so the form state seeds correctly from
        // the row being edited (state is initialised once at mount,
        // not on each re-render).
        key={editing?.id ?? "new"}
        open={dialogOpen}
        editing={editing}
        onClose={() => {
          setDialogOpen(false);
          setEditing(null);
        }}
        onCreated={invalidate}
      />

      <ScheduleDetailDialog
        open={detailRow !== null}
        row={detailRow}
        onClose={() => setDetailRow(null)}
        onNavigate={navigate}
      />
    </>
  );
}
