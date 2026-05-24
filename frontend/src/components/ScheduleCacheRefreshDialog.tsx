/**
 * ScheduleCacheRefreshDialog — create or edit a Catalog Freshness
 * cache refresh schedule (recurring or one-shot).
 *
 * The form mirrors the shape of ``NewScheduleDialog`` in
 * routes/Schedules.tsx but for the ``kind='cache_refresh'`` variant:
 * LLM-profile and review-strategy fields are gone, and a
 * ``Recurrence`` dropdown + "Show cron syntax" toggle let the user
 * dial in a cron expression without typing one.
 *
 * Edit mode kicks in when an ``editing`` row is supplied — title and
 * verbs flip to "Edit", form fields hydrate from the row, and the
 * mutation submits a PATCH instead of a POST. Recurrence dropdown
 * back-resolves from ``editing.cron_expr`` via ``cronToRecurrence``
 * so the friendly label appears instead of a raw expression.
 */

import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import {
  Button,
  Dialog,
  Field,
  Input,
  Select,
  useToast,
} from "./ui";
import { api, apiFetch, type ScheduleCreatePayload, type ScheduleRow } from "../lib/api";
import ScopeTree, { picksToScopeJson, type SchemaPick } from "./ScopeTree";
import {
  RECURRENCE_OPTIONS,
  type Recurrence,
  cronToRecurrence,
  intervalToCron,
} from "../lib/cron";

interface DbProfileSummary {
  name: string;
  backend: string;
  database: string;
  catalog?: string;
}

const CATALOG_BACKENDS = new Set(["databricks", "bigquery"]);

function formatLocalDatetime(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function defaultLocalDatetime() {
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

/**
 * Best-effort IANA timezone list. Modern Chromium / Firefox ship
 * ``Intl.supportedValuesOf("timeZone")`` which returns the full ~600-
 * entry CLDR set; older runtimes fall back to a hand-picked subset
 * covering the AMX user base + the major UTC offsets. Always
 * includes ``UTC`` and the browser's resolved zone so the user can
 * one-click their own city even when it's missing from the fallback
 * list.
 */
function listTimezones(): string[] {
  const seen = new Set<string>();
  const push = (tz: string | undefined | null) => {
    if (tz && !seen.has(tz)) seen.add(tz);
  };
  push("UTC");
  push(browserTz());
  try {
    const anyIntl = Intl as unknown as {
      supportedValuesOf?: (key: string) => string[];
    };
    if (typeof anyIntl.supportedValuesOf === "function") {
      for (const tz of anyIntl.supportedValuesOf("timeZone")) push(tz);
    }
  } catch {
    /* fallback below */
  }
  for (const tz of [
    "Africa/Cairo",
    "Africa/Johannesburg",
    "Africa/Lagos",
    "America/Anchorage",
    "America/Bogota",
    "America/Buenos_Aires",
    "America/Chicago",
    "America/Denver",
    "America/Halifax",
    "America/Los_Angeles",
    "America/Mexico_City",
    "America/New_York",
    "America/Phoenix",
    "America/Sao_Paulo",
    "America/Toronto",
    "America/Vancouver",
    "Asia/Bangkok",
    "Asia/Dubai",
    "Asia/Hong_Kong",
    "Asia/Jakarta",
    "Asia/Jerusalem",
    "Asia/Karachi",
    "Asia/Kolkata",
    "Asia/Kuala_Lumpur",
    "Asia/Manila",
    "Asia/Riyadh",
    "Asia/Seoul",
    "Asia/Shanghai",
    "Asia/Singapore",
    "Asia/Taipei",
    "Asia/Tehran",
    "Asia/Tokyo",
    "Atlantic/Azores",
    "Australia/Brisbane",
    "Australia/Melbourne",
    "Australia/Perth",
    "Australia/Sydney",
    "Europe/Amsterdam",
    "Europe/Athens",
    "Europe/Berlin",
    "Europe/Brussels",
    "Europe/Bucharest",
    "Europe/Dublin",
    "Europe/Helsinki",
    "Europe/Istanbul",
    "Europe/Kyiv",
    "Europe/Lisbon",
    "Europe/London",
    "Europe/Madrid",
    "Europe/Moscow",
    "Europe/Oslo",
    "Europe/Paris",
    "Europe/Prague",
    "Europe/Rome",
    "Europe/Stockholm",
    "Europe/Vienna",
    "Europe/Warsaw",
    "Europe/Zurich",
    "Pacific/Auckland",
    "Pacific/Honolulu",
  ]) {
    push(tz);
  }
  return Array.from(seen).sort((a, b) => a.localeCompare(b));
}

export interface ScheduleCacheRefreshDialogProps {
  open: boolean;
  onClose: () => void;
  onCreated?: () => void;
  /**
   * Optional initial profile name. When the dialog is launched from
   * the Catalog Freshness pill we already know which profile the
   * user is looking at; default-selecting it skips a click.
   */
  initialProfile?: string | null;
  /**
   * When supplied, the dialog runs in edit mode against this row:
   * fields hydrate from the row, recurrence dropdown back-resolves
   * from ``cron_expr``, and submit issues a PATCH. ``null`` = create.
   */
  editing?: ScheduleRow | null;
}

export default function ScheduleCacheRefreshDialog({
  open,
  onClose,
  onCreated,
  initialProfile,
  editing,
}: ScheduleCacheRefreshDialogProps) {
  const isEdit = Boolean(editing);
  const toast = useToast();
  const qc = useQueryClient();

  const [name, setName] = useState(editing?.name ?? "");
  const [fireAtLocal, setFireAtLocal] = useState(
    editing?.fire_at_local ?? defaultLocalDatetime(),
  );
  const [fireAtTz, setFireAtTz] = useState(editing?.fire_at_tz ?? browserTz());
  const timezoneOptions = useMemo(() => listTimezones(), []);
  const [dbProfile, setDbProfile] = useState(
    editing?.db_profile ?? initialProfile ?? "",
  );
  const [database, setDatabase] = useState(
    editing?.database ?? editing?.catalog ?? "",
  );
  const [scopePicks, setScopePicks] = useState<SchemaPick[]>([]);
  const [recurrence, setRecurrence] = useState<Recurrence>(() => {
    if (isEdit) return cronToRecurrence(editing?.cron_expr ?? null);
    return "6h";
  });
  const [showCron, setShowCron] = useState(false);
  const [customCron, setCustomCron] = useState(editing?.cron_expr ?? "0 */6 * * *");
  // Deep schedules profile columns + exact row counts (deep_sync) instead
  // of the shallow skeleton/comment warm. Preserved across edits via the
  // ``deep`` flag stored inside scope_json.
  const [deepSync, setDeepSync] = useState<boolean>(
    Boolean((editing?.scope_json as Record<string, unknown> | null | undefined)?.deep),
  );
  const [error, setError] = useState<string | null>(null);

  const dbProfilesQ = useQuery({
    queryKey: ["profiles", "db"],
    queryFn: () =>
      apiFetch<{ profiles: DbProfileSummary[] }>("/api/profiles/db"),
    enabled: open,
  });

  const selectedProfile = dbProfilesQ.data?.profiles.find(
    (p) => p.name === dbProfile,
  );
  const isCatalogBackend = Boolean(
    selectedProfile && CATALOG_BACKENDS.has(selectedProfile.backend),
  );

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

  if (open && dbProfile && !database && databaseOptions.length) {
    setDatabase(databaseOptions[0]);
  }
  if (open && !isEdit && !dbProfile && dbProfilesQ.data?.profiles?.length) {
    setDbProfile(dbProfilesQ.data.profiles[0].name);
  }

  const previewCron = useMemo(
    () => intervalToCron(recurrence, customCron),
    [recurrence, customCron],
  );

  function handleRecurrenceChange(next: Recurrence) {
    setRecurrence(next);
    const opt = RECURRENCE_OPTIONS.find((o) => o.value === next);
    if (next !== "custom" && opt?.cron) {
      setCustomCron(opt.cron);
    }
  }

  const mutation = useMutation({
    mutationFn: (body: ScheduleCreatePayload) =>
      isEdit
        ? api.patchSchedule(editing!.id, body)
        : api.createSchedule(body),
    onSuccess: () => {
      toast.push({
        tone: "success",
        title: isEdit ? "Cache refresh updated" : "Cache refresh scheduled",
      });
      qc.invalidateQueries({ queryKey: ["schedules"] });
      qc.invalidateQueries({ queryKey: ["catalog-freshness"] });
      setError(null);
      onCreated?.();
      onClose();
    },
    onError: (err) =>
      setError(err instanceof Error ? err.message : String(err)),
  });

  function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    if (!isEdit) {
      const picked = new Date(fireAtLocal).getTime();
      if (Number.isFinite(picked) && picked <= Date.now()) {
        setError("Fire time must be in the future.");
        return;
      }
    }
    const cron = intervalToCron(recurrence, customCron);
    if (recurrence === "custom" && !cron) {
      setError(
        "Custom cron expression is required when recurrence is set to 'Custom'.",
      );
      return;
    }
    const payload: ScheduleCreatePayload = {
      name: name.trim() || `Cache refresh — ${dbProfile}`,
      fire_at_local: fireAtLocal,
      fire_at_tz: fireAtTz,
      db_profile: dbProfile,
      database: isCatalogBackend ? null : database || null,
      catalog: isCatalogBackend ? database || null : null,
      scope: { ...picksToScopeJson(scopePicks), deep: deepSync },
      cron_expr: cron,
    };
    // Only set ``kind`` on create — PATCH never changes a row's kind.
    if (!isEdit) {
      payload.kind = "cache_refresh";
    }
    mutation.mutate(payload);
  }

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={isEdit ? `Edit cache refresh #${editing!.id}` : "Schedule cache refresh"}
      size="lg"
    >
      <form onSubmit={onSubmit} className="space-y-4">
        <div className="grid grid-cols-1 items-start gap-x-4 gap-y-4 md:grid-cols-2">
          <Field label="Name" description="Defaults to “Cache refresh — <profile>”.">
            <Input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Hourly metadata refresh"
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
              disabled={dbProfilesQ.isLoading || isEdit}
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
          <Field label={isCatalogBackend ? "Catalog" : "Database"} required>
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
          <Field
            label="First fire"
            required
            description="Local time in the chosen timezone. Recurring schedules use this as the first fire; later fires follow the cron."
          >
            <Input
              type="datetime-local"
              value={fireAtLocal}
              onChange={(e) => setFireAtLocal(e.target.value)}
              min={isEdit ? undefined : nowLocalDatetime()}
              required
            />
          </Field>
          <Field
            label="Timezone"
            required
            description="IANA timezone — schedule fires honour DST."
          >
            <Select
              value={fireAtTz}
              onChange={(e) => setFireAtTz(e.target.value)}
              required
            >
              {timezoneOptions.map((tz) => (
                <option key={tz} value={tz}>
                  {tz}
                </option>
              ))}
            </Select>
          </Field>
          <Field
            label="Recurrence"
            description="One-shot fires once; recurring re-arms after every fire."
          >
            <Select
              value={recurrence}
              onChange={(e) => handleRecurrenceChange(e.target.value as Recurrence)}
            >
              {RECURRENCE_OPTIONS.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </Select>
          </Field>
          <Field
            label="Scope"
            description="Leave empty to refresh the whole database. Tick schemas to limit; expand for tables and columns."
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
          <div className="md:col-span-2">
            <label className="flex items-start gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={deepSync}
                onChange={(e) => setDeepSync(e.target.checked)}
                className="mt-0.5"
              />
              <span className="text-sm">
                <span className="font-medium">Deep sync</span>
                <span className="block text-xs text-ink-dim">
                  Profile columns + exact row counts each run (slower —
                  issues a COUNT(*) per table). Leave off for a fast
                  inventory-only refresh.
                </span>
              </span>
            </label>
          </div>
        </div>
        <div className="rounded-md border border-border bg-surface-muted px-3 py-2 text-xs">
          <button
            type="button"
            onClick={() => setShowCron((v) => !v)}
            className="text-accent-ink underline-offset-2 hover:underline"
          >
            {showCron ? "Hide cron syntax" : "Show cron syntax"}
          </button>
          {showCron && (
            <div className="mt-2 space-y-2">
              <p className="text-ink-dim">
                Resolved cron expression (
                {recurrence === "custom" ? "edit below" : "from recurrence"}
                ):
              </p>
              {recurrence === "custom" ? (
                <Input
                  value={customCron}
                  onChange={(e) => setCustomCron(e.target.value)}
                  placeholder="0 */6 * * *"
                  className="font-mono"
                />
              ) : (
                <code className="block rounded bg-surface-subtle px-2 py-1 font-mono text-ink">
                  {previewCron ?? "(one-shot — no cron)"}
                </code>
              )}
              <p className="text-[11px] text-ink-dim">
                Five-field standard cron (minute hour dom month dow). Pick
                "Custom" to override.
              </p>
            </div>
          )}
        </div>
        {error && (
          <p className="rounded-md border border-critical/40 bg-critical/10 px-3 py-2 text-sm text-critical">
            {error}
          </p>
        )}
        <p className="rounded-md border border-border bg-surface-muted px-3 py-2 text-xs text-ink-dim">
          Heads-up — AMX is invocation-based. For this schedule to fire on
          time, keep AMX/Studio open at fire time OR install the
          background daemon. At the AMX REPL prompt:{" "}
          <code className="font-mono text-ink">/analyze schedule install-daemon</code>
          .
        </p>
        <div className="flex items-center justify-end gap-2 pt-1">
          <Button variant="secondary" size="md" onClick={onClose} type="button">
            Cancel
          </Button>
          <Button
            type="submit"
            variant="primary"
            size="md"
            disabled={mutation.isPending || !dbProfile}
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
