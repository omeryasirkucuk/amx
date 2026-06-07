import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Blocks, Download, Loader2, RefreshCw, Trash2 } from "lucide-react";

import EmptyState from "../../components/EmptyState";
import { Card, CardBody, CardHeader } from "../../components/Card";
import { Badge, Button, useToast } from "../../components/ui";
import { ApiError, apiFetch } from "../../lib/api";
import { tokenQuerySuffix } from "../../lib/auth";

interface EditorStatus {
  id: string;
  label: string;
  cli_path: string;
  installed: boolean;
  version: string | null;
}

interface VsCodeStatus {
  editors: EditorStatus[];
  bundled_version: string | null;
}

/** Build the authenticated download URL for the bundled VSIX. The
 *  anchor can't carry an Authorization header, so the token rides as
 *  a query param — same pattern the SSE endpoints use. */
function vsixDownloadHref(): string {
  const suffix = tokenQuerySuffix();
  return suffix ? `/api/vscode/vsix?${suffix}` : "/api/vscode/vsix";
}

function mutationErrorTitle(prefix: string, err: unknown): string {
  if (err instanceof ApiError) {
    return err.hint ? `${prefix}: ${err.detail} ${err.hint}` : `${prefix}: ${err.detail}`;
  }
  return `${prefix}: ${(err as Error).message}`;
}

export default function VsCodeTab() {
  const qc = useQueryClient();
  const toast = useToast();
  const statusQuery = useQuery<VsCodeStatus>({
    queryKey: ["vscode", "status"],
    queryFn: () => apiFetch<VsCodeStatus>("/api/vscode/status"),
  });

  if (statusQuery.isPending) {
    return (
      <div className="flex items-center gap-2 py-8 text-sm text-ink-dim">
        <Loader2 size={14} className="animate-spin" /> Loading editor extension status…
      </div>
    );
  }
  if (statusQuery.error || !statusQuery.data) {
    return (
      <div className="rounded-md border border-critical/40 bg-critical/10 px-3 py-2 text-sm text-critical">
        Failed to load editor extension status.
      </div>
    );
  }

  const data = statusQuery.data;
  const refresh = () => qc.invalidateQueries({ queryKey: ["vscode"] });

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader
          title="AMX extension for VS Code"
          description="Install the bundled editor extension so VS Code (and compatible editors) can browse the AMX catalog in place."
        />
        <CardBody>
          <p className="text-sm text-ink-dim">
            The extension ships inside this AMX installation — no Marketplace account needed, and
            its version always matches the server it talks to.{" "}
            <span className="font-medium text-ink">
              Bundled extension: {data.bundled_version ? `v${data.bundled_version}` : "unavailable"}
            </span>
          </p>
        </CardBody>
      </Card>

      {data.editors.length === 0 ? (
        <NoEditorsCard />
      ) : (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          {data.editors.map((editor) => (
            <EditorCard
              key={editor.id}
              editor={editor}
              bundledVersion={data.bundled_version}
              onChanged={refresh}
              toast={toast}
            />
          ))}
        </div>
      )}

      <p className="text-xs text-ink-muted">
        Using an editor AMX didn't detect?{" "}
        <a
          href={vsixDownloadHref()}
          download
          className="inline-flex items-center gap-1 text-accent hover:underline"
        >
          <Download size={11} /> Download .vsix
        </a>{" "}
        and install it via your editor's "Install from VSIX…" command.
      </p>
    </div>
  );
}

function editorBadge(editor: EditorStatus, bundledVersion: string | null) {
  if (!editor.installed) return <Badge tone="neutral">not installed</Badge>;
  if (bundledVersion && editor.version && editor.version !== bundledVersion) {
    return (
      <Badge tone="warning">
        update available (v{editor.version} → v{bundledVersion})
      </Badge>
    );
  }
  return (
    <Badge tone="positive" dot>
      installed{editor.version ? ` v${editor.version}` : ""}
    </Badge>
  );
}

function EditorCard({
  editor,
  bundledVersion,
  onChanged,
  toast,
}: {
  editor: EditorStatus;
  bundledVersion: string | null;
  onChanged: () => void;
  toast: ReturnType<typeof useToast>;
}) {
  const install = useMutation({
    mutationFn: () =>
      apiFetch<{ ok: boolean }>("/api/vscode/install", {
        method: "POST",
        body: JSON.stringify({ editor: editor.id }),
      }),
    onSuccess: () => {
      toast.push({ title: `Extension installed into ${editor.label}`, tone: "success" });
      onChanged();
    },
    onError: (err: unknown) => {
      toast.push({ title: mutationErrorTitle("Install failed", err), tone: "error" });
    },
  });

  const uninstall = useMutation({
    mutationFn: () =>
      apiFetch<{ ok: boolean }>("/api/vscode/uninstall", {
        method: "POST",
        body: JSON.stringify({ editor: editor.id }),
      }),
    onSuccess: () => {
      toast.push({ title: `Extension removed from ${editor.label}`, tone: "info" });
      onChanged();
    },
    onError: (err: unknown) => {
      toast.push({ title: mutationErrorTitle("Uninstall failed", err), tone: "error" });
    },
  });

  return (
    <Card>
      <CardHeader
        title={
          <span className="flex flex-col gap-1.5 sm:flex-row sm:items-center sm:justify-between sm:gap-2">
            <span>{editor.label}</span>
            {editorBadge(editor, bundledVersion)}
          </span>
        }
      />
      <CardBody className="space-y-3">
        <p className="truncate font-mono text-xs text-ink-muted" title={editor.cli_path}>
          {editor.cli_path}
        </p>

        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="primary"
            size="sm"
            onClick={() => install.mutate()}
            disabled={install.isPending}
          >
            {install.isPending ? (
              <Loader2 size={13} className="animate-spin" />
            ) : (
              <RefreshCw size={13} />
            )}
            {editor.installed ? "Reinstall" : "Install"}
          </Button>
          {editor.installed && (
            <Button
              variant="danger"
              size="sm"
              onClick={() => uninstall.mutate()}
              disabled={uninstall.isPending}
            >
              {uninstall.isPending ? (
                <Loader2 size={13} className="animate-spin" />
              ) : (
                <Trash2 size={13} />
              )}
              Uninstall
            </Button>
          )}
        </div>
      </CardBody>
    </Card>
  );
}

function NoEditorsCard() {
  return (
    <EmptyState
      icon={Blocks}
      title="No editor CLI detected"
      description={
        <span className="block space-y-1">
          <span className="block">
            AMX looks for the <code className="text-accent">code</code>,{" "}
            <code className="text-accent">cursor</code>,{" "}
            <code className="text-accent">windsurf</code> and{" "}
            <code className="text-accent">codium</code> launchers on your PATH. To install the
            extension manually: download the .vsix below, open your editor's command palette, run{" "}
            <span className="font-medium text-ink">"Extensions: Install from VSIX…"</span> and pick
            the downloaded file.
          </span>
        </span>
      }
      actions={
        <a
          href={vsixDownloadHref()}
          download
          className="inline-flex items-center gap-1.5 text-sm text-accent hover:underline"
        >
          <Download size={13} /> Download .vsix
        </a>
      }
    />
  );
}
