/**
 * Admin Members tab — lists all workspace members with role management.
 *
 * Admin-only row actions: Promote to admin, Demote to viewer, Revoke access.
 * The "Demote" button is disabled when the target row is the only remaining admin.
 */

import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { ShieldCheck, ShieldOff, UserX, UserCheck } from "lucide-react";

import { apiFetch } from "../../lib/api";
import { Badge, Tooltip, useToast } from "../../components/ui";
import EmptyState from "../../components/EmptyState";
import Skeleton from "../../components/ui/Skeleton";
import { cn } from "../../lib/cn";

interface Member {
  id: string;
  username: string;
  hostname: string;
  display_name: string | null;
  email: string | null;
  role: string;
  first_seen_at: string | null;
  last_seen_at: string | null;
  client_version: string | null;
  revoked_at: string | null;
}

interface MembersResponse {
  members: Member[];
  count: number;
}

interface MeResponse {
  username: string;
  hostname: string;
  role: string;
}

function formatDate(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString(undefined, {
      dateStyle: "short",
      timeStyle: "short",
    });
  } catch {
    return iso;
  }
}

export default function AdminMembers() {
  const qc = useQueryClient();
  const toast = useToast();

  const me = useQuery<MeResponse>({
    queryKey: ["admin-me"],
    queryFn: () => apiFetch<MeResponse>("/api/admin/me"),
    staleTime: 60_000,
  });

  const members = useQuery<MembersResponse>({
    queryKey: ["admin-members"],
    queryFn: () => apiFetch<MembersResponse>("/api/admin/members"),
    refetchInterval: 60_000,
  });

  const isAdmin = me.data?.role === "admin";

  // Count of active (non-revoked) admins
  const adminCount =
    members.data?.members.filter(
      (m) => m.role === "admin" && !m.revoked_at,
    ).length ?? 0;

  const promote = useMutation({
    mutationFn: (username: string) =>
      apiFetch("/api/admin/promote", {
        method: "POST",
        body: JSON.stringify({ username }),
      }),
    onSuccess: (_data, username) => {
      qc.invalidateQueries({ queryKey: ["admin-members"] });
      toast.push({ title: `${username} promoted to admin`, tone: "success" });
    },
    onError: (err: Error) => {
      toast.push({ title: "Promote failed", description: err.message, tone: "error" });
    },
  });

  const demote = useMutation({
    mutationFn: (username: string) =>
      apiFetch("/api/admin/demote", {
        method: "POST",
        body: JSON.stringify({ username }),
      }),
    onSuccess: (_data, username) => {
      qc.invalidateQueries({ queryKey: ["admin-members"] });
      toast.push({ title: `${username} demoted to viewer`, tone: "success" });
    },
    onError: (err: Error) => {
      toast.push({ title: "Demote failed", description: err.message, tone: "error" });
    },
  });

  const revoke = useMutation({
    mutationFn: (username: string) =>
      apiFetch("/api/admin/revoke", {
        method: "POST",
        body: JSON.stringify({ username }),
      }),
    onSuccess: (_data, username) => {
      qc.invalidateQueries({ queryKey: ["admin-members"] });
      toast.push({ title: `${username} revoked`, tone: "success" });
    },
    onError: (err: Error) => {
      toast.push({ title: "Revoke failed", description: err.message, tone: "error" });
    },
  });

  const unrevoke = useMutation({
    mutationFn: (username: string) =>
      apiFetch("/api/admin/unrevoke", {
        method: "POST",
        body: JSON.stringify({ username }),
      }),
    onSuccess: (_data, username) => {
      qc.invalidateQueries({ queryKey: ["admin-members"] });
      toast.push({ title: `${username} reinstated`, tone: "success" });
    },
    onError: (err: Error) => {
      toast.push({ title: "Unrevoke failed", description: err.message, tone: "error" });
    },
  });

  if (members.isLoading) {
    return (
      <div className="space-y-2">
        {[...Array(3)].map((_, i) => (
          <Skeleton key={i} className="h-12 w-full" />
        ))}
      </div>
    );
  }

  if (members.isError) {
    return (
      <EmptyState
        title="Could not load members"
        description={(members.error as Error).message}
      />
    );
  }

  const list = members.data?.members ?? [];

  if (list.length === 0) {
    return (
      <EmptyState
        title="No members yet"
        description="Members appear here once they connect to the shared workspace."
      />
    );
  }

  return (
    <div className="overflow-hidden rounded-lg border border-border">
      {/* Desktop table */}
      <table className="hidden w-full text-sm sm:table">
        <thead className="border-b border-border bg-surface-subtle/50">
          <tr>
            <th className="px-4 py-2 text-left font-medium text-ink-dim">User</th>
            <th className="px-4 py-2 text-left font-medium text-ink-dim">Role</th>
            <th className="hidden px-4 py-2 text-left font-medium text-ink-dim md:table-cell">
              First seen
            </th>
            <th className="hidden px-4 py-2 text-left font-medium text-ink-dim lg:table-cell">
              Last seen
            </th>
            <th className="hidden px-4 py-2 text-left font-medium text-ink-dim lg:table-cell">
              Version
            </th>
            {isAdmin && (
              <th className="px-4 py-2 text-right font-medium text-ink-dim">
                Actions
              </th>
            )}
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {list.map((m) => {
            const isMe =
              m.username === me.data?.username &&
              m.hostname === me.data?.hostname;
            const isLastAdmin = m.role === "admin" && adminCount <= 1;
            const isRevoked = !!m.revoked_at;
            return (
              <tr
                key={m.id}
                className={cn(
                  "transition-colors hover:bg-surface-subtle/40",
                  isRevoked && "opacity-50",
                )}
              >
                <td className="px-4 py-3">
                  <div className="flex items-center gap-2">
                    <span className="font-medium text-ink">
                      {m.username}
                      {isMe && (
                        <span className="ml-1.5 rounded bg-accent-soft px-1.5 py-px text-[10px] font-medium text-accent-ink">
                          you
                        </span>
                      )}
                    </span>
                    <span className="font-mono text-xs text-ink-dim">
                      @{m.hostname}
                    </span>
                  </div>
                  {isRevoked && (
                    <span className="text-xs text-critical">Revoked</span>
                  )}
                </td>
                <td className="px-4 py-3">
                  <Badge
                    tone={
                      m.role === "admin"
                        ? "info"
                        : isRevoked
                          ? "warning"
                          : "neutral"
                    }
                  >
                    {isRevoked ? "revoked" : m.role}
                  </Badge>
                </td>
                <td className="hidden px-4 py-3 text-xs text-ink-dim md:table-cell">
                  {formatDate(m.first_seen_at)}
                </td>
                <td className="hidden px-4 py-3 text-xs text-ink-dim lg:table-cell">
                  {formatDate(m.last_seen_at)}
                </td>
                <td className="hidden px-4 py-3 font-mono text-xs text-ink-dim lg:table-cell">
                  {m.client_version || "—"}
                </td>
                {isAdmin && (
                  <td className="px-4 py-3">
                    <div className="flex items-center justify-end gap-1">
                      {!isRevoked && m.role !== "admin" && (
                        <Tooltip content="Promote to admin">
                          <button
                            type="button"
                            disabled={promote.isPending}
                            onClick={() => promote.mutate(m.username)}
                            aria-label={`Promote ${m.username} to admin`}
                            className="rounded p-1.5 text-ink-dim hover:bg-surface-subtle hover:text-ink"
                          >
                            <ShieldCheck size={14} />
                          </button>
                        </Tooltip>
                      )}
                      {!isRevoked && m.role === "admin" && (
                        <Tooltip
                          content={
                            isLastAdmin
                              ? "Cannot demote the only admin"
                              : "Demote to viewer"
                          }
                        >
                          <span>
                            <button
                              type="button"
                              disabled={isLastAdmin || demote.isPending}
                              onClick={() => demote.mutate(m.username)}
                              aria-label={`Demote ${m.username} to viewer`}
                              className={cn(
                                "rounded p-1.5 text-ink-dim",
                                isLastAdmin
                                  ? "cursor-not-allowed opacity-40"
                                  : "hover:bg-surface-subtle hover:text-ink",
                              )}
                            >
                              <ShieldOff size={14} />
                            </button>
                          </span>
                        </Tooltip>
                      )}
                      {!isRevoked && (
                        <Tooltip content="Revoke access">
                          <button
                            type="button"
                            disabled={revoke.isPending}
                            onClick={() => revoke.mutate(m.username)}
                            aria-label={`Revoke ${m.username}`}
                            className="rounded p-1.5 text-ink-dim hover:bg-surface-subtle hover:text-critical"
                          >
                            <UserX size={14} />
                          </button>
                        </Tooltip>
                      )}
                      {isRevoked && (
                        <Tooltip content="Reinstate">
                          <button
                            type="button"
                            disabled={unrevoke.isPending}
                            onClick={() => unrevoke.mutate(m.username)}
                            aria-label={`Reinstate ${m.username}`}
                            className="rounded p-1.5 text-ink-dim hover:bg-surface-subtle hover:text-ink"
                          >
                            <UserCheck size={14} />
                          </button>
                        </Tooltip>
                      )}
                    </div>
                  </td>
                )}
              </tr>
            );
          })}
        </tbody>
      </table>

      {/* Mobile card list */}
      <ul className="divide-y divide-border sm:hidden">
        {list.map((m) => {
          const isMe =
            m.username === me.data?.username &&
            m.hostname === me.data?.hostname;
          const isRevoked = !!m.revoked_at;
          return (
            <li
              key={m.id}
              className={cn(
                "flex items-center justify-between gap-3 px-4 py-3",
                isRevoked && "opacity-50",
              )}
            >
              <div className="min-w-0">
                <div className="flex items-center gap-1.5">
                  <span className="font-medium text-ink">
                    {m.username}
                    {isMe && (
                      <span className="ml-1 rounded bg-accent-soft px-1 py-px text-[10px] text-accent-ink">
                        you
                      </span>
                    )}
                  </span>
                  <Badge
                    tone={
                      m.role === "admin"
                        ? "info"
                        : isRevoked
                          ? "warning"
                          : "neutral"
                    }
                  >
                    {isRevoked ? "revoked" : m.role}
                  </Badge>
                </div>
                <div className="mt-0.5 truncate font-mono text-xs text-ink-dim">
                  @{m.hostname}
                </div>
              </div>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
