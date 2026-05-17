/**
 * Read-only lineage view for shared canvas links.
 *
 * URL shape: ``/lineage/share#<encoded-payload>``. The encoded
 * payload lives in the hash, never sent to the server. Decoded into
 * a ``LineagePayload`` and rendered by ``LineageCanvas`` without any
 * mutation affordances — drag-to-connect and right-click verdicts
 * stay off, so a shared link is genuinely read-only.
 */

import { useEffect, useMemo, useState } from "react";
import { Link, useLocation } from "react-router-dom";
import { ArrowLeft, ShieldCheck } from "lucide-react";

import { decodeLineageShare } from "../lib/lineageShare";
import type { LineageEdge, LineagePayload } from "../lib/api";
import { LineageCanvas } from "../components/LineageCanvas";
import { EdgePanel } from "../components/EdgePanel";
import PageHeader from "../components/PageHeader";
import { Badge } from "../components/ui";

export default function LineageShare() {
  const location = useLocation();
  const [selectedEdge, setSelectedEdge] = useState<LineageEdge | null>(null);
  const blob = useMemo(() => location.hash.replace(/^#/, ""), [location.hash]);
  const payload = useMemo<LineagePayload | null>(() => decodeLineageShare(blob), [blob]);

  useEffect(() => {
    setSelectedEdge(null);
  }, [blob]);

  if (!payload) {
    return (
      <div className="flex flex-col gap-3 p-6 text-sm">
        <PageHeader
          title="Lineage · shared"
          description="No payload in URL hash, or the encoded data is malformed."
        />
        <Link to="/lineage" className="text-accent-default">
          ← Back to Lineage hub
        </Link>
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col gap-3">
      <PageHeader
        title="Lineage · shared (read-only)"
        description="This canvas was decoded from a share link. Mutations are disabled — copy to your own profile to start editing."
        actions={
          <Badge tone="neutral">
            <span className="inline-flex items-center gap-1">
              <ShieldCheck className="h-3 w-3" /> read-only
            </span>
          </Badge>
        }
      />
      <div className="grid h-[calc(100vh-220px)] grid-cols-[minmax(0,1fr)_320px] gap-3">
        <div className="overflow-hidden rounded-xl border border-surface-border bg-surface-raised">
          <LineageCanvas payload={payload} onSelectEdge={setSelectedEdge} />
        </div>
        <EdgePanel edge={selectedEdge} />
      </div>
      <div className="text-xs text-fg-muted">
        <Link to="/lineage" className="inline-flex items-center gap-1 hover:text-fg-default">
          <ArrowLeft className="h-3 w-3" /> Back to Lineage hub
        </Link>
      </div>
    </div>
  );
}
