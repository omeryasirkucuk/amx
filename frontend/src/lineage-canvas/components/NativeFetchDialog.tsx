/**
 * Native lineage fetch dialog.
 *
 * Pulls a table's lineage straight from the database's own lineage
 * system (Unity Catalog for Databricks) on demand: drill the cache
 * tree (profile → database → schema → table), pick one table, and AMX
 * fetches the upstream producers / downstream consumers (tables,
 * notebooks, jobs, dashboards, vector indexes), materialises them, and
 * seeds a saved artifact the canvas opens automatically. Entities the
 * active token can't read still appear as name-only nodes.
 *
 * The picker reuses the same cache tree as the rest of AMX
 * (``/api/db/cache/tree/*``) but is ungated on sync state — native
 * fetch is exactly what you reach for before a profile is fully
 * synced.
 */

import { useEffect, useState } from "react";
import { Waypoints } from "lucide-react";

import Modal from "../../components/Modal";
import { Button, Checkbox } from "../../components/ui";
import { lineageFetchNative, type NativeFetchResult } from "../../lib/api";
import { CacheTableTreePicker } from "./CacheTableTreePicker";
import type { PickedTable } from "./LineageMultiTablePicker";

interface Props {
  open: boolean;
  onClose: () => void;
  /** Fired after a successful fetch with the result (carries the
   *  seeded ``artifact_id`` the canvas should open). */
  onDone: (result: NativeFetchResult) => void;
}

function fqnOf(t: PickedTable): string {
  return [t.database, t.schema, t.table].filter(Boolean).join(".");
}

export function NativeFetchDialog({ open, onClose, onDone }: Props) {
  const [picked, setPicked] = useState<PickedTable | null>(null);
  const [withColumns, setWithColumns] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (open) {
      setPicked(null);
      setWithColumns(false);
      setError("");
    }
  }, [open]);

  async function handleFetch() {
    if (busy || !picked) return;
    setBusy(true);
    setError("");
    try {
      const result = await lineageFetchNative({
        profile: picked.profile,
        fqn: fqnOf(picked),
        withColumns,
      });
      onDone(result);
    } catch (e) {
      setError((e as Error).message || "Fetch failed.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <Modal
      open={open}
      onClose={onClose}
      size="md"
      title={
        <span className="inline-flex items-center gap-2">
          <Waypoints size={14} /> Fetch lineage from the database
        </span>
      }
      description="Pick a table — AMX reads the platform's own lineage (Unity Catalog) for it: upstream producers and downstream consumers. Works even when the profile isn't fully synced."
      footer={
        <div className="flex w-full items-center justify-between gap-2">
          <span className="text-[11px] text-fg-muted">
            {error ? (
              <span className="text-danger-ink">{error}</span>
            ) : picked ? (
              fqnOf(picked)
            ) : (
              "Pick a table to fetch."
            )}
          </span>
          <div className="flex gap-2">
            <Button variant="secondary" size="sm" onClick={onClose}>
              Cancel
            </Button>
            <Button variant="primary" size="sm" disabled={!picked || busy} onClick={handleFetch}>
              {busy ? "Fetching…" : "Fetch lineage"}
            </Button>
          </div>
        </div>
      }
    >
      <div className="space-y-3">
        <CacheTableTreePicker value={picked} onChange={setPicked} />
        <Checkbox
          label="Also fetch column-level lineage"
          description="One extra API call per column — slower, but draws column edges."
          checked={withColumns}
          onChange={(e) => setWithColumns(e.target.checked)}
        />
      </div>
    </Modal>
  );
}
