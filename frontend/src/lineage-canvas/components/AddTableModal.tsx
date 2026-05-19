/**
 * Multi-select Add Table modal.
 *
 * Surfaces the cached catalog tree across every DB profile in a
 * single hierarchical picker (profile → database → schema → table)
 * so the user can tick any number of tables in one open / close
 * cycle. The old per-table chevron flow forced them to re-enter the
 * modal once per pick — exhausting on canvases that need 8–10
 * seed tables.
 *
 * Adds are still optimistic: the modal calls ``onPick`` once per
 * checked table with empty ``columns``, and column enrichment runs
 * in the background for each via ``fetchTableColumns`` (the canvas
 * already dedupes when the second ``onPick`` for the same node
 * lands with the freshly-fetched column list).
 */

import { useEffect, useMemo, useState } from "react";
import { Plus } from "lucide-react";

import { Button } from "../../components/ui";
import Modal from "../../components/Modal";
import { fetchTableColumns } from "../amx-bridge/catalog";
import type { AddTablePick } from "../types";
import {
  LineageMultiTablePicker,
  tableKey,
  type PickedTable,
} from "./LineageMultiTablePicker";

interface Props {
  open: boolean;
  onClose: () => void;
  defaultProfile: string;
  onPick: (pick: AddTablePick) => void;
  /** Called once after the batch finishes adding ``count`` tables,
   *  giving the canvas a chance to re-layout so the staggered
   *  positions do not stack on top of each other. */
  onBatchAdded?: (count: number) => void;
}

export function AddTableModal({ open, onClose, onPick, onBatchAdded }: Props) {
  const [selected, setSelected] = useState<Map<string, PickedTable>>(
    () => new Map(),
  );
  const [adding, setAdding] = useState(false);

  useEffect(() => {
    if (open) setSelected(new Map());
  }, [open]);

  const count = selected.size;
  const summary = useMemo(() => {
    if (count === 0) return "";
    if (count === 1) {
      const only = Array.from(selected.values())[0];
      return `${only.schema}.${only.table}`;
    }
    return `${count} tables`;
  }, [count, selected]);

  async function handleAdd() {
    if (adding || count === 0) return;
    setAdding(true);
    try {
      // Emit one optimistic onPick per table (empty columns) so the
      // canvas paints all nodes immediately, then chase each with a
      // background column fetch — Canvas.onPickTable dedupes by
      // node id and merges the columns when the fetch lands.
      for (const p of selected.values()) {
        onPick({
          profile: p.profile,
          backend: p.backend,
          database: p.database,
          schema: p.schema,
          table: p.table,
          columns: [],
        });
      }
      onClose();
      // Let the canvas re-arrange once the optimistic batch has
      // committed — without this the N nodes land in a tight
      // diagonal stagger and overlap heavily on dense picks.
      onBatchAdded?.(count);
      // Fire-and-forget column enrichment; failures stay silent so the
      // node stays draggable even when the catalog endpoint is cold.
      for (const p of selected.values()) {
        void fetchTableColumns({
          profile: p.profile,
          database: p.database,
          schema: p.schema,
          table: p.table,
        })
          .then((cols) => {
            if (!cols.length) return;
            onPick({
              profile: p.profile,
              backend: p.backend,
              database: p.database,
              schema: p.schema,
              table: p.table,
              columns: cols,
            });
          })
          .catch(() => undefined);
      }
    } finally {
      setAdding(false);
    }
  }

  return (
    <Modal
      open={open}
      onClose={onClose}
      size="md"
      title={
        <span className="inline-flex items-center gap-2">
          <Plus size={14} /> Add tables to canvas
        </span>
      }
      description="Tick any number of tables across any DB profile — they all land on the canvas in one shot."
      footer={
        <div className="flex w-full items-center justify-between gap-2">
          <span className="text-[11px] text-fg-muted">
            {count > 0 ? `Selected: ${summary}` : "Nothing selected yet."}
          </span>
          <div className="flex gap-2">
            <Button variant="secondary" size="sm" onClick={onClose}>
              Cancel
            </Button>
            <Button
              variant="primary"
              size="sm"
              disabled={count === 0 || adding}
              loading={adding}
              onClick={handleAdd}
            >
              {count > 1 ? `Add ${count} tables` : "Add table"}
            </Button>
          </div>
        </div>
      }
    >
      <div className="max-h-[480px] overflow-y-auto pr-1">
        <LineageMultiTablePicker
          selected={selected}
          onChange={setSelected}
        />
      </div>
    </Modal>
  );
}

// ``defaultProfile`` is intentionally accepted but unused — the new
// multi-select tree shows every profile up front so there's no single
// "default profile" to pre-select. Keeping the prop avoids a wider
// signature change at the Canvas call site.
void tableKey;
