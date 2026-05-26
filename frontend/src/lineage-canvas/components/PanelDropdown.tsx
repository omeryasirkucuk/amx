/**
 * PanelDropdown — the small pill-style select used in the lineage detail
 * panel (asset-type filter, direction filter), mirroring Databricks'
 * "Tables ▾" / "Up and Downstream ▾" controls. A button shows the active
 * option; a click opens a menu; selecting or clicking outside closes it.
 */

import { useEffect, useRef, useState } from "react";
import { Check, ChevronDown } from "lucide-react";
import clsx from "clsx";

export interface DropdownOption {
  value: string;
  label: string;
  icon?: React.ReactNode;
}

interface Props {
  value: string;
  options: DropdownOption[];
  onChange: (value: string) => void;
  className?: string;
}

export function PanelDropdown({ value, options, onChange, className }: Props) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const active = options.find((o) => o.value === value) ?? options[0];

  useEffect(() => {
    if (!open) return;
    function onDoc(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [open]);

  return (
    <div ref={ref} className={clsx("relative", className)}>
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className={clsx(
          "flex w-full items-center justify-between gap-2 rounded-md border px-3 py-1.5 text-[12.5px]",
          open ? "border-accent-default text-ink" : "border-surface-border text-ink",
        )}
      >
        <span className="inline-flex items-center gap-1.5 truncate">
          {active?.icon}
          {active?.label}
        </span>
        <ChevronDown size={13} className="shrink-0 text-fg-muted" />
      </button>
      {open && (
        <div className="absolute left-0 top-full z-30 mt-1 max-h-72 w-max min-w-full overflow-y-auto rounded-md border border-surface-border bg-surface-raised py-1 shadow-2xl">
          {options.map((o) => (
            <button
              key={o.value}
              type="button"
              onClick={() => {
                onChange(o.value);
                setOpen(false);
              }}
              className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[12.5px] text-ink hover:bg-surface"
            >
              <span className="flex w-4 shrink-0 justify-center">
                {o.value === value && <Check size={13} className="text-accent-ink" />}
              </span>
              {o.icon}
              <span className="truncate">{o.label}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
