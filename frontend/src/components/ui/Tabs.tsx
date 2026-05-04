import {
  createContext,
  useContext,
  useId,
  type KeyboardEvent,
  type ReactNode,
} from "react";

import { cn } from "../../lib/cn";

interface TabsContextValue {
  value: string;
  onChange: (next: string) => void;
  baseId: string;
}

const TabsContext = createContext<TabsContextValue | null>(null);

function useTabs() {
  const ctx = useContext(TabsContext);
  if (!ctx) throw new Error("Tabs.* must be rendered inside <Tabs>");
  return ctx;
}

interface TabsProps {
  value: string;
  onValueChange: (next: string) => void;
  children: ReactNode;
  className?: string;
}

export function Tabs({ value, onValueChange, children, className }: TabsProps) {
  const baseId = useId();
  return (
    <TabsContext.Provider value={{ value, onChange: onValueChange, baseId }}>
      <div className={cn("flex flex-col gap-4", className)}>{children}</div>
    </TabsContext.Provider>
  );
}

interface TabsListProps {
  children: ReactNode;
  className?: string;
  /** "underline" (default) gives a bottom border + active accent line.
      "pill" gives a segmented control look on a subtle background. */
  variant?: "underline" | "pill";
}

export function TabsList({ children, className, variant = "underline" }: TabsListProps) {
  const wrapperClass =
    variant === "underline"
      ? "flex items-center gap-4 border-b border-border"
      : "inline-flex items-center gap-1 rounded-md bg-surface-subtle p-1";
  return (
    <div
      role="tablist"
      className={cn(wrapperClass, className)}
      onKeyDown={(e: KeyboardEvent<HTMLDivElement>) => {
        if (e.key !== "ArrowLeft" && e.key !== "ArrowRight") return;
        const tabs = Array.from(
          e.currentTarget.querySelectorAll<HTMLElement>("[role='tab']"),
        );
        const idx = tabs.findIndex((t) => t === document.activeElement);
        if (idx < 0) return;
        const dir = e.key === "ArrowRight" ? 1 : -1;
        const next = tabs[(idx + dir + tabs.length) % tabs.length];
        next?.focus();
        next?.click();
        e.preventDefault();
      }}
    >
      {children}
    </div>
  );
}

interface TabProps {
  value: string;
  children: ReactNode;
  icon?: ReactNode;
  className?: string;
  variant?: "underline" | "pill";
  badge?: ReactNode;
}

export function Tab({ value, children, icon, className, variant = "underline", badge }: TabProps) {
  const { value: active, onChange, baseId } = useTabs();
  const isActive = active === value;
  const baseClass =
    variant === "underline"
      ? cn(
          "relative -mb-px inline-flex items-center gap-1.5 border-b-2 px-1 pb-2 pt-1 text-sm font-medium transition-colors duration-fast",
          isActive
            ? "border-accent text-ink"
            : "border-transparent text-ink-muted hover:text-ink",
        )
      : cn(
          "inline-flex items-center gap-1.5 rounded px-2.5 py-1 text-xs font-medium transition-colors duration-fast",
          isActive
            ? "bg-surface-raised text-ink shadow-xs"
            : "text-ink-muted hover:text-ink",
        );
  return (
    <button
      type="button"
      role="tab"
      id={`${baseId}-tab-${value}`}
      aria-selected={isActive}
      aria-controls={`${baseId}-panel-${value}`}
      tabIndex={isActive ? 0 : -1}
      onClick={() => onChange(value)}
      className={cn(baseClass, "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-1 focus-visible:ring-offset-bg", className)}
    >
      {icon}
      {children}
      {badge}
    </button>
  );
}

interface TabPanelProps {
  value: string;
  children: ReactNode;
  className?: string;
}

export function TabPanel({ value, children, className }: TabPanelProps) {
  const { value: active, baseId } = useTabs();
  if (active !== value) return null;
  return (
    <div
      role="tabpanel"
      id={`${baseId}-panel-${value}`}
      aria-labelledby={`${baseId}-tab-${value}`}
      tabIndex={0}
      className={cn("focus:outline-none", className)}
    >
      {children}
    </div>
  );
}
