import type { ReactNode } from "react";
import { Sparkles } from "lucide-react";

import Dialog from "./ui/Dialog";
import Button from "./ui/Button";

interface ScopeOption {
  label: string;
  description?: ReactNode;
  onClick: () => void;
  loading?: boolean;
  disabled?: boolean;
}

interface Props {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  description?: ReactNode;
  /** "Sadece bu asset" — fast, single LLM call. Rendered as the secondary button. */
  singleOption: ScopeOption;
  /** "Bütün asset" — bulk run, spawns analyze worker. Rendered as the primary button. */
  bulkOption: ScopeOption;
  cancelLabel?: string;
}

/**
 * Two-action variant of AlertDialog used by the Database / Schema /
 * Table pages where "Generate" needs to ask whether the user wants
 * just this asset's own description or a bulk run that walks every
 * child asset under it. Replaces the previous pattern of two
 * separate buttons in the header.
 */
export default function GenerateScopeDialog({
  open,
  onClose,
  title,
  description,
  singleOption,
  bulkOption,
  cancelLabel = "Cancel",
}: Props) {
  const anyLoading = !!(singleOption.loading || bulkOption.loading);
  return (
    <Dialog
      open={open}
      onClose={onClose}
      preventBackdropClose
      hideCloseButton
      size="md"
      footer={
        <Button variant="ghost" size="sm" onClick={onClose} disabled={anyLoading}>
          {cancelLabel}
        </Button>
      }
    >
      <div className="flex items-start gap-3">
        <span className="mt-0.5 inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-accent-soft text-accent-ink">
          <Sparkles size={18} />
        </span>
        <div className="min-w-0 flex-1">
          <h2 className="text-sm font-semibold text-ink">{title}</h2>
          {description && (
            <p className="mt-1 text-sm text-ink-muted">{description}</p>
          )}
          <div className="mt-4 flex flex-col gap-2">
            <ScopeButton option={singleOption} variant="secondary" disabled={anyLoading} />
            <ScopeButton option={bulkOption} variant="primary" disabled={anyLoading} />
          </div>
        </div>
      </div>
    </Dialog>
  );
}

function ScopeButton({
  option,
  variant,
  disabled,
}: {
  option: ScopeOption;
  variant: "primary" | "secondary";
  disabled: boolean;
}) {
  return (
    <button
      type="button"
      onClick={option.onClick}
      disabled={disabled || option.disabled}
      className={
        "group flex w-full items-start gap-3 rounded-lg border px-3.5 py-3 text-left transition-colors duration-fast disabled:cursor-not-allowed disabled:opacity-50 " +
        (variant === "primary"
          ? "border-accent/40 bg-accent-soft/40 hover:border-accent hover:bg-accent-soft"
          : "border-border bg-surface hover:border-border-strong hover:bg-surface-raised")
      }
    >
      <Sparkles
        size={14}
        className={
          (variant === "primary" ? "text-accent-ink" : "text-ink-muted") + " mt-0.5 shrink-0"
        }
      />
      <span className="min-w-0 flex-1">
        <span className="block text-sm font-medium text-ink">
          {option.label}
          {option.loading && (
            <span className="ml-2 text-xs font-normal text-ink-dim">…running</span>
          )}
        </span>
        {option.description && (
          <span className="mt-0.5 block text-xs text-ink-muted">{option.description}</span>
        )}
      </span>
    </button>
  );
}
