import type { ReactNode } from "react";
import { AlertTriangle } from "lucide-react";

import Dialog from "./Dialog";
import Button from "./Button";

interface Props {
  open: boolean;
  onClose: () => void;
  onConfirm: () => void;
  title: ReactNode;
  description?: ReactNode;
  confirmLabel?: string;
  cancelLabel?: string;
  /** Confirm button variant. Use "danger" for destructive actions. */
  tone?: "danger" | "primary";
  loading?: boolean;
}

/**
 * Confirm-or-cancel dialog for destructive actions. Wraps Dialog
 * with a fixed two-button footer + warning icon.
 */
export default function AlertDialog({
  open,
  onClose,
  onConfirm,
  title,
  description,
  confirmLabel = "Confirm",
  cancelLabel = "Cancel",
  tone = "danger",
  loading,
}: Props) {
  return (
    <Dialog
      open={open}
      onClose={onClose}
      preventBackdropClose
      hideCloseButton
      size="sm"
      footer={
        <>
          <Button variant="ghost" size="sm" onClick={onClose} disabled={loading}>
            {cancelLabel}
          </Button>
          <Button
            variant={tone === "danger" ? "danger" : "primary"}
            size="sm"
            onClick={onConfirm}
            loading={loading}
            autoFocus
          >
            {confirmLabel}
          </Button>
        </>
      }
    >
      <div className="flex items-start gap-3">
        <span
          className={
            tone === "danger"
              ? "mt-0.5 inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-critical-soft text-critical"
              : "mt-0.5 inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-accent-soft text-accent-ink"
          }
        >
          <AlertTriangle size={18} />
        </span>
        <div className="min-w-0">
          <h2 className="text-sm font-semibold text-ink">{title}</h2>
          {description && (
            <p className="mt-1 text-sm text-ink-muted">{description}</p>
          )}
        </div>
      </div>
    </Dialog>
  );
}
