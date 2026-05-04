import {
  useEffect,
  useRef,
  useState,
  type KeyboardEvent,
  type ReactNode,
} from "react";
import { Check, Loader2, Pencil, X } from "lucide-react";

import { cn } from "../../lib/cn";

interface Props {
  /** Current persisted value. */
  value: string | null | undefined;
  /** Persist the new value. Throw to surface an error. */
  onSave: (next: string) => Promise<void> | void;
  /** Display when value is empty. */
  emptyLabel?: ReactNode;
  /** Render the input as a textarea instead of a single-line input. */
  multiline?: boolean;
  /** Placeholder while editing. */
  placeholder?: string;
  /** Optional override for the surrounding container className. */
  className?: string;
  /** Read-only mode hides the edit affordance entirely. */
  readOnly?: boolean;
  /** Render value with an italic ink-dim style when empty. */
  italicEmpty?: boolean;
  /** Optional auxiliary action shown next to the edit pencil (e.g. Generate). */
  actions?: ReactNode;
}

/**
 * Inline-editable text. Click the pencil (or Enter while focused) to
 * switch to an editor; press Esc to cancel, Cmd/Ctrl+Enter (or click
 * the save button) to commit. Surfaces a spinner while the save
 * promise is in flight and an inline error message if it rejects.
 */
export default function InlineEditText({
  value,
  onSave,
  emptyLabel = "Click to add description",
  multiline = false,
  placeholder = "",
  className,
  readOnly,
  italicEmpty,
  actions,
}: Props) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState<string>(value ?? "");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement | HTMLTextAreaElement | null>(null);

  useEffect(() => {
    setDraft(value ?? "");
  }, [value]);

  useEffect(() => {
    if (editing) {
      // Defer focus so the input mounts before we focus.
      requestAnimationFrame(() => {
        inputRef.current?.focus();
        const t = inputRef.current as HTMLInputElement | HTMLTextAreaElement | null;
        if (t && "value" in t) t.setSelectionRange(t.value.length, t.value.length);
      });
    }
  }, [editing]);

  const trimmed = (draft ?? "").trim();
  const original = (value ?? "").trim();
  const isDirty = trimmed !== original;

  function startEdit() {
    if (readOnly) return;
    setDraft(value ?? "");
    setError(null);
    setEditing(true);
  }
  function cancel() {
    setDraft(value ?? "");
    setError(null);
    setEditing(false);
  }
  async function save() {
    if (!isDirty) {
      setEditing(false);
      return;
    }
    setSaving(true);
    setError(null);
    try {
      await onSave(trimmed);
      setEditing(false);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Save failed.");
    } finally {
      setSaving(false);
    }
  }

  function onKeyDown(e: KeyboardEvent<HTMLInputElement | HTMLTextAreaElement>) {
    if (e.key === "Escape") {
      e.preventDefault();
      cancel();
    } else if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      void save();
    } else if (e.key === "Enter" && !multiline) {
      e.preventDefault();
      void save();
    }
  }

  if (editing) {
    const Editor = multiline ? "textarea" : "input";
    return (
      <div className={cn("flex flex-col gap-1.5", className)}>
        <Editor
          ref={inputRef as never}
          value={draft}
          onChange={(e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) =>
            setDraft(e.target.value)
          }
          onKeyDown={onKeyDown}
          placeholder={placeholder}
          disabled={saving}
          rows={multiline ? 3 : undefined}
          className={cn(
            "w-full rounded-md border border-accent bg-surface-raised px-2.5 py-1.5 text-sm text-ink",
            "focus:outline-none focus:ring-2 focus:ring-accent/30",
            "disabled:cursor-not-allowed disabled:opacity-60",
            multiline && "resize-y min-h-[64px]",
          )}
        />
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={save}
            disabled={saving || !isDirty}
            className="inline-flex h-7 items-center gap-1.5 rounded-md bg-accent px-2.5 text-xs font-medium text-white shadow-xs transition hover:bg-accent-ink disabled:cursor-not-allowed disabled:opacity-50"
          >
            {saving ? <Loader2 size={11} className="animate-spin" /> : <Check size={11} />}
            Save
          </button>
          <button
            type="button"
            onClick={cancel}
            disabled={saving}
            className="inline-flex h-7 items-center gap-1.5 rounded-md border border-border bg-surface-raised px-2.5 text-xs text-ink-muted transition hover:bg-surface-subtle hover:text-ink disabled:cursor-not-allowed disabled:opacity-50"
          >
            <X size={11} />
            Cancel
          </button>
          <span className="ml-1 text-[10px] text-ink-dim">
            {multiline ? "⌘↵ save · esc cancel" : "↵ save · esc cancel"}
          </span>
        </div>
        {error && <p className="text-xs text-critical">{error}</p>}
      </div>
    );
  }

  const isEmpty = !value || !value.trim();
  return (
    <div
      className={cn(
        "group inline-flex max-w-full items-start gap-1.5",
        !readOnly && "cursor-text",
        className,
      )}
      onClick={startEdit}
      role={readOnly ? undefined : "button"}
      tabIndex={readOnly ? undefined : 0}
      onKeyDown={(e) => {
        if (readOnly) return;
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          startEdit();
        }
      }}
    >
      <span
        className={cn(
          "min-w-0 flex-1 whitespace-pre-wrap break-words",
          isEmpty && italicEmpty ? "italic text-ink-dim" : "text-ink",
        )}
      >
        {isEmpty ? emptyLabel : value}
      </span>
      {!readOnly && (
        <span className="mt-0.5 inline-flex items-center gap-1 opacity-0 transition-opacity duration-fast group-hover:opacity-100 focus-visible:opacity-100">
          {actions}
          <Pencil size={11} className="text-ink-dim" />
        </span>
      )}
    </div>
  );
}
