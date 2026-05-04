import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { createPortal } from "react-dom";
import {
  AlertCircle,
  AlertTriangle,
  CheckCircle,
  Info,
  X,
} from "lucide-react";

import { cn } from "../../lib/cn";

export type ToastTone = "success" | "error" | "warning" | "info";

interface ToastInput {
  title: ReactNode;
  description?: ReactNode;
  tone?: ToastTone;
  /** Auto-dismiss after ms. Default 4500. Pass 0 to require manual close. */
  duration?: number;
}

interface ToastRecord extends ToastInput {
  id: number;
}

interface ToastContextValue {
  push: (toast: ToastInput) => number;
  dismiss: (id: number) => void;
}

const ToastContext = createContext<ToastContextValue | null>(null);

const toneIcon = {
  success: CheckCircle,
  error: AlertCircle,
  warning: AlertTriangle,
  info: Info,
};

const toneClasses: Record<ToastTone, string> = {
  success: "border-positive/40 bg-positive-soft text-ink",
  error: "border-critical/40 bg-critical-soft text-ink",
  warning: "border-warning/40 bg-warning-soft text-ink",
  info: "border-info/40 bg-info-soft text-ink",
};

const iconClasses: Record<ToastTone, string> = {
  success: "text-positive",
  error: "text-critical",
  warning: "text-warning",
  info: "text-info",
};

/**
 * Toast notification provider. Wrap once near the app root then call
 * `useToast().push({ title, tone })` from anywhere.
 *
 * Renders toasts top-right via a portal. Each toast auto-dismisses
 * after `duration` ms (default 4500). Pass `duration: 0` to require
 * manual dismissal.
 */
export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<ToastRecord[]>([]);
  const idRef = useRef(0);
  const timersRef = useRef<Map<number, ReturnType<typeof setTimeout>>>(new Map());

  const dismiss = useCallback((id: number) => {
    setToasts((cur) => cur.filter((t) => t.id !== id));
    const tmr = timersRef.current.get(id);
    if (tmr) {
      clearTimeout(tmr);
      timersRef.current.delete(id);
    }
  }, []);

  const push = useCallback(
    (toast: ToastInput) => {
      idRef.current += 1;
      const id = idRef.current;
      const record: ToastRecord = { ...toast, id, tone: toast.tone ?? "info" };
      setToasts((cur) => [...cur, record]);
      const duration = toast.duration ?? 4500;
      if (duration > 0) {
        const tmr = setTimeout(() => dismiss(id), duration);
        timersRef.current.set(id, tmr);
      }
      return id;
    },
    [dismiss],
  );

  useEffect(() => {
    const map = timersRef.current;
    return () => {
      map.forEach((t) => clearTimeout(t));
      map.clear();
    };
  }, []);

  const value = useMemo(() => ({ push, dismiss }), [push, dismiss]);

  return (
    <ToastContext.Provider value={value}>
      {children}
      {createPortal(
        <div
          className="pointer-events-none fixed right-4 top-4 z-[60] flex w-80 flex-col gap-2"
          aria-live="polite"
        >
          {toasts.map((t) => {
            const Icon = toneIcon[t.tone ?? "info"];
            return (
              <div
                key={t.id}
                role="status"
                className={cn(
                  "pointer-events-auto flex items-start gap-2.5 rounded-lg border bg-surface-raised px-3 py-2.5 shadow-md animate-slide-in-up",
                  toneClasses[t.tone ?? "info"],
                )}
              >
                <Icon
                  size={16}
                  className={cn("mt-0.5 shrink-0", iconClasses[t.tone ?? "info"])}
                />
                <div className="min-w-0 flex-1">
                  <div className="text-sm font-medium text-ink">{t.title}</div>
                  {t.description && (
                    <div className="mt-0.5 text-xs text-ink-muted">
                      {t.description}
                    </div>
                  )}
                </div>
                <button
                  type="button"
                  onClick={() => dismiss(t.id)}
                  aria-label="Dismiss"
                  className="-mr-1 -mt-1 rounded p-1 text-ink-dim hover:bg-surface-subtle hover:text-ink"
                >
                  <X size={13} />
                </button>
              </div>
            );
          })}
        </div>,
        document.body,
      )}
    </ToastContext.Provider>
  );
}

export function useToast() {
  const ctx = useContext(ToastContext);
  if (!ctx) throw new Error("useToast must be used inside <ToastProvider>");
  return ctx;
}
