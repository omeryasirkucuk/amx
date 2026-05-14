import type { ComponentType, ReactNode, SVGProps } from "react";
import { AlertCircle, RotateCcw } from "lucide-react";

import { ApiError } from "../../lib/api";
import { cn } from "../../lib/cn";
import EmptyState from "../EmptyState";
import Button from "./Button";
import Skeleton from "./Skeleton";

type Status = "loading" | "error" | "empty";

interface Props {
  status: Status;
  className?: string;

  /** Loading: number of body-block skeletons rendered under the header line. */
  loadingBlocks?: number;
  /** Loading: hide the leading title-line skeleton (use when the page already
   *  renders its own static title above this component). */
  hideLoadingTitle?: boolean;

  /** Error: the thrown value (ApiError gets its status + hint rendered). */
  error?: unknown;
  /** Error: invoked when the user clicks "Try again". */
  onRetry?: () => void;
  /** Error: override the auto-derived title. */
  errorTitle?: ReactNode;

  /** Empty: optional decoration. */
  emptyIcon?: ComponentType<SVGProps<SVGSVGElement>>;
  emptyTitle?: ReactNode;
  emptyDescription?: ReactNode;
  emptyActions?: ReactNode;
}

/**
 * Route-level loading / error / empty primitive. Drop in where a page
 * body is gated on a single query so failures surface visibly instead
 * of spinning forever or falling back to a `"Loading…"` string.
 *
 * Error variant pulls `status` + `detail` + `hint` from `ApiError` so
 * the backend's structured response (FastAPI `HTTPException(detail={...})`)
 * is rendered verbatim — no string munging at the call site.
 */
export default function RouteState({
  status,
  className,
  loadingBlocks = 3,
  hideLoadingTitle,
  error,
  onRetry,
  errorTitle,
  emptyIcon,
  emptyTitle,
  emptyDescription,
  emptyActions,
}: Props) {
  if (status === "loading") {
    return (
      <div
        role="status"
        aria-busy="true"
        aria-live="polite"
        className={cn("space-y-3", className)}
      >
        {!hideLoadingTitle && <Skeleton className="h-5 w-1/3" />}
        {Array.from({ length: Math.max(1, loadingBlocks) }).map((_, i) => (
          <Skeleton key={i} shape="block" className="h-24 w-full" />
        ))}
      </div>
    );
  }

  if (status === "error") {
    const apiError = error instanceof ApiError ? error : null;
    const fallbackMessage =
      error instanceof Error
        ? error.message
        : "Something went wrong loading this view.";
    const title =
      errorTitle ??
      (apiError ? `Request failed (${apiError.status})` : "Request failed");
    const description = apiError ? apiError.detail : fallbackMessage;
    const hint = apiError?.hint;
    return (
      <EmptyState
        icon={AlertCircle}
        title={title}
        description={
          hint ? (
            <span>
              {description}
              <span className="mt-1 block text-ink-dim">{hint}</span>
            </span>
          ) : (
            description
          )
        }
        actions={
          onRetry ? (
            <Button
              variant="secondary"
              size="sm"
              leadingIcon={<RotateCcw className="h-3.5 w-3.5" />}
              onClick={onRetry}
            >
              Try again
            </Button>
          ) : undefined
        }
        className={className}
      />
    );
  }

  return (
    <EmptyState
      icon={emptyIcon}
      title={emptyTitle ?? "Nothing to show"}
      description={emptyDescription}
      actions={emptyActions}
      className={className}
    />
  );
}
