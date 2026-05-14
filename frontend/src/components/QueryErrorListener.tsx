import { useEffect } from "react";

import {
  describeQueryError,
  subscribeQueryErrors,
} from "../lib/queryErrorBus";
import { useToast } from "./ui";

/**
 * Subscribes to query/mutation failures published by the QueryClient
 * caches and surfaces each through the toast layer. Mount once near
 * the app root inside <ToastProvider>.
 *
 * Filtering happens upstream: queries that render their own inline
 * error UI (Audit, Ask, RunsCompare cell deep-dives) flag themselves
 * with `meta: { silentError: true }` so the cache's onError skips the
 * publish step entirely.
 */
export default function QueryErrorListener() {
  const toast = useToast();
  useEffect(() => {
    return subscribeQueryErrors((event) => {
      const { title, description, hint } = describeQueryError(event.error);
      toast.push({
        tone: "error",
        title,
        description: hint ? `${description} — ${hint}` : description,
        duration: 6000,
      });
    });
  }, [toast]);
  return null;
}
