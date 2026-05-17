/**
 * Legacy /lineage/:profile/:anchor URL handler.
 *
 * Previous saves navigated here using the user-typed canvas *name* as
 * the second segment, which collided with the route's expectation
 * that the segment is a table anchor FQN. The new save flow navigates
 * to /lineage?artifact=<id> instead. This component preserves the old
 * deep links by best-effort resolving the legacy slug to an artifact
 * id via the list endpoint and redirecting; if nothing matches we
 * fall through to the blank canvas (rather than the previous behavior
 * of silently failing to load).
 */

import { useEffect } from "react";
import { Navigate, useNavigate, useParams } from "react-router-dom";

import { lineageList } from "../lib/api";

export default function LineageRedirect() {
  const { profile = "", anchor = "" } = useParams();
  const navigate = useNavigate();

  useEffect(() => {
    let cancelled = false;
    async function resolve() {
      try {
        const out = await lineageList(profile);
        if (cancelled) return;
        const match = (out.artifacts || []).find(
          (a) =>
            String(a.name).toLowerCase() === String(anchor).toLowerCase() ||
            String(a.name).replace(/[^A-Za-z0-9_-]+/g, "_") === String(anchor),
        );
        if (match) {
          navigate(`/lineage?artifact=${match.id}`, { replace: true });
          return;
        }
      } catch {
        // Fall through — render the blank canvas.
      }
      navigate("/lineage", { replace: true });
    }
    void resolve();
    return () => {
      cancelled = true;
    };
  }, [profile, anchor, navigate]);

  return <Navigate to="/lineage" replace />;
}
