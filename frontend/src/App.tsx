import { lazy, Suspense } from "react";
import { Routes, Route, Navigate, useLocation } from "react-router-dom";

import AppShell from "./components/AppShell";
import ErrorBoundary from "./components/ErrorBoundary";
import InstallBanner from "./components/InstallBanner";
import QueryErrorListener from "./components/QueryErrorListener";
import { ToastProvider } from "./components/ui";

// Each route is loaded on demand. The user landing on /ask no longer
// pulls the /runs detail page (~30 KB raw, ~10 KB gzip) into the
// initial bundle, and the same goes for every other route. Vite emits
// one JS chunk per dynamic import; React.Suspense renders the
// fallback while the chunk is in flight.
//
// AppShell + ToastProvider + ErrorBoundary stay eager because they
// are visible before the route renders and unmounting them on every
// navigation would kill the toast queue and reset the error boundary.
const Landing = lazy(() => import("./routes/Landing"));
const Home = lazy(() => import("./routes/Home"));
const Profile = lazy(() => import("./routes/Profile"));
const Database = lazy(() => import("./routes/Database"));
const Schema = lazy(() => import("./routes/Schema"));
const Table = lazy(() => import("./routes/Table"));
const RunsList = lazy(() => import("./routes/RunsList"));
const RunDetail = lazy(() => import("./routes/RunDetail"));
const RunNew = lazy(() => import("./routes/RunNew"));
const RunsCompare = lazy(() => import("./routes/RunsCompare"));
const Ask = lazy(() => import("./routes/Ask"));
const Pending = lazy(() => import("./routes/Pending"));
const Settings = lazy(() => import("./routes/Settings"));
const System = lazy(() => import("./routes/System"));
const Audit = lazy(() => import("./routes/Audit"));
const Schedules = lazy(() => import("./routes/Schedules"));
const Pricing = lazy(() => import("./routes/Pricing"));
const DbCache = lazy(() => import("./routes/DbCache"));
// Lineage rebuilt around the new lineage-canvas package — one
// component serves /lineage, /lineage/new, /lineage/saved, and the
// legacy /lineage/:profile/:anchor URL (the latter redirects to the
// canvas with the resolved artifact id when one exists).
const Lineage = lazy(() => import("./routes/Lineage"));
const LineageRedirect = lazy(() => import("./routes/LineageRedirect"));
const Pages = lazy(() => import("./routes/Pages"));
const PageNew = lazy(() => import("./routes/PageNew"));
const PageEdit = lazy(() => import("./routes/PageEdit"));
const WorkspaceAdmin = lazy(() => import("./routes/WorkspaceAdmin"));

// Suspense fallback is intentionally minimal — chunk fetches over
// loopback land in milliseconds, so a heavy skeleton would flash
// then immediately disappear. Empty space avoids layout flicker.
const RouteFallback = () => <div aria-hidden="true" />;

// Top-level route map. The shell carries the persistent left tree +
// top bar; each <Route element> renders inside the shell's main
// canvas. ToastProvider sits above Routes so any handler can call
// useToast() without prop drilling.
//
// Browse paths encode the full scope so two tabs on different
// profiles never bleed state into each other:
//   /db/:profile/:database/...   (2-level: Postgres, MySQL, ...)
//   /cat/:profile/:catalog/...   (3-level: Databricks, BigQuery)
export default function App() {
  // ErrorBoundary sits above ToastProvider + Routes so an uncaught
  // throw from any route lands on the fallback UI instead of leaving
  // the user staring at a blank page. Provider/provider context state
  // is rebuilt on reload — that's intentional, the boundary's reload
  // affordance is what users will reach for.
  const location = useLocation();
  return (
    <ErrorBoundary resetKey={location.pathname}>
      <ToastProvider>
        <QueryErrorListener />
        <InstallBanner />
        <Suspense fallback={<RouteFallback />}>
          <Routes>
            <Route element={<AppShell />}>
              <Route index element={<Landing />} />
              <Route path="overview" element={<Home />} />
              <Route path="db" element={<Navigate to="/" replace />} />
              <Route path="db/:profile" element={<Profile />} />
              <Route path="db/:profile/:database" element={<Database />} />
              <Route path="db/:profile/:database/:schema" element={<Schema />} />
              <Route path="db/:profile/:database/:schema/:table" element={<Table />} />
              <Route path="cat" element={<Navigate to="/" replace />} />
              <Route path="cat/:profile" element={<Profile />} />
              <Route path="cat/:profile/:catalog" element={<Database />} />
              <Route path="cat/:profile/:catalog/:schema" element={<Schema />} />
              <Route path="cat/:profile/:catalog/:schema/:table" element={<Table />} />
              <Route path="runs" element={<RunsList />} />
              <Route path="runs/new" element={<RunNew />} />
              <Route path="runs/compare" element={<RunsCompare />} />
              <Route path="runs/:runId" element={<RunDetail />} />
              <Route path="ask" element={<Ask />} />
              <Route path="pending" element={<Pending />} />
              <Route path="audit" element={<Audit />} />
              <Route path="settings" element={<Settings />} />
              <Route path="system" element={<System />} />
              <Route path="pricing" element={<Pricing />} />
              <Route path="db-cache" element={<DbCache />} />
              <Route path="lineage" element={<Lineage />} />
              <Route path="lineage/saved" element={<Lineage />} />
              <Route path="lineage/new" element={<Lineage />} />
              <Route path="lineage/share" element={<Lineage />} />
              <Route path="lineage/:profile/:anchor" element={<LineageRedirect />} />
              <Route path="pages" element={<Pages />} />
              <Route path="pages/new" element={<PageNew />} />
              <Route path="pages/:pageId" element={<PageEdit />} />
              <Route path="runs/schedules" element={<Schedules />} />
              <Route
                path="runs/catalog-refresh-schedules"
                element={<Navigate to="/db-cache" replace />}
              />
              <Route path="workspace-admin" element={<WorkspaceAdmin />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Route>
          </Routes>
        </Suspense>
      </ToastProvider>
    </ErrorBoundary>
  );
}
