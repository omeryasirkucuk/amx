import { Routes, Route, Navigate } from "react-router-dom";

import AppShell from "./components/AppShell";
import { ToastProvider } from "./components/ui";
import Home from "./routes/Home";
import Database from "./routes/Database";
import Schema from "./routes/Schema";
import Table from "./routes/Table";
import RunsList from "./routes/RunsList";
import RunDetail from "./routes/RunDetail";
import RunNew from "./routes/RunNew";
import RunsCompare from "./routes/RunsCompare";
import Ask from "./routes/Ask";
import Pending from "./routes/Pending";
import Settings from "./routes/Settings";
import System from "./routes/System";

// Top-level route map. The shell carries the persistent left tree +
// top bar; each <Route element> renders inside the shell's main
// canvas. ToastProvider sits above Routes so any handler can call
// useToast() without prop drilling.
export default function App() {
  return (
    <ToastProvider>
      <Routes>
        <Route element={<AppShell />}>
          <Route index element={<Home />} />
          <Route path="db" element={<Navigate to="/" replace />} />
          <Route path="db/:profile" element={<Database />} />
          <Route path="db/:profile/:schema" element={<Schema />} />
          <Route path="db/:profile/:schema/:table" element={<Table />} />
          <Route path="runs" element={<RunsList />} />
          <Route path="runs/new" element={<RunNew />} />
          <Route path="runs/compare" element={<RunsCompare />} />
          <Route path="runs/:runId" element={<RunDetail />} />
          <Route path="ask" element={<Ask />} />
          <Route path="pending" element={<Pending />} />
          <Route path="settings" element={<Settings />} />
          <Route path="system" element={<System />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Route>
      </Routes>
    </ToastProvider>
  );
}
