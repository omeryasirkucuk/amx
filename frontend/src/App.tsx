import { Routes, Route, Navigate } from "react-router-dom";

import AppShell from "./components/AppShell";
import Home from "./routes/Home";
import Schema from "./routes/Schema";
import Table from "./routes/Table";
import RunsList from "./routes/RunsList";
import RunDetail from "./routes/RunDetail";
import Ask from "./routes/Ask";
import Pending from "./routes/Pending";
import Settings from "./routes/Settings";

// Top-level route map. The shell carries the persistent left tree +
// top bar; each <Route element> renders inside the shell's main
// canvas.
//
// Routes ship in PR-B for the read-only paths. The action and chat
// pages (Ask / Pending / Runs / Settings) render placeholder cards
// today and graduate in PR-C, PR-D, PR-E.
export default function App() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route index element={<Home />} />
        <Route path="db" element={<Navigate to="/" replace />} />
        <Route path="db/:profile" element={<Home />} />
        <Route path="db/:profile/:schema" element={<Schema />} />
        <Route path="db/:profile/:schema/:table" element={<Table />} />
        <Route path="runs" element={<RunsList />} />
        <Route path="runs/:runId" element={<RunDetail />} />
        <Route path="ask" element={<Ask />} />
        <Route path="pending" element={<Pending />} />
        <Route path="settings" element={<Settings />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  );
}
