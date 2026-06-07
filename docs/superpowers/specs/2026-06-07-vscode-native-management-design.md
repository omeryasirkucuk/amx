# VS Code Extension: Full-Native Management

## Problem

The extension's first release is read-heavy: the Activity Bar trees
list profiles, catalog assets, runs, and schedules, and the Studio
panels render them — but management lives elsewhere. Right-click
menus were never contributed to `package.json` (the tree items carry
`contextValue`s that nothing targets), profile CRUD requires the
Studio Settings panel, runs cannot be started or cancelled from the
trees, and schedules cannot be created. Monitoring works; managing
does not.

## Goal

Every management operation runs natively inside VS Code through
step-by-step wizards — the editor counterpart of the REPL's
wizard-first command philosophy. No flow requires the Studio iframe
(panels remain available as a richer alternative, never a
requirement).

Scope (all four areas, one release):

1. Profile CRUD + test + activate (DB / LLM / Docs / Code)
2. Catalog operations: sync, deep sync, description edit/generate
3. Run lifecycle: start (scoped), cancel, rerun, live progress
4. Schedule CRUD on top of the existing pause/resume/run-now

## Key enabler: server-provided form schemas

`GET /api/profiles/db/backends` returns, per backend, a complete
machine-readable `field_specs` array: `{name, kind: text|int|password|
select|bool, label, help, secret, required, group: basic|advanced,
options}`. `GET /api/profiles/llm/providers` carries `needs_key` /
`needs_base`. The wizards are therefore **data-driven**: one generic
engine renders any backend, and a new backend added server-side needs
zero extension changes.

## Architecture

New `vscode-extension/src/management/` package, one module per
domain, all consuming the existing service layer (`AmxClient`,
`CatalogCache`, `ServerManager`) and registered from
`management/index.ts` (called by `commands/index.ts`).

### wizard.ts — generic wizard engine

- Pure state machine: `WizardStep[]` definitions + `runWizard(steps,
  port)`; the `PromptPort` interface abstracts `showQuickPick` /
  `showInputBox` so unit tests inject a scripted port — no VS Code UI
  mocking.
- Renders `field_specs` directly: `select` → QuickPick with options,
  `password`/`secret` → InputBox `{password: true}`, `int` →
  validated InputBox, `bool` → Yes/No QuickPick, `help` →
  placeholder/prompt text.
- `required` enforcement, default values (e.g. `default_port`),
  Back navigation via QuickPick buttons, Esc at any step = clean
  abort (nothing persisted).
- Basic fields always; advanced fields behind one "Configure
  advanced options?" gate, mirroring the REPL wizard.

### profiles.ts

- `amx.profiles.add` (per kind): backend/provider picker → name
  (validated unique) → basic field walk → advanced gate → `PUT
  /api/profiles/{kind}/{name}` → offer "Test connection" → offer
  "Set active".
- `amx.profiles.edit`: fetch masked detail, multi-select of fields
  to change, walk only those, PUT the patch. Secrets re-entered only
  when explicitly selected (masked values are never echoed back).
- `amx.profiles.delete` (confirm dialog), `amx.profiles.test`
  (progress + classified error surface), existing activate commands
  move onto the context menu.
- Secrets travel once over loopback to the server, which stores them
  in the OS keyring — the extension never persists them.

### catalogOps.ts

- Profile node: `amx.catalog.sync` / `amx.catalog.deepSync` — `POST
  /api/catalog/sync` (scoped to the node's profile), progress
  notification, `CatalogCache.invalidate()` on completion.
- Table/column node: `amx.catalog.editDescription` — InputBox
  prefilled with the current effective description → "Apply to
  catalog" (`POST /api/comments/local`) or "Apply to database"
  (`PUT /api/comments/...`), sharing the apply helper with the
  editor's generate flow; `amx.generateDescription` reused as-is;
  `amx.catalog.analyzeTable` opens the run wizard with the scope
  prefilled; `amx.catalog.copyName` copies `schema.table[.column]`.

### runs.ts

- `amx.runs.start`: profile → schema (from `CatalogCache`) → tables
  multi-pick (`canPickMany`, "all tables" shortcut) → confirmation
  summary → `POST /api/runs` with `{scope, db_profile, database,
  catalog}` → `withProgress` notification subscribed to
  `/api/runs/{job_id}/events` (SSE), reporting step labels; terminal
  event refreshes the History tree and offers "Open Run".
- History context menu: Open (existing deep link), `amx.runs.rerun`
  (rerun endpoint), `amx.runs.cancel` (`POST
  /api/runs/{live_job_id}/cancel`) — Cancel only on rows whose
  `live_job_id` is set, via an `amx.run.running` contextValue.

### schedules.ts

- `amx.schedules.create`: name → kind (`analyze` | `cache_refresh`)
  → trigger (`time` with `fire_at_local` + IANA tz, or `change`, or
  recurring via `cron_expr`) → db_profile → scope (reuses the run
  wizard's scope picker) → llm_profile → `POST /api/schedules`.
- `amx.schedules.edit` (field picker → PATCH), `amx.schedules.delete`
  (confirm → DELETE). Existing pause/resume/run-now become context
  menu entries instead of the click-through action picker.

### package.json contributions

- ~22 new commands (category AMX), `view/item/context` menu entries
  keyed on the existing `contextValue`s (`amx.dbProfile`,
  `amx.llmProfile`, `amx.docsProfile`, `amx.codeProfile`,
  `amx.catalogProfile`, `amx.catalogSchema`, `amx.catalogTable`,
  `amx.catalogColumn`, `amx.run` / `amx.run.running`,
  `amx.schedule`), inline icons for the highest-frequency actions
  (`+` Add Profile on group roots, sync on catalog profile nodes).
- Trees gain the new contextValues where missing (profiles group
  roots, schedule rows).

## Error handling

- `AmxApiError` `detail` + `hint` surfaced in every failure toast.
- Wizard abort (Esc) at any step discards all collected state.
- Server death mid-wizard: the submitting step fails with the
  classified error and offers Retry without losing answers.

## Testing

- Unit (vitest): wizard engine — step sequencing, validation, Back
  navigation, abort, field_specs rendering — via a scripted
  `PromptPort`; contract fixtures for `/db/backends` and
  `/llm/providers` payloads.
- Integration: fake Studio grows PUT/POST/DELETE routes (profiles,
  sync, runs, schedules) recording received bodies; the suite drives
  an end-to-end add-profile wizard through an injected scripted port
  and asserts the recorded PUT body; command-registration assertions
  for every new command.
- Manual: the verify checklist gains a management pass (add → test →
  activate → sync → run → cancel → schedule).

## Constraints

- Local development only: no commits to main, no push, no CI, no
  deploy until explicit user approval (the release step bundles the
  pending webview/CSP fixes with this work).
- English-only strings; cross-platform; no new runtime npm deps.
