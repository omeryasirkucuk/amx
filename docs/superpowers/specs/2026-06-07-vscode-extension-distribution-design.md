# VS Code Extension Distribution Through AMX

## Problem

The VS Code extension exists only as a locally built VSIX. Publishing
to the Marketplace requires a publisher account and adds a release
surface; worse, an independently versioned extension can drift from
the installed amx-cli (the embedded-mode capability skew already
bit once). A from-scratch user has no installation path.

## Decision

Distribute the extension THROUGH AMX itself, mirroring the existing
MCP integration: a wizard-first `/vscode` CLI command and a Studio
Settings tab install the VSIX that ships INSIDE the amx-cli wheel.
No Marketplace, no publisher account. Version lockstep is structural:
the VSIX in the wheel always matches the server it talks to.

## Design

### Packaging (vendored VSIX)

- `amx/assets/vsix/amx-vscode.vsix` committed to the repo (same
  philosophy as the vendored `amx/web/static` SPA: end users need no
  Node). ~64 KB.
- `pyproject.toml` package-data: `"amx.assets" = ["vsix/*.vsix"]`.
- `make vsix` target: builds the extension (`npm ci && npm run
  package` in vscode-extension/), stamping `package.json`'s version
  to the current `amx.__version__` before `vsce package`, then copies
  the VSIX to `amx/assets/vsix/amx-vscode.vsix`.
- CI freshness check (like the web-bundle gate): when
  `vscode-extension/**` changes, the committed VSIX must have been
  rebuilt — enforced by comparing the VSIX's embedded
  `extension/package.json` content hash against a fresh build, or
  pragmatically: the vscode-extension CI job runs `make vsix` and
  fails on `git diff --quiet amx/assets/vsix`.

### Resolver module — `amx/vscode_ext/`

- `installer.py` (pure, testable):
  - `bundled_vsix_path() -> Path` via `importlib.resources`.
  - `discover_editors() -> list[EditorInfo]` — candidates per OS:
    PATH lookups (`code`, `code-insiders`, `cursor`, `windsurf`,
    `codium`), macOS app bundles
    (`/Applications/<App>.app/Contents/Resources/app/bin/<cli>`),
    Windows `%LOCALAPPDATA%/Programs/...` cmd shims, Linux
    `/usr/bin`, `/usr/share/.../bin`, snap. `EditorInfo = {id,
    label, cli_path}`.
  - `extension_status(editor) -> {installed: bool, version: str|None}`
    via `<cli> --list-extensions --show-versions` (greps
    `amx.amx-vscode@`).
  - `install(editor) -> InstallResult` via `<cli>
    --install-extension <vsix> --force`; `uninstall(editor)` via
    `--uninstall-extension amx.amx-vscode`.
  - All subprocess calls use explicit argv lists, timeouts, and
    capture stderr for classified errors (cross-platform rule).

### CLI — `/vscode` (llm namespace, next to /mcp)

- Registered in `slash_commands.py` under `llm` ("IDE integration"
  group): short_desc "Install the AMX VS Code extension into your
  editor"; long_desc lists subcommands.
- `amx/cli_support/commands/vscode_ext.py` mirroring mcp.py:
  - bare `/vscode` → wizard: pick detected editor → install →
    status summary. Zero detected editors → print the VSIX path +
    manual "Install from VSIX…" instructions.
  - `/vscode install [editor]`, `/vscode status`,
    `/vscode uninstall [editor]` as optional power-user shortcuts
    (wizard-first rule).

### Studio — Settings → "VS Code" tab + API

- `amx/web/routers/vscode_ext.py` (`/api/vscode`):
  - `GET /api/vscode/status` → `{editors: [{id, label, cli_path,
    installed, version}], bundled_version}`.
  - `POST /api/vscode/install` body `{editor}` → runs install,
    returns refreshed status (classified errors on failure).
  - `POST /api/vscode/uninstall` body `{editor}`.
  - `GET /api/vscode/vsix` → serves the bundled VSIX
    (FileResponse, auth required) for manual installs.
- Frontend: new `Tab = "vscode"` in Settings.tsx with a card per
  detected editor (label, CLI path, installed version vs bundled
  version, Install/Reinstall/Uninstall buttons), an "out of date"
  badge when installed != bundled, and a Download .vsix link.
  Responsive per house style.

### Extension-side niceties (vscode-extension/)

- Version is stamped at build time from `amx.__version__` (no manual
  bumps). The extension already detects server version via
  `/api/version`; when the SERVER is newer than the extension, show
  a one-time hint: "AMX was upgraded — run /vscode install to update
  the editor extension."

## Error handling

- No editor CLI found → wizard prints VSIX path + manual steps;
  Studio shows the Download link prominently.
- Install subprocess failure → stderr tail surfaced via the
  classified `{message, hint}` error shape (CLI + API).

## Testing

- Python unit tests: editor discovery with monkeypatched PATH/exists,
  status parsing fixtures, install argv construction (no real
  subprocess), bundled path resolution; router tests with a stubbed
  installer (status shape, install happy/error paths, vsix download
  auth + content-type).
- Frontend: Settings tab renders status fixtures (existing vitest
  setup in frontend/ if present; otherwise rely on tsc + lint).
- Manual: /vscode wizard on this machine installs into VS Code.

## Constraints

- All four house surfaces: English-only, cross-platform, wizard-first
  CLI (no new top-level Click group — `/vscode` registers like /mcp),
  schema-descriptions rule untouched (no new tables).
- Local only until release approval. The Studio tab changes the
  frontend bundle → release flow is deploy.sh → PR → merge.
