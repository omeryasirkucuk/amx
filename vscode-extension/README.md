# AMX for VS Code

Manage AMX — database metadata, AI-generated descriptions, runs,
lineage, and documentation — without leaving VS Code.

## Features

- **Activity Bar views** — browse DB/LLM/docs/code profiles, the
  indexed catalog (schemas → tables → columns with descriptions), run
  history, and schedules.
- **Studio panels** — Ask chat, run launch and monitoring, the lineage
  canvas, documentation pages, and settings open as editor panels
  backed by the local AMX Studio server.
- **SQL editor intelligence** — hover a table or column name in a SQL
  file to see its catalog description; completion for table/column
  names; CodeLens actions to open an asset in Studio or generate a
  description; optional documentation-coverage diagnostics.
- **REPL bridge** — `AMX: Open REPL` starts the interactive `amx`
  session in the integrated terminal.
- **Zero-setup runtime** — the extension finds an existing AMX
  installation (interpreter or `amx` binary) or, with your consent,
  installs `amx-cli` into a managed virtual environment. Either way it
  shares the same `~/.amx` configuration and history as the CLI.

## Requirements

- Python 3.10+ (only when AMX is not already installed).
- AMX `>= 0.19` for embedded Studio panels (earlier servers work for
  trees and editor features, but panels need the embedded host mode).

## How it connects

The extension reuses a Studio server already running on this machine
(launched from the REPL via `/studio`) when one is found, or starts
its own headless instance bound to `127.0.0.1` with a per-session
bearer token. Stop/restart it any time via `AMX: Stop Studio Server`
/ `AMX: Restart Studio Server`; logs are in the "AMX Studio" output
channel.

## Settings

See the `amx.*` section in Settings for runtime paths, server
behavior, catalog cache TTL, and the `amx.editor.*` group controlling
hover/completion/CodeLens/diagnostics.

## Development

```
cd vscode-extension
npm install
npm run build        # typecheck + bundle
npm test             # vitest unit suite
npm run package      # production VSIX
```

Launch the "Run Extension" configuration in VS Code to start an
Extension Development Host.
