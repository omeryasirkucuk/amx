# Changelog

This file is the **public, high-signal** changelog for AMX (what contributors/users should expect on GitHub).

For day-to-day development notes (longer, more granular), use `CHANGELOG.local.md` in your checkout (gitignored).

## [0.1.7] — 2026-04-20

### Removed
- **`amx db load` / `/load`**: AMX is scoped to metadata extraction only; CSV bulk loading was removed (use your own import tools).

### Added
- **Named profiles** for LLMs, document roots, and codebases in `~/.amx/config.yml`, with session commands: `/llm-profiles`, `/use-llm`, `/add-llm-profile`, `/remove-llm-profile`, `/doc-profiles`, `/use-doc`, `/add-doc-profile`, `/remove-doc-profile`, `/code-profiles`, `/use-code`, `/add-code-profile`, `/remove-code-profile`.
- **Friendlier `analyze codebase`**: optional `--schema` (defaults to session `/schema`); flags like `--sap_s6p` are rewritten to `--schema sap_s6p` for both the interactive shell and the `amx` CLI entrypoint.

### Fixed
- **LiteLLM model id**: OpenAI (and other) models without a slash now get the correct provider prefix (e.g. `gpt-4o` → `openai/gpt-4o`) so provider detection no longer fails.

## [0.1.6] — 2026-04-20

### Fixed
- **Raw ANSI escape codes** (`?[1;35m…`) no longer appear on session start — removed `patch_stdout()` entirely and use the standard Rich `console` for all output between prompts.
- **Ghost `amx>` lines on terminal resize** eliminated — output now happens strictly *between* `PromptSession.prompt()` calls, so prompt-toolkit no longer redraws stale prompt lines when the terminal is resized.
- Simplified internal architecture: removed `_interactive_console`, `_ipt_*` helpers, and `patch_stdout` dependency; all session output uses the shared `console` from `amx.utils.console`.

## [0.1.5] — 2026-04-28

### Fixed
- **Interactive session rendering**: prevent raw ANSI fragments like `?[1;35m` showing up in Terminal.app by using a Rich `Console(force_terminal=True)` bound to the patched stdout during interactive sessions.

## [0.1.4] — 2026-04-28

### Fixed
- **macOS Terminal.app resize**: reduce duplicated `amx>` “ghost prompts” by keeping Rich output and `PromptSession` under the same `patch_stdout()` for the whole interactive session (and disabling mouse reporting on the prompt).

## [0.1.3] — 2026-04-28

### Fixed
- **Compatibility**: import `HTML` from `prompt_toolkit.formatted_text` (older prompt-toolkit releases don’t provide `prompt_toolkit.formatted_html`).

## [0.1.2] — 2026-04-28

### Fixed
- Interactive session: reduce duplicated `amx>` prompt spam during terminal resize by routing command output through prompt-toolkit while the session UI is active.

### Added
- Interactive session: `Esc` on an empty line returns to the root namespace (similar “go back” ergonomics to agent CLIs).

### Changed
- Changelog workflow: keep detailed history locally in `CHANGELOG.local.md` (ignored by git) while keeping this public changelog updated for releases.
