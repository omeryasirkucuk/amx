# Changelog

This file is the **public, high-signal** changelog for AMX (what contributors/users should expect on GitHub).

For day-to-day development notes (longer, more granular), use `CHANGELOG.local.md` in your checkout (gitignored).

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
