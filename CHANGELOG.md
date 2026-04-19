# Changelog

This file is the **public, high-signal** changelog for AMX (what contributors/users should expect on GitHub).

For day-to-day development notes (longer, more granular), use `CHANGELOG.local.md` in your checkout (gitignored).

## [0.1.2] — 2026-04-28

### Fixed
- Interactive session: reduce duplicated `amx>` prompt spam during terminal resize by routing command output through prompt-toolkit while the session UI is active.

### Added
- Interactive session: `Esc` on an empty line returns to the root namespace (similar “go back” ergonomics to agent CLIs).

### Changed
- Changelog workflow: keep detailed history locally in `CHANGELOG.local.md` (ignored by git) while keeping this public changelog updated for releases.
