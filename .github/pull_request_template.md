<!--
Thanks for contributing to AMX!

Title format: use Conventional Commits (feat:, fix:, docs:, refactor:, test:, chore:, perf:).
The release tooling derives the next version from these prefixes.
-->

## Summary

<!-- One or two sentences on what changed and why. Link the issue if there is one. -->

## Test plan

<!-- How did you verify this works? Commands run, scenarios exercised, screenshots if UI/CLI output changed. -->

- [ ] `pytest -ra`
- [ ] `ruff check amx tests` and `ruff format --check amx tests`
- [ ] Manual verification (describe)

## Risk

<!--
Pick one and add a sentence:
- Low: localised change, covered by tests, no schema/config impact
- Medium: touches user-facing flow or DB connector; manual check recommended
- High: changes secret handling, config schema, release flow, or public API; needs careful review
-->

## Checklist

- [ ] Conventional Commit title
- [ ] Updated docs / README if behaviour changed
- [ ] No secrets, API keys, or sample credentials added to the repo
- [ ] No direct push to `main` (this PR is the path)
