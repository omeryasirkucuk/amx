# Convenience targets for AMX contributors.
# CI runs the same commands directly in workflow YAML; this file is
# strictly developer ergonomics.

.PHONY: help install test lint format type web-build web-dev web-clean \
        perf-install perf-baseline perf-compare perf-clean

help:
	@echo "AMX make targets:"
	@echo "  make install      Editable install with all dev extras."
	@echo "  make test         Run pytest -q."
	@echo "  make lint         Run ruff check + format --check."
	@echo "  make format       Apply ruff format."
	@echo "  make type         Run mypy."
	@echo "  make web-build    Build the /studio SPA into amx/web/static/."
	@echo "  make web-dev      Run uvicorn + Vite dev server side-by-side."
	@echo "  make web-clean    Wipe amx/web/static/ (will need rebuild)."
	@echo "  make perf-install Install the optional [perf] extra."
	@echo "  make perf-baseline Capture a benchmark baseline JSON."
	@echo "  make perf-compare  Compare current benchmarks against the latest baseline."
	@echo "  make perf-clean    Remove all locally captured benchmark JSON files."

install:
	pip install -e ".[dev]"

test:
	pytest -q

lint:
	ruff check amx tests
	ruff format --check amx tests

format:
	ruff format amx tests

type:
	mypy amx

# ── /studio web UI ───────────────────────────────────────────────────
# The Vite project under frontend/ is repo-only — it never ships in
# the wheel. ``web-build`` produces the dist files, drops them into
# amx/web/static/, and that vendored output IS in the wheel.

web-build:
	@if [ ! -d frontend ]; then \
	  echo "frontend/ doesn't exist yet — PR-B initialises the Vite project."; \
	  exit 0; \
	fi
	cd frontend && npm ci && npm run build

web-dev:
	@if [ ! -d frontend ]; then \
	  echo "frontend/ doesn't exist yet — PR-B initialises the Vite project."; \
	  exit 1; \
	fi
	@echo "Run AMX Studio in dev mode in two terminals:"
	@echo "  Terminal A: cd frontend && npm run dev"
	@echo "  Terminal B: amx /studio --no-open"
	@echo "Then point your browser at http://127.0.0.1:5173/?t=<token>"
	@echo "(Vite proxies /api/* to the uvicorn backend.)"

web-clean:
	@if [ -d amx/web/static ]; then \
	  find amx/web/static -mindepth 1 -not -name 'index.html' -delete; \
	  echo "Wiped amx/web/static/ (kept index.html placeholder)."; \
	fi

# ── Performance benchmarks (opt-in) ──────────────────────────────────
# The ``perf`` extras and tests/perf/ tree are not part of the wheel
# and never run in the default ``pytest`` invocation. They exist so
# contributors can capture before/after deltas when working on a hot
# path (FK resolution, embedding rebuild, profile_table, etc.).

PERF_BASELINE_DIR ?= tests/perf/baselines

perf-install:
	pip install -e ".[perf]"

perf-baseline:
	@mkdir -p $(PERF_BASELINE_DIR)
	pytest tests/perf -m perf \
	  --benchmark-only \
	  --benchmark-json=$(PERF_BASELINE_DIR)/$(shell date -u +%Y%m%dT%H%M%SZ).json
	@echo "Baseline written to $(PERF_BASELINE_DIR)/."

perf-compare:
	@latest=$$(ls -1t $(PERF_BASELINE_DIR)/*.json 2>/dev/null | head -1); \
	if [ -z "$$latest" ]; then \
	  echo "No baseline JSON in $(PERF_BASELINE_DIR)/. Run 'make perf-baseline' first."; \
	  exit 1; \
	fi; \
	echo "Comparing current run against $$latest"; \
	pytest tests/perf -m perf --benchmark-only --benchmark-compare="$$latest"

perf-clean:
	@rm -f $(PERF_BASELINE_DIR)/*.json
	@echo "Removed baseline JSON files in $(PERF_BASELINE_DIR)/."
