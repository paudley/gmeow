# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help
.SUFFIXES:

UV ?= uv
PYTHON ?= python
GO ?= go
GMEOW ?= $(UV) run gmeow
CONFIG ?= config.toml
HOST ?= 127.0.0.1
PORT ?= 8765
IMAP_HOST ?= 127.0.0.1
IMAP_PORT ?= 1143
PROJECT_ID ?=
LIMIT ?= 25
EXPORT_DIR ?= dist/archive-export

empty :=
space := $(empty) $(empty)

ifneq ($(strip $(TERM)),dumb)
BOLD := \033[1m
DIM := \033[2m
RESET := \033[0m
BLUE := \033[38;5;39m
GREEN := \033[38;5;42m
YELLOW := \033[38;5;214m
RED := \033[38;5;203m
else
BOLD :=
DIM :=
RESET :=
BLUE :=
GREEN :=
YELLOW :=
RED :=
endif

define section
	@printf '$(BLUE)==>$(RESET) $(BOLD)%s$(RESET)\n' "$(1)"
endef

define note
	@printf '  $(GREEN)•$(RESET) %s\n' "$(1)"
endef

define warn
	@printf '  $(YELLOW)!$(RESET) %s\n' "$(1)"
endef

.PHONY: help advice install update lock doctor check quick-check test compile build clean go-format go-test go-build go-check release-check release-audit \
	serve serve-public serve-imap status sync sync-history maintenance resilience events jobs dead-letter retry-dead repair-cache \
	storage analyze prune-objects sidecars refresh-search refresh-graph refresh-derived intelligence-enqueue intelligence-worker intelligence-rebuild \
	categories-seed categories-discover categories-recategorize categories-stats archive-status archive-complete archive-verify archive-refresh archive-export archive-verify-export archive-restore retention-policies imap-status imap-refresh provision-plan submodules ethos-install

help: ## Show this help screen.
	@printf '\n$(BOLD)Gmeow$(RESET) $(DIM)local Gmail MCP/REST intelligence server$(RESET)\n\n'
	@printf '$(BOLD)Usage$(RESET)\n'
	@printf '  make <target> [CONFIG=%s] [HOST=%s] [PORT=%s]\n\n' "$(CONFIG)" "$(HOST)" "$(PORT)"
	@printf '$(BOLD)Setup$(RESET)\n'
	@awk 'BEGIN{FS=":.*##"; section=""} /^[a-zA-Z0-9_.-]+:.*##/ { \
		if ($$1 ~ /^(advice|install|update|lock|doctor|submodules|ethos-install)$$/) printf "  $(GREEN)%-24s$(RESET) %s\n", $$1, $$2 \
	}' $(MAKEFILE_LIST)
	@printf '\n$(BOLD)Development Checks$(RESET)\n'
	@awk 'BEGIN{FS=":.*##"} /^[a-zA-Z0-9_.-]+:.*##/ { \
		if ($$1 ~ /^(check|quick-check|test|compile|build|clean|go-format|go-test|go-build|go-check|release-check|release-audit)$$/) printf "  $(GREEN)%-24s$(RESET) %s\n", $$1, $$2 \
	}' $(MAKEFILE_LIST)
	@printf '\n$(BOLD)Run and Sync$(RESET)\n'
	@awk 'BEGIN{FS=":.*##"} /^[a-zA-Z0-9_.-]+:.*##/ { \
		if ($$1 ~ /^(serve|serve-public|serve-imap|status|sync|sync-history|maintenance|resilience|events|jobs|dead-letter|retry-dead|repair-cache)$$/) printf "  $(GREEN)%-24s$(RESET) %s\n", $$1, $$2 \
	}' $(MAKEFILE_LIST)
	@printf '\n$(BOLD)Storage, Intelligence, Categories, Archive$(RESET)\n'
	@awk 'BEGIN{FS=":.*##"} /^[a-zA-Z0-9_.-]+:.*##/ { \
		if ($$1 ~ /^(storage|analyze|prune-objects|sidecars|refresh-search|refresh-graph|refresh-derived|intelligence-enqueue|intelligence-worker|intelligence-rebuild|categories-seed|categories-discover|categories-recategorize|categories-stats|archive-status|archive-complete|archive-verify|archive-refresh|archive-export|archive-verify-export|archive-restore|retention-policies|imap-status|imap-refresh|provision-plan)$$/) printf "  $(GREEN)%-24s$(RESET) %s\n", $$1, $$2 \
	}' $(MAKEFILE_LIST)
	@printf '\n$(BOLD)Examples$(RESET)\n'
	@printf '  make install\n'
	@printf '  make serve PORT=8765\n'
	@printf '  make sync-history LIMIT=500\n'
	@printf '  make archive-complete LIMIT=25\n'
	@printf '  make provision-plan PROJECT_ID=my-gcp-project\n\n'

advice: ## Print setup advice and current local readiness.
	$(call section,Setup advice)
	@if ! command -v $(UV) >/dev/null 2>&1; then \
		printf '  $(RED)missing$(RESET) uv. Install from https://docs.astral.sh/uv/.\n'; \
	else \
		printf '  $(GREEN)ok$(RESET) uv: %s\n' "$$($(UV) --version)"; \
	fi
	@if [ -f "$(CONFIG)" ]; then \
		printf '  $(GREEN)ok$(RESET) config: $(CONFIG)\n'; \
	else \
		printf '  $(YELLOW)missing$(RESET) $(CONFIG). Run: cp config.toml-example $(CONFIG)\n'; \
	fi
	@if [ -f "config.toml-example" ]; then \
		printf '  $(GREEN)ok$(RESET) public example config exists\n'; \
	fi
	@if [ -f "$$HOME/.config/gmeow/secrets.sops.yaml" ]; then \
		printf '  $(GREEN)ok$(RESET) SOPS secrets file: ~/.config/gmeow/secrets.sops.yaml\n'; \
	else \
		printf '  $(YELLOW)note$(RESET) no ~/.config/gmeow/secrets.sops.yaml; file-based credentials or another configured SOPS path must exist.\n'; \
	fi
	@if command -v sops >/dev/null 2>&1; then \
		printf '  $(GREEN)ok$(RESET) sops: %s\n' "$$(sops --version 2>/dev/null | head -n 1)"; \
	else \
		printf '  $(YELLOW)note$(RESET) sops not found; needed only when [gmeow.secrets] is configured.\n'; \
	fi
	@if command -v exiftool >/dev/null 2>&1; then printf '  $(GREEN)ok$(RESET) exiftool available\n'; else printf '  $(YELLOW)note$(RESET) exiftool missing; attachment metadata extraction will be reduced.\n'; fi
	@if command -v tesseract >/dev/null 2>&1; then printf '  $(GREEN)ok$(RESET) tesseract available\n'; else printf '  $(YELLOW)note$(RESET) tesseract missing; image OCR will be skipped.\n'; fi
	@if command -v pdftotext >/dev/null 2>&1; then printf '  $(GREEN)ok$(RESET) poppler pdftotext available\n'; else printf '  $(YELLOW)note$(RESET) pdftotext missing; PDF text extraction will be reduced.\n'; fi

install: ## Install/sync development dependencies with uv.
	$(call section,Syncing development environment)
	$(UV) sync --extra test

update: ## Upgrade dependencies and refresh uv.lock.
	$(call section,Updating dependencies)
	$(UV) lock --upgrade
	$(UV) sync --extra test

lock: ## Resolve uv.lock without upgrading.
	$(call section,Resolving lockfile)
	$(UV) lock

doctor: ## Check Gmail/config connectivity through the gmeow CLI.
	$(call section,Running doctor)
	$(GMEOW) --config "$(CONFIG)" doctor

compile: ## Compile Python sources.
	$(call section,Compiling Python)
	$(UV) run $(PYTHON) -m compileall main.py src migrations tests scripts

test: ## Run tests.
	$(call section,Running tests)
	$(UV) run pytest tests

quick-check: compile test ## Run compile and tests.

build: ## Build wheel and sdist.
	$(call section,Building package)
	$(UV) build

go-format: ## Format Go sources.
	$(call section,Formatting Go)
	$(GO) fmt ./...

go-test: ## Run Go tests.
	$(call section,Running Go tests)
	$(GO) test ./...

go-build: ## Build Go binaries.
	$(call section,Building Go binaries)
	$(GO) build ./cmd/gmeow ./cmd/gmeow-admin ./cmd/gmeow-worker

go-check: go-format go-test go-build ## Run the Go Phase 00 quality gate.

release-check: ## Run public release hygiene checks.
	$(call section,Running public release hygiene check)
	$(UV) run $(PYTHON) scripts/public_release_check.py

release-audit: compile test build release-check ## Run the full local release gate.
	$(call section,Checking whitespace)
	git -c core.whitespace=blank-at-eol,blank-at-eof,space-before-tab diff --check

check: go-check release-audit ## Alias for the full local verification gate.

clean: ## Remove build/test caches and package artifacts.
	$(call section,Cleaning generated artifacts)
	rm -rf dist build .pytest_cache .ruff_cache .mypy_cache htmlcov .coverage
	find . -type d -name __pycache__ -prune -exec rm -rf {} +

serve: ## Start REST/MCP server on HOST/PORT.
	$(call section,Starting Gmeow REST/MCP server)
	$(GMEOW) --config "$(CONFIG)" serve --host "$(HOST)" --port "$(PORT)"

serve-public: ## Start server on 0.0.0.0; only use behind external auth/TLS.
	$(call warn,This exposes unauthenticated local Gmail access. Use only behind a trusted auth/TLS layer.)
	$(GMEOW) --config "$(CONFIG)" serve --host "0.0.0.0" --port "$(PORT)"

serve-imap: ## Start read-only IMAP server.
	$(call section,Starting read-only IMAP server)
	$(GMEOW) --config "$(CONFIG)" serve-imap --host "$(IMAP_HOST)" --port "$(IMAP_PORT)"

status: ## Show sync/corpus status.
	$(GMEOW) --config "$(CONFIG)" status

sync: ## Run configured priority sync.
	$(GMEOW) --config "$(CONFIG)" sync

sync-history: ## Run Gmail History API sync; override LIMIT=500.
	$(GMEOW) --config "$(CONFIG)" sync-history --limit "$(LIMIT)"

maintenance: ## Show maintenance status.
	$(GMEOW) --config "$(CONFIG)" maintenance-status

resilience: ## Show resilience/self-recovery status.
	$(GMEOW) --config "$(CONFIG)" resilience-status

events: ## Show recent operational events; override LIMIT=50.
	$(GMEOW) --config "$(CONFIG)" ops-events --limit "$(LIMIT)"

jobs: ## Show intelligence jobs; optionally pass STATUS=pending.
	$(GMEOW) --config "$(CONFIG)" jobs $(if $(STATUS),--status "$(STATUS)",) --limit "$(LIMIT)"

dead-letter: ## Show dead-letter jobs.
	$(GMEOW) --config "$(CONFIG)" dead-letter --limit "$(LIMIT)"

retry-dead: ## Retry dead-letter jobs; override LIMIT=N.
	$(GMEOW) --config "$(CONFIG)" retry-dead $(if $(LIMIT),--limit "$(LIMIT)",)

repair-cache: ## Dry-run cache repair; pass APPLY=1 to mutate.
	$(GMEOW) --config "$(CONFIG)" repair-cache $(if $(APPLY),--apply,)

storage: ## Show storage diagnostics.
	$(GMEOW) --config "$(CONFIG)" storage-diagnostics

analyze: ## Run PostgreSQL analyze on storage tables.
	$(GMEOW) --config "$(CONFIG)" analyze-storage

prune-objects: ## Dry-run orphan object pruning; pass APPLY=1 to mutate.
	$(GMEOW) --config "$(CONFIG)" prune-orphan-objects $(if $(APPLY),--apply,)

sidecars: ## Refresh attachment sidecars and enqueue attachment intelligence.
	$(GMEOW) --config "$(CONFIG)" refresh-attachment-sidecars

refresh-search: ## Refresh message search columns; override LIMIT=N.
	$(GMEOW) --config "$(CONFIG)" refresh-message-search $(if $(LIMIT),--limit "$(LIMIT)",)

refresh-graph: ## Refresh graph profiles and edge stats.
	$(GMEOW) --config "$(CONFIG)" refresh-graph-profiles
	$(GMEOW) --config "$(CONFIG)" refresh-graph-edges

refresh-derived: ## Refresh derived refs, summaries, timelines, and summary views.
	$(GMEOW) --config "$(CONFIG)" refresh-content-refs
	$(GMEOW) --config "$(CONFIG)" refresh-summaries
	$(GMEOW) --config "$(CONFIG)" refresh-timelines
	$(GMEOW) --config "$(CONFIG)" refresh-summary-views

intelligence-enqueue: ## Enqueue all cached messages/attachments for intelligence work.
	$(GMEOW) --config "$(CONFIG)" enqueue-intelligence

intelligence-worker: ## Drain intelligence queue; override LIMIT=N.
	$(GMEOW) --config "$(CONFIG)" run-intelligence-worker $(if $(LIMIT),--limit "$(LIMIT)",)

intelligence-rebuild: ## Enqueue all intelligence work and drain until empty.
	$(GMEOW) --config "$(CONFIG)" rebuild-intelligence

categories-seed: ## Seed default categories/rules.
	$(GMEOW) --config "$(CONFIG)" seed-categories

categories-discover: ## Discover learned categories; override HOURS=48 LIMIT=N.
	$(GMEOW) --config "$(CONFIG)" discover-categories --since-hours "$(or $(HOURS),48)" $(if $(LIMIT),--limit "$(LIMIT)",)

categories-recategorize: ## Recalculate categories; optional HOURS=N LIMIT=N.
	$(GMEOW) --config "$(CONFIG)" recategorize $(if $(HOURS),--since-hours "$(HOURS)",) $(if $(LIMIT),--limit "$(LIMIT)",)

categories-stats: ## Show category stats.
	$(GMEOW) --config "$(CONFIG)" category-stats

archive-status: ## Show archive completeness status; override LIMIT=50.
	$(GMEOW) --config "$(CONFIG)" archive-status --limit "$(LIMIT)"

archive-complete: ## Complete archive data for cached messages; override LIMIT=25.
	$(GMEOW) --config "$(CONFIG)" complete-archive --limit "$(LIMIT)"

archive-verify: ## Verify CAS objects; override LIMIT=N.
	$(GMEOW) --config "$(CONFIG)" verify-objects $(if $(LIMIT),--limit "$(LIMIT)",)

archive-refresh: ## Refresh archive state rows; override LIMIT=N.
	$(GMEOW) --config "$(CONFIG)" refresh-archive-states $(if $(LIMIT),--limit "$(LIMIT)",)

archive-export: ## Export archive bundle; override EXPORT_DIR=dist/archive-export.
	$(GMEOW) --config "$(CONFIG)" export-archive "$(EXPORT_DIR)"

archive-verify-export: ## Verify exported archive bundle; override EXPORT_DIR=dist/archive-export.
	$(GMEOW) --config "$(CONFIG)" verify-archive "$(EXPORT_DIR)"

archive-restore: ## Dry-run restore exported archive; pass APPLY=1 to restore.
	$(GMEOW) --config "$(CONFIG)" restore-archive "$(EXPORT_DIR)" $(if $(APPLY),--apply,)

retention-policies: ## List retention policies.
	$(GMEOW) --config "$(CONFIG)" retention-policies

imap-status: ## Show read-only IMAP status.
	$(GMEOW) --config "$(CONFIG)" imap-status

imap-refresh: ## Refresh IMAP mailbox projections.
	$(GMEOW) --config "$(CONFIG)" refresh-imap

provision-plan: ## Print Google Cloud provisioning commands; requires PROJECT_ID=...
	@if [ -z "$(PROJECT_ID)" ]; then printf '$(RED)PROJECT_ID is required.$(RESET)\nUsage: make provision-plan PROJECT_ID=my-gcp-project\n' >&2; exit 2; fi
	$(GMEOW) --config "$(CONFIG)" provision-plan "$(PROJECT_ID)"

submodules: ## Initialize/update submodules.
	git submodule update --init --recursive

ethos-install: submodules ## Run coding-ethos submodule install.
	$(MAKE) -C coding-ethos install
