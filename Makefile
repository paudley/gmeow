# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help
.SUFFIXES:

UV ?= uv
PYTHON ?= python
GO ?= go
GMEOW ?= $(GO) run ./cmd/gmeow
GMEOW_ADMIN ?= $(GO) run ./cmd/gmeow-admin
CONFIG ?= gmeow.toml

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

.PHONY: help advice install update lock doctor check quick-check test compile type-check build clean go-format go-vet go-test go-build go-check python-intel-test python-intel-build release-check release-audit status submodules ethos-install

help: ## Show this help screen.
	@printf '\n$(BOLD)Gmeow$(RESET) $(DIM)local Gmail MCP/REST intelligence server$(RESET)\n\n'
	@printf '$(BOLD)Usage$(RESET)\n'
	@printf '  make <target> [CONFIG=%s]\n\n' "$(CONFIG)"
	@printf '$(BOLD)Setup$(RESET)\n'
	@awk 'BEGIN{FS=":.*##"; section=""} /^[a-zA-Z0-9_.-]+:.*##/ { \
		if ($$1 ~ /^(advice|install|update|lock|doctor|submodules|ethos-install)$$/) printf "  $(GREEN)%-24s$(RESET) %s\n", $$1, $$2 \
	}' $(MAKEFILE_LIST)
	@printf '\n$(BOLD)Development Checks$(RESET)\n'
	@awk 'BEGIN{FS=":.*##"} /^[a-zA-Z0-9_.-]+:.*##/ { \
		if ($$1 ~ /^(check|quick-check|test|compile|type-check|build|clean|go-format|go-vet|go-test|go-build|go-check|python-intel-test|python-intel-build|release-check|release-audit)$$/) printf "  $(GREEN)%-24s$(RESET) %s\n", $$1, $$2 \
	}' $(MAKEFILE_LIST)
	@printf '\n$(BOLD)Examples$(RESET)\n'
	@printf '  make install\n'
	@printf '  make advice\n'
	@printf '  make check\n'
	@printf '  GMEOW_SOPS_UNLOCK_KEY=... make status\n\n'

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
		printf '  $(YELLOW)missing$(RESET) $(CONFIG). Run: cp gmeow.toml-example $(CONFIG)\n'; \
	fi
	@if [ -f "gmeow.toml-example" ]; then \
		printf '  $(GREEN)ok$(RESET) public example config exists\n'; \
	fi
	@if [ -n "$${GMEOW_SOPS_UNLOCK_KEY:-}" ] || [ -f "$$HOME/.config/gmeow/key.txt" ]; then \
		printf '  $(GREEN)ok$(RESET) SOPS unlock key source configured\n'; \
	else \
		printf '  $(YELLOW)missing$(RESET) set GMEOW_SOPS_UNLOCK_KEY or create ~/.config/gmeow/key.txt\n'; \
	fi
	@if command -v sops >/dev/null 2>&1; then \
		printf '  $(GREEN)ok$(RESET) sops: %s\n' "$$(sops --version 2>/dev/null | head -n 1)"; \
	else \
		printf '  $(YELLOW)note$(RESET) sops not found; needed for encrypted configs with referenced secrets.\n'; \
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

doctor: ## Validate Go Phase 00 config.
	$(call section,Validating Go config)
	$(GMEOW_ADMIN) --config "$(CONFIG)" config validate

compile: ## Compile Python sources.
	$(call section,Compiling Python)
	$(UV) run $(PYTHON) -m compileall main.py src migrations tests scripts

test: ## Run tests.
	$(call section,Running tests)
	$(UV) run pytest tests

type-check: ## Run Python static type checks.
	$(call section,Running Python type checks)
	$(UV) run mypy

quick-check: compile type-check test ## Run compile, type checks, and tests.

build: ## Build wheel and sdist.
	$(call section,Building package)
	$(UV) build

go-format: ## Format Go sources.
	$(call section,Formatting Go)
	$(GO) fmt ./...

go-vet: ## Run Go static analysis.
	$(call section,Running Go vet)
	$(GO) vet ./...

go-test: ## Run Go tests.
	$(call section,Running Go tests)
	$(GO) test ./...

go-build: ## Build Go binaries.
	$(call section,Building Go binaries)
	$(GO) build ./cmd/gmeow ./cmd/gmeow-admin ./cmd/gmeow-worker

go-check: go-format go-vet go-test go-build ## Run the Go Phase 00 quality gate.

python-intel-test: ## Run gmeow-intel tests.
	$(call section,Running gmeow-intel tests)
	cd python && $(UV) run pytest

python-intel-build: ## Build gmeow-intel distribution.
	$(call section,Building gmeow-intel package)
	cd python && $(UV) build

release-check: ## Run public release hygiene checks.
	$(call section,Running public release hygiene check)
	$(UV) run $(PYTHON) scripts/public_release_check.py

release-audit: compile type-check test build python-intel-test python-intel-build release-check ## Run the full local release gate.
	$(call section,Checking whitespace)
	git -c core.whitespace=blank-at-eol,blank-at-eof,space-before-tab diff --check

check: go-check release-audit ## Alias for the full local verification gate.

clean: ## Remove build/test caches and package artifacts.
	$(call section,Cleaning generated artifacts)
	rm -rf dist build .pytest_cache .ruff_cache .mypy_cache htmlcov .coverage
	find . -type d -name __pycache__ -prune -exec rm -rf {} +

status: ## Validate config and show Go Phase 00 startup status.
	$(GMEOW) --config "$(CONFIG)" status

submodules: ## Initialize/update submodules.
	git submodule update --init --recursive

ethos-install: submodules ## Run coding-ethos submodule install.
	$(MAKE) -C coding-ethos install
