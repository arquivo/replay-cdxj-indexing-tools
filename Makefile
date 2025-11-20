# Makefile for replay-cdxj-indexing-tools
# Build automation for development, testing, and deployment workflows

# Default Python version
PYTHON ?= python3
VENV_DIR ?= venv
SKIP_VENV ?= 0

# Export SKIP_VENV so it's available in sub-make calls
export SKIP_VENV

ifeq ($(SKIP_VENV),1)
    VENV_BIN =
    PIP = pip
    PYTEST = pytest
    FLAKE8 = flake8
    PYLINT = pylint
    MYPY = mypy
    BLACK = black
else
    VENV_BIN = $(VENV_DIR)/bin
    PIP = $(VENV_BIN)/pip
    PYTEST = $(VENV_BIN)/pytest
    FLAKE8 = $(VENV_BIN)/flake8
    PYLINT = $(VENV_BIN)/pylint
    MYPY = $(VENV_BIN)/mypy
    BLACK = $(VENV_BIN)/black
endif

help: ## Show this help message
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
.PHONY: help

venv: ## Create virtual environment
ifneq ($(SKIP_VENV),1)
	@echo "Creating virtual environment with $(PYTHON)..."
	$(PYTHON) -m venv $(VENV_DIR)
	@echo "✓ Virtual environment created at $(VENV_DIR)"
	@echo ""
	@echo "To activate:"
	@echo "  source $(VENV_BIN)/activate"
else
	@echo "Skipping virtual environment creation (SKIP_VENV=1)"
endif
.PHONY: venv

install: venv ## Install package in development mode
	@echo "Installing package in development mode..."
	$(PIP) install --upgrade pip
	$(PIP) install -e .
	@echo "✓ Package installed"
ifneq ($(SKIP_VENV),1)
	@echo ""
	@echo "Available commands:"
	@echo "  $(VENV_BIN)/merge-flat-cdxj"
	@echo "  $(VENV_BIN)/filter-blocklist"
	@echo "  $(VENV_BIN)/filter-excessive-urls"
	@echo "  $(VENV_BIN)/flat-cdxj-to-zipnum"
	@echo "  $(VENV_BIN)/cdxj-index-collection"
endif
.PHONY: install

install-dev: venv ## Install package with development dependencies
	@echo "Installing package with development dependencies..."
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"
	@echo "✓ Development environment ready"
.PHONY: install-dev

test: test-python test-shell ## Run all tests (Python + shell)
	@echo ""
	@echo "=========================================="
	@echo "✓ All tests passed!"
	@echo "=========================================="
.PHONY: test

test-python: ## Run Python tests only
	@echo "Running Python tests..."
	$(PYTEST) tests/ -v
.PHONY: test-python

test-shell: ## Run shell script tests only
	@echo "Running shell script tests..."
	bash tests/test_process_collection_simple.sh
.PHONY: test-shell

test-coverage: ## Run tests with HTML coverage report
	@echo "Running tests with coverage..."
	$(PYTEST) tests/ --cov=replay_cdxj_indexing_tools --cov-report=html --cov-report=term -v
	@echo ""
	@echo "Coverage report generated in htmlcov/index.html"
.PHONY: test-coverage

lint: ## Run code quality checks (flake8, pylint, mypy)
	@echo "Running code quality checks..."
	@echo "→ flake8..."
	$(FLAKE8) replay_cdxj_indexing_tools/ tests/
	@echo "→ pylint..."
	$(PYLINT) replay_cdxj_indexing_tools/
	@echo "→ mypy..."
	$(MYPY)
	@echo "✓ Lint checks complete"
.PHONY: lint

format: ## Format code with black
	@echo "Formatting code with black..."
	$(BLACK) replay_cdxj_indexing_tools/ tests/ --line-length=100
	@echo "✓ Code formatted"
.PHONY: format

benchmark: ## Run performance benchmarks
	@echo "Running performance benchmarks..."
	@echo ""
	@echo "1. Blocklist performance (current setup):"
	$(VENV_BIN)/python benchmark_blocklist.py
	@echo ""
	@echo "2. TB-scale stress test:"
	$(VENV_BIN)/python benchmark_blocklist_scale.py
	@echo ""
	@echo "3. Parallel filter comparison:"
	$(VENV_BIN)/python benchmark_parallel_filter.py
.PHONY: benchmark

clean: ## Remove build artifacts and cache files
	@echo "Cleaning build artifacts and cache files..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name ".coverage" -delete
	rm -rf build/ dist/ htmlcov/ .coverage coverage.xml
	@echo "✓ Clean complete"
.PHONY: clean

distclean: clean ## Remove everything including venv
	@echo "Removing virtual environment..."
	rm -rf $(VENV_DIR)
	@echo "✓ Distribution clean complete"
.PHONY: distclean

# Quick development workflow targets
dev: install-dev test ## Quick: setup dev environment and run tests
	@echo ""
	@echo "✓ Development environment ready and tested"
.PHONY: dev

check: lint test ## Run lint and tests
	@echo ""
	@echo "✓ All checks passed"
.PHONY: check

# CI simulation
ci: install-dev test-coverage lint ## Simulate CI checks
	@echo ""
	@echo "✓ CI checks complete"
.PHONY: ci

# Docker-based multi-version testing
DOCKER_RUN = docker run --rm -v $(PWD):/app -w /app

# Helper function to run CI on a specific Python version
define run_ci_version
	@echo "Running CI checks with Python $(1)..."
	@LOG=$$(mktemp); \
	if $(DOCKER_RUN) python:$(1) bash -c "make ci SKIP_VENV=1" > $$LOG 2>&1; then \
		echo "✓ CI checks complete (Python $(1))"; \
		rm -f $$LOG; \
	else \
		echo "✗ CI checks failed (Python $(1))"; \
		echo "Log output:"; \
		cat $$LOG; \
		rm -f $$LOG; \
		exit 1; \
	fi
endef

ci-py38: ## Run full CI checks in Python 3.8 container
	$(call run_ci_version,3.8)
.PHONY: ci-py38

ci-py39: ## Run full CI checks in Python 3.9 container
	$(call run_ci_version,3.9)
.PHONY: ci-py39

ci-py310: ## Run full CI checks in Python 3.10 container
	$(call run_ci_version,3.10)
.PHONY: ci-py310

ci-py311: ## Run full CI checks in Python 3.11 container
	$(call run_ci_version,3.11)
.PHONY: ci-py311

ci-py312: ## Run full CI checks in Python 3.12 container
	$(call run_ci_version,3.12)
.PHONY: ci-py312

ci-all: ci-py38 ci-py39 ci-py310 ci-py311 ci-py312 ## Run full CI checks on all Python versions
	@echo ""
	@echo "=========================================="
	@echo "✓ All Python versions passed CI checks!"
	@echo "=========================================="
.PHONY: ci-all
