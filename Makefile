# Makefile for replay-cdxj-indexing-tools
# Build automation for development, testing, and deployment workflows

# Default Python version
PYTHON ?= python3
VENV_DIR ?= venv
VENV_BIN = $(VENV_DIR)/bin

help: ## Show this help message
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
.PHONY: help

venv: ## Create virtual environment
	@echo "Creating virtual environment with $(PYTHON)..."
	$(PYTHON) -m venv $(VENV_DIR)
	@echo "✓ Virtual environment created at $(VENV_DIR)"
	@echo ""
	@echo "To activate:"
	@echo "  source $(VENV_BIN)/activate"
.PHONY: venv

install: venv ## Install package in development mode
	@echo "Installing package in development mode..."
	$(VENV_BIN)/pip install --upgrade pip
	$(VENV_BIN)/pip install -e .
	@echo "✓ Package installed"
	@echo ""
	@echo "Available commands:"
	@echo "  $(VENV_BIN)/merge-cdxj"
	@echo "  $(VENV_BIN)/filter-blocklist"
	@echo "  $(VENV_BIN)/filter-excessive-urls"
	@echo "  $(VENV_BIN)/cdxj-to-zipnum"
	@echo "  $(VENV_BIN)/cdxj-index-collection"
.PHONY: install

install-dev: venv ## Install package with development dependencies
	@echo "Installing package with development dependencies..."
	$(VENV_BIN)/pip install --upgrade pip
	$(VENV_BIN)/pip install -e .
	$(VENV_BIN)/pip install pytest pytest-cov pylint flake8 black mypy
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
	$(VENV_BIN)/pytest tests/ -v
.PHONY: test-python

test-shell: ## Run shell script tests only
	@echo "Running shell script tests..."
	bash tests/test_process_collection_simple.sh
.PHONY: test-shell

test-coverage: ## Run tests with HTML coverage report
	@echo "Running tests with coverage..."
	$(VENV_BIN)/pytest tests/ --cov=replay_cdxj_indexing_tools --cov-report=html --cov-report=term -v
	@echo ""
	@echo "Coverage report generated in htmlcov/index.html"
.PHONY: test-coverage

lint: ## Run code quality checks (flake8, pylint, mypy)
	@echo "Running code quality checks..."
	@echo "→ flake8..."
	$(VENV_BIN)/flake8 replay_cdxj_indexing_tools/ tests/
	@echo "→ pylint..."
	$(VENV_BIN)/pylint replay_cdxj_indexing_tools/
	@echo "→ mypy..."
	$(VENV_BIN)/mypy
	@echo "✓ Lint checks complete"
.PHONY: lint

format: ## Format code with black
	@echo "Formatting code with black..."
	$(VENV_BIN)/black replay_cdxj_indexing_tools/ tests/ --line-length=100
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
