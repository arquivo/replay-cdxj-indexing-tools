# Installation Guide

## Requirements

- Python 3.12 or higher
- pip package manager
- Virtual environment (recommended)

## Option 1: Install from Source (Development)

Recommended for development and contributing to the project.

```bash
# Clone the repository
git clone git@github.com:arquivo/replay-cdxj-indexing-tools.git
cd replay-cdxj-indexing-tools

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in editable mode
pip install -e .

# Or install with development dependencies
pip install -e ".[dev]"
```

## Option 2: Install from Git

Install directly from GitHub:

```bash
pip install git+https://github.com/arquivo/replay-cdxj-indexing-tools.git
```

## Option 3: Install from PyPI

(When/if published to PyPI)

```bash
pip install replay-cdxj-indexing-tools
```

## Verify Installation

Check that all commands are available:

```bash
# Check merge command
merge-cdxj --help

# Check filter commands
filter-blocklist --help
filter-excessive-urls --help

# Check zipnum command
cdxj-to-zipnum --help
```

## Development Setup

For development with testing and code quality tools:

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/

# Run with coverage
pytest --cov=replay_cdxj_indexing_tools tests/

# Format code
black replay_cdxj_indexing_tools/ tests/

# Lint code
flake8 replay_cdxj_indexing_tools/ tests/

# Type checking
mypy replay_cdxj_indexing_tools/
```

## Dependencies

Core dependencies (automatically installed):
- **pywb >= 2.9.1** - Web archive replay toolkit
  - Includes warcio, surt, and other web archive tools

Development dependencies (optional):
- **pytest >= 7.0** - Testing framework
- **pytest-cov >= 4.0** - Coverage reporting
- **black >= 23.0** - Code formatter
- **flake8 >= 6.0** - Linting
- **mypy >= 1.0** - Type checking

## Troubleshooting

### Python Version Issues

If you get version errors:

```bash
# Check Python version
python3 --version

# Use specific Python version
python3.12 -m venv venv
```

### Permission Errors

On Linux/macOS, if you get permission errors:

```bash
# Don't use sudo with pip
# Use virtual environment instead
python3 -m venv venv
source venv/bin/activate
pip install -e .
```

### Import Errors

If tools are installed but not found:

```bash
# Reinstall in editable mode
pip install -e . --force-reinstall

# Check installation
pip list | grep replay-cdxj
```

### pywb Dependency Issues

If pywb installation fails:

```bash
# Install system dependencies first (Ubuntu/Debian)
sudo apt-get install python3-dev build-essential

# Install system dependencies (macOS)
brew install python@3.12

# Then retry installation
pip install -e .
```
