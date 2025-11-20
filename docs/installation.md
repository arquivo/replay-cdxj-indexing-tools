# Installation Guide

## Requirements

- Python 3.8 or higher
- pip package manager
- Virtual environment (recommended)
- Docker (optional, for containerized deployment)

## Option 1: Docker (Recommended for Production)

The easiest way to use the tools without managing Python dependencies:

```bash
# Pull the latest image from Docker Hub
docker pull arquivo/replay-cdxj-indexing-tools:latest

# Verify installation
docker run arquivo/replay-cdxj-indexing-tools:latest

# Run a command
docker run -v /path/to/data:/data arquivo/replay-cdxj-indexing-tools:latest \
    merge-flat-cdxj --help
```

### Docker Usage Examples

```bash
# Merge CDXJ files
docker run -v $(pwd)/data:/data arquivo/replay-cdxj-indexing-tools:latest \
    merge-flat-cdxj /data/output/merged.cdxj /data/input/*.cdxj

# Convert to ZipNum
docker run -v $(pwd)/data:/data arquivo/replay-cdxj-indexing-tools:latest \
    flat-cdxj-to-zipnum -i /data/input/file.cdxj -o /data/output/

# Full pipeline
docker run -v $(pwd)/data:/data arquivo/replay-cdxj-indexing-tools:latest \
    sh -c "merge-flat-cdxj - /data/input/*.cdxj | \
           filter-blocklist -i - -b /data/blocklist.txt | \
           filter-excessive-urls auto -i - -n 1000 | \
           flat-cdxj-to-zipnum -o /data/output/ -i -"

# Process a collection
docker run -v /path/to/collections:/data arquivo/replay-cdxj-indexing-tools:latest \
    cdxj-index-collection COLLECTION-2024-11 --incremental
```

### Building Docker Image Locally

```bash
# Clone and build
git clone https://github.com/arquivo/replay-cdxj-indexing-tools.git
cd replay-cdxj-indexing-tools
docker build -t arquivo/replay-cdxj-indexing-tools:local .

# Run locally built image
docker run -v $(pwd)/data:/data arquivo/replay-cdxj-indexing-tools:local merge-flat-cdxj --help
```

## Option 2: Install from Source (Development)

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

## Option 3: Install from Git

Install directly from GitHub:

```bash
pip install git+https://github.com/arquivo/replay-cdxj-indexing-tools.git
```

## Option 4: Install from PyPI

(When/if published to PyPI)

```bash
pip install replay-cdxj-indexing-tools
```

## Verify Installation

Check that all commands are available:

```bash
# Check merge command
merge-flat-cdxj --help

# Check filter commands
filter-blocklist --help
filter-excessive-urls --help

# Check zipnum command
flat-cdxj-to-zipnum --help
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
