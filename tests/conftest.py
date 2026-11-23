"""Pytest configuration file."""

import sys
from pathlib import Path

import pytest

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture(name="no_retry_delay")
def _no_retry_delay(monkeypatch):
    """Retry failed remote reads without waiting."""
    monkeypatch.setattr("replay_cdxj_indexing_tools.storage._RETRY_BACKOFF", 0)
    monkeypatch.setattr("replay_cdxj_indexing_tools.storage._MAX_RETRY_DELAY", 0)
