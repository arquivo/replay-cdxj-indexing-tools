"""
Shared helpers for the command-line tools.
"""

import argparse


def positive_int(value: str) -> int:
    """argparse type: accept only integers greater than zero."""
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError(f"must be a positive integer, got {value}")
    return ivalue
