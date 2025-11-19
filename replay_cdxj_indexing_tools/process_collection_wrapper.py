#!/usr/bin/env python3
"""
Wrapper to execute the cdxj-index-collection.sh script.

This provides a Python entry point for the bash reference implementation script.
"""

import os
import sys
import subprocess


def main():
    """Execute the cdxj-index-collection.sh script with all arguments passed through."""
    # Get the directory where this module is installed
    module_dir = os.path.dirname(os.path.abspath(__file__))

    # Path to the shell script
    script_path = os.path.join(module_dir, "cdxj-index-collection.sh")

    # Check if script exists
    if not os.path.exists(script_path):
        print(f"Error: Could not find cdxj-index-collection.sh at {script_path}", file=sys.stderr)
        sys.exit(1)

    # Make sure it's executable
    if not os.access(script_path, os.X_OK):
        print(f"Error: {script_path} is not executable", file=sys.stderr)
        print("Try: chmod +x " + script_path, file=sys.stderr)
        sys.exit(1)

    # Execute the script with all arguments
    try:
        # Pass all command-line arguments (except the script name itself)
        result = subprocess.run(
            ["bash", script_path] + sys.argv[1:],
            check=False,  # Don't raise exception, just pass through exit code
        )
        sys.exit(result.returncode)
    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Error executing script: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
