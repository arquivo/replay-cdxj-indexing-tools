#!/usr/bin/env python3
"""
addfield_to_flat_cdxj.py

Add custom JSON fields to flat CDXJ records. Unlike filters that remove data,
this tool enriches records by adding new metadata fields to existing CDXJ lines.

Designed to work in parallel on individual CDXJ files before merging for maximum
performance, following the same pattern as filter-blocklist.

COMMAND-LINE USAGE
==================

Basic field addition:

    # Add custom fields from command line
    addfield-to-flat-cdxj -i input.cdxj -o output.cdxj -f collection=AWP999
      -f source=arquivo

    # Read from stdin, write to stdout (pipeline mode)
    cat input.cdxj | addfield-to-flat-cdxj -i - -f batch=daily > enriched.cdxj

    # Add timestamp-based fields
    addfield-to-flat-cdxj -i input.cdxj -o output.cdxj -f indexed_date=$(date +%Y%m%d)

Using custom field functions:

    # Use Python function for complex logic (requires ADDFIELD_PLUGIN_DIR, see below)
    addfield-to-flat-cdxj -i input.cdxj -o output.cdxj \\
      --function "$ADDFIELD_PLUGIN_DIR/addfield_func.py"

Pipeline usage (recommended - add fields before merge):

    # Parallel field addition in collection processing
    parallel -j 16 'addfield-to-flat-cdxj -i {} -o {.}.enriched.cdxj -f collection=COL' ::: *.cdxj
    merge-flat-cdxj - *.enriched.cdxj | flat-cdxj-to-zipnum -o indexes -i -

CUSTOM FIELD FUNCTIONS
=======================

SECURITY WARNING: --function executes the given Python file's module-level code
immediately and with the full privileges of this process (equivalent to running
`python function_path`). It is a trusted-operator-only flag -- never pass a path
that came from untrusted or remote input.

--function is disabled unless the ADDFIELD_PLUGIN_DIR environment variable names
an operator-controlled directory. The function file must sit directly inside that
directory, and both the directory and the file must be owned by the current user
(or root) and must not be group- or world-writable. Every load is logged to stderr
at WARNING level with the resolved absolute path for auditing.

    mkdir -m 700 "$HOME/addfield-plugins"
    install -m 644 addfield_func.py "$HOME/addfield-plugins/"
    export ADDFIELD_PLUGIN_DIR="$HOME/addfield-plugins"

For complex field logic, create a Python file with an addfield() function:

    # addfield_func.py
    import json

    def addfield(surt_key, timestamp, json_data):
        '''
        Add custom fields to CDXJ record.

        Args:
            surt_key: SURT key (e.g., "pt,arquivo)/")
            timestamp: Timestamp string (e.g., "20231115120000")
            json_data: Parsed JSON dict from CDXJ line

        Returns:
            dict: Modified JSON data with new fields
        '''
        # Extract year from timestamp
        year = timestamp[:4]
        json_data['collection'] = f'ARQUIVO-{year}'

        # Add domain from URL
        if 'url' in json_data:
            from urllib.parse import urlparse
            domain = urlparse(json_data['url']).netloc
            json_data['domain'] = domain

        return json_data

Then use it:

    addfield-to-flat-cdxj -i input.cdxj -o output.cdxj \\
      --function "$ADDFIELD_PLUGIN_DIR/addfield_func.py"

CDXJ FORMAT
===========

Input:  pt,arquivo)/ 20231115120000 {"url": "...", "status": "200"}
Output: pt,arquivo)/ 20231115120000 {"url": "...", "status": "200", "collection": "ARQUIVO-2023"}

The JSON object is enriched with additional fields while preserving original data.

PYTHON API
==========

    from replay_cdxj_indexing_tools.addfield.addfield_to_flat_cdxj import (
        addfield_to_cdxj,
        load_addfield_function
    )

    # Simple field addition
    addfield_to_cdxj(
        'input.cdxj',
        'output.cdxj',
        fields={'collection': 'ARQUIVO-2024', 'source': 'web'}
    )

    # Using custom function (requires ADDFIELD_PLUGIN_DIR, see above)
    addfield_func = load_addfield_function('/srv/addfield-plugins/addfield_func.py')
    addfield_to_cdxj('input.cdxj', 'output.cdxj', addfield_func=addfield_func)

"""

import argparse
import importlib.abc
import importlib.machinery
import importlib.util
import json
import logging
import os
import shlex
import stat
import sys
from types import CodeType
from typing import Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# --function only loads files directly inside this operator-controlled directory.
# Fail closed: if it is unset, --function refuses to run (CWE-94).
ADDFIELD_PLUGIN_DIR_ENV = "ADDFIELD_PLUGIN_DIR"


def parse_cdxj_line(line: str) -> Tuple[str, str, str, Optional[dict]]:
    """
    Parse CDXJ line into components.

    CDXJ format: <surt_key> <timestamp> [<json>]
    Example: pt,governo,www)/ 20230615120000 {"url": "...", "status": "200"}

    Args:
        line: CDXJ line

    Returns:
        Tuple of (surt_key, timestamp, json_str, json_data)
        json_data is None if no JSON present

    Example:
        >>> line = 'pt,arquivo)/ 20231115120000 {"url": "https://arquivo.pt/"}'
        >>> surt, ts, json_str, data = parse_cdxj_line(line)
        >>> print(surt, ts, data['url'])
        pt,arquivo)/ 20231115120000 https://arquivo.pt/
    """
    parts = line.split(" ", 2)

    if len(parts) < 2:
        raise ValueError(f"Invalid CDXJ line (missing timestamp): {line[:50]}")

    surt_key = parts[0]
    timestamp = parts[1]
    json_str = parts[2] if len(parts) == 3 else ""

    # Parse JSON if present
    json_data = None
    if json_str.strip():
        try:
            json_data = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in CDXJ line: {e}") from e

    return surt_key, timestamp, json_str, json_data


def format_cdxj_line(surt_key: str, timestamp: str, json_data: Optional[dict]) -> str:
    """
    Format CDXJ line from components.

    Args:
        surt_key: SURT key
        timestamp: Timestamp string
        json_data: JSON data dict (or None)

    Returns:
        Formatted CDXJ line (with newline)

    Example:
        >>> data = {'url': 'https://arquivo.pt/', 'status': '200'}
        >>> line = format_cdxj_line('pt,arquivo)/', '20231115120000', data)
        >>> print(line.strip())
        pt,arquivo)/ 20231115120000 {"url": "https://arquivo.pt/", "status": "200"}
    """
    if json_data:  # pylint: disable=no-else-return  # else makes both branches explicit
        # Use separators to minimize whitespace (compact JSON)
        json_str = json.dumps(json_data, ensure_ascii=False, separators=(",", ":"))
        return f"{surt_key} {timestamp} {json_str}\n"
    else:
        return f"{surt_key} {timestamp}\n"


def addfield_to_cdxj(
    input_path: str,
    output_path: str = "-",
    fields: Optional[Dict[str, str]] = None,
    addfield_func: Optional[Callable] = None,
    buffer_size: int = 1024 * 1024,
    verbose: bool = False,
) -> Tuple[int, int]:
    """
    Add fields to flat CDXJ records.

    Reads CDXJ line by line, parses JSON, adds fields, and writes enriched output.
    You can provide either simple key-value fields or a custom field function.

    Args:
        input_path: Input CDXJ file, or '-' for stdin
        output_path: Output file, or '-' for stdout (default: stdout)
        fields: Dictionary of field_name -> value to add (default: None)
        addfield_func: Custom function(surt, timestamp, json_data) -> json_data (default: None)
        buffer_size: I/O buffer size in bytes (default: 1MB)
        verbose: Print statistics to stderr (default: False)

    Returns:
        Tuple of (lines_processed, lines_skipped)

    Raises:
        ValueError: If neither fields nor addfield_func provided, or if both provided

    Example:
        >>> # Simple field addition
        >>> processed, skipped = addfield_to_cdxj(
        ...     'input.cdxj',
        ...     'output.cdxj',
        ...     fields={'collection': 'ARQUIVO-2024', 'batch': 'daily'}
        ... )
        >>> print(f"Processed {processed} lines")
    """
    if not fields and not addfield_func:
        raise ValueError("Must provide either 'fields' or 'addfield_func'")

    if fields and addfield_func:
        raise ValueError("Cannot provide both 'fields' and 'addfield_func' (choose one)")

    lines_processed = 0
    lines_skipped = 0

    # Open input
    if input_path == "-":
        input_fh = sys.stdin
    else:  # pylint: disable-next=consider-using-with  # stdin/file conditional; finally closes
        input_fh = open(input_path, "r", encoding="utf-8", buffering=buffer_size)

    # Open output
    if output_path == "-":
        output_fh = sys.stdout
    else:  # pylint: disable-next=consider-using-with  # stdin/file conditional; finally closes
        output_fh = open(output_path, "w", encoding="utf-8", buffering=buffer_size)

    try:
        for line_num, line in enumerate(input_fh, 1):
            line = line.rstrip("\n\r")

            # Skip empty lines
            if not line:
                lines_skipped += 1
                output_fh.write("\n")
                continue

            try:
                # Parse CDXJ line
                surt_key, timestamp, _, json_data = parse_cdxj_line(line)

                # Initialize JSON data if not present
                if json_data is None:
                    json_data = {}

                # Apply field addition
                if addfield_func:
                    # Use custom function
                    json_data = addfield_func(surt_key, timestamp, json_data)
                elif fields:  # mypy: fields is guaranteed to be not None here
                    # Add simple fields
                    for field_name, field_value in fields.items():
                        json_data[field_name] = field_value

                # Write enriched line
                output_line = format_cdxj_line(surt_key, timestamp, json_data)
                output_fh.write(output_line)

                lines_processed += 1

            except (ValueError, json.JSONDecodeError) as e:
                if verbose:
                    print(
                        f"Warning: Skipping invalid line {line_num}: {str(e)[:50]}",
                        file=sys.stderr,
                    )
                lines_skipped += 1
                # Write original line unchanged
                output_fh.write(line + "\n")

        return lines_processed, lines_skipped

    finally:
        if input_path != "-":
            input_fh.close()
        if output_path != "-":
            output_fh.close()


def _check_trusted_stat(st: os.stat_result, path: str, kind: str) -> None:
    """
    Refuse plugin directories/files that someone other than the operator can modify.

    Raises:
        PermissionError: If not owned by the current user or root, or if group-
            or world-writable (CWE-732)
    """
    if st.st_uid not in (os.geteuid(), 0):
        raise PermissionError(
            f"Refusing to load addfield function: {kind} {path!r} is owned by uid "
            f"{st.st_uid}; it must be owned by the current user (uid {os.geteuid()}) or root"
        )
    if st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        shared = st.st_mode & (stat.S_ISVTX | stat.S_ISGID) or st.st_uid != os.geteuid()
        if stat.S_ISDIR(st.st_mode) and shared:
            # Shared (sticky/setgid, e.g. /tmp) or not ours: chmod would be wrong or impossible.
            hint = (
                "use a private directory you own instead, e.g. "
                'mkdir -m 700 "$HOME/addfield-plugins"'
            )
        elif path.isprintable():
            hint = f"run: chmod go-w {shlex.quote(path)}"
        else:
            # shlex.quote() keeps newlines, which could forge stderr lines (CWE-117).
            hint = "remove group/other write permission (chmod go-w) from the path above"
        raise PermissionError(
            f"Refusing to load addfield function: {kind} {path!r} is group- or "
            f"world-writable (mode {stat.S_IMODE(st.st_mode):o}); {hint}"
        )


def _read_trusted_plugin(function_path: str) -> Tuple[str, bytes]:
    """
    Apply the --function trust checks and return (resolved_path, source bytes).

    See load_addfield_function() for the rules and the exceptions raised.
    """
    plugin_dir = os.environ.get(ADDFIELD_PLUGIN_DIR_ENV)
    if not plugin_dir:
        raise PermissionError(
            f"Loading addfield functions from files (--function) is disabled because "
            f"{ADDFIELD_PLUGIN_DIR_ENV} is not set. To enable it, put the function "
            f"file in a directory that only you (or root) can write to and set "
            f"{ADDFIELD_PLUGIN_DIR_ENV} to that directory, e.g.: "
            f'mkdir -m 700 "$HOME/addfield-plugins" && '
            f'install -m 644 my_func.py "$HOME/addfield-plugins/" && '
            f'export {ADDFIELD_PLUGIN_DIR_ENV}="$HOME/addfield-plugins"'
        )
    if os.name != "posix":
        raise PermissionError(
            "Loading addfield functions from files (--function) is only supported on "
            "POSIX systems (ownership/permission checks are required)"
        )

    # expanduser: Docker ENV / systemd Environment= do not expand "~".
    resolved_plugin_dir = os.path.realpath(os.path.expanduser(plugin_dir))
    if resolved_plugin_dir == os.sep:
        raise PermissionError(
            f"{ADDFIELD_PLUGIN_DIR_ENV}={plugin_dir!r} resolves to the filesystem root; "
            "set it to a dedicated plugin directory"
        )
    resolved_path = os.path.realpath(function_path)

    if os.path.dirname(resolved_path) != resolved_plugin_dir:
        raise ValueError(
            f"Function file {resolved_path!r} is outside the allowed plugin "
            f"directory {resolved_plugin_dir!r} (see {ADDFIELD_PLUGIN_DIR_ENV}); "
            "it must be placed directly inside that directory"
        )

    # Open once and check/read through the descriptors (no re-open by path).
    try:
        dir_fd = os.open(resolved_plugin_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError as e:
        raise IOError(
            f"Cannot open {ADDFIELD_PLUGIN_DIR_ENV} directory {resolved_plugin_dir!r}: "
            f"{e.strerror}"
        ) from e
    try:
        _check_trusted_stat(os.fstat(dir_fd), resolved_plugin_dir, "plugin directory")
        name = os.path.basename(resolved_path)
        try:
            pre_stat = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
        except OSError as e:
            raise IOError(f"Cannot load module from {function_path!r}: {e.strerror}") from e
        # Reject FIFOs/devices/symlinks before open() can block or act on them.
        if not stat.S_ISREG(pre_stat.st_mode):
            raise IOError(f"Cannot load module from {function_path!r}: not a regular file")
        try:
            file_fd = os.open(
                name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_NOCTTY,
                dir_fd=dir_fd,
            )
        except OSError as e:
            raise IOError(f"Cannot load module from {function_path!r}: {e.strerror}") from e
    finally:
        os.close(dir_fd)

    try:
        # Authoritative (TOCTOU-safe) checks on the opened descriptor.
        file_stat = os.fstat(file_fd)
        if not stat.S_ISREG(file_stat.st_mode):
            raise IOError(f"Cannot load module from {function_path!r}: not a regular file")
        # Regular file: restore blocking reads (O_NONBLOCK only guarded the open).
        os.set_blocking(file_fd, True)
        _check_trusted_stat(file_stat, resolved_path, "function file")
        with open(file_fd, "rb", closefd=False) as fh:
            source = fh.read()
    finally:
        os.close(file_fd)

    return resolved_path, source


class _VettedSourceLoader(importlib.abc.InspectLoader):
    """Import loader serving source bytes already read from a vetted descriptor.

    It never opens the path again, so the code that runs is exactly the code that
    passed the checks in _read_trusted_plugin (CWE-367).
    """

    def __init__(self, path: str, source: bytes) -> None:
        self._path = path
        self._source = source

    def get_source(self, fullname: str) -> str:
        return importlib.util.decode_source(self._source)

    def get_code(self, fullname: str) -> CodeType:
        return compile(self._source, self._path, "exec", dont_inherit=True)

    def is_package(self, fullname: str) -> bool:
        return False


def load_addfield_function(function_path: str) -> Callable:
    """
    Load field addition function from Python file.

    The file must define an addfield(surt_key, timestamp, json_data) function
    that returns the modified json_data dict.

    Args:
        function_path: Path to Python file with addfield() function. Must resolve
            to a file directly inside ADDFIELD_PLUGIN_DIR.

    Returns:
        The addfield function

    Raises:
        PermissionError: If ADDFIELD_PLUGIN_DIR is unset, the platform is not
            POSIX, or the plugin directory/file fails the ownership/permission check
        ValueError: If function_path does not resolve directly inside ADDFIELD_PLUGIN_DIR
        IOError: If the plugin directory or file cannot be opened, or the file is
            not a regular file (including a symlink swapped in after resolution)
        AttributeError: If addfield() function not found in file

    Security:
        Runs the file's module-level code with this process's privileges (CWE-94);
        fails closed unless ADDFIELD_PLUGIN_DIR is set. Every load and refusal is
        logged at WARNING level. See SECURITY.md, threat model section 6.

    Example:
        >>> # export ADDFIELD_PLUGIN_DIR=/srv/addfield-plugins
        >>> addfield_func = load_addfield_function('/srv/addfield-plugins/my_addfield.py')
        >>> result = addfield_func('pt,arquivo)/', '20231115120000', {'url': '...'})
    """
    try:
        resolved_path, source = _read_trusted_plugin(function_path)
    except (OSError, ValueError) as e:
        logger.warning("Refused --function file %r: %s", function_path, e)
        raise

    logger.warning(
        "Executing arbitrary Python code from --function file: %r. This runs "
        "module-level code with the privileges of this process; only use "
        "trusted, locally-authored files.",
        resolved_path,
    )

    # Run the bytes already read (not the path) so what was checked runs. The
    # module is not registered in sys.modules.
    loader = _VettedSourceLoader(resolved_path, source)
    spec = importlib.machinery.ModuleSpec("addfield_module", loader, origin=resolved_path)
    spec.has_location = True  # sets __file__; nothing is read from the filesystem
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)

    addfield_func = getattr(module, "addfield", None)
    if addfield_func is None:
        raise AttributeError(
            f"Module {function_path!r} must define an addfield"
            "(surt_key, timestamp, json_data) function"
        )

    return addfield_func


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="Add custom fields to flat CDXJ records",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Add simple fields
  %(prog)s -i input.cdxj -o output.cdxj -f collection=ARQUIVO-2024 -f source=web

  # Pipeline mode (stdin/stdout)
  cat input.cdxj | %(prog)s -i - -f batch=daily > enriched.cdxj

  # Parallel field addition (recommended for large collections)
  parallel -j 16 '%(prog)s -i {} -o {.}.enriched.cdxj -f collection=COL' ::: *.cdxj

  # Use custom field function (requires ADDFIELD_PLUGIN_DIR, see below)
  ADDFIELD_PLUGIN_DIR="$HOME/addfield-plugins" \\
    %(prog)s -i input.cdxj -o output.cdxj --function "$HOME/addfield-plugins/addfield_func.py"

  # Combine with pipeline
  %(prog)s -i input.cdxj -f year=2024 | filter-blocklist -i - -b list.txt

Custom Field Function File Format:
  Create a Python file with an addfield() function:

    # addfield_func.py
    def addfield(surt_key, timestamp, json_data):
        # Add custom fields based on data
        json_data['collection'] = 'ARQUIVO-2024'
        json_data['year'] = timestamp[:4]
        return json_data

  --function is disabled unless ADDFIELD_PLUGIN_DIR is set. The file must be
  directly inside that directory, and both must be owned by you (or root) and
  not be group- or world-writable. The file's code runs with this process's
  privileges; each run logs one audit WARNING line to stderr.

    mkdir -m 700 "$HOME/addfield-plugins"
    install -m 644 addfield_func.py "$HOME/addfield-plugins/"
    export ADDFIELD_PLUGIN_DIR="$HOME/addfield-plugins"
    %(prog)s -i input.cdxj --function "$ADDFIELD_PLUGIN_DIR/addfield_func.py"
        """,
    )

    parser.add_argument("-i", "--input", required=True, help="Input CDXJ file, or - for stdin")
    parser.add_argument(
        "-o", "--output", default="-", help="Output file, or - for stdout (default: stdout)"
    )

    # Field addition options (mutually exclusive)
    field_group = parser.add_mutually_exclusive_group(required=True)
    field_group.add_argument(
        "-f",
        "--field",
        action="append",
        dest="fields",
        metavar="KEY=VALUE",
        help="Add field to JSON (can be used multiple times). Format: key=value",
    )
    field_group.add_argument(
        "--function",
        dest="function_file",
        help=(
            "Python file with addfield(surt_key, timestamp, json_data) function. "
            "TRUSTED-OPERATOR-ONLY: executes the file's module-level code with this "
            "process's privileges. Disabled unless ADDFIELD_PLUGIN_DIR is set; the "
            "file must be directly inside it (see below)."
        ),
    )

    parser.add_argument("-v", "--verbose", action="store_true", help="Print statistics to stderr")

    args = parser.parse_args()

    # Ensure the --function audit/refusal warnings reach stderr even when the
    # caller has not configured logging (no-op if a handler already exists).
    logging.basicConfig(format="%(levelname)s: %(name)s: %(message)s")

    try:
        # Parse fields or load function
        if args.fields:
            # Parse key=value pairs
            fields_dict = {}
            for field_spec in args.fields:
                if "=" not in field_spec:
                    print(
                        f"Error: Invalid field format '{field_spec}' (expected key=value)",
                        file=sys.stderr,
                    )
                    sys.exit(1)

                key, value = field_spec.split("=", 1)
                fields_dict[key] = value

            if args.verbose:
                print(f"Adding {len(fields_dict)} fields to CDXJ records", file=sys.stderr)
                for k, v in fields_dict.items():
                    print(f"  {k} = {v}", file=sys.stderr)

            # Add fields
            lines_processed, lines_skipped = addfield_to_cdxj(
                args.input, args.output, fields=fields_dict, verbose=args.verbose
            )

        else:
            # Load and use custom function
            if args.verbose:
                print(
                    f"Loading field addition function from {args.function_file!r}...",
                    file=sys.stderr,
                )

            addfield_func = load_addfield_function(args.function_file)

            if args.verbose:
                print("Adding fields to CDXJ records...", file=sys.stderr)

            lines_processed, lines_skipped = addfield_to_cdxj(
                args.input, args.output, addfield_func=addfield_func, verbose=args.verbose
            )

        # Print statistics
        total = lines_processed + lines_skipped
        print(
            f"# Processed {lines_processed} lines, skipped {lines_skipped} lines (total: {total})",
            file=sys.stderr,
        )

    except KeyboardInterrupt:
        print("\n# Interrupted by user", file=sys.stderr)
        sys.exit(1)
    except SyntaxError as e:
        # str(SyntaxError) embeds the raw filename; repr() it (CWE-117). Some errors
        # (e.g. NUL bytes on Python >= 3.11) carry no location.
        location = ""
        if e.filename is not None and e.lineno is not None:
            location = f" ({e.filename!r}, line {e.lineno})"
        print(f"Error: {e.msg}{location}", file=sys.stderr)
        sys.exit(1)
    except (IOError, OSError, ValueError, AttributeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
