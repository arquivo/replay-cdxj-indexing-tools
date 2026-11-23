#!/usr/bin/env python3
"""
Tests that cdxj-index-collection.sh calls flat-cdxj-to-zipnum with flags its CLI accepts.

The shell script builds the ZipNum stage of its pipeline by hand, so nothing ties its
flags to the converter's argparse definition. These tests read every
flat-cdxj-to-zipnum command out of the script, expand the shell variables it uses,
and run the resulting argument list through the real ``parse_args``. A flag that the
CLI does not define makes argparse exit, which fails the test.
"""

import io
import re
import shlex
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from replay_cdxj_indexing_tools.zipnum.flat_cdxj_to_zipnum import parse_args

SCRIPT = (
    Path(__file__).resolve().parent.parent
    / "replay_cdxj_indexing_tools"
    / "cdxj-index-collection.sh"
)
COMMAND = "flat-cdxj-to-zipnum"
SHELL_VAR = re.compile(r"\$\{?(\w+)\}?")
CONTROL_OPERATORS = {"|", "||", "&&", ";", "&", "|&"}
OUTPUT_DIR = "/tmp/zipnum-out"


def script_default(name):
    """Return the value of a top-level ``NAME=value`` assignment in the script."""
    match = re.search(rf"^{name}=(\S+)$", SCRIPT.read_text(), re.MULTILINE)
    if match is None:
        raise AssertionError(f"{name} has no default assignment in {SCRIPT.name}")
    return match.group(1)


def zipnum_invocations(script_text, values):
    """
    Return the argument lists of every flat-cdxj-to-zipnum command in ``script_text``.

    Backslash continuations are joined, and each line that mentions the command is
    tokenised like a shell line: quotes are honoured, ``#`` comments are dropped and
    control operators (``|``, ``||``, ``&&``, ``;``, ``&``) end a command. Only
    commands whose first word is flat-cdxj-to-zipnum are returned, so ``command -v``
    checks and log messages that mention the name are skipped.

    Shell variables in those commands are replaced with ``values``. Every value must
    be a single non-empty word, so the expansion matches what bash would pass; a
    variable missing from ``values`` fails, so a new variable in the call has to be
    given a value here.
    """
    for name, value in values.items():
        if not re.fullmatch(r"\S+", value):
            raise AssertionError(f"value for ${name} must be one non-empty word: {value!r}")

    def expand(match):
        name = match.group(1)
        if name not in values:
            raise AssertionError(f"{COMMAND} uses ${name}; give it a value in this test")
        return values[name]

    invocations = []
    for line in script_text.replace("\\\n", " ").splitlines():
        if COMMAND not in line:
            continue
        lexer = shlex.shlex(line, posix=True, punctuation_chars=True)
        lexer.whitespace_split = True
        command = []
        for token in [*lexer, ";"]:
            if token not in CONTROL_OPERATORS:
                command.append(token)
                continue
            if command and command[0] == COMMAND:
                invocations.append([SHELL_VAR.sub(expand, word) for word in command[1:]])
            command = []
    return invocations


class TestZipnumInvocationFlags(unittest.TestCase):
    """The script's flat-cdxj-to-zipnum calls parse with the converter's own parser."""

    def test_script_invocations_are_accepted(self):
        """Every call in the script parses, with the script's default SHARD_SIZE."""
        default = script_default("SHARD_SIZE")
        found = zipnum_invocations(
            SCRIPT.read_text(), {"OUTPUT_DIR": OUTPUT_DIR, "SHARD_SIZE": default}
        )
        self.assertTrue(found, f"no {COMMAND} invocation found in {SCRIPT.name}")
        for argv in found:
            with self.subTest(argv=argv):
                args = parse_args(argv)
                self.assertEqual(args.shard_size, int(default))
                self.assertEqual(args.input, "-")
                self.assertEqual(args.output, OUTPUT_DIR)

    def test_flag_mismatch_is_caught(self):
        """A call using flags the converter does not define fails to parse."""
        text = 'flat-cdxj-to-zipnum -o "$OUTPUT_DIR" -i - -n "$SHARD_SIZE" $COMPRESS\n'
        values = {"OUTPUT_DIR": OUTPUT_DIR, "SHARD_SIZE": "100", "COMPRESS": "--compress"}
        [argv] = zipnum_invocations(text, values)
        with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
            parse_args(argv)

    def test_finds_calls_after_control_operators(self):
        """Calls after && or ; are found; quoted | and trailing comments are handled."""
        text = (
            'echo "a | b" && mkdir -p "$OUTPUT_DIR" && \\\n'
            '    flat-cdxj-to-zipnum -o "$OUTPUT_DIR" -i - -s 7  # comment -n 1\n'
        )
        self.assertEqual(
            zipnum_invocations(text, {"OUTPUT_DIR": OUTPUT_DIR}),
            [["-o", OUTPUT_DIR, "-i", "-", "-s", "7"]],
        )


if __name__ == "__main__":
    unittest.main()
