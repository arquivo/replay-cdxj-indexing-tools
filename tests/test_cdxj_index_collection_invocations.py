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


def script_default(name):
    """Return the value of a top-level ``NAME=value`` assignment in the script."""
    match = re.search(rf"^{name}=(\S+)$", SCRIPT.read_text(), re.MULTILINE)
    if match is None:
        raise AssertionError(f"{name} has no default assignment in {SCRIPT.name}")
    return match.group(1)


def zipnum_invocations(values):
    """
    Return the argument lists of every flat-cdxj-to-zipnum command in the script.

    Backslash continuations are joined and pipelines are split into their stages, so
    only stages whose first word is the command count; ``command -v`` checks and log
    messages that mention the name are skipped. Shell variables are replaced with
    ``values``; a variable missing from ``values`` fails with an AssertionError so that
    a new variable in the invocation has to be given a value here.
    """

    def expand(match):
        name = match.group(1)
        if name not in values:
            raise AssertionError(f"{COMMAND} uses ${name}; give it a value in this test")
        return shlex.quote(values[name])

    text = SCRIPT.read_text().replace("\\\n", " ")
    invocations = []
    for line in text.splitlines():
        if COMMAND not in line or line.lstrip().startswith("#"):
            continue
        for stage in line.split("|"):
            words = shlex.split(stage)
            if not words or words[0] != COMMAND:
                continue
            invocations.append(shlex.split(SHELL_VAR.sub(expand, stage))[1:])
    return invocations


class TestZipnumInvocationFlags(unittest.TestCase):
    """The script's flat-cdxj-to-zipnum calls parse with the converter's own parser."""

    def invocations(self, shard_size):
        found = zipnum_invocations({"OUTPUT_DIR": "/tmp/zipnum-out", "SHARD_SIZE": shard_size})
        self.assertTrue(found, f"no {COMMAND} invocation found in {SCRIPT.name}")
        return found

    def test_default_shard_size_is_accepted(self):
        """The script's default SHARD_SIZE reaches the converter as its shard size."""
        default = script_default("SHARD_SIZE")
        for argv in self.invocations(default):
            with self.subTest(argv=argv):
                args = parse_args(argv)
                self.assertEqual(args.shard_size, int(default))
                self.assertEqual(args.input, "-")
                self.assertEqual(args.output, "/tmp/zipnum-out")

    def test_custom_shard_size_is_passed_through(self):
        """A --shard-size value given to the script is the converter's shard size."""
        for argv in self.invocations("250"):
            with self.subTest(argv=argv):
                self.assertEqual(parse_args(argv).shard_size, 250)

    def test_unknown_flag_is_rejected(self):
        """A flag the converter does not define fails, so a mismatch cannot pass."""
        with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
            parse_args(["-o", "/tmp/zipnum-out", "-i", "-", "-n", "100"])


if __name__ == "__main__":
    unittest.main()
