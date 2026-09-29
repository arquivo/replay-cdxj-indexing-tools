#!/usr/bin/env python3
"""
Test suite for cdxj-stats built-in sinks (mimetype, domain-host, domain-etld1).

Test Coverage:
--------------
1. MimetypeSink counting, with and without group-by
2. Domain-host vs domain-etld1 key derivation, including multi-label
   public-suffix domains (e.g. co.uk) and same-host caching
3. Streaming flush-on-key-change behavior and end-of-stream flush
4. group-by=year row shape, including oldest/latest-in-year columns
5. Option validation errors
"""

import unittest

from replay_cdxj_indexing_tools.stats.sinks.domain import DomainEtld1Sink, DomainHostSink
from replay_cdxj_indexing_tools.stats.sinks.mimetype import MimetypeSink


class TestMimetypeSink(unittest.TestCase):
    """Test MimetypeSink counting behavior."""

    def test_counts_without_group_by(self):
        sink = MimetypeSink()
        sink.configure()

        for mime in ("text/html", "text/html", "image/png"):
            self.assertIsNone(sink.process("com,example)/", "20200101000000", {"mime": mime}))

        rows = list(sink.flush())
        self.assertEqual(rows, [("text/html", "2"), ("image/png", "1")])
        self.assertEqual(sink.fieldnames(), ("mime", "count"))
        self.assertIsNone(sink.output_suffix())

    def test_missing_mime_counted_as_empty_string(self):
        sink = MimetypeSink()
        sink.configure()

        sink.process("com,example)/", "20200101000000", {})
        sink.process("com,example)/", "20200101000000", None)

        rows = list(sink.flush())
        self.assertEqual(rows, [("", "2")])

    def test_group_by_year(self):
        sink = MimetypeSink()
        sink.configure(**{"group-by": "year"})

        sink.process("com,example)/", "20190101000000", {"mime": "text/html"})
        sink.process("com,example)/", "20200601000000", {"mime": "text/html"})
        sink.process("com,example)/", "20200601000000", {"mime": "text/html"})

        rows = list(sink.flush())
        self.assertEqual(sink.fieldnames(), ("mime", "year", "count"))
        self.assertIn(("text/html", "2020", "2"), rows)
        self.assertIn(("text/html", "2019", "1"), rows)

    def test_invalid_group_by_rejected(self):
        sink = MimetypeSink()
        with self.assertRaises(ValueError):
            sink.configure(**{"group-by": "month"})

    def test_unknown_option_rejected(self):
        sink = MimetypeSink()
        with self.assertRaises(ValueError):
            sink.configure(top="100")


class TestDomainHostSink(unittest.TestCase):
    """Test DomainHostSink streaming accumulation."""

    def test_single_host_accumulates_until_key_change(self):
        sink = DomainHostSink()
        sink.configure()

        rows = sink.process("pt,exemplo,www)/", "20200101000000", {"status": "200"})
        self.assertIsNone(rows)
        rows = sink.process("pt,exemplo,www)/img", "20200601000000", {"status": "404"})
        self.assertIsNone(rows)

        # Key change flushes the previous host's accumulated row.
        rows = sink.process("pt,outro,www)/", "20210101000000", {"status": "200"})
        self.assertEqual(
            list(rows), [("pt,exemplo,www)", "2", "1", "20200101000000", "20200601000000")]
        )

        final_rows = list(sink.flush())
        self.assertEqual(
            final_rows, [("pt,outro,www)", "1", "1", "20210101000000", "20210101000000")]
        )

    def test_fieldnames_without_group_by(self):
        sink = DomainHostSink()
        sink.configure()
        self.assertEqual(
            sink.fieldnames(),
            ("domain", "total_captures", "captures_status_200", "oldest_capture", "latest_capture"),
        )
        self.assertIsNone(sink.output_suffix())

    def test_group_by_year_row_shape(self):
        sink = DomainHostSink()
        sink.configure(**{"group-by": "year"})

        sink.process("pt,exemplo,www)/", "20190101000000", {"status": "200"})
        sink.process("pt,exemplo,www)/", "20200601000000", {"status": "404"})

        rows = list(sink.flush())
        self.assertEqual(
            sink.fieldnames(),
            (
                "domain",
                "year",
                "total_captures",
                "captures_status_200",
                "oldest_capture",
                "latest_capture",
            ),
        )
        self.assertEqual(sink.output_suffix(), "year")
        self.assertIn(
            ("pt,exemplo,www)", "2019", "1", "1", "20190101000000", "20190101000000"), rows
        )
        self.assertIn(
            ("pt,exemplo,www)", "2020", "1", "0", "20200601000000", "20200601000000"), rows
        )

    def test_out_of_order_timestamps_within_same_host_still_tracked(self):
        sink = DomainHostSink()
        sink.configure()

        sink.process("pt,exemplo,www)/", "20200601000000", {"status": "200"})
        sink.process("pt,exemplo,www)/", "20150101000000", {"status": "200"})

        rows = list(sink.flush())
        self.assertEqual(rows[0][3], "20150101000000")  # oldest_capture
        self.assertEqual(rows[0][4], "20200601000000")  # latest_capture


class TestDomainEtld1Sink(unittest.TestCase):
    """Test DomainEtld1Sink eTLD+1 key derivation."""

    def test_groups_subdomains_under_registered_domain(self):
        sink = DomainEtld1Sink()
        sink.configure()

        sink.process("pt,exemplo,blog)/", "20190101000000", {"status": "200"})
        rows = sink.process("pt,exemplo,www)/", "20200101000000", {"status": "200"})
        self.assertIsNone(rows)  # same eTLD+1 as previous host, no flush yet

        final_rows = list(sink.flush())
        self.assertEqual(len(final_rows), 1)
        self.assertEqual(final_rows[0][0], "pt,exemplo,")
        self.assertEqual(final_rows[0][1], "2")  # total_captures

    def test_multi_label_public_suffix(self):
        """example.co.uk should be grouped as "uk,co,example," not "uk,co,"."""
        sink = DomainEtld1Sink()
        sink.configure()

        sink.process("uk,co,example,www)/", "20210101000000", {"status": "200"})
        rows = list(sink.flush())

        self.assertEqual(rows[0][0], "uk,co,example,")

    def test_distinct_registered_domains_flush_separately(self):
        sink = DomainEtld1Sink()
        sink.configure()

        rows = sink.process("pt,exemplo,www)/", "20200101000000", {"status": "200"})
        self.assertIsNone(rows)

        rows = sink.process("pt,outro,www)/", "20200101000000", {"status": "200"})
        self.assertEqual(
            list(rows), [("pt,exemplo,", "1", "1", "20200101000000", "20200101000000")]
        )

    def test_same_host_repeated_uses_cached_etld1_key(self):
        sink = DomainEtld1Sink()
        sink.configure()

        sink.process("pt,exemplo,www)/a", "20200101000000", {"status": "200"})
        sink.process("pt,exemplo,www)/b", "20200101000000", {"status": "200"})

        self.assertEqual(sink._last_host_key, "pt,exemplo,www)")  # pylint: disable=protected-access
        rows = list(sink.flush())
        self.assertEqual(rows[0][1], "2")


if __name__ == "__main__":
    unittest.main()
