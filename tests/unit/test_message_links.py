from __future__ import annotations

import unittest

from sightglass.source.links import extract_links, hint_evidence, normalize_url


class MessageLinkTests(unittest.TestCase):
    def test_normalization_preserves_private_url_components_and_balanced_path(self):
        link = normalize_url("HTTPS://user:pass@例子.测试:443/a_(b)?utm=x#secret）。")
        assert link is not None
        self.assertEqual(link["normalized_host"], "xn--fsqu00a.xn--0zwm56d")
        self.assertEqual(link["path"], "/a_(b)")
        self.assertEqual(link["query_text"], "utm=x")
        self.assertEqual(link["fragment_text"], "secret")
        self.assertNotIn(":443", link["normalized_url"])

    def test_text_card_and_forwarded_provenance_keep_distinct_occurrences(self):
        links, complete = extract_links(
            "https://synthetic.example/a。 https://synthetic.example/a,",
            {
                "link": {"raw_url": "https://card.example/x?token=synth#f", "title": "Card"},
                "forwarded_chat": {
                    "items": [{"sender": "Synthetic inner", "text": "https://inner.example/"}]
                },
            },
        )
        self.assertTrue(complete)
        self.assertEqual(len(links), 4)
        self.assertEqual([link.ordinal for link in links[:2]], [0, 1])
        self.assertEqual(links[-1].source_path, "forwarded_chat.items[0].text")
        self.assertEqual(links[2].title, "Card")

    def test_approximate_hostname_is_candidate_evidence_only(self):
        self.assertEqual(
            hint_evidence("www.ailover-atlas.example", ("ailovers.example",)),
            ("hostname_hint_fuzzy",),
        )
        self.assertFalse(hint_evidence("irrelevant.example", ("ailovers.example",)))

    def test_invalid_or_truncated_input_reports_partial(self):
        links, complete = extract_links("https://synthetic.example:invalid/", {})
        self.assertEqual(links, [])
        self.assertFalse(complete)
        self.assertFalse(extract_links(None, {"forwarded_chat": {"truncated": True}})[1])
