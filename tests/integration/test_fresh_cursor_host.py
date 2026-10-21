"""Fresh paging through hosts whose existing catalog exposes refresh but no view."""

from __future__ import annotations

import unittest
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError
from tests.integration import test_replica_reads as fixtures


class RefreshOnlyHostTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = fixtures.ReplicaReadTests()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.expected = self.fixture.append_source(425, prefix="synthetic-refresh-host")
        self.fixture.offline()

    def first_page(self) -> tuple[dict[str, Any], dict[str, Any]]:
        arguments = {
            "mode": "recent", "conversation_id": self.fixture.group,
            "limit": 500, "refresh": True, "voice": "off", "include_resources": "none",
        }
        _, frozen = self.fixture.capture(arguments)
        with self.fixture.service.captured_provider(frozen):
            page = self.fixture.tools.wechat_read_messages(**arguments, response_profile="brief")
        self.assertTrue(page["page"]["next_cursor"])
        self.assertEqual(page["source_receipt"]["view"], "fresh")
        return arguments, page

    def test_refresh_only_host_drains_multiple_fresh_pages_using_guidance(self) -> None:
        arguments, page = self.first_page()
        seen: set[str] = set()
        captures = 1
        for _ in range(5):
            rows = self.fixture.repository.frozen_message_rows(
                tuple(row[0] for row in page["messages"])
            )
            source_ids = {str(row["source_message_id"]) for row in rows}
            self.assertEqual(len(source_ids), len(rows))
            self.assertFalse(seen & source_ids)
            seen.update(source_ids)
            cursor = page["page"].get("next_cursor")
            if not cursor:
                break
            action = next(
                action for action in page["next_actions"] if action["kind"] == "result_page"
            )
            self.assertNotIn("view", action.get("arguments", {}))
            arguments = {key: value for key, value in arguments.items()
                         if key not in action["clear_arguments"]}
            arguments.update(action.get("arguments", {}))
            arguments[action["parameter"]] = cursor
            self.assertNotIn("refresh", arguments)
            self.assertNotIn("view", arguments)
            self.assertFalse(self.fixture.service.local_message_read_ready(arguments))
            plan, frozen = self.fixture.capture(arguments)
            self.assertLessEqual(plan.limit, 200)
            with self.fixture.service.captured_provider(frozen):
                page = self.fixture.tools.wechat_read_messages(
                    **arguments, response_profile="brief"
                )
            self.assertNotEqual(page.get("ok"), False, page)
            self.assertEqual(page["source_receipt"]["view"], "fresh")
            captures += 1
        else:
            self.fail("refresh-only host paging did not terminate")
        self.assertGreater(captures, 2)
        self.assertTrue(self.expected <= seen)

    def test_signed_fresh_cursor_routes_to_source_without_optional_view(self) -> None:
        arguments, page = self.first_page()
        continuation = {key: value for key, value in arguments.items() if key != "refresh"}
        continuation["cursor"] = page["page"]["next_cursor"]
        self.assertFalse(self.fixture.service.local_message_read_ready(continuation))
        _, frozen = self.fixture.capture(continuation)
        with self.fixture.service.captured_provider(frozen):
            result = self.fixture.service.read_messages(**continuation)
        self.assertEqual(result["source_receipt"]["view"], "fresh")

    def test_explicit_wrong_view_and_tampered_cursor_fail_locally(self) -> None:
        arguments, page = self.first_page()
        continuation = {key: value for key, value in arguments.items() if key != "refresh"}
        continuation["cursor"] = page["page"]["next_cursor"]
        wrong_view = dict(continuation, view="replica")
        self.assertTrue(self.fixture.service.local_message_read_ready(wrong_view))
        with self.assertRaises(SightglassError) as caught:
            self.fixture.service.read_messages(**wrong_view)
        self.assertEqual(caught.exception.code, ErrorCode.CURSOR_INVALID)
        body, signature = continuation["cursor"].split(".", 1)
        invalid = body + "." + ("A" if signature[0] != "A" else "B") + signature[1:]
        malformed = dict(continuation, cursor=invalid)
        self.assertTrue(self.fixture.service.local_message_read_ready(malformed))
        with self.assertRaises(SightglassError) as caught:
            self.fixture.service.read_messages(**malformed)
        self.assertEqual(caught.exception.code, ErrorCode.CURSOR_INVALID)

    def test_fresh_cursor_scope_recheck_remains_local(self) -> None:
        arguments, page = self.first_page()
        continuation = {key: value for key, value in arguments.items() if key != "refresh"}
        continuation["cursor"] = page["page"]["next_cursor"]
        wrong_scope = dict(continuation, system_policy="omit")
        self.assertTrue(self.fixture.service.local_message_read_ready(wrong_scope))
        with self.assertRaises(SightglassError) as caught:
            self.fixture.service.read_messages(**wrong_scope)
        self.assertEqual(caught.exception.code, ErrorCode.CURSOR_INVALID)

    def test_fresh_context_guidance_reanchors_using_refresh_without_view(self) -> None:
        _, first = self.first_page()
        arguments = {
            "mode": "context", "message_id": first["messages"][10][0],
            "before": 1, "after": 1, "limit": 3,
            "refresh": True, "voice": "off", "include_resources": "none",
        }
        _, frozen = self.fixture.capture(arguments)
        with self.fixture.service.captured_provider(frozen):
            context = self.fixture.tools.wechat_read_messages(
                **arguments, response_profile="brief"
            )
        actions = [action for action in context["next_actions"]
                   if action["kind"] == "context_edge"]
        self.assertTrue(actions)
        action = actions[0]
        self.assertNotIn("view", action["arguments"])
        self.assertIs(action["arguments"]["refresh"], True)
        follow_up = {key: value for key, value in arguments.items()
                     if key not in action["clear_arguments"]}
        follow_up.update(action["arguments"])
        _, result = self.fixture.read_capture(follow_up)
        self.assertEqual(result["source_receipt"]["view"], "fresh")


if __name__ == "__main__":
    unittest.main()
