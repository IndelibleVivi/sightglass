"""Cold-source discovery for wechat_find_links / wechat_retrieve.

These tests exercise the bounded, cancellable preparation path that makes a known
source-only (never-admitted) link or discussion discoverable under the default
on_demand residency, and that re-discovers a positive after its cached body and
derived index rows were released or expired. They also pin the restrictions: a
warm resident subset never proves full source scope, admitted rows stay selective,
scope/time/sender/kind remain hard constraints, and no query text reaches the
private sidecar or a signed token.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import ExitStack, closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

from sightglass.reader.service import SEARCH_PREPARATION_MESSAGE_BUDGET
from sightglass.residency.repository import ResidencyRepository
from sightglass.runtime.lanes import RuntimeLanes
from sightglass.runtime.search_preparation import DISCOVERY_SCHEMA, SearchPreparation
from sightglass.runtime.source_worker import SourceWorker
from sightglass.source.synthetic import DEFAULT_OBSERVED_AT, create_synthetic_source
from sightglass.storage import StorageBudget, StorageSettings
from tests.fixtures.factory import build_test_stack

DISPOSABLE_SECRET = "synthetic-cold-needle"
GROUP = "conv_group"
DIRECT = "conv_direct"


def _append_source_row(
    root: Path,
    *,
    message_id: str,
    conversation_id: str,
    content: str,
    sent_at: str,
    rowid: int,
    sender: str = "wxid_demo_member",
) -> None:
    """Append one observed source message to the synthetic shard.

    Content lives only in the disposable provider source, so a bounded source
    discovery scan can genuinely find it; nothing is pre-admitted into window.db.
    """

    with closing(sqlite3.connect(root / "messages-1.db")) as connection:
        connection.execute(
            """
            INSERT INTO messages(
                source_message_id, source_conversation_id, source_time_raw,
                sent_at_utc, observed_at_utc, sort_seq, source_rowid,
                wechat_type, raw_content, is_outgoing, sender_internal_id,
                sender_local_token, sender_surface_label, resources_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                conversation_id,
                sent_at,
                sent_at,
                DEFAULT_OBSERVED_AT,
                1,
                rowid,
                1,
                content,
                0,
                sender,
                None,
                "原账号昵称",
                "[]",
            ),
        )
        connection.commit()


class ColdRetrievalDiscoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "source"
        create_synthetic_source(self.root)
        self.window = Path(self.temp.name) / "state" / "window.db"
        self._extra_rows = 0
        self._build()
        self.assertTrue(self.tools.wechat_status()["ready"])
        self.service.sync_source_once(conversation_limit=100)
        self.account = self.repository.active_account_ids()[0]
        self.group = next(
            str(row["conversation_id"])
            for row in self.repository.account_conversations(self.account)
            if row["kind"] == "group"
        )

    def _build(self) -> None:
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            self.root, self.window, residency_default="on_demand"
        )
        self.service.storage = StorageBudget(
            self.repository.database.path.parent,
            self.repository.database.path,
            StorageSettings(min_free_bytes=0),
        )
        self.repository.database.storage = self.service.storage
        self.store = ResidencyRepository(self.repository.database)
        self.worker = SourceWorker(self.service)
        self.manager = self._manager()

    def _manager(self) -> SearchPreparation:
        return SearchPreparation(
            self.service,
            self.window.with_name("search-preparation.json"),
            binding="synthetic-binding",
            lanes=RuntimeLanes(),
            source_worker=self.worker,
        )

    def _source_row(
        self,
        name: str,
        content: str,
        *,
        seconds: int = 0,
        conv: str = GROUP,
        sender: str = "wxid_demo_member",
    ) -> str:
        self._extra_rows += 1
        message_id = f"source-cold-{name}"
        _append_source_row(
            self.root,
            message_id=message_id,
            conversation_id=conv,
            content=content,
            sent_at=f"2026-09-20T00:00:{seconds:02d}+00:00",
            rowid=90_000 + self._extra_rows,
            sender=sender,
        )
        return message_id

    def _refresh(self) -> None:
        """Re-open the provider so it reports the appended source rows."""
        self.manager.stop()
        self.tools.close()
        self._build()

    def tearDown(self) -> None:
        self.manager.stop()
        self.tools.close()
        self.temp.cleanup()

    def _poll(self, call, token: str | None, deadline: float, **extra):
        while time.monotonic() < deadline:
            page = call(**extra, reading_token=token) if token else call(**extra)
            if page.get("schema") != DISCOVERY_SCHEMA:
                return page
            if page.get("state") == "failed":
                return page
            if page.get("state") == "expired":
                token = None
                continue
            if page.get("state") == "partial":
                token = None
                continue
            token = page.get("reading_token")
            time.sleep(0.01)
        self.fail(f"synthetic cold discovery did not finish: {self.manager._jobs}")

    def _ready(self, call, token: str | None, **extra):
        """Drive one cold request to a real result across bounded runs.

        Polls the preparation token; once a real result arrives, if the selected
        scope is still only partially prepared it keeps resuming (explicit continuation,
        which continues from the durable checkpoint) until the scope is complete.
        """

        self.manager.start()
        deadline = time.monotonic() + 30
        result = self._poll(call, token, deadline, **extra)
        rounds = 0
        while (
            isinstance(result.get("source_receipt"), dict)
            and result["source_receipt"].get("discovery_preparation", {}).get("complete") is False
            and rounds < 60
        ):
            continuation = result["source_receipt"]["source_continuation"]["reading_token"]
            result = self._poll(call, continuation, deadline, **extra)
            rounds += 1
        return result

    # -- empty source-only scope ------------------------------------------------

    def test_empty_source_only_scope_finds_link_positive(self) -> None:
        target = self._source_row(
            "link-target",
            "look here https://cold-discovery.example/path?q=synthetic#frag",
        )
        self._refresh()
        with self.repository.database.connection() as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM message_links").fetchone()[0], 0
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM read_lease").fetchone()[0], 0
            )
        self.assertTrue(
            self.service.local_only_tool_call(
                "wechat_find_links", {"domains": ["cold-discovery.example"]}
            )
        )

        pending = self.tools.wechat_find_links(domains=["cold-discovery.example"])
        self.assertEqual(pending["schema"], DISCOVERY_SCHEMA, pending)
        self.assertEqual(pending["state"], "preparing")
        self.assertNotIn("items", pending)
        result = self._ready(
            self.tools.wechat_find_links,
            pending["reading_token"],
            domains=["cold-discovery.example"],
        )
        self.assertEqual(result["schema"], "sightglass.link-search.v1", result)
        self.assertEqual(
            {item["normalized_host"] for item in result["items"]}, {"cold-discovery.example"}
        )
        self.assertTrue(result["source_receipt"]["freshness"]["prepared_subset"])
        self.assertIn("discovery_preparation", result["source_receipt"])
        with self.repository.database.connection() as connection:
            resident = {
                row[0]
                for row in connection.execute(
                    "SELECT source_message_id FROM messages WHERE body_available=1"
                )
            }
        self.assertIn(target, resident)

    def test_empty_source_only_scope_finds_retrieve_positive(self) -> None:
        self._source_row(
            "retrieve-target",
            f"discussion about {DISPOSABLE_SECRET} and https://retrieve-cold.example/x",
        )
        self._refresh()
        pending = self.tools.wechat_retrieve(DISPOSABLE_SECRET)
        self.assertEqual(pending["state"], "preparing", pending)
        result = self._ready(
            self.tools.wechat_retrieve, pending["reading_token"], concept=DISPOSABLE_SECRET
        )
        self.assertEqual(result["schema"], "sightglass.retrieval-results.v1", result)
        focus = {
            message_id
            for context in result["contexts"]
            for message_id in context["focus_message_ids"]
        }
        self.assertTrue(focus)

    # -- released stock whose derived rows are gone ------------------------------

    def test_released_scope_rediscovers_positive(self) -> None:
        self._source_row(
            "release-target", "cached https://released-cold.example/a?token=synthetic"
        )
        self._refresh()
        self.store.set(self.group, mode="keep")
        self.service.sync_source_once(conversation_limit=100, initial_tail=100)
        from sightglass.model.links import LinkRepository

        links = LinkRepository(self.repository.database)
        while links.backfill_batch()["state"] != "ready":
            pass
        self.store.set(self.group, mode="on_demand")
        self.store.release_stock(self.group, plan=self.store.stock_preview(self.group)["plan"])
        with self.repository.database.connection() as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM message_links").fetchone()[0], 0
            )
        pending = self.tools.wechat_find_links(domains=["released-cold.example"])
        self.assertEqual(pending["state"], "preparing", pending)
        result = self._ready(
            self.tools.wechat_find_links,
            pending["reading_token"],
            domains=["released-cold.example"],
        )
        self.assertEqual(
            {item["normalized_host"] for item in result["items"]}, {"released-cold.example"}
        )

    # -- expired bodies with stale index rows -----------------------------------

    def test_expired_bodies_with_stale_index_rows_still_discover(self) -> None:
        self._source_row("expiry-target", "https://expiry-cold.example/x")
        self._refresh()
        self.store.set(self.group, mode="keep")
        self.service.sync_source_once(conversation_limit=100, initial_tail=100)
        from sightglass.model.links import LinkRepository

        links = LinkRepository(self.repository.database)
        while links.backfill_batch()["state"] != "ready":
            pass
        self.store.set(self.group, mode="on_demand")
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE message_body_residency SET expires_at=? WHERE conversation_id=?",
                ((datetime.now(UTC) - timedelta(days=1)).isoformat(), self.group),
            )
        with self.repository.database.connection() as connection:
            self.assertGreater(
                connection.execute("SELECT COUNT(*) FROM message_links").fetchone()[0], 0
            )
        self.assertFalse(
            self.repository.retrieval_candidate_resident(
                (self.group,),
                epoch=self.service._projection_inventory_epoch(),
            )
        )
        self.assertTrue(
            self.service.local_only_tool_call(
                "wechat_find_links", {"domains": ["expiry-cold.example"]}
            )
        )
        pending = self.tools.wechat_find_links(domains=["expiry-cold.example"])
        self.assertEqual(pending["state"], "preparing", pending)
        result = self._ready(
            self.tools.wechat_find_links,
            pending["reading_token"],
            domains=["expiry-cold.example"],
        )
        self.assertEqual(
            {item["normalized_host"] for item in result["items"]}, {"expiry-cold.example"}
        )

    # -- warm semantics stay intact ---------------------------------------------

    def test_warm_complete_scope_never_opens_source(self) -> None:
        self._source_row("warm-link", "https://warm-cold.example/keep")
        self._refresh()
        self.store.set(self.group, mode="keep")
        self.service.sync_source_once(conversation_limit=100, initial_tail=100)
        from sightglass.model.links import LinkRepository

        while LinkRepository(self.repository.database).backfill_batch()["state"] != "ready":
            pass
        # Keep the real manager installed: a genuinely warm, completely covered
        # scope must be classified local and answer without opening the source.
        self.assertTrue(
            self.service.local_only_tool_call(
                "wechat_find_links",
                {"domains": ["warm-cold.example"], "conversation_ids": [self.group]},
            )
        )
        with patch.object(
            self.provider, "snapshot", side_effect=AssertionError("warm scope opened source")
        ):
            result = self.tools.wechat_find_links(
                domains=["warm-cold.example"], conversation_ids=[self.group]
            )
        self.assertEqual(result["schema"], "sightglass.link-search.v1", result)
        self.assertEqual(
            {item["normalized_host"] for item in result["items"]}, {"warm-cold.example"}
        )

    # -- truthful continuation, scope, privacy ----------------------------------

    def test_cold_first_page_is_not_an_empty_result(self) -> None:
        pending = self.tools.wechat_find_links(domains=["absent-cold.example"])
        self.assertEqual(pending["schema"], DISCOVERY_SCHEMA, pending)
        self.assertEqual(pending["state"], "preparing")
        self.assertNotIn("items", pending)
        self.assertFalse(pending["results_complete"])

    def test_query_text_never_enters_the_private_sidecar(self) -> None:
        self._source_row("private-target", f"secret body {DISPOSABLE_SECRET}")
        self._refresh()
        pending = self.tools.wechat_retrieve(DISPOSABLE_SECRET)
        self.assertEqual(pending["state"], "preparing", pending)
        disk = self.window.with_name("search-preparation.json").read_text()
        self.assertNotIn(DISPOSABLE_SECRET, disk)
        self.assertNotIn('"query":', disk)
        self.assertNotIn(DISPOSABLE_SECRET, pending["reading_token"])

    def test_private_source_checkpoint_never_enters_results_or_cursor(self) -> None:
        for index in range(4):
            self._source_row(f"private-binding-{index}",
                             f"https://private-binding.example/{index}", seconds=index)
        self._refresh()
        pending = self.tools.wechat_find_links(
            domains=["private-binding.example"], conversation_ids=[self.group], limit=1
        )
        result = self._ready(
            self.tools.wechat_find_links, pending["reading_token"],
            domains=["private-binding.example"], conversation_ids=[self.group], limit=1,
        )
        cursor = result["page"]["next_cursor"]
        self.assertIsNotNone(cursor)
        decoded = self.service.token_codec.decode(cursor)
        for exposed in (result, decoded):
            serialized = json.dumps(exposed)
            for private in ("generations", "resume_checkpoint", "message-shard-1",
                            "generation-1a", "source-cold-private-binding"):
                self.assertNotIn(private, serialized)

    def test_domain_and_conversation_scope_are_hard_constraints(self) -> None:
        self._source_row("group-target", "https://scope-cold.example/group", conv=GROUP)
        self._source_row("direct-target", "https://scope-cold.example/direct", conv=DIRECT)
        self._refresh()
        pending = self.tools.wechat_find_links(
            domains=["scope-cold.example"], conversation_ids=[self.group]
        )
        self.assertEqual(pending["state"], "preparing", pending)
        result = self._ready(
            self.tools.wechat_find_links,
            pending["reading_token"],
            domains=["scope-cold.example"],
            conversation_ids=[self.group],
        )
        self.assertTrue(result["items"])
        self.assertTrue(
            all(
                item["conversation"]["conversation_id"] == self.group
                for item in result["items"]
            )
        )

    def test_link_query_and_domain_must_match_the_same_observed_link(self) -> None:
        keep = self._source_row("and-keep", "https://and-cold.example/needle")
        skip = self._source_row("and-skip", "https://and-cold.example/unrelated")
        self._refresh()
        result = self._ready(self.tools.wechat_find_links, None,
            query="needle", domains=["and-cold.example"], conversation_ids=[self.group])
        self.assertEqual(len(result["items"]), 1, result)
        with self.repository.database.connection() as connection:
            rows = {row[0] for row in connection.execute(
                "SELECT source_message_id FROM messages WHERE body_available=1")}
        self.assertIn(keep, rows)
        self.assertNotIn(skip, rows)

    def test_retrieval_can_recall_a_plain_text_hint(self) -> None:
        self._source_row("text-hint", "synthetic auxiliaryneedle discussion")
        self._refresh()
        result = self._ready(self.tools.wechat_retrieve, None,
            concept="nonliteral concept", hints=["auxiliaryneedle"],
            conversation_ids=[self.group])
        self.assertTrue(result["contexts"], result)

    def _source_burst(self, target_text: str, *, later: int, conv: str = GROUP) -> str:
        """Append one older matching target plus ``later`` newer unrelated rows.

        The unrelated rows are strictly newer, so a single most-recent source page
        exposes only them; the target is reachable only by bounded incremental
        traversal deeper into the conversation.
        """

        message_id = self._source_row("older-positive", target_text, seconds=0, conv=conv)
        for index in range(1, later + 1):
            self._source_row(f"unrelated-{index}", f"unrelated chatter {index}",
                             seconds=index, conv=conv)
        return message_id

    def test_links_beyond_first_page_and_beyond_one_batch(self) -> None:
        self._source_burst("https://older-positive.example/a", later=59)
        self._refresh()
        pending = self.tools.wechat_find_links(
            domains=["older-positive.example"], conversation_ids=[self.group]
        )
        self.assertEqual(pending["state"], "preparing", pending)
        result = self._ready(
            self.tools.wechat_find_links,
            pending["reading_token"],
            domains=["older-positive.example"],
            conversation_ids=[self.group],
        )
        self.assertEqual(result["schema"], "sightglass.link-search.v1", result)
        self.assertEqual(
            {item["normalized_host"] for item in result["items"]}, {"older-positive.example"}
        )
        self.assertTrue(result["source_receipt"]["discovery_preparation"]["complete"])

    def test_retrieve_beyond_first_page_and_beyond_one_batch(self) -> None:
        self._source_burst(
            f"older positive discussion about {DISPOSABLE_SECRET} https://older-retrieve.example/x",
            later=59,
        )
        self._refresh()
        pending = self.tools.wechat_retrieve(
            DISPOSABLE_SECRET, conversation_ids=[self.group]
        )
        self.assertEqual(pending["state"], "preparing", pending)
        result = self._ready(
            self.tools.wechat_retrieve,
            pending["reading_token"],
            concept=DISPOSABLE_SECRET,
            conversation_ids=[self.group],
        )
        self.assertEqual(result["schema"], "sightglass.retrieval-results.v1", result)
        self.assertTrue(result["contexts"])
        self.assertTrue(result["source_receipt"]["discovery_preparation"]["complete"])

    def test_links_traversal_continues_across_multiple_attempts(self) -> None:
        # More unrelated rows than one attempt may scan, so the job needs several
        # bounded attempts; each keeps a durable source position.
        self._source_burst("https://deep-positive.example/a", later=12)
        self._refresh()
        pending = self.tools.wechat_find_links(
            domains=["deep-positive.example"], conversation_ids=[self.group]
        )
        token = pending["reading_token"]
        # A first poll immediately after start must stay preparing (not empty).
        with patch(
            "sightglass.reader.service.DISCOVERY_CONVERSATION_SCAN_BUDGET", 8
        ):
            self.manager.start()
            first = self.tools.wechat_find_links(
                domains=["deep-positive.example"],
                conversation_ids=[self.group],
                reading_token=token,
            )
        self.assertEqual(first["schema"], DISCOVERY_SCHEMA, first)
        self.assertEqual(first["state"], "preparing", first)
        result = self._ready(
            self.tools.wechat_find_links,
            token,
            domains=["deep-positive.example"],
            conversation_ids=[self.group],
        )
        self.assertEqual(
            {item["normalized_host"] for item in result["items"]}, {"deep-positive.example"}
        )

    def test_cold_retrieve_materializes_neighbor_link_evidence(self) -> None:
        # A textual question without hostname hint, and a URL-only answer beside it.
        self._source_row(
            "question",
            f"where is the {DISPOSABLE_SECRET} companion tool",
            seconds=0,
        )
        self._source_row(
            "answer",
            "https://neighbor-link.example/companion",
            seconds=2,
        )
        self._refresh()
        pending = self.tools.wechat_retrieve(
            DISPOSABLE_SECRET, conversation_ids=[self.group]
        )
        self.assertEqual(pending["state"], "preparing", pending)
        result = self._ready(
            self.tools.wechat_retrieve,
            pending["reading_token"],
            concept=DISPOSABLE_SECRET,
            conversation_ids=[self.group],
        )
        self.assertTrue(result["contexts"], result)
        first = result["contexts"][0]
        self.assertIn("neighbor-link.example", {link["normalized_host"] for link in first["links"]})
        self.assertGreater(len(first["messages"]), 0)

    def test_context_crosses_page_edge_and_stays_bounded_in_dense_chat(self) -> None:
        for index in range(100):
            self._source_row(f"dense-{index}", "synthetic unrelated chatter", seconds=30)
        self._source_row("edge-answer", "https://edge-context.example/answer", seconds=30)
        self._source_row("edge-question", DISPOSABLE_SECRET, seconds=30)
        for index in range(4):
            self._source_row(f"newer-{index}", "synthetic newer row", seconds=40 + index)
        self._refresh()
        actual = self.provider.scan_discovery_page

        def small_discovery_page(*args, **kwargs):
            # Force the focus onto a discovery-page edge; context reads retain
            # their own bounded radius and must reach the unseen next page.
            kwargs["limit"] = min(5, kwargs["limit"])
            return actual(*args, **kwargs)

        with patch.object(self.provider, "prepare_search_page",
                          side_effect=AssertionError("repeated full preparation scan")), \
             patch.object(self.provider, "scan_discovery_page", side_effect=small_discovery_page):
            result = self._ready(self.tools.wechat_retrieve, None,
                concept=DISPOSABLE_SECRET, conversation_ids=[self.group])
        self.assertIn("edge-context.example", {
            link["normalized_host"] for context in result["contexts"]
            for link in context["links"]
        })
        facts = result["source_receipt"]["discovery_preparation"]
        self.assertEqual(facts["matched_message_count"], 1)
        self.assertEqual(facts["pending_conversation_count"], 0)
        self.assertLessEqual(facts["message_count"], 17)
        self.assertLessEqual(facts["scanned_row_count"], facts["scan_budget"])
        self.assertFalse(result["source_receipt"]["complete"])

    def test_partial_polls_and_duplicate_continuations_do_not_advance_twice(self) -> None:
        self._source_burst("https://explicit-resume.example/a", later=12)
        self._refresh()
        arguments: dict[str, Any] = {"domains": ["explicit-resume.example"],
                     "conversation_ids": [self.group]}
        first = self.tools.wechat_find_links(**arguments)
        token = first["reading_token"]
        job = next(iter(self.manager._jobs.values()))
        with patch("sightglass.reader.service.DISCOVERY_CONVERSATION_SCAN_BUDGET", 5):
            self.manager._attempt(job)
            checkpoint = json.loads(json.dumps(job["checkpoint"]))
            for _ in range(10):
                partial = self.tools.wechat_find_links(**arguments, reading_token=token)
                self.assertEqual(job["checkpoint"], checkpoint)
                self.assertEqual(job["attempts"], 1)
            continuation = partial["source_receipt"]["source_continuation"]["reading_token"]
            resumed = self.tools.wechat_find_links(**arguments, reading_token=continuation)
            self.assertEqual(resumed["state"], "preparing")
            self.manager._attempt(job)
            self.assertEqual(job["attempts"], 2)
            same = self.tools.wechat_find_links(**arguments, reading_token=continuation)
            self.assertEqual(same["schema"], "sightglass.link-search.v1")
            self.assertEqual(job["state"], "partial")
            self.assertEqual(job["attempts"], 2)
            # A genuinely fresh request starts a fresh query, leaving old progress.
            fresh = self.tools.wechat_find_links(**arguments)
            self.assertNotEqual(fresh["reading_token"], token)
            self.assertEqual(len(self.manager._jobs), 2)

    def test_explicit_reply_parent_respects_time_scope_before_admission(self) -> None:
        from sightglass.source.message_identity import native_message_token

        original = native_message_token(GROUP, "server", (731,))
        _append_source_row(self.root, message_id=original, conversation_id=GROUP,
            content="synthetic parent outside the context radius",
            sent_at="2026-09-19T00:00:00+00:00", rowid=731,
            sender="wxid_demo_member")
        reply = self._source_row("explicit-reply", "placeholder", seconds=30)
        with closing(sqlite3.connect(self.root / "messages-1.db")) as connection:
            connection.execute(
                "UPDATE messages SET wechat_type=49,source_message_id=?,raw_content=? "
                "WHERE source_message_id=?", (
                    native_message_token(GROUP, "server", (732,)),
                    "<msg><appmsg><type>57</type><title>synthetic-reply-needle</title>"
                    "<refermsg><svrid>731</svrid><content>synthetic quote</content>"
                    "</refermsg></appmsg></msg>", reply))
            connection.commit()
        self._refresh()
        bounded = self._ready(self.tools.wechat_retrieve, None,
            concept="synthetic-reply-needle", conversation_ids=[self.group],
            after="2026-09-20T00:00:00+00:00")
        self.assertTrue(bounded["contexts"], bounded)
        with self.repository.database.connection() as connection:
            self.assertIsNone(connection.execute(
                "SELECT 1 FROM messages WHERE source_message_id=?", (original,)
            ).fetchone())
        whole = self._ready(self.tools.wechat_retrieve, None,
            concept="synthetic-reply-needle", conversation_ids=[self.group])
        self.assertTrue(whole["contexts"][0]["reply_edges"], whole)

    def test_kind_filter_does_not_cache_an_unrelated_text_focus(self) -> None:
        target = self._source_row("wrong-kind", "synthetic-kind-needle")
        self._refresh()
        result = self._ready(self.tools.wechat_retrieve, None,
            concept="synthetic-kind-needle", kinds=["image"],
            conversation_ids=[self.group])
        self.assertEqual(result["schema"], "sightglass.retrieval-results.v1", result)
        with self.repository.database.connection() as connection:
            self.assertIsNone(connection.execute(
                "SELECT 1 FROM messages WHERE source_message_id=?", (target,)
            ).fetchone())

    def test_hint_never_drops_a_plain_lexical_concept_hit(self) -> None:
        self._source_row(
            "lexical-only",
            f"the body mentions {DISPOSABLE_SECRET} with no matching host",
            seconds=0,
        )
        self._refresh()
        pending = self.tools.wechat_retrieve(
            DISPOSABLE_SECRET,
            hints=["unmatched-hostname.example"],
            conversation_ids=[self.group],
        )
        self.assertEqual(pending["state"], "preparing", pending)
        result = self._ready(
            self.tools.wechat_retrieve,
            pending["reading_token"],
            concept=DISPOSABLE_SECRET,
            hints=["unmatched-hostname.example"],
            conversation_ids=[self.group],
        )
        self.assertTrue(result["contexts"], result)

    def test_participant_scope_routes_cold_and_filters(self) -> None:
        self._source_row(
            "participant-target",
            f"{DISPOSABLE_SECRET} from one speaker",
            seconds=0,
            sender="wxid_demo_member",
        )
        self._source_row(
            "participant-other",
            f"{DISPOSABLE_SECRET} from another speaker",
            seconds=2,
            sender="wxid_demo_member2",
        )
        self._refresh()
        candidate = self.tools.wechat_find_participants(
            conversation_id=self.group, query="demo_member"
        )["candidates"]
        self.assertTrue(candidate)
        participant_id = candidate[0]["participant_id"]
        # A cold participant-scoped call must not be misclassified as warm.
        self.assertTrue(
            self.service.local_only_tool_call(
                "wechat_retrieve",
                {"concept": DISPOSABLE_SECRET, "participant_ids": [participant_id]},
            )
        )
        pending = self.tools.wechat_retrieve(
            DISPOSABLE_SECRET,
            participant_ids=[participant_id],
            conversation_ids=[self.group],
        )
        self.assertEqual(pending["state"], "preparing", pending)
        result = self._ready(
            self.tools.wechat_retrieve,
            pending["reading_token"],
            concept=DISPOSABLE_SECRET,
            participant_ids=[participant_id],
            conversation_ids=[self.group],
        )
        self.assertEqual(result["schema"], "sightglass.retrieval-results.v1", result)

    def test_discovery_token_accepted_via_cursor_for_compat(self) -> None:
        self._source_row("cursor-compat", "https://cursor-compat.example/a", seconds=0)
        self._refresh()
        pending = self.tools.wechat_find_links(domains=["cursor-compat.example"])
        token = pending["reading_token"]
        self.manager.start()
        deadline = time.monotonic() + 20
        result = None
        while time.monotonic() < deadline:
            result = self.tools.wechat_find_links(
                domains=["cursor-compat.example"], cursor=token
            )
            if result.get("schema") != DISCOVERY_SCHEMA:
                break
            if result.get("state") in {"failed", "expired"}:
                break
            time.sleep(0.01)
        assert result is not None
        self.assertEqual(result["schema"], "sightglass.link-search.v1", result)

    def test_partial_preparation_never_claims_complete_source_scope(self) -> None:
        # One matching target behind more unrelated rows than one bounded run scans.
        self._source_burst("https://partial.example/a", later=12)
        self._refresh()
        self.manager.start()
        with patch("sightglass.reader.service.DISCOVERY_CONVERSATION_SCAN_BUDGET", 5):
            pending = self.tools.wechat_find_links(
                domains=["partial.example"], conversation_ids=[self.group]
            )
            token = pending["reading_token"]
            deadline = time.monotonic() + 10
            result = None
            while time.monotonic() < deadline:
                result = self.tools.wechat_find_links(
                    domains=["partial.example"],
                    conversation_ids=[self.group],
                    reading_token=token,
                )
                if result.get("schema") != DISCOVERY_SCHEMA:
                    break
                if result.get("state") == "failed":
                    break
                token = result.get("reading_token")
                time.sleep(0.01)
        self.assertIsNotNone(result)
        assert result is not None
        # A bounded run that did not finish must expose the prepared subset with an
        # honest incomplete receipt and an actionable continuation, never a
        # complete-scope claim and never an indefinite "preparing".
        self.assertEqual(result["schema"], "sightglass.link-search.v1", result)
        preparation = result["source_receipt"]["discovery_preparation"]
        self.assertFalse(preparation["complete"])
        self.assertFalse(result["source_receipt"]["complete"])
        self.assertTrue(result["source_receipt"]["source_continuation"]["available"])
        # The saved position makes real forward progress on the next run.
        self.assertTrue(next(iter(self.manager._jobs.values()))["checkpoint"]["positions"])

    def test_warm_routing_keeps_real_manager_and_avoids_source(self) -> None:
        self._source_row("warm-manager", "https://warm-manager.example/keep", seconds=0)
        self._refresh()
        self.store.set(self.group, mode="keep")
        self.service.sync_source_once(conversation_limit=100, initial_tail=100)
        from sightglass.model.links import LinkRepository

        while LinkRepository(self.repository.database).backfill_batch()["state"] != "ready":
            pass
        self.assertTrue(
            self.service.local_only_tool_call(
                "wechat_find_links",
                {"domains": ["warm-manager.example"], "conversation_ids": [self.group]},
            )
        )
        with patch.object(
            self.provider, "snapshot", side_effect=AssertionError("warm manager opened source")
        ):
            result = self.tools.wechat_find_links(
                domains=["warm-manager.example"], conversation_ids=[self.group]
            )
        self.assertEqual(result["schema"], "sightglass.link-search.v1", result)

    def test_selected_shard_replacement_fails_stale_after_restart(self) -> None:
        # Two conversations to prepare so interruption can checkpoint the first.
        self._source_row("gen-group", "https://gen-cold.example/group", conv=GROUP)
        self._source_row("gen-direct", "https://gen-cold.example/direct", conv=DIRECT)
        self._refresh()
        pending = self.tools.wechat_find_links(
            domains=["gen-cold.example"], conversation_ids=[self.group, self.direct_id()]
        )
        token = pending["reading_token"]
        job = next(iter(self.manager._jobs.values()))
        from sightglass.runtime.search_preparation import SearchPreparation  # noqa: F401

        actual = self.provider.scan_discovery_page
        calls: list[str] = []

        def interrupt_after_first(account, conversation, **kwargs):
            calls.append(conversation)
            if len(calls) == 2:
                self.manager._stop.set()
                from sightglass.operations import check_operation_budget

                check_operation_budget()
            return actual(account, conversation, **kwargs)

        with patch.object(self.provider, "scan_discovery_page", side_effect=interrupt_after_first):
            self.manager._attempt(job)
        self.assertEqual(job["state"], "preparing", job)
        self.assertEqual(job["checkpoint"]["done"], 1)
        self.assertTrue(job["checkpoint"]["generations"])

        # Replace the selected shard generation, then resume the job.
        manifest_path = self.root / "source.json"
        import json as _json

        manifest = _json.loads(manifest_path.read_text())
        for shard in manifest["shards"]:
            shard["generation_id"] = (
                "generation-1b"
                if shard["logical_key"] == "message-shard-1"
                else "generation-2b"
            )
        manifest_path.write_text(_json.dumps(manifest))
        self.manager = self._manager()
        self.manager.start()
        deadline = time.monotonic() + 20
        result = None
        while time.monotonic() < deadline:
            result = self.tools.wechat_find_links(
                domains=["gen-cold.example"],
                conversation_ids=[self.group, self.direct_id()],
                reading_token=token,
            )
            if result.get("state") == "failed" or result.get("schema") != DISCOVERY_SCHEMA:
                break
            if result.get("schema") == DISCOVERY_SCHEMA and result.get("state") == "ready":
                break
            time.sleep(0.01)
        assert result is not None
        if result.get("schema") == DISCOVERY_SCHEMA and result.get("state") == "failed":
            self.assertEqual(result["error"]["code"], "SOURCE_GENERATION_CHANGED")

    def test_rejected_admission_leaves_no_identity_writes(self) -> None:
        self._source_row(
            "reject-target",
            f"{DISPOSABLE_SECRET} https://reject-cold.example/x",
            seconds=0,
            sender="wxid_demo_member",
        )
        self._refresh()
        before = self._participant_and_alias_rows()
        pending = self.tools.wechat_find_links(domains=["reject-cold.example"])
        token = pending["reading_token"]
        from sightglass.contracts.errors import (
            ErrorCode as _ErrorCode,
        )
        from sightglass.contracts.errors import (
            SightglassError as _Err,
        )

        def reject(context, messages, **kwargs):
            # A failure after canonical resolution but before commit must roll back
            # every identity/alias write from the same short admission transaction.
            raise _Err(_ErrorCode.STORAGE_PRESSURE)

        with patch.object(self.service, "_ingest_prepared_messages", side_effect=reject):
            self.manager.start()
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                page = self.tools.wechat_find_links(
                    domains=["reject-cold.example"], reading_token=token
                )
                if page.get("state") == "failed":
                    break
                time.sleep(0.02)
        self.assertEqual(self._participant_and_alias_rows(), before)

    def _participant_and_alias_rows(self):
        """Stable identity surface: participant IDs and alias keys, no timestamps."""

        with self.repository.database.connection() as connection:
            participants = {
                str(row[0])
                for row in connection.execute("SELECT participant_id FROM participants")
            }
            aliases = {
                (str(row[0]), str(row[1]))
                for row in connection.execute(
                    "SELECT conversation_id, normalized_alias FROM conversation_aliases"
                )
            }
        return participants, aliases

    def direct_id(self) -> str:
        return next(
            str(row["conversation_id"])
            for row in self.repository.account_conversations(self.account)
            if row["kind"] == "direct"
        )

    def test_ready_poll_in_local_only_scope_returns_results_and_receipt(self) -> None:
        self._source_row("local-ready", "https://local-ready.example/a", seconds=0)
        self._refresh()
        pending = self.tools.wechat_find_links(domains=["local-ready.example"])
        token = pending["reading_token"]
        self.manager.start()
        deadline = time.monotonic() + 20
        # Wait for the job to become ready without a source-backed read.
        while time.monotonic() < deadline:
            job = next(iter(self.manager._jobs.values()))
            if job["state"] == "ready":
                break
            time.sleep(0.01)
        self.assertEqual(next(iter(self.manager._jobs.values()))["state"], "ready")
        from sightglass.operations import local_read_only_scope

        with (
            local_read_only_scope(),
            patch.object(
                self.provider,
                "snapshot",
                side_effect=AssertionError("local ready poll opened source"),
            ),
        ):
            result = self.tools.wechat_find_links(
                domains=["local-ready.example"], reading_token=token
            )
        self.assertEqual(result["schema"], "sightglass.link-search.v1", result)
        self.assertEqual(
            {item["normalized_host"] for item in result["items"]}, {"local-ready.example"}
        )
        self.assertIn("discovery_preparation", result["source_receipt"])

    def test_preparing_poll_does_not_require_foreground_source(self) -> None:
        self._source_row("slow-poll", "https://slow-poll.example/a", seconds=0)
        self._refresh()
        pending = self.tools.wechat_find_links(domains=["slow-poll.example"])
        token = pending["reading_token"]
        # A poll never samples the provider itself; only the worker does.
        with patch.object(
            self.provider, "snapshot", side_effect=AssertionError("poll opened source")
        ):
            page = self.tools.wechat_find_links(
                domains=["slow-poll.example"], reading_token=token
            )
        self.assertEqual(page["schema"], DISCOVERY_SCHEMA, page)
        self.assertEqual(page["state"], "preparing", page)

    def test_pagination_continuation_keeps_preparation_status(self) -> None:
        for index in range(4):
            self._source_row(
                f"page-{index}", "https://paged-cold.example/a", seconds=index
            )
        self._refresh()
        pending = self.tools.wechat_find_links(
            domains=["paged-cold.example"], limit=1, conversation_ids=[self.group]
        )
        result = self._ready(
            self.tools.wechat_find_links,
            pending["reading_token"],
            domains=["paged-cold.example"],
            limit=1,
            conversation_ids=[self.group],
        )
        self.assertEqual(result["schema"], "sightglass.link-search.v1", result)
        cursor = result["page"]["next_cursor"]
        self.assertIsNotNone(cursor)
        next_page = self.tools.wechat_find_links(
            domains=["paged-cold.example"],
            limit=1,
            conversation_ids=[self.group],
            cursor=cursor,
        )
        self.assertEqual(next_page["schema"], "sightglass.link-search.v1", next_page)
        self.assertIn("discovery_preparation", next_page["source_receipt"])

    def test_more_than_message_budget_terminates_with_continuation(self) -> None:
        # >200 matching rows: one bounded run must stop with usable results and a
        # real continuation instead of looping forever, and the continuation must
        # reach the remaining matches.
        host = "budget-positive.example"
        for index in range(210):
            self._source_row(f"budget-{index}", f"https://{host}/a{index}",
                             seconds=index % 60, conv=GROUP)
        self._refresh()
        self.manager.start()
        pending = self.tools.wechat_find_links(
            domains=[host], conversation_ids=[self.group], limit=1
        )
        self.assertEqual(pending["schema"], DISCOVERY_SCHEMA, pending)
        token = pending["reading_token"]
        deadline = time.monotonic() + 30
        first_partial = None
        while time.monotonic() < deadline:
            result = self.tools.wechat_find_links(
                domains=[host], conversation_ids=[self.group], limit=1, reading_token=token
            )
            if result.get("schema") != DISCOVERY_SCHEMA:
                first_partial = result
                break
            if result.get("state") == "failed":
                self.fail(result)
            token = result.get("reading_token")
            time.sleep(0.01)
        self.assertIsNotNone(first_partial)
        assert first_partial is not None
        self.assertEqual(first_partial["schema"], "sightglass.link-search.v1", first_partial)
        preparation = first_partial["source_receipt"]["discovery_preparation"]
        self.assertFalse(preparation["complete"])
        self.assertTrue(first_partial["source_receipt"]["source_continuation"]["available"])
        # The bounded run exposed a real item, not an empty indefinite state.
        self.assertTrue(first_partial["items"])
        # Continuing eventually reaches every match and reports completion.
        final = self._ready(
            self.tools.wechat_find_links,
            first_partial["source_receipt"]["source_continuation"]["reading_token"],
            domains=[host], conversation_ids=[self.group], limit=1,
        )
        self.assertTrue(final["source_receipt"]["discovery_preparation"]["complete"])

    def test_each_bounded_run_reports_honest_scan_and_terminates(self) -> None:
        # A cold scope with several pages must terminate every bounded run with
        # usable/progressing evidence, and never spin: each run scans a bounded
        # number of rows and either completes or exposes a resume checkpoint.
        host = "bounded-positive.example"
        for index in range(120):
            self._source_row(f"bounded-{index}", f"https://{host}/a{index}",
                             seconds=index % 60, conv=GROUP)
        self._refresh()
        self.manager.start()
        pending = self.tools.wechat_find_links(
            domains=[host], conversation_ids=[self.group], limit=1
        )
        token = pending["reading_token"]
        deadline = time.monotonic() + 30
        runs = 0
        while time.monotonic() < deadline:
            result = self.tools.wechat_find_links(
                domains=[host], conversation_ids=[self.group], limit=1, reading_token=token
            )
            if result.get("schema") != DISCOVERY_SCHEMA:
                runs += 1
                preparation = result["source_receipt"]["discovery_preparation"]
                self.assertLessEqual(
                    preparation["message_count"], SEARCH_PREPARATION_MESSAGE_BUDGET
                )
                if preparation["complete"]:
                    break
                # Continue with the explicitly returned continuation token.
                token = result["source_receipt"]["source_continuation"]["reading_token"]
                continue
            self.assertNotEqual(result.get("state"), "failed")
            token = result.get("reading_token")
            time.sleep(0.01)
        # Bounded number of runs (progress each time), not an unbounded spin.
        self.assertLess(runs, 30)

    # -- bounded worker deadline after a committed page ---------------------------

    def _deadline_after_first_committed_page(self, host, *, fail_metadata=False, observe=None):
        """Commit page one, then hit the worker deadline on the second page."""
        self._source_row(f"{host}-late-positive", f"https://{host}/remaining-positive")
        for index in range(80):
            self._source_row(f"{host}-noise-{index}", "synthetic noise without URL",
                             seconds=index % 60)
        self._source_row(f"{host}-early-positive", f"https://{host}/known-positive")
        self._refresh()
        arguments: dict[str, Any] = {"domains": [host], "conversation_ids": [self.group]}
        first = self.tools.wechat_find_links(**arguments)
        job = next(iter(self.manager._jobs.values()))
        scan = self.provider.scan_discovery_page
        pages = {"n": 0}

        def slow_second_page(*args, **kwargs):
            pages["n"] += 1
            if pages["n"] == 2:
                time.sleep(0.55)
            return scan(*args, **kwargs)

        with ExitStack() as patches:
            patches.enter_context(patch("sightglass.runtime.search_preparation.ATTEMPT_SECONDS",
                                        0.5))
            patches.enter_context(patch.object(
                self.provider, "scan_discovery_page", side_effect=slow_second_page
            ))
            if observe is not None:
                save = self.manager._save
                fail = self.manager._fail

                def fail_save(*, maintenance=False):
                    if maintenance and (job["state"] == "partial" or fail_metadata):
                        raise OSError("synthetic terminal metadata commit failure")
                    return save(maintenance=maintenance)

                def observe_before_failure(failed_job, error):
                    observer = threading.Thread(target=lambda: observe.append(
                        self.tools.wechat_find_links(**arguments,
                                                     reading_token=first["reading_token"])
                    ))
                    observer.start()
                    observer.join(timeout=3)
                    self.assertFalse(observer.is_alive(), "publication lock was not released")
                    fail(failed_job, error)

                patches.enter_context(patch.object(self.manager, "_save", side_effect=fail_save))
                patches.enter_context(patch.object(self.manager, "_fail",
                                                   side_effect=observe_before_failure))
            self.manager._attempt(job)
        return job, arguments, first

    def test_worker_deadline_after_committed_page_publishes_resumable_partial(self) -> None:
        host = "deadline-probe.example"
        job, arguments, first = self._deadline_after_first_committed_page(host)
        self.assertEqual(job["state"], "partial", job)
        checkpoint = job["checkpoint"]
        self.assertTrue(self.manager._checkpoint_has_progress(checkpoint))
        self.assertTrue(checkpoint.get("positions"))
        self.assertTrue(checkpoint.get("generations"))
        poll = self.tools.wechat_find_links(**arguments, reading_token=first["reading_token"])
        self.assertIsNone(poll.get("error"), poll)
        self.assertIs(poll["source_receipt"]["discovery_preparation"]["complete"], False)
        continuation = poll["source_receipt"]["source_continuation"]["reading_token"]
        self.assertTrue(continuation)
        preparation = poll["source_receipt"]["discovery_preparation"]
        self.assertGreaterEqual(preparation["message_count"], 1)
        self.assertEqual(preparation["message_budget"], 200)
        self.assertEqual(preparation["scan_budget"], 1000)
        self.assertEqual(preparation["conversation_budget"], 25)
        for unknown in ("scanned_row_count", "admitted_this_attempt"):
            self.assertIsNone(preparation.get(unknown), preparation)
        with self.repository.database.connection() as connection:
            residents = connection.execute(
                "SELECT COUNT(*) FROM messages WHERE body_available=1"
            ).fetchone()[0]
        self.assertEqual(residents, 1)
        resumed = self.tools.wechat_find_links(**arguments, reading_token=continuation)
        self.assertIsNone(resumed.get("error"))

    def test_deadline_partial_restart_reuses_committed_checkpoint(self) -> None:
        host = "deadline-restart.example"
        job, arguments, first = self._deadline_after_first_committed_page(host)
        self.assertEqual(job["state"], "partial", job)
        continuation = self.tools.wechat_find_links(
            **arguments, reading_token=first["reading_token"]
        )["source_receipt"]["source_continuation"]["reading_token"]
        checkpoint = json.loads(json.dumps(job["checkpoint"]))
        self.manager.stop()
        self.tools.close()
        self._build()
        self.manager.start()
        deadline = time.monotonic() + 20
        result = None
        while time.monotonic() < deadline:
            result = self.tools.wechat_find_links(**arguments, reading_token=continuation)
            if result.get("schema") != DISCOVERY_SCHEMA:
                break
            time.sleep(0.02)
        assert result is not None
        restarted_job = next(
            value for value in self.manager._jobs.values() if value.get("kind") == "discovery"
        )
        self.assertEqual(restarted_job["checkpoint"]["chosen"], checkpoint["chosen"])
        self.assertEqual(restarted_job["run"], 1)
        self.assertEqual(restarted_job["state"], "ready")
        self.assertEqual({item["path"] for item in result["items"]},
                         {"/known-positive", "/remaining-positive"})
        self.assertEqual(restarted_job["checkpoint"]["matched_count"], 2)

    def test_deadline_partial_continuation_is_idempotent_and_scope_bound(self) -> None:
        host = "deadline-scope.example"
        job, arguments, first = self._deadline_after_first_committed_page(host)
        poll = self.tools.wechat_find_links(**arguments, reading_token=first["reading_token"])
        continuation = poll["source_receipt"]["source_continuation"]["reading_token"]
        checkpoint = json.loads(json.dumps(job["checkpoint"]))
        repeat_a = self.tools.wechat_find_links(**arguments, reading_token=continuation)
        repeat_b = self.tools.wechat_find_links(**arguments, reading_token=continuation)
        self.assertEqual(job["attempts"], 1)
        self.assertIsNone(repeat_a.get("error"))
        self.assertIsNone(repeat_b.get("error"))
        changed = self.tools.wechat_find_links(
            domains=["other-scope.example"], conversation_ids=[self.group],
            reading_token=continuation,
        )
        self.assertEqual(changed.get("code"), "CURSOR_INVALID", changed)
        changed_limit = self.tools.wechat_find_links(
            domains=[host], conversation_ids=[self.group], limit=3,
            reading_token=continuation,
        )
        self.assertEqual(changed_limit.get("code"), "CURSOR_INVALID", changed_limit)
        self.assertEqual(job["checkpoint"], checkpoint)
        self.manager._attempt(job)
        self.assertEqual(job["state"], "ready")
        completed = self.tools.wechat_find_links(**arguments, reading_token=continuation)
        repeated = self.tools.wechat_find_links(**arguments, reading_token=continuation)
        self.assertEqual(job["run"], 1)
        self.assertEqual(job["attempts"], 2)
        self.assertEqual(job["checkpoint"]["matched_count"], 2)
        self.assertEqual(completed["items"], repeated["items"])
        self.assertEqual({item["path"] for item in completed["items"]},
                         {"/known-positive", "/remaining-positive"})

    def test_failed_partial_save_never_exposes_undurable_terminal_result(self) -> None:
        for fail_metadata in (False, True):
            with self.subTest(fail_metadata=fail_metadata):
                observed = []
                job, arguments, first = self._deadline_after_first_committed_page(
                    f"save-failure-{fail_metadata}.example", fail_metadata=fail_metadata,
                    observe=observed,
                )
                self.assertEqual(len(observed), 1)
                self.assertEqual(observed[0]["state"], "preparing")
                self.assertNotIn("source_continuation", observed[0])
                result = self.tools.wechat_find_links(**arguments,
                                                     reading_token=first["reading_token"])
                self.assertEqual(result["state"], "preparing" if fail_metadata else "failed")
                self.assertNotIn("result", job)
                self.assertTrue(self.manager._checkpoint_has_progress(job["checkpoint"]))
                if fail_metadata:
                    self.assertEqual(self.manager._progress[job["id"]]["phase"],
                                     "state_commit_pending")
                    self.assertIn(job["id"], self.manager._pending_failures)
                else:
                    self.assertEqual(result["error"]["code"], "INTERNAL_ERROR")
                self.manager.stop()
                self.tools.close()
                # Each subcase uses independent query/job state in the same disposable source.
                self.manager.path.unlink()
                self._build()

    def test_deadline_without_committed_progress_fails_closed(self) -> None:
        host = "never-progress.example"
        for index in range(200):
            self._source_row(f"noise-{index}", "synthetic noise without URL", seconds=index % 60)
        self._refresh()
        arguments: dict[str, Any] = {"domains": [host], "conversation_ids": [self.group]}
        first = self.tools.wechat_find_links(**arguments)
        job = next(iter(self.manager._jobs.values()))
        scan = self.provider.scan_discovery_page

        def slow_first_page(*args, **kwargs):
            time.sleep(0.6)
            return scan(*args, **kwargs)

        with patch("sightglass.runtime.search_preparation.ATTEMPT_SECONDS", 0.5), patch.object(
            self.provider, "scan_discovery_page", side_effect=slow_first_page
        ):
            self.manager._attempt(job)
        self.assertFalse(self.manager._checkpoint_has_progress(job["checkpoint"]))
        self.assertEqual(job["state"], "failed", job)
        poll = self.tools.wechat_find_links(**arguments, reading_token=first["reading_token"])
        self.assertEqual(poll["state"], "failed", poll)
        self.assertEqual(poll["error"]["code"], "SERVICE_TIMEOUT")

    def test_deadline_partial_respects_cancel_and_policy_fence(self) -> None:
        host = "deadline-fence.example"
        job, arguments, first = self._deadline_after_first_committed_page(host)
        self.assertEqual(job["state"], "partial", job)
        from dataclasses import replace

        self.service.reader.policy = replace(
            self.service.reader.policy, denied_conversation_ids=frozenset({self.group})
        )
        continuation = self.tools.wechat_find_links(
            **arguments, reading_token=first["reading_token"]
        )
        self.assertEqual(continuation.get("code"), "CURSOR_INVALID", continuation)

    def test_cancelled_discovery_deadline_is_not_a_partial(self) -> None:
        host = "cancel-probe.example"
        job, arguments, first = self._deadline_after_first_committed_page(host)
        self.assertEqual(job["state"], "partial", job)
        job.update(state="preparing")
        self.manager._stop.set()
        self.manager._attempt(job)
        self.manager._stop.clear()
        self.assertEqual(job["state"], "preparing", job)
        self.assertNotIn("error", job)


    def test_mid_conversation_shard_replacement_fails_closed(self) -> None:
        # One matching target behind more rows than one run scans, so a bounded run
        # stops with a saved position mid-conversation. Replacing the selected shard
        # generation before the resume must fail closed, not resume a stale position.
        self._source_burst("https://mid-page.example/a", later=12)
        self._refresh()
        self.manager.start()
        with patch("sightglass.reader.service.DISCOVERY_CONVERSATION_SCAN_BUDGET", 5):
            pending = self.tools.wechat_find_links(
                domains=["mid-page.example"], conversation_ids=[self.group]
            )
            token = pending["reading_token"]
            deadline = time.monotonic() + 10
            partial = None
            while time.monotonic() < deadline:
                partial = self.tools.wechat_find_links(
                    domains=["mid-page.example"],
                    conversation_ids=[self.group],
                    reading_token=token,
                )
                if partial.get("schema") != DISCOVERY_SCHEMA:
                    break
                token = partial.get("reading_token")
                time.sleep(0.01)
            self.assertIsNotNone(partial)
            assert partial is not None
            self.assertFalse(
                partial["source_receipt"]["discovery_preparation"]["complete"]
            )
            # Replace the selected shard generation, then resume.
            import json as _json

            manifest_path = self.root / "source.json"
            manifest = _json.loads(manifest_path.read_text())
            for shard in manifest["shards"]:
                shard["generation_id"] += "-replaced"
            manifest_path.write_text(_json.dumps(manifest))
            self._refresh()
            self.manager.start()
            outcome = None
            deadline = time.monotonic() + 15
            resumed = self.tools.wechat_find_links(
                domains=["mid-page.example"], conversation_ids=[self.group],
                reading_token=partial["source_receipt"]["source_continuation"]["reading_token"],
            )
            token = resumed["reading_token"]
            while time.monotonic() < deadline:
                outcome = self.tools.wechat_find_links(
                    domains=["mid-page.example"],
                    conversation_ids=[self.group],
                    reading_token=token,
                )
                if outcome.get("schema") != DISCOVERY_SCHEMA:
                    break
                if outcome.get("state") == "failed":
                    break
                time.sleep(0.01)
            self.assertIsNotNone(outcome)
            assert outcome is not None
            if outcome.get("schema") == DISCOVERY_SCHEMA:
                self.assertEqual(outcome["state"], "failed", outcome)
                self.assertEqual(outcome["error"]["code"], "SOURCE_GENERATION_CHANGED")
            else:
                self.assertEqual(outcome.get("code"), "SOURCE_GENERATION_CHANGED", outcome)

    def test_released_keep_conversation_goes_cold_and_rehydrates(self) -> None:
        # keep + historic complete + released bodies must still classify cold.
        self._source_row("rehydrate", "https://rehydrate.example/a", seconds=0)
        self._refresh()
        self.store.set(self.group, mode="keep")
        self.service.sync_source_once(conversation_limit=100, initial_tail=100)
        preview = self.store.stock_preview(self.group)
        self.store.release_stock(self.group, plan=preview["plan"])
        # Historic complete flags remain; the body is gone.
        state = self.repository.source_conversation_state(self.group)
        self.assertIsNotNone(state)
        self.assertFalse(
            self.repository.retrieval_candidate_resident((self.group,))
        )
        self.assertTrue(
            self.service.local_only_tool_call(
                "wechat_find_links",
                {"domains": ["rehydrate.example"], "conversation_ids": [self.group]},
            )
        )
        pending = self.tools.wechat_find_links(
            domains=["rehydrate.example"], conversation_ids=[self.group]
        )
        self.assertEqual(pending["schema"], DISCOVERY_SCHEMA, pending)
        result = self._ready(
            self.tools.wechat_find_links, pending["reading_token"],
            domains=["rehydrate.example"], conversation_ids=[self.group],
        )
        self.assertEqual(
            {item["normalized_host"] for item in result["items"]}, {"rehydrate.example"}
        )

    def test_links_only_does_not_admit_text_without_links(self) -> None:
        self._source_row("text-only", "plain chatter with no url here", seconds=0)
        self._source_row("with-link", "https://links-only.example/a", seconds=2)
        self._refresh()
        pending = self.tools.wechat_find_links(
            domains=["links-only.example"], conversation_ids=[self.group]
        )
        result = self._ready(
            self.tools.wechat_find_links, pending["reading_token"],
            domains=["links-only.example"], conversation_ids=[self.group],
        )
        self.assertEqual(
            {item["normalized_host"] for item in result["items"]}, {"links-only.example"}
        )
        # The URL-less chatter must not have been admitted as a link candidate.
        with self.repository.database.connection() as connection:
            text_only = connection.execute(
                "SELECT body_available FROM messages WHERE source_message_id=?",
                ("source-cold-text-only",),
            ).fetchone()
        self.assertTrue(text_only is None or not text_only[0])

    def test_links_cursor_pagination_is_local_and_materialized(self) -> None:
        self._source_row("warm-a", "https://warm-page.example/a", seconds=0)
        self._source_row("warm-b", "https://warm-page.example/b", seconds=1)
        self._refresh()
        self.store.set(self.group, mode="keep")
        self.service.sync_source_once(conversation_limit=100, initial_tail=100)
        from sightglass.model.links import LinkRepository

        while LinkRepository(self.repository.database).backfill_batch()["state"] != "ready":
            pass
        first = self.tools.wechat_find_links(
            domains=["warm-page.example"], conversation_ids=[self.group], limit=1
        )
        cursor = first["page"]["next_cursor"]
        self.assertIsNotNone(cursor)
        self.assertTrue(
            self.service.local_only_tool_call(
                "wechat_find_links",
                {
                    "domains": ["warm-page.example"],
                    "conversation_ids": [self.group],
                    "cursor": cursor,
                },
            )
        )
        with patch.object(
            self.provider, "snapshot", side_effect=AssertionError("pagination opened source")
        ):
            second = self.tools.wechat_find_links(
                domains=["warm-page.example"], conversation_ids=[self.group],
                limit=1, cursor=cursor,
            )
        self.assertEqual(second["schema"], "sightglass.link-search.v1", second)

    def test_malformed_retrieval_token_is_local_and_fails_closed(self) -> None:
        self.assertTrue(
            self.service.local_only_tool_call(
                "wechat_find_links", {"domains": ["x.example"], "reading_token": "not-a-token"}
            )
        )
        with patch.object(
            self.provider, "snapshot", side_effect=AssertionError("bad token opened source")
        ):
            result = self.tools.wechat_find_links(
                domains=["x.example"], reading_token="not-a-token"
            )
        self.assertEqual(result.get("code"), "CURSOR_INVALID", result)


class NativeColdDiscoveryIntegrationTests(unittest.TestCase):
    def test_unindexed_native_candidates_reach_mcp_links_and_context(self) -> None:
        from sightglass.residency.decisions import ResidencySettings
        from tests.integration import test_native_source_provider as native

        fixture = native.NativeSourceProviderTests("setUp")
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        base = 1_725_000_100
        for local_id, content in (
            (800, "synthetic unindexed discussion"),
            (801, "https://native-cold.example/project"),
            (802, "synthetic unrelated neighbor"),
        ):
            fixture._insert_message(local_id=local_id, server_id=9000 + local_id,
                sort_seq=local_id, create_time=base + local_id - 800, content=content)
        tools = fixture._reader_tools()
        self.addCleanup(tools.close)
        service = tools.service
        database = service.repository.database
        ResidencyRepository(database).set_settings(ResidencySettings(default_mode="on_demand"))
        service.storage = StorageBudget(database.path.parent, database.path,
            StorageSettings(min_free_bytes=0))
        database.storage = service.storage
        manager = SearchPreparation(service, database.path.with_name("search-preparation.json"),
            binding="synthetic-native-cold", lanes=RuntimeLanes(),
            source_worker=SourceWorker(service))
        self.addCleanup(manager.stop)
        self.assertTrue(tools.wechat_status()["ready"])
        service.sync_source_once(conversation_limit=100)
        conversation = next(iter(service.reader.policy.allowed_conversation_ids))
        with database.connection() as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0], 0)

        def finish(call, **arguments):
            initial = call(**arguments)
            self.assertEqual(initial.get("state"), "preparing", initial)
            job = next(job for job in manager._jobs.values() if job["state"] == "preparing")
            manager._attempt(job)
            result = call(**arguments, reading_token=initial["reading_token"])
            self.assertNotEqual(result.get("schema"), DISCOVERY_SCHEMA, result)
            self.assertTrue(result["source_receipt"]["discovery_preparation"]["complete"], result)
            self.assertFalse(result["source_receipt"]["complete"])
            return result

        with patch.object(fixture.provider, "read_range",
                          side_effect=AssertionError("unindexed chronological paging")), \
             patch.object(fixture.provider, "get_message",
                          wraps=fixture.provider.get_message) as canonical:
            links = finish(tools.wechat_find_links, domains=["native-cold.example"],
                           conversation_ids=[conversation])
            self.assertEqual(len(links["items"]), 1, links)
            self.assertGreater(canonical.call_count, 0)
            with database.connection() as connection:
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0], 1
                )
            discussion = finish(tools.wechat_retrieve, concept="synthetic unindexed discussion",
                                conversation_ids=[conversation])
            self.assertTrue(discussion["contexts"], discussion)
            self.assertIn("native-cold.example", {
                link["normalized_host"] for item in discussion["contexts"] for link in item["links"]
            })
