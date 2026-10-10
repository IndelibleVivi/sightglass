from __future__ import annotations

import hashlib
import json
import unittest
from contextlib import closing
from dataclasses import replace
from typing import Any
from unittest import mock

from sightglass.contracts.capture import CaptureCeiling, CaptureRequest
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.source.base import SourceScope
from sightglass.source.capture.executor import CaptureExecutor
from sightglass.source.capture.frozen import FrozenCaptureProvider
from sightglass.source.capture.resource import resource_descriptor_digest
from sightglass.source.identity import opaque_id
from sightglass.source.synthetic import SYNTHETIC_IMAGE_KEY, _v2_image_bytes
from tests.integration import test_native_source_provider as native_fixtures


class NativeCaptureBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = native_fixtures.NativeSourceProviderTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.provider = self.fixture.provider
        self.executor = CaptureExecutor(
            self.provider,
            CaptureCeiling(
                self.fixture.account_key, frozenset({self.fixture.conversation}), "fixture-egress"
            ),
            source_instance_id="fixture-native-capture",
        )

    def request(self) -> CaptureRequest:
        return CaptureRequest(
            "fixture-native-request",
            "recent",
            self.fixture.account_key,
            "fixture-policy",
            conversation_source_id=self.fixture.conversation,
            limit=2,
        )

    def capture(self):
        return self.executor.capture(
            self.request(), stream_epoch="fixture-native-stream", sequence=1
        )

    def warm_routing_catalog(self) -> None:
        with self.provider.snapshot() as snapshot:
            self.provider.read_recent(
                self.fixture.account_key, self.fixture.conversation, 2, snapshot
            )

    def add_target_table(self, connection: Any) -> None:
        table = self.fixture._table_name(self.fixture.conversation)
        connection.execute(
            f"""CREATE TABLE [{table}](
                local_id INTEGER, server_id INTEGER, local_type INTEGER, sort_seq INTEGER,
                real_sender_id INTEGER, create_time INTEGER, status INTEGER,
                message_content BLOB, WCDB_CT_message_content INTEGER, packed_info_data BLOB
            )"""
        )
        connection.execute(
            f"INSERT INTO [{table}] VALUES (21,121,1,21,0,1725000111,0,'fixture new shard',0,NULL)"
        )
        connection.commit()

    def assert_source_rejected(self, document: Any, reason: str) -> None:
        self.assertEqual(document.receipt.terminal, "rejected")
        self.assertEqual(document.receipt.reason, reason)
        self.assertEqual(document.receipt.coverage.kind, "none")
        self.assertEqual(document.evidence.messages, ())

    def test_native_v6_epoch_and_exact_conversation_capture_are_preserved(self) -> None:
        scope = SourceScope.conversation(self.fixture.account_key, self.fixture.conversation)
        original_scope_fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "schema": "sightglass.macos-wechat.scope.v1",
                    "kind": scope.kind,
                    "account_id": scope.account_id,
                    "conversation_source_id": scope.conversation_source_id,
                    "source_message_id": scope.source_message_id,
                    "source_resource_key_digest": None,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        self.assertEqual(self.provider._scope_fingerprint(scope), original_scope_fingerprint)
        with mock.patch.object(
            self.provider, "list_conversations", side_effect=AssertionError("exact lookup")
        ):
            envelope = self.capture()
        document = envelope.document()
        self.assertEqual(document.receipt.terminal, "complete", document.receipt.reason)
        self.assertEqual(len(document.evidence.messages), 2)
        frozen = FrozenCaptureProvider(envelope)
        self.assertEqual(frozen.descriptor.implementation, self.provider.descriptor.implementation)
        self.assertEqual(frozen.origin_epoch, self.executor.origin_epoch)
        self.assertNotIn(str(self.fixture.root).encode(), envelope.metadata)
        self.assertEqual(set(dict(document.origin.selected_generations)), {"message/message_0.db"})

    def test_complete_message_sender_evidence_avoids_unrequested_roster_scan(self) -> None:
        # A tiny message read must not depend on older rows needed only by an
        # independent participant-discovery operation.
        with mock.patch.object(
            self.provider, "list_participants", side_effect=AssertionError("unexpected roster")
        ) as roster:
            envelope = self.capture()
        document = envelope.document()
        self.assertEqual(document.receipt.terminal, "complete", document.receipt.reason)
        self.assertEqual(len(document.evidence.messages), 2)
        self.assertTrue(document.origin.message_sender_evidence_complete)
        self.assertEqual(document.evidence.participants, ())
        self.assertTrue(all(message.sender_keys for message in document.evidence.messages))
        self.assertTrue(FrozenCaptureProvider(envelope).descriptor.message_sender_evidence_complete)
        roster.assert_not_called()

    def test_incomplete_sender_evidence_retains_roster_capture(self) -> None:
        descriptor = replace(self.provider.descriptor, message_sender_evidence_complete=False)
        with (
            mock.patch.object(type(self.provider), "descriptor", new_callable=mock.PropertyMock,
                              return_value=descriptor),
            mock.patch.object(self.provider, "list_participants",
                              wraps=self.provider.list_participants) as roster,
        ):
            document = self.capture().document()
        self.assertEqual(document.receipt.terminal, "complete", document.receipt.reason)
        self.assertFalse(document.origin.message_sender_evidence_complete)
        self.assertTrue(document.evidence.participants)
        roster.assert_called_once()

    def test_selected_identity_conflict_keeps_content_free_cause_across_capture(self) -> None:
        failure = SightglassError(
            ErrorCode.SOURCE_INCOMPLETE,
            details={"warning_codes": ["duplicate_message_identity_conflict"],
                     "private_detail": "synthetic-unprojectable-detail"},
        )
        with mock.patch.object(self.provider, "read_recent", side_effect=failure):
            envelope = self.capture()
        document = envelope.document()
        self.assertEqual(document.receipt.terminal, "rejected")
        self.assertEqual(document.evidence.messages, ())
        self.assertNotIn(b"synthetic-unprojectable-detail", envelope.metadata)
        with self.assertRaises(SightglassError) as caught:
            FrozenCaptureProvider(envelope)
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)
        self.assertEqual(caught.exception.details,
                         {"warning_codes": ["duplicate_message_identity_conflict"]})

    def test_uncatalogued_source_error_details_never_cross_capture(self) -> None:
        failure = SightglassError(
            ErrorCode.SOURCE_INCOMPLETE,
            details={"warning_codes": ["synthetic-unprojectable-detail"]},
        )
        with mock.patch.object(self.provider, "read_recent", side_effect=failure):
            envelope = self.capture()
        self.assertEqual(envelope.document().receipt.reason, "SOURCE_INCOMPLETE")
        self.assertNotIn(b"synthetic-unprojectable-detail", envelope.metadata)
        with self.assertRaises(SightglassError) as caught:
            FrozenCaptureProvider(envelope)
        self.assertEqual(caught.exception.details, {})

    def test_malformed_warning_metadata_still_seals_generic_rejection(self) -> None:
        for codes in (None, "duplicate_message_identity_conflict",
                      {"duplicate_message_identity_conflict": True}):
            with self.subTest(codes=codes):
                failure = SightglassError(ErrorCode.SOURCE_INCOMPLETE,
                                         details={"warning_codes": codes})
                with mock.patch.object(self.provider, "read_recent", side_effect=failure):
                    envelope = self.capture()
                self.assertEqual(envelope.document().receipt.terminal, "rejected")
                self.assertEqual(envelope.document().receipt.reason, "SOURCE_INCOMPLETE")
                with self.assertRaises(SightglassError) as caught:
                    FrozenCaptureProvider(envelope)
                self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)
                self.assertEqual(caught.exception.details, {})

    def test_unchanged_cached_contacts_bind_session_without_rereading_metadata(self) -> None:
        self.warm_routing_catalog()
        original_open = self.provider._open_scoped_connection
        opened: list[str] = []
        contact_queries: list[str] = []

        def track_open(relative: str, *, auxiliary: bool):
            value = original_open(relative, auxiliary=auxiliary)
            opened.append(relative)
            if relative == "contact/contact.db":
                value[0].set_trace_callback(contact_queries.append)
            return value

        with mock.patch.object(self.provider, "_open_scoped_connection", track_open):
            document = self.capture().document()
        self.assertEqual(document.receipt.terminal, "complete", document.receipt.reason)
        self.assertEqual(opened.count("contact/contact.db"), 1)
        self.assertFalse(
            any("SELECT username, nick_name, remark" in sql for sql in contact_queries)
        )
        self.assertEqual(set(dict(document.origin.selected_generations)), {"message/message_0.db"})
        self.assertEqual(self.provider.connection_cache_status()["scoped_handles"], 0)

    def test_cached_contacts_wal_correction_rejects_capture_then_refreshes_metadata(self) -> None:
        with closing(self.fixture._connect_new("contact/contact.db")) as writer:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            writer.execute(
                "UPDATE contact SET remark='fixture initial contact' WHERE username=?",
                (self.fixture.conversation,),
            )
            writer.commit()
            self.warm_routing_catalog()
            original = self.provider.read_recent

            def capture_then_correct_contact(*args: Any, **kwargs: Any):
                page = original(*args, **kwargs)
                writer.execute(
                    "UPDATE contact SET remark='fixture corrected contact' WHERE username=?",
                    (self.fixture.conversation,),
                )
                writer.commit()
                return page

            with mock.patch.object(self.provider, "read_recent", capture_then_correct_contact):
                rejected = self.capture().document()
            self.assert_source_rejected(rejected, "SOURCE_GENERATION_CHANGED")
            refreshed = self.capture().document()
        self.assertEqual(refreshed.receipt.terminal, "complete", refreshed.receipt.reason)
        self.assertEqual(refreshed.evidence.conversations[0].title, "fixture corrected contact")
        self.assertEqual(self.provider.connection_cache_status()["scoped_handles"], 0)

    def test_old_contact_cache_provenance_is_reread_in_the_current_pinned_view(self) -> None:
        with closing(self.fixture._connect_new("contact/contact.db")) as writer:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            writer.execute(
                "UPDATE contact SET remark='fixture cached contact' WHERE username=?",
                (self.fixture.conversation,),
            )
            writer.commit()
            self.warm_routing_catalog()
            old_key, old_catalog = next(iter(self.provider._contacts_by_generation.items()))
            writer.execute(
                "UPDATE contact SET remark='fixture newer contact' WHERE username=?",
                (self.fixture.conversation,),
            )
            writer.commit()
            current_revision = self.provider._dependency_revision("contact/contact.db")
            self.assertNotEqual(old_catalog.revision, current_revision)
            original_key = self.provider._message_shard_catalog_key
            original_open = self.provider._open_scoped_connection
            contact_queries: list[str] = []

            def stale_contact_key(state: Any, relative: str):
                # Model a generation lookup made before the current view opened.
                # Correctness must use cache provenance, not trust this key alone.
                if relative == "contact/contact.db":
                    return old_key
                return original_key(state, relative)

            def trace_open(relative: str, *, auxiliary: bool):
                value = original_open(relative, auxiliary=auxiliary)
                if relative == "contact/contact.db":
                    value[0].set_trace_callback(contact_queries.append)
                return value

            with (
                mock.patch.object(self.provider, "_message_shard_catalog_key", stale_contact_key),
                mock.patch.object(self.provider, "_open_scoped_connection", trace_open),
            ):
                document = self.capture().document()
        self.assertEqual(document.receipt.terminal, "complete", document.receipt.reason)
        self.assertEqual(document.evidence.conversations[0].title, "fixture newer contact")
        self.assertEqual(
            sum("SELECT username, nick_name, remark" in sql for sql in contact_queries), 1
        )
        self.assertEqual(self.provider._contacts_by_generation[old_key].revision, current_revision)
        self.assertIsNot(self.provider._contacts_by_generation[old_key], old_catalog)
        self.assertEqual(self.provider.connection_cache_status()["scoped_handles"], 0)

    def test_unopened_unrelated_wal_does_not_invalidate_operation(self) -> None:
        unrelated = self.fixture._add_probe_message_shard("1")
        with closing(self.fixture._connect_new(unrelated)) as writer:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            writer.execute("INSERT INTO shard_probe VALUES(2)")
            writer.commit()
            with self.provider.snapshot() as snapshot:
                self.provider.read_recent(
                    self.fixture.account_key, self.fixture.conversation, 2, snapshot
                )
            original = self.provider.read_recent
            opened: list[str] = []
            real_open = self.provider._open_scoped_connection

            def capture_and_append(*args: Any, **kwargs: Any):
                result = original(*args, **kwargs)
                writer.execute("INSERT INTO shard_probe VALUES(3)")
                writer.commit()
                return result

            def track_open(relative: str, *, auxiliary: bool):
                opened.append(relative)
                return real_open(relative, auxiliary=auxiliary)

            with (
                mock.patch.object(self.provider, "read_recent", side_effect=capture_and_append),
                mock.patch.object(self.provider, "_open_scoped_connection", side_effect=track_open),
            ):
                document = self.capture().document()
        self.assertEqual(document.receipt.terminal, "complete", document.receipt.reason)
        # It stayed unopened during body selection; one new validation view proves
        # the changed WAL still contains no target table instead of trusting cache.
        self.assertEqual(opened.count(unrelated), 1)
        self.assertNotIn(unrelated, dict(document.origin.selected_generations))
        self.assertEqual(self.provider.connection_cache_status()["scoped_handles"], 0)

    def test_cached_negative_route_rejects_target_table_created_before_seal(self) -> None:
        unrelated = self.fixture._add_probe_message_shard("1")
        with closing(self.fixture._connect_new(unrelated)) as writer:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            writer.execute("INSERT INTO shard_probe VALUES(2)")
            writer.commit()
            self.warm_routing_catalog()
            original = self.provider.read_recent

            def capture_then_add_table(*args: Any, **kwargs: Any):
                page = original(*args, **kwargs)
                self.add_target_table(writer)
                return page

            with mock.patch.object(self.provider, "read_recent", capture_then_add_table):
                document = self.capture().document()
        self.assert_source_rejected(document, "SOURCE_GENERATION_CHANGED")
        self.assertEqual(self.provider.connection_cache_status()["scoped_handles"], 0)

    def test_cached_negative_route_cannot_bind_old_absence_to_a_new_revision(self) -> None:
        unrelated = self.fixture._add_probe_message_shard("1")
        with closing(self.fixture._connect_new(unrelated)) as writer:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            writer.execute("INSERT INTO shard_probe VALUES(2)")
            writer.commit()
            self.warm_routing_catalog()
            original = self.provider._message_shard_catalog
            changed = False

            def cached_then_add_table(state: Any, relative: str):
                nonlocal changed
                catalog = original(state, relative)
                if relative == unrelated and not changed:
                    changed = True
                    self.add_target_table(writer)
                return catalog

            with mock.patch.object(self.provider, "_message_shard_catalog", cached_then_add_table):
                document = self.capture().document()
        self.assertTrue(changed)
        self.assert_source_rejected(document, "SOURCE_GENERATION_CHANGED")

    def test_negative_recheck_rejects_commit_after_readonly_view_was_pinned(self) -> None:
        unrelated = self.fixture._add_probe_message_shard("1")
        with closing(self.fixture._connect_new(unrelated)) as writer:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            writer.execute("INSERT INTO shard_probe VALUES(2)")
            writer.commit()
            self.warm_routing_catalog()
            original_read = self.provider.read_recent
            original_open = self.provider._open_scoped_connection
            changed = False

            def capture_then_append(*args: Any, **kwargs: Any):
                page = original_read(*args, **kwargs)
                writer.execute("INSERT INTO shard_probe VALUES(3)")
                writer.commit()
                return page

            def pin_then_add_table(relative: str, *, auxiliary: bool):
                nonlocal changed
                connection = original_open(relative, auxiliary=auxiliary)
                if relative == unrelated and not changed:
                    changed = True
                    self.add_target_table(writer)
                return connection

            with (
                mock.patch.object(self.provider, "read_recent", capture_then_append),
                mock.patch.object(self.provider, "_open_scoped_connection", pin_then_add_table),
            ):
                document = self.capture().document()
        self.assertTrue(changed)
        self.assert_source_rejected(document, "SOURCE_GENERATION_CHANGED")
        self.assertEqual(self.provider.connection_cache_status()["scoped_handles"], 0)

    def test_new_enrolled_candidate_rejects_even_without_target_table(self) -> None:
        original = self.provider.read_recent

        def capture_then_add_candidate(*args: Any, **kwargs: Any):
            page = original(*args, **kwargs)
            self.fixture._add_probe_message_shard("2")
            return page

        with mock.patch.object(self.provider, "read_recent", capture_then_add_candidate):
            document = self.capture().document()
        self.assert_source_rejected(document, "SOURCE_GENERATION_CHANGED")

    def test_new_unenrolled_candidate_is_incomplete(self) -> None:
        original = self.provider.read_recent

        def capture_then_add_unenrolled_candidate(*args: Any, **kwargs: Any):
            page = original(*args, **kwargs)
            relative = self.fixture._add_probe_message_shard("2")
            self.provider._keys.pop(relative)
            return page

        with mock.patch.object(self.provider, "read_recent", capture_then_add_unenrolled_candidate):
            document = self.capture().document()
        self.assert_source_rejected(document, "SOURCE_INCOMPLETE")

    def test_moved_negative_candidate_rejects_cached_absence(self) -> None:
        unrelated = self.fixture._add_probe_message_shard("1")
        self.warm_routing_catalog()
        original = self.provider.read_recent

        def capture_then_move_candidate(*args: Any, **kwargs: Any):
            page = original(*args, **kwargs)
            path = self.fixture.source / unrelated
            path.rename(path.with_suffix(".retired"))
            return page

        with mock.patch.object(self.provider, "read_recent", capture_then_move_candidate):
            document = self.capture().document()
        self.assert_source_rejected(document, "SOURCE_GENERATION_CHANGED")

    def test_selected_wal_change_becomes_terminal_rejection(self) -> None:
        with closing(self.fixture._connect_new("message/message_0.db")) as writer:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            original = self.provider.read_recent

            def capture_and_correct(*args: Any, **kwargs: Any):
                result = original(*args, **kwargs)
                table = self.fixture._table_name(self.fixture.conversation)
                writer.execute(
                    f"UPDATE [{table}] SET message_content='fixture correction' WHERE local_id=1"
                )
                writer.commit()
                return result

            with mock.patch.object(self.provider, "read_recent", side_effect=capture_and_correct):
                document = self.capture().document()
        self.assertEqual(document.receipt.terminal, "rejected")
        self.assertEqual(document.receipt.reason, "SOURCE_GENERATION_CHANGED")
        self.assertEqual(document.receipt.coverage.kind, "none")
        self.assertEqual(document.evidence.messages, ())

    def test_declared_batch_scope_cannot_seek_another_conversation(self) -> None:
        other = "synthetic-other-conversation"
        self.fixture._add_contact_history_without_session(other)
        with self.provider.session(
            SourceScope.conversations(self.fixture.account_key, (self.fixture.conversation,))
        ) as snapshot:
            self.assertIsNotNone(
                self.provider.get_conversation(
                    self.fixture.account_key, self.fixture.conversation, snapshot
                )
            )
            with self.assertRaises(SightglassError) as caught:
                self.provider.get_conversation(self.fixture.account_key, other, snapshot)
            self.assertEqual(caught.exception.code, ErrorCode.CONVERSATION_NOT_FOUND)

    def test_native_exact_resource_authenticates_binding_once_and_decodes_on_edge(self) -> None:
        self.provider.close()
        self.fixture._insert_image_message(local_id=31, server_id=131)
        self.fixture._enroll_image_mapping_databases()
        self.fixture._add_image_resource_row(local_id=31, stem=self.fixture._IMAGE_STEM)
        (self.fixture._image_directory() / f"{self.fixture._IMAGE_STEM}.dat").write_bytes(
            _v2_image_bytes(self.fixture._PLAIN_IMAGE)
        )
        self.fixture.provider.close()
        requested_keys: list[str] = []

        def load_image_key(account: str) -> str:
            requested_keys.append(account)
            return SYNTHETIC_IMAGE_KEY.hex()

        provider = self.fixture._enrolled_image_provider(load_image_key)
        self.fixture.provider = provider
        executor = CaptureExecutor(
            provider, self.executor.ceiling, source_instance_id=self.executor.source_instance_id
        )
        with provider.snapshot() as snapshot:
            message = next(
                item
                for item in provider.read_recent(
                    self.fixture.account_key, self.fixture.conversation, 20, snapshot
                ).messages
                if item.wechat_type == 3
            )
        descriptor = message.resources[0]
        message_id = opaque_id(
            "wxmsg", opaque_id("wxacct", self.fixture.account_key), message.source_message_id
        )
        revision_json = json.dumps(
            {
                "resource_id": opaque_id("wxres", message_id, descriptor.source_resource_key),
                "message_id": message_id,
                "source_resource_key": descriptor.source_resource_key,
                "availability": descriptor.availability,
                "resolver_json": '{"active":true}',
                "kind": descriptor.kind,
                "mime_type": descriptor.mime_type,
                "declared_size": descriptor.declared_size,
                "declared_hash": descriptor.declared_hash,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        request = replace(
            self.request(),
            operation="resource",
            focus_source_message_id=message.source_message_id,
            resource_key=descriptor.source_resource_key,
            resource_descriptor=descriptor,
            resource_descriptor_digest=resource_descriptor_digest(descriptor),
            resource_revision_json=revision_json,
            expected_resource_revision=hashlib.sha256(revision_json.encode()).hexdigest(),
        )
        with (
            mock.patch.object(provider, "get_message", side_effect=AssertionError("no hydration")),
            mock.patch.object(provider, "session", wraps=provider.session) as session,
        ):
            envelope = executor.capture(request, stream_epoch="fixture-native-stream", sequence=1)
        document = envelope.document()
        self.assertEqual(document.receipt.terminal, "complete", document.receipt.reason)
        self.assertEqual(session.call_count, 1)
        self.assertEqual(document.evidence.messages, ())
        self.assertEqual(envelope.resource, self.fixture._PLAIN_IMAGE)
        self.assertEqual(len(requested_keys), 1)
        self.assertNotIn(SYNTHETIC_IMAGE_KEY.hex().encode(), envelope.metadata)
        self.assertNotIn(str(self.fixture.root).encode(), envelope.metadata)
        # A forged owning message cannot make an authenticated native locator serve
        # another binding, even when the descriptor and core revision are supplied.
        forged_revision = json.loads(revision_json)
        forged_revision["message_id"] = opaque_id(
            "wxmsg", opaque_id("wxacct", self.fixture.account_key), "synthetic-forged-message"
        )
        forged_json = json.dumps(forged_revision, sort_keys=True, separators=(",", ":"))
        forged = executor.capture(
            replace(
                request,
                focus_source_message_id="synthetic-forged-message",
                resource_revision_json=forged_json,
                expected_resource_revision=hashlib.sha256(forged_json.encode()).hexdigest(),
            ),
            stream_epoch="fixture-native-stream",
            sequence=2,
        ).document()
        self.assertEqual(forged.receipt.terminal, "rejected")
        self.assertEqual(forged.receipt.coverage.kind, "none")
        read_resource = provider.read_resource

        def capture_and_change_payload(*args: Any, **kwargs: Any):
            payload = read_resource(*args, **kwargs)
            (self.fixture._image_directory() / f"{self.fixture._IMAGE_STEM}.dat").write_bytes(
                _v2_image_bytes(self.fixture._PLAIN_IMAGE + b"fixture source correction")
            )
            return payload

        with mock.patch.object(provider, "read_resource", side_effect=capture_and_change_payload):
            changed_envelope = executor.capture(
                request, stream_epoch="fixture-native-stream", sequence=3
            )
        changed = changed_envelope.document()
        self.assertEqual(changed.receipt.terminal, "rejected")
        self.assertEqual(changed.receipt.reason, "SOURCE_GENERATION_CHANGED")
        self.assertEqual(changed.receipt.coverage.kind, "none")
        self.assertEqual(changed_envelope.resource, b"")
