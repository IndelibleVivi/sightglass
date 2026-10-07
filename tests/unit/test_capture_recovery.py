from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from pathlib import Path
from unittest import mock

from sightglass.cli import main
from sightglass.contracts.capture import (
    CaptureAck,
    CaptureCeiling,
    CaptureProtocolError,
    CaptureRequest,
)
from sightglass.runtime.activation import write_activation
from sightglass.runtime.capture_journal import CAPTURE_REQUEST_TOOL, WindowCaptureJournal, _identity
from sightglass.runtime.capture_recovery import (
    apply_recovery,
    inspect_edge_recovery,
    prepare_recovery,
    read_private_json,
    recover_edge,
    write_private_json,
)
from sightglass.runtime.config import ConfigStore, SightglassConfig
from sightglass.runtime.edge import EdgeSpool
from sightglass.runtime.edge_relay import edge_token_hash
from sightglass.runtime.edge_runtime import EdgeSettings
from sightglass.source.capture import CaptureExecutor
from sightglass.source.remote import RemoteCaptureSettings
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


class CaptureRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        source = create_synthetic_source(self.root / "source")
        provider, repository, service, tools = build_test_stack(
            source,
            self.root / "core" / "window.db",
            default_projection=None,
        )
        service.sync_source_once(initial_tail=200)
        self.repository, self.journal = repository, WindowCaptureJournal(repository.database)
        group = repository.conversation_id_for(
            repository.account_id_for("synthetic-account-demo"),
            "conv_group",
        )
        self.delivery_id = tools.wechat_read_messages(mode="updates", conversation_id=group)[
            "page"
        ]["delivery_id"]
        self.before_delivery = self.delivery_state()
        self.executor = CaptureExecutor(
            provider,
            CaptureCeiling("synthetic-account-demo", frozenset({"conv_group"}), "synthetic-egress"),
            source_instance_id="synthetic-instance",
        )
        self.request = CaptureRequest(
            "synthetic-request",
            "recent",
            "synthetic-account-demo",
            "synthetic-policy",
            conversation_source_id="conv_group",
        )
        self.envelope = self.executor.capture(
            self.request, stream_epoch="synthetic-stream", sequence=1
        )
        self.settings = EdgeSettings(
            "synthetic",
            source / "source.json",
            "synthetic-instance",
            "synthetic-account-demo",
            frozenset({"conv_group"}),
            "synthetic-egress",
            self.executor.origin_epoch,
            "synthetic-stream",
            self.root / "edge",
            "synthetic-host",
            self.root / "identity",
            "synthetic-capability",
            self.root / "edge-grant",
            "synthetic-edge-generation",
            "synthetic-core-generation",
        )
        write_activation(
            self.settings.activation_path,
            generation=self.settings.activation_generation,
            state="active",
            role="edge",
            namespace=self.settings.spool_directory,
        )
        self.edge_settings_path = self.root / "edge.json"
        write_private_json(self.edge_settings_path, self.settings.as_dict())
        self.spool = EdgeSpool.initialize(
            self.settings.spool_directory,
            source_instance_id=self.settings.source_instance_id,
            account_id=self.settings.account_id,
            origin_epoch=self.settings.origin_epoch,
            stream_epoch=self.settings.stream_epoch,
        )
        self.addCleanup(self.spool.close)
        self.spool.store(self.envelope)
        with self.repository.database.transaction() as connection:
            self.journal._write(
                _identity("request", self.request.request_id),
                CAPTURE_REQUEST_TOOL,
                {
                    "generation": self.settings.core_generation,
                    "request": json.loads(
                        json.dumps(self.request, default=lambda value: value.__dict__)
                    ),
                },
                "2026-10-08T00:00:00+00:00",
            )
            connection.execute(
                "UPDATE access_receipts SET outcome='pending' WHERE receipt_id=?",
                (_identity("request", self.request.request_id),),
            )

    def loss(self) -> dict:
        self.spool.close()
        payload = self.settings.spool_directory / "pending.capture"
        payload.write_bytes(b"synthetic-corrupt-payload")
        state = inspect_edge_recovery(self.settings)
        self.assertTrue(state["epoch_lost"])
        return state

    def plan(self, edge_state: dict | None = None, next_sequence: int = 2):
        return prepare_recovery(
            self.journal,
            source_instance_id=self.settings.source_instance_id,
            account_id=self.settings.account_id,
            origin_epoch=self.settings.origin_epoch,
            activation_generation=self.settings.core_generation,
            enrolled_epoch=self.settings.stream_epoch,
            new_epoch="synthetic-stream-next",
            next_sequence=next_sequence,
            edge_state=edge_state,
        )

    def delivery_state(self) -> dict:
        value = self.repository.delivery(self.delivery_id)
        assert value is not None
        return dict(value)

    def test_unreceived_exact_loss_is_atomic_and_preserves_reader_state(self) -> None:
        plan = self.plan(self.loss())
        receipt = apply_recovery(self.journal, plan)
        self.assertEqual(receipt["ack"]["terminal"], "epoch_loss")
        self.assertEqual(apply_recovery(self.journal, plan), receipt)
        self.assertEqual(self.delivery_state(), self.before_delivery)
        recover_edge(self.edge_settings_path, receipt, whole_spool_lost=False)
        recover_edge(self.edge_settings_path, receipt, whole_spool_lost=False)
        settings = EdgeSettings.load(self.edge_settings_path)
        self.assertEqual(settings.stream_epoch, plan.target.stream_epoch)
        with EdgeSpool(
            settings.spool_directory,
            source_instance_id=settings.source_instance_id,
            account_id=settings.account_id,
            origin_epoch=settings.origin_epoch,
        ) as spool:
            self.assertIsNone(spool.pending())
            self.assertEqual(spool.next_sequence, 2)
            self.assertFalse(spool.epoch_lost)
        self.assertEqual(
            self.journal.stream_position(settings.source_instance_id, settings.account_id),
            plan.target,
        )

    def test_wire_ack_loss_reuses_immutable_accepted_receipt_under_corruption(self) -> None:
        document = self.envelope.document()
        ack = CaptureAck(
            "synthetic-stream",
            1,
            document.origin.batch_id,
            self.envelope.digest,
            self.request.request_id,
            "accepted",
            "synthetic-existing-receipt",
        )
        with self.repository.database.transaction():
            self.journal.record_terminal(document, ack)
        plan = self.plan(self.loss())
        receipt = apply_recovery(self.journal, plan)
        self.assertEqual(receipt["ack"]["terminal"], "accepted")
        self.assertEqual(self.journal.batch_receipt(document.origin.batch_id), ack)
        recover_edge(self.edge_settings_path, receipt, whole_spool_lost=False)
        self.assertEqual(self.delivery_state(), self.before_delivery)

    def test_invalid_floor_unknown_request_and_changed_stream_fail_closed(self) -> None:
        state = self.loss()
        with self.assertRaisesRegex(CaptureProtocolError, "reset_sequence"):
            self.plan(state, next_sequence=1)
        plan = self.plan(state)
        with self.repository.database.transaction() as connection:
            connection.execute(
                "DELETE FROM access_receipts WHERE tool_name=?", (CAPTURE_REQUEST_TOOL,)
            )
        with self.assertRaisesRegex(CaptureProtocolError, "request_not_authorized"):
            apply_recovery(self.journal, plan)
        self.assertIsNone(
            self.journal.stream_position(self.settings.source_instance_id, self.settings.account_id)
        )
        self.assertIsNone(self.journal.batch_receipt(self.envelope.document().origin.batch_id))
        self.assertEqual(self.delivery_state(), self.before_delivery)

    def test_failure_before_new_epoch_commit_rolls_back_loss_receipt_and_request(self) -> None:
        plan = self.plan(self.loss())
        original = self.journal.record_position

        def fail_new_epoch(source, account, position, **kwargs):
            if position == plan.target:
                raise RuntimeError("synthetic recovery commit failure")
            return original(source, account, position, **kwargs)

        with mock.patch.object(self.journal, "record_position", side_effect=fail_new_epoch):
            with self.assertRaisesRegex(RuntimeError, "synthetic recovery"):
                apply_recovery(self.journal, plan)
        self.assertIsNone(self.journal.batch_receipt(self.envelope.document().origin.batch_id))
        with self.repository.database.connection() as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT outcome FROM access_receipts WHERE tool_name=?", (CAPTURE_REQUEST_TOOL,)
                ).fetchone()[0],
                "pending",
            )
        self.assertEqual(apply_recovery(self.journal, plan)["ack"]["terminal"], "epoch_loss")

    def test_whole_spool_loss_requires_new_directory_and_never_restarts_at_one(self) -> None:
        plan = self.plan()
        receipt = apply_recovery(self.journal, plan)
        self.spool.close()
        with self.assertRaisesRegex(CaptureProtocolError, "new_empty_directory"):
            recover_edge(self.edge_settings_path, receipt, whole_spool_lost=True)
        settings = replace(
            self.settings,
            spool_directory=self.root / "replacement-edge",
            activation_generation="synthetic-edge-next",
        )
        write_activation(
            settings.activation_path,
            generation=settings.activation_generation,
            state="active",
            role="edge",
            namespace=settings.spool_directory,
            expected_generation=self.settings.activation_generation,
            predecessor=self.settings.activation_generation,
        )
        write_private_json(
            self.edge_settings_path, settings.as_dict(), expected=self.settings.as_dict()
        )
        recover_edge(self.edge_settings_path, receipt, whole_spool_lost=True)
        recover_edge(self.edge_settings_path, receipt, whole_spool_lost=True)
        self.assertEqual(
            inspect_edge_recovery(EdgeSettings.load(self.edge_settings_path))["next_sequence"], 2
        )

    def test_operator_cli_exports_private_plan_and_retries_exact_core_apply(self) -> None:
        state = self.loss()
        settings_path = self.root / "remote.json"
        remote = RemoteCaptureSettings(
            self.settings.source_instance_id,
            self.settings.account_id,
            self.settings.conversations,
            self.settings.egress_revision,
            self.settings.stream_epoch,
            edge_token_hash("synthetic-edge-token"),
            self.root / "capture.sock",
            self.executor.provider.descriptor,
        )
        remote.write(settings_path)
        config = replace(
            SightglassConfig.create(self.root / "core", self.root / "source"),
            source_kind="remote-capture",
            source_instance_id=self.settings.source_instance_id,
            source_settings_path=settings_path,
            reader_default_view="replica",
            activation_generation=self.settings.core_generation,
            activation_path=self.root / "core-grant",
        )
        assert config.activation_path is not None
        write_activation(
            config.activation_path,
            generation=config.activation_generation,
            state="active",
            role="core",
            namespace=config.window_db_path,
        )
        store = ConfigStore(self.root / "config.json")
        store.save(config)
        state_path, plan_path = self.root / "edge-state.json", self.root / "plan.json"
        write_private_json(state_path, state)
        with redirect_stdout(io.StringIO()):
            main(
                [
                    "--config",
                    str(store.path),
                    "capture",
                    "recovery-plan",
                    "--edge-state",
                    str(state_path),
                    "--new-epoch",
                    "synthetic-stream-next",
                    "--next-sequence",
                    "2",
                    "--output",
                    str(plan_path),
                ]
            )
            for name in ("receipt.json", "retry-receipt.json"):
                main(
                    [
                        "--config",
                        str(store.path),
                        "capture",
                        "recover",
                        "--plan-file",
                        str(plan_path),
                        "--output",
                        str(self.root / name),
                    ]
                )
        receipt = read_private_json(self.root / "receipt.json")
        self.assertEqual(read_private_json(self.root / "retry-receipt.json"), receipt)
        self.assertEqual(
            RemoteCaptureSettings.load(settings_path).stream_epoch, "synthetic-stream-next"
        )
        self.assertEqual(plan_path.stat().st_mode & 0o777, 0o600)
        with redirect_stdout(io.StringIO()):
            main(
                [
                    "edge-recover",
                    "--settings",
                    str(self.edge_settings_path),
                    "--receipt-file",
                    str(self.root / "receipt.json"),
                ]
            )
        self.assertEqual(
            EdgeSettings.load(self.edge_settings_path).stream_epoch, "synthetic-stream-next"
        )

    def test_remote_migration_cut_rejects_unreceived_or_mismatched_terminal(self) -> None:
        from sightglass.runtime.migration_cut import logical_installation_cut, verify_committed_cut
        from sightglass.runtime.migration_state import frozen_installation

        remote = RemoteCaptureSettings(
            self.settings.source_instance_id,
            self.settings.account_id,
            self.settings.conversations,
            self.settings.egress_revision,
            self.settings.stream_epoch,
            edge_token_hash("synthetic-edge-token"),
            self.root / "capture.sock",
            self.executor.provider.descriptor,
        )
        settings_path = self.root / "remote.json"
        remote.write(settings_path)
        config = replace(
            SightglassConfig.create(self.root / "core", self.root / "source"),
            source_kind="remote-capture",
            source_settings_path=settings_path,
            activation_generation=self.settings.core_generation,
            activation_path=self.root / "core-grant",
        )
        assert config.activation_path is not None
        write_activation(
            config.activation_path,
            generation=config.activation_generation,
            state="active",
            role="core",
            namespace=config.window_db_path,
        )
        config.socket_path.parent.mkdir(mode=0o700)
        secret = config.data_dir / "token-secret"
        secret.write_bytes(b"synthetic-migration-secret")
        secret.chmod(0o600)
        state = self.spool.recovery_state()
        self.spool.close()
        with self.assertRaisesRegex(RuntimeError, "stopped edge state"):
            with frozen_installation(config):
                self.fail("missing edge export was admitted")
        with self.assertRaisesRegex(RuntimeError, "one committed cut"):
            with frozen_installation(config, edge_state=state):
                self.fail("unreceived edge pending was admitted")
        document = self.envelope.document()
        ack = CaptureAck(
            "synthetic-stream",
            1,
            document.origin.batch_id,
            self.envelope.digest,
            self.request.request_id,
            "accepted",
            "synthetic-committed-receipt",
        )
        with self.repository.database.transaction():
            self.journal.record_terminal(document, ack)
        with self.assertRaisesRegex(RuntimeError, "exact durable"):
            logical_installation_cut(config, edge_state=state)
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE access_receipts SET outcome='accepted' WHERE tool_name=?",
                (CAPTURE_REQUEST_TOOL,),
            )
        with frozen_installation(config, edge_state=state) as transfer:
            self.assertIsNotNone(transfer.logical_cut)
            assert transfer.logical_cut is not None
            verify_committed_cut(config.window_db_path, transfer.logical_cut)
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE reader_deliveries SET status='acknowledged' WHERE delivery_id=?",
                (self.delivery_id,),
            )
        with self.assertRaisesRegex(RuntimeError, "committed cut"):
            verify_committed_cut(config.window_db_path, transfer.logical_cut)


if __name__ == "__main__":
    unittest.main()
