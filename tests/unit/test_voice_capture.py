from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.resources.types import ResourceReadPayload
from sightglass.voice.capture import (
    STAGING_DIRECTORY_NAME,
    CapturedVoice,
    VoiceCapture,
)

ACCOUNT_ID = "synthetic-account"
MESSAGE_ID = "synthetic-message"


def read_payload(
    data: bytes,
    *,
    content_kind: str = "audio",
    mime_type: str = "audio/silk",
) -> ResourceReadPayload:
    return ResourceReadPayload(
        descriptor={"resource_id": "wxres_probe"},
        data=data,
        mime_type=mime_type,
        content_kind=content_kind,  # type: ignore[arg-type]
    )


class StubResourceService:
    """Minimal stand-in that raises the same codes the resource service raises."""

    def __init__(self, payload: ResourceReadPayload | None = None, error: Exception | None = None):
        self.payload = payload
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def read_resource(self, **kwargs: Any) -> ResourceReadPayload:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        assert self.payload is not None
        return self.payload


class StubRepository:
    def __init__(
        self,
        *,
        fingerprint: str | None = "fingerprint-1",
        binding: str | None = None,
        active: bool = True,
        account: bool = True,
        owner: str = ACCOUNT_ID,
    ) -> None:
        self.fingerprint = fingerprint
        self.binding = binding
        self.active = active
        self.has_account = account
        self.owner = owner

    def resource_context(self, resource_id: str) -> dict[str, Any]:
        return {
            "resource_id": resource_id,
            "message_id": MESSAGE_ID,
            "account_id": self.owner,
            "resolver_json": json.dumps(
                {"active": self.active, "binding_fingerprint": self.fingerprint}
            ),
        }

    def active_accounts(self) -> list[dict[str, Any]]:
        if not self.has_account:
            return []
        return [{"account_id": ACCOUNT_ID, "account_binding_id": self.binding}]


def job(
    *,
    revision: str = "fingerprint-1",
    binding: str | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "job_id": "vjob_probe",
        "resource_id": "wxres_probe",
        "resource_revision": revision,
        "account_id": ACCOUNT_ID,
        "account_binding_id": binding,
    }
    base.update(overrides)
    return base


class CaptureHarness(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.staging = self.root / STAGING_DIRECTORY_NAME

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def capture_with(
        self,
        *,
        payload: ResourceReadPayload | None = None,
        error: Exception | None = None,
        repository: StubRepository | None = None,
        **kwargs: Any,
    ) -> tuple[VoiceCapture, StubResourceService, StubRepository]:
        service = StubResourceService(payload, error)
        repo = repository if repository is not None else StubRepository()
        return (
            VoiceCapture(service, repo, self.staging, **kwargs),
            service,
            repo,
        )


class StagingTests(CaptureHarness):
    def test_prepare_root_is_private_and_not_a_symlink(self) -> None:
        capture, _, _ = self.capture_with()
        capture.prepare_root()
        mode = stat.S_IMODE(self.staging.lstat().st_mode)
        self.assertEqual(mode, 0o700)
        self.assertEqual(mode & 0o077, 0)

    def test_prepare_root_rejects_a_symlinked_root(self) -> None:
        target = self.root / "elsewhere"
        target.mkdir(mode=0o700)
        self.staging.symlink_to(target)
        capture, _, _ = self.capture_with()
        with self.assertRaises(RuntimeError):
            capture.prepare_root()

    def test_prepare_root_rejects_a_shared_directory(self) -> None:
        self.staging.mkdir(mode=0o755)
        os.chmod(self.staging, 0o755)
        capture, _, _ = self.capture_with()
        with self.assertRaises(RuntimeError):
            capture.prepare_root()

    def test_staging_path_never_escapes_the_root(self) -> None:
        capture, _, _ = self.capture_with()
        for job_id in ("../../etc/passwd", "a/b/c", "..", ""):
            with self.subTest(job_id=job_id):
                path = capture.staging_path(job_id, "silk")
                self.assertEqual(path.parent, self.staging)
                self.assertNotIn("/", path.name)

    def test_sweep_only_removes_aged_regular_files(self) -> None:
        self.staging.mkdir(mode=0o700)
        stale = self.staging / "stale.silk"
        fresh = self.staging / "fresh.silk"
        stale.write_bytes(b"old")
        fresh.write_bytes(b"new")
        nested = self.staging / "nested"
        nested.mkdir()
        (nested / "inner.silk").write_bytes(b"inner")
        aged = time.time() - 7200
        os.utime(stale, (aged, aged))

        capture, _, _ = self.capture_with()
        removed = capture.sweep_stale()
        self.assertEqual(removed, 1)
        self.assertFalse(stale.exists())
        self.assertTrue(fresh.exists())
        self.assertTrue((nested / "inner.silk").exists())


class CaptureGuardTests(CaptureHarness):
    def run_capture(self, **kwargs: Any) -> CapturedVoice:
        payload = kwargs.pop("payload", read_payload(b"\x02#!SILK_V3payload"))
        capture, _, _ = self.capture_with(payload=payload, **kwargs)
        return capture.capture(job())

    def assertBlocked(self, reason: str, **kwargs: Any) -> SightglassError:
        with self.assertRaises(SightglassError) as caught:
            self.run_capture(**kwargs)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)
        self.assertEqual(caught.exception.details["reason"], reason)
        self.assertEqual(caught.exception.details["stage"], "capture")
        self.assertFalse(caught.exception.retryable)
        return caught.exception

    def assertTransient(self, code: ErrorCode, **kwargs: Any) -> SightglassError:
        with self.assertRaises(SightglassError) as caught:
            self.run_capture(**kwargs)
        self.assertEqual(caught.exception.code, code)
        self.assertTrue(caught.exception.retryable)
        self.assertEqual(caught.exception.details["stage"], "capture")
        return caught.exception

    def test_capture_writes_private_staging_and_pins_the_digest(self) -> None:
        data = b"\x02#!SILK_V3" + bytes(range(64))
        captured = self.run_capture(payload=read_payload(data))
        self.assertEqual(captured.input_digest, hashlib.sha256(data).hexdigest())
        self.assertEqual(captured.byte_size, len(data))
        self.assertEqual(captured.resource_revision, "fingerprint-1")
        self.assertEqual(captured.silk_path.read_bytes(), data)
        self.assertEqual(stat.S_IMODE(captured.silk_path.lstat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.staging.lstat().st_mode), 0o700)
        captured.release()
        self.assertFalse(captured.silk_path.exists())

    def test_capture_reads_the_original_with_the_byte_cap(self) -> None:
        capture, service, _ = self.capture_with(
            payload=read_payload(b"\x02#!SILK_V3payload"), max_bytes=4096
        )
        capture.capture(job())
        self.assertEqual(len(service.calls), 1)
        self.assertEqual(service.calls[0]["mode"], "original")
        self.assertEqual(service.calls[0]["max_bytes"], 4096)

    def test_non_audio_original_is_blocked(self) -> None:
        self.assertBlocked(
            "not_audio_original",
            payload=read_payload(b"\x02#!SILK_V3payload", content_kind="image"),
        )

    def test_unexpected_mime_is_blocked(self) -> None:
        self.assertBlocked(
            "unexpected_audio_mime",
            payload=read_payload(b"\x02#!SILK_V3payload", mime_type="audio/mpeg"),
        )

    def test_empty_original_is_blocked(self) -> None:
        self.assertBlocked("empty_original", payload=read_payload(b""))

    def test_oversized_original_is_blocked(self) -> None:
        self.assertBlocked(
            "input_too_large", payload=read_payload(b"\x02#!SILK_V3" + b"x" * 32), max_bytes=16
        )

    def test_incomplete_job_is_blocked(self) -> None:
        capture, _, _ = self.capture_with(payload=read_payload(b"\x02#!SILK_V3payload"))
        for field in ("job_id", "resource_id", "resource_revision", "account_id"):
            with self.subTest(field=field):
                with self.assertRaises(SightglassError) as caught:
                    capture.capture(job(**{field: ""}))
                self.assertEqual(caught.exception.details["reason"], "job_input_incomplete")

    def test_message_identity_comes_from_the_recorded_resource(self) -> None:
        capture, _, _ = self.capture_with(payload=read_payload(b"\x02#!SILK_V3payload"))
        captured = capture.capture(job())
        self.assertEqual(captured.message_id, MESSAGE_ID)

    def test_resource_owned_by_another_account_is_blocked(self) -> None:
        self.assertBlocked(
            "resource_account_mismatch", repository=StubRepository(owner="synthetic-other")
        )

    def test_revision_change_is_blocked(self) -> None:
        self.assertBlocked(
            "resource_revision_changed", repository=StubRepository(fingerprint="fingerprint-2")
        )

    def test_unrecorded_revision_is_blocked(self) -> None:
        self.assertBlocked(
            "resource_revision_unrecorded", repository=StubRepository(fingerprint=None)
        )

    def test_binding_change_is_blocked(self) -> None:
        self.assertBlocked(
            "account_binding_changed", repository=StubRepository(binding="binding-2")
        )

    def test_inactive_resource_is_blocked(self) -> None:
        self.assertBlocked("resource_inactive", repository=StubRepository(active=False))

    def test_missing_account_is_blocked(self) -> None:
        self.assertBlocked("account_missing", repository=StubRepository(account=False))

    def test_invalid_resolver_json_is_blocked(self) -> None:
        class BrokenRepository(StubRepository):
            def resource_context(self, resource_id: str) -> dict[str, Any]:
                return {"resource_id": resource_id, "resolver_json": "not json"}

        self.assertBlocked("resource_resolver_invalid", repository=BrokenRepository())

    def test_pause_and_generation_change_stay_retryable(self) -> None:
        for code in (
            ErrorCode.SERVICE_PAUSED,
            ErrorCode.SOURCE_GENERATION_CHANGED,
            ErrorCode.SERVICE_TIMEOUT,
        ):
            with self.subTest(code=code):
                self.assertTransient(code, error=SightglassError(code))

    def test_denial_and_missing_source_are_blocked(self) -> None:
        for code, reason in (
            (ErrorCode.POLICY_DENIED, "resource_policy_denied"),
            (ErrorCode.RESOURCE_NOT_FOUND, "resource_resource_not_found"),
            (ErrorCode.SOURCE_PERMISSION_DENIED, "resource_source_permission_denied"),
            (ErrorCode.SOURCE_KEY_MISSING, "resource_source_key_missing"),
        ):
            with self.subTest(code=code):
                error = SightglassError(code)
                exception = self.assertBlocked(reason, error=error)
                self.assertEqual(exception.details["stage"], "capture")

    def test_blocked_capture_leaves_no_staging_bytes(self) -> None:
        self.assertBlocked("resource_revision_changed", repository=StubRepository(fingerprint="x2"))
        remaining = list(self.staging.iterdir()) if self.staging.exists() else []
        self.assertEqual(remaining, [])


if __name__ == "__main__":
    unittest.main()
