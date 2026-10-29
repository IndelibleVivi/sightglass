from __future__ import annotations

import json
import os
import plistlib
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from sightglass.runtime import source_access
from sightglass.runtime.source_access import (
    HELPER_SCHEMA,
    HELPER_VERSION,
    ReceiverContext,
    SourceAccessError,
    SourceAccessIdentity,
    SourceAccessLease,
    account_identity_from_source_root,
    authorize_source_access,
    check_helper,
    derive_receiver_proof,
    shared_config_identities_from_source_root,
    validate_helper,
)

RECEIVER_PROOF = "a" * 64


def _account_fixture(root: Path) -> tuple[Path, Path, Path, Path]:
    all_accounts = root / "xwechat_files"
    account = all_accounts / "synthetic-account"
    database = account / "db_storage"
    (database / "contact").mkdir(parents=True)
    (database / "session").mkdir()
    (database / "contact" / "contact.db").write_bytes(b"synthetic-contact")
    (database / "session" / "session.db").write_bytes(b"synthetic-session")
    shared = all_accounts / "all_users" / "config" / "global_config"
    shared.parent.mkdir(parents=True)
    shared.write_bytes(b"synthetic-shared")
    Path(f"{shared}.crc").write_bytes(b"synthetic-meta")
    return all_accounts, account, database, shared


def _fake_helper(root: Path, *, behavior: str = "strict") -> tuple[Path, Path]:
    root.mkdir(parents=True, mode=0o700)
    os.chmod(root, 0o700)
    bundle = root / "SightglassSourceAccess.app"
    executable = bundle / "Contents" / "MacOS" / "SightglassSourceAccess"
    executable.parent.mkdir(parents=True, mode=0o700)
    os.chmod(bundle, 0o700)
    os.chmod(bundle / "Contents", 0o700)
    os.chmod(executable.parent, 0o700)
    (bundle / "Contents" / "Info.plist").write_bytes(
        b"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>CFBundleIdentifier</key><string>dev.sightglass.source-access</string>
<key>CFBundleExecutable</key><string>SightglassSourceAccess</string>
<key>CFBundlePackageType</key><string>APPL</string>
</dict></plist>
"""
    )
    os.chmod(bundle / "Contents" / "Info.plist", 0o600)
    request_path = root / "request.json"
    body = f"""import base64, hashlib, json, os, sys, time
request_path = {str(request_path)!r}
behavior = {behavior!r}
raw = sys.stdin.buffer.readline({source_access.MAX_REQUEST_BYTES + 1})
if not raw:
    sys.exit(2)
request = json.loads(raw)
held_mode = request["mode"]
with open(request_path, "w", encoding="utf-8") as handle:
    json.dump(request, handle, sort_keys=True)
def digest(item):
    payload = (
        "sightglass.source-access.item.v3\\0"
        + item["kind"] + "\\0" + item["path"] + "\\0"
        + str(item["device"]) + "\\0" + str(item["inode"])
    ).encode()
    return hashlib.sha256(payload).hexdigest()
account = request["expectedAccount"]
shared = request["expectedSharedConfig"]
shared_meta = request["expectedSharedConfigMeta"]
mode = request["mode"]
account_bookmark = base64.b64encode(b"synthetic-account-bookmark").decode()
shared_bookmark = base64.b64encode(b"synthetic-shared-bookmark").decode()
shared_meta_bookmark = base64.b64encode(b"synthetic-shared-meta-bookmark").decode()
response = {{
    "schema": {HELPER_SCHEMA!r},
    "ok": True,
    "mode": mode,
    "nonce": request["nonce"],
    "code": None,
    "helperVersion": {HELPER_VERSION!r},
    "helperDigest": request["expectedHelperDigest"],
    "capabilityPresent": mode != "check",
    "grantsAccess": mode != "check",
    "account": None if mode == "check" else account,
    "accountDigest": None if mode == "check" else digest(account),
    "sharedConfig": shared if mode != "check" else None,
    "sharedConfigBookmark": shared_bookmark if mode != "check" and shared else None,
    "sharedConfigDigest": digest(shared) if mode != "check" and shared else None,
    "sharedConfigMeta": shared_meta if mode != "check" else None,
    "sharedConfigMetaBookmark": (
        shared_meta_bookmark if mode != "check" and shared_meta else None
    ),
    "sharedConfigMetaDigest": (
        digest(shared_meta) if mode != "check" and shared_meta else None
    ),
    "accountBookmark": None if mode == "check" else account_bookmark,
    "receiverProofAccepted": mode != "check",
    "receiverVerifier": request["expectedReceiverVerifier"] if mode != "check" else None,
    "entitlementReady": True,
    "containerReady": True,
}}
expected_verifier = hashlib.sha256(
    ("sightglass.source-access.receiver-verifier.v1\\0"
     + (request["receiverProof"] or "")).encode()
).hexdigest()
if behavior == "cancel":
    response.update(ok=False, code="source_access_picker_cancelled",
                    grantsAccess=False, capabilityPresent=False)
elif behavior == "timeout":
    time.sleep(5)
    sys.exit(0)
elif behavior == "malformed":
    sys.stdout.write("{{not-json}}\\n")
    sys.exit(0)
elif behavior == "oversized":
    sys.stdout.write("x" * ({source_access.MAX_RESPONSE_BYTES} + 1) + "\\n")
    sys.exit(0)
elif behavior == "wrong-nonce":
    response["nonce"] = "wrong-nonce"
    response["receiverVerifier"] = expected_verifier
elif behavior == "wrong-root":
    response["account"] = {{**account, "path": account["path"] + "-foreign"}}
    response["accountDigest"] = digest(response["account"])
    response["receiverVerifier"] = expected_verifier
elif behavior == "wrong-credential":
    response["ok"] = False
    response["code"] = "source_access_receiver_unauthorized"
    response["receiverProofAccepted"] = False
elif behavior == "spoof-correlation":
    response["receiverProofAccepted"] = True
    response["receiverVerifier"] = "0" * 64
elif behavior == "diagnostic-spoof":
    response["diagnostic"] = request["receiverProof"]
    response["receiverVerifier"] = expected_verifier
elif behavior == "stale":
    response["ok"] = False
    response["code"] = "source_access_capability_stale"
    response["grantsAccess"] = False
elif behavior == "hold-until-eof":
    pass
elif behavior == "changed-helper":
    with open(__file__, "ab") as handle:
        handle.write(b"\\n")
print(json.dumps(response, sort_keys=True, separators=(",", ":")), flush=True)
if held_mode == "lease" and behavior == "hold-until-eof":
    while sys.stdin.buffer.read(1):
        pass
"""
    script = root / "source_access_fake.py"
    script.write_text(body)
    script.chmod(0o600)
    executable.write_text(
        f"#!/bin/sh\nexec {json.dumps(sys.executable)} {json.dumps(str(script))}\n"
    )
    executable.chmod(0o700)
    return bundle, request_path


class SourceAccessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        _, self.account, self.database, self.shared = _account_fixture(self.root)
        self.helper, self.request_path = _fake_helper(self.root / "helper")
        self.account_identity = account_identity_from_source_root(self.database)
        self.shared_identity = shared_config_identities_from_source_root(self.database)

    def test_helper_validation_and_check_mode_never_grant(self) -> None:
        artifact = validate_helper(self.helper)
        report = check_helper(self.helper)
        self.assertTrue(report.entitlement_ready)
        self.assertFalse(report.grants_access)
        self.assertEqual(artifact.app_path, self.helper)
        request = json.loads(self.request_path.read_text())
        self.assertEqual(request["mode"], "check")
        self.assertIsNone(request["expectedAccount"])
        self.assertFalse(request["includeSharedConfig"])
        serialized = repr(report)
        self.assertNotIn(str(self.account), serialized)
        self.assertNotIn("synthetic-account-bookmark", serialized)

    def test_account_scope_is_exact_and_shared_grant_is_exact_files(self) -> None:
        self.assertEqual(self.account_identity.path, str(self.account))
        self.assertEqual(self.account_identity.kind, "account_directory")
        self.assertEqual(self.shared_identity[0].path, str(self.shared))
        self.assertEqual(self.shared_identity[1].path, f"{self.shared}.crc")
        self.assertNotEqual(self.shared_identity[0].path, str(self.shared.parent))
        with self.assertRaisesRegex(SourceAccessError, "layout"):
            account_identity_from_source_root(self.account.parent)

    def test_authorize_forwards_only_expected_scope_and_never_exposes_bookmarks(self) -> None:
        with mock.patch.object(
            source_access,
            "_resolve_reports",
            return_value=source_access.ResolvedAccess(
                account_url=mock.Mock(),
                shared_config_url=mock.Mock(),
                shared_config_meta_url=mock.Mock(),
            ),
        ):
            report, artifact = authorize_source_access(
                self.helper,
                self.account_identity,
                expected_shared_config=self.shared_identity,
                receiver_proof=RECEIVER_PROOF,
            )
        self.assertTrue(report.grants_access)
        self.assertEqual(artifact.digest, report.helper_digest)
        self.assertEqual(report.account_bookmark, "c3ludGhldGljLWFjY291bnQtYm9va21hcms=")
        request = json.loads(self.request_path.read_text())
        self.assertEqual(request["expectedAccount"], self.account_identity.as_request())
        self.assertEqual(request["expectedSharedConfig"], self.shared_identity[0].as_request())
        self.assertEqual(request["expectedSharedConfigMeta"], self.shared_identity[1].as_request())
        self.assertTrue(request["includeSharedConfig"])
        self.assertNotIn("bookmark", json.dumps(request).lower())

    def test_cancel_and_stale_fail_closed_without_a_fallback(self) -> None:
        for behavior, code in (
            ("cancel", "source_access_picker_cancelled"),
            ("stale", "source_access_capability_stale"),
        ):
            with self.subTest(behavior=behavior):
                helper, _ = _fake_helper(self.root / f"helper-{behavior}", behavior=behavior)
                with self.assertRaisesRegex(SourceAccessError, code):
                    authorize_source_access(
                        helper,
                        self.account_identity,
                        receiver_proof=RECEIVER_PROOF,
                    )

    def test_malformed_oversized_timeout_and_nonce_are_bounded(self) -> None:
        for behavior, code in (
            ("malformed", "source_access_response_invalid"),
            ("oversized", "source_access_response_size_invalid"),
            ("wrong-nonce", "source_access_nonce_mismatch"),
            ("wrong-root", "source_access_account_binding_changed"),
            ("spoof-correlation", "source_access_receiver_unauthorized"),
            ("diagnostic-spoof", "source_access_response_invalid"),
        ):
            with self.subTest(behavior=behavior):
                helper, _ = _fake_helper(self.root / f"helper-{behavior}", behavior=behavior)
                with self.assertRaisesRegex(SourceAccessError, code):
                    authorize_source_access(
                        helper,
                        self.account_identity,
                        receiver_proof=RECEIVER_PROOF,
                    )
        helper, _ = _fake_helper(self.root / "helper-timeout", behavior="timeout")
        started = time.monotonic()
        with self.assertRaisesRegex(SourceAccessError, "source_access_timeout"):
            check_helper(helper, timeout=0.2)
        self.assertLess(time.monotonic() - started, 2.0)

    def test_receiver_proof_is_purpose_and_scope_separated(self) -> None:
        context = ReceiverContext(
            source_instance_id="synthetic-instance",
            account_id="synthetic-account",
            account_path=str(self.account),
        )
        proof = derive_receiver_proof("synthetic-edge-secret", context)
        self.assertRegex(proof, r"^[a-f0-9]{64}$")
        self.assertNotEqual(proof, "synthetic-edge-secret")
        self.assertEqual(
            proof,
            derive_receiver_proof("synthetic-edge-secret", context),
        )
        for other in (
            ReceiverContext(
                source_instance_id="other-instance",
                account_id="synthetic-account",
                account_path=str(self.account),
            ),
            ReceiverContext(
                source_instance_id="synthetic-instance",
                account_id="other-account",
                account_path=str(self.account),
            ),
            ReceiverContext(
                source_instance_id="synthetic-instance",
                account_id="synthetic-account",
                account_path=str(self.account.parent),
            ),
        ):
            self.assertNotEqual(proof, derive_receiver_proof("synthetic-edge-secret", other))
        self.assertNotEqual(proof, derive_receiver_proof("different-edge-secret", context))

    def test_wrong_missing_and_spoofed_receiver_credentials_fail_closed(self) -> None:
        helper, _ = _fake_helper(
            self.root / "helper-wrong-credential",
            behavior="wrong-credential",
        )
        with self.assertRaisesRegex(SourceAccessError, "source_access_receiver_unauthorized"):
            authorize_source_access(
                helper,
                self.account_identity,
                receiver_proof=RECEIVER_PROOF,
            )
        with self.assertRaisesRegex(SourceAccessError, "source_access_receiver"):
            derive_receiver_proof("", self.context())
        # Nonce/helper-digest matching alone cannot make a spoofed root proof pass.
        self.assertNotEqual(
            RECEIVER_PROOF,
            derive_receiver_proof(
                "synthetic-edge-secret",
                ReceiverContext(
                    source_instance_id="synthetic-instance",
                    account_id="synthetic-account",
                    account_path=str(self.account.parent),
                ),
            ),
        )

    def test_no_relay_secret_enters_request_response_or_exports(self) -> None:
        edge_secret = "synthetic-edge-secret-value"
        proof = derive_receiver_proof(edge_secret, self.context())
        with mock.patch.object(
            source_access,
            "_resolve_reports",
            return_value=source_access.ResolvedAccess(
                account_url=mock.Mock(),
                shared_config_url=None,
                shared_config_meta_url=None,
            ),
        ):
            report, _artifact = authorize_source_access(
                self.helper,
                self.account_identity,
                receiver_proof=proof,
            )
        request_bytes = self.request_path.read_bytes()
        self.assertNotIn(edge_secret.encode(), request_bytes)
        self.assertNotIn(edge_secret, repr(report))
        self.assertNotIn(proof, repr(report))

    def context(self) -> ReceiverContext:
        return ReceiverContext(
            source_instance_id="synthetic-instance",
            account_id="synthetic-account",
            account_path=str(self.account),
        )

    def test_lease_requires_complete_binding_and_releases_reverse_order(self) -> None:
        stops: list[str] = []

        class FakeURL:
            def __init__(self, name: str) -> None:
                self.name = name

            def stopAccessingSecurityScopedResource(self) -> None:
                stops.append(self.name)

        urls = {
            "account": FakeURL("account"),
            "shared": FakeURL("shared"),
            "meta": FakeURL("meta"),
        }

        def resolve(bookmark: str, expected: SourceAccessIdentity) -> FakeURL:
            del bookmark
            if expected.kind == "account_directory":
                return urls["account"]
            if expected.kind == "shared_config_file":
                return urls["shared"]
            return urls["meta"]

        with mock.patch.object(source_access, "_resolve_bookmark", side_effect=resolve):
            lease = SourceAccessLease(
                self.helper,
                self.account_identity,
                expected_shared_config=self.shared_identity,
                receiver_proof=RECEIVER_PROOF,
            )
            lease.close()
        self.assertEqual(stops, ["meta", "shared", "account"])
        self.assertTrue(lease.closed)

    def test_receiver_resolves_before_helper_eof(self) -> None:
        helper, _ = _fake_helper(self.root / "helper-eof-blocked", behavior="hold-until-eof")
        resolved = False
        owners = []
        exchange = source_access._exchange

        def capture_exchange(*args, **kwargs):
            result = exchange(*args, **kwargs)
            owners.append(result[1])
            return result

        class FakeURL:
            def stopAccessingSecurityScopedResource(self) -> None:
                pass

        def resolve(bookmark: str, expected: SourceAccessIdentity) -> FakeURL:
            del bookmark, expected
            nonlocal resolved
            self.assertFalse(owners[0].process.stdin.closed)
            self.assertIsNone(owners[0].process.poll())
            resolved = True
            return FakeURL()

        with (
            mock.patch.object(source_access, "_resolve_bookmark", side_effect=resolve),
            mock.patch.object(source_access, "_exchange", side_effect=capture_exchange),
        ):
            lease = SourceAccessLease(
                helper,
                self.account_identity,
                receiver_proof=RECEIVER_PROOF,
                timeout=2.0,
            )
            self.assertIsNotNone(lease._owner)
            assert lease._owner is not None
            self.assertIsNone(lease._owner.process.poll())
            self.assertTrue(resolved)
            self.assertFalse(lease.closed)
            lease.close()
            self.assertEqual(owners[0].process.returncode, 0)
            self.assertTrue(owners[0].process.stdin.closed)
        self.assertTrue(resolved)

    def test_lease_resolution_failure_releases_account_scope(self) -> None:
        stops: list[str] = []

        class FakeURL:
            def stopAccessingSecurityScopedResource(self) -> None:
                stops.append("account")

        def resolve(bookmark: str, expected: SourceAccessIdentity) -> FakeURL:
            del bookmark
            if expected.kind == "account_directory":
                return FakeURL()
            raise SourceAccessError("source_access_capability_denied")

        with (
            mock.patch.object(source_access, "_resolve_bookmark", side_effect=resolve),
            self.assertRaisesRegex(SourceAccessError, "capability_denied"),
        ):
            SourceAccessLease(
                self.helper,
                self.account_identity,
                expected_shared_config=self.shared_identity,
                receiver_proof=RECEIVER_PROOF,
            )
        self.assertEqual(stops, ["account"])

    def test_narrow_mode_omits_an_optional_grant_from_the_lease_response(self) -> None:
        with mock.patch.object(source_access, "_resolve_bookmark", return_value=object()):
            lease = SourceAccessLease(
                self.helper,
                self.account_identity,
                receiver_proof=RECEIVER_PROOF,
            )
            lease.close()
        request = json.loads(self.request_path.read_text())
        self.assertFalse(request["includeSharedConfig"])
        self.assertIsNone(request["expectedSharedConfig"])
        self.assertIsNone(request["expectedSharedConfigMeta"])

    def test_compiled_helper_check_mode_when_available(self) -> None:
        if sys.platform != "darwin":
            self.skipTest("source-access helper is macOS-only")
        output = self.root / "compiled" / "SightglassSourceAccess.app"
        repository = Path(__file__).resolve().parents[2]
        script = repository / "scripts" / "build-source-access-helper.sh"
        result = subprocess.run(
            [str(script), str(output)],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 2:
            self.skipTest(result.stderr.strip() or "Swift toolchain unavailable")
        self.assertEqual(result.returncode, 0, result.stderr)
        # The sandbox's application container must never be the operator's
        # persistent-grant container, even after the feature is installed.
        test_identifier = "dev.sightglass.source-access.generated-tests"
        info_path = output / "Contents/Info.plist"
        info = plistlib.loads(info_path.read_bytes())
        info["CFBundleIdentifier"] = test_identifier
        info_path.write_bytes(plistlib.dumps(info))
        signed = subprocess.run(
            [
                "codesign",
                "--force",
                "--sign",
                "-",
                "--entitlements",
                str(
                    repository
                    / "swift/sightglass-source-access/SightglassSourceAccess.entitlements"
                ),
                str(output),
            ],
            capture_output=True,
            timeout=30,
        )
        self.assertEqual(signed.returncode, 0, signed.stderr.decode(errors="replace"))
        probe = subprocess.run(
            [str(output / "Contents" / "MacOS" / "SightglassSourceAccess")],
            input=b"",
            capture_output=True,
            timeout=10,
        )
        if probe.returncode in {-6, 134} and not probe.stdout and not probe.stderr:
            self.skipTest("ad-hoc sandboxed app launch denied in this automation environment")
        self.assertIn(probe.returncode, {0, 1}, probe.stderr.decode(errors="replace"))
        with mock.patch.object(source_access, "HELPER_BUNDLE_ID", test_identifier):
            report = check_helper(output)
            with self.assertRaisesRegex(SourceAccessError, "source_access_capability_missing"):
                SourceAccessLease(
                    output,
                    self.account_identity,
                    receiver_proof=RECEIVER_PROOF,
                )
        self.assertTrue(report.entitlement_ready)
        self.assertFalse(report.grants_access)
        self.assertFalse(report.capability_present)

    @unittest.skipUnless(sys.platform == "darwin", "Swift/AppKit contract is macOS-only")
    def test_real_swift_protocol_without_os_grants(self) -> None:
        # Compile the production DTO/parser/encoder with a pure test driver. No
        # sandbox, container, bookmark, Keychain, picker or account call is made.
        helper, _ = _fake_helper(self.root / "swift-contract")
        source = (
            Path(__file__).resolve().parents[2]
            / "swift/sightglass-source-access/SightglassSourceAccess.swift"
        )
        prefix, _main = source.read_text().split("@main\nprivate struct Main {", 1)
        driver = r"""
@main
private struct ProtocolProbe {
    static func main() {
        var accepted: Request?
        do {
            let request = try readRequest()
            accepted = request
            if request.mode != "check" {
                throw HelperError(code: "source_access_capability_missing")
            }
            try emit(Response(
                schema: protocolSchema, ok: true, mode: request.mode,
                nonce: request.nonce, code: nil, helperVersion: helperVersion,
                helperDigest: request.expectedHelperDigest,
                capabilityPresent: false, grantsAccess: false,
                account: nil, accountDigest: nil, sharedConfig: nil,
                sharedConfigBookmark: nil, sharedConfigDigest: nil,
                sharedConfigMeta: nil, sharedConfigMetaBookmark: nil,
                sharedConfigMetaDigest: nil, accountBookmark: nil,
                receiverProofAccepted: false, receiverVerifier: nil,
                entitlementReady: true, containerReady: true
            ))
        } catch let error as HelperError {
            try? emit(errorResponse(error.code, request: accepted))
            exit(1)
        } catch {
            exit(2)
        }
    }
}
"""
        probe = self.root / "ProtocolProbe.swift"
        probe.write_text(prefix + driver)
        executable = helper / "Contents/MacOS/SightglassSourceAccess"
        import shutil

        compiler = shutil.which("swiftc")
        if compiler is None:
            self.skipTest("Swift compiler unavailable")
        compiled = subprocess.run(
            [compiler, "-parse-as-library", str(probe), "-o", str(executable)],
            capture_output=True,
            timeout=60,
        )
        self.assertEqual(compiled.returncode, 0, compiled.stderr.decode(errors="replace"))
        executable.chmod(0o700)
        report = check_helper(helper, timeout=3)
        self.assertFalse(report.grants_access)
        self.assertIsNone(report.account_bookmark)
        # A request is a line, not an EOF-terminated stream or one pipe read.
        # Keep stdin open and split JSON across writes, as short writes may do.
        import select

        artifact = validate_helper(helper)
        nonce = "synthetic_nonce_000000000000"
        request = source_access._request("check", nonce, artifact, None, None, None)
        with subprocess.Popen(
            [str(executable)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        ) as process:
            assert process.stdin is not None and process.stdout is not None
            try:
                process.stdin.write(request[:5])
                process.stdin.flush()
                self.assertEqual(select.select([process.stdout], [], [], 0.05)[0], [])
                process.stdin.write(request[5:])
                process.stdin.flush()
                response = source_access._read_line(process, 3)
                self.assertEqual(response["nonce"], nonce)
                self.assertIs(response["ok"], True)
                process.wait(timeout=3)
            finally:
                source_access._terminate(process)
        with self.assertRaisesRegex(SourceAccessError, "source_access_capability_missing"):
            SourceAccessLease(
                helper,
                self.account_identity,
                receiver_proof=RECEIVER_PROOF,
                timeout=3,
            )

    @unittest.skipUnless(sys.platform == "darwin", "Swift/AppKit contract is macOS-only")
    def test_real_swift_capability_storage_and_publication_uncertainty(self) -> None:
        # Test the production writer on owned synthetic data. Replacing only its
        # directory owner keeps this test outside every real app/grant container.
        import shutil

        compiler = shutil.which("swiftc")
        if compiler is None:
            self.skipTest("Swift compiler unavailable")
        repository = Path(__file__).resolve().parents[2]
        source = repository / "swift/sightglass-source-access/SightglassSourceAccess.swift"
        prefix = source.read_text().split("@main\nprivate struct Main {", 1)[0]
        begin = prefix.index("private func containerDirectory()")
        end = prefix.index("private func capabilityPath()", begin)
        prefix = (
            prefix[:begin]
            + """
private func containerDirectory() throws -> URL {
    URL(fileURLWithPath: CommandLine.arguments[1], isDirectory: true)
}
"""
            + prefix[end:]
        )
        sync = "        try synchronizeDirectory(directory)"
        self.assertEqual(prefix.count(sync), 1)
        prefix = prefix.replace(
            sync,
            """
        if CommandLine.arguments.contains("--fail-after-publication") {
            throw HelperError(code: "synthetic_directory_sync_failure")
        }
"""
            + sync,
        )
        driver = r"""
@main
private struct StorageProbe {
    static func main() {
        let item = StoredItem(
            identity: ItemIdentity(path: "/synthetic/account", inode: 1, device: 1,
                                   kind: "account_directory"),
            bookmark: Data(CommandLine.arguments[2].utf8).base64EncodedString(),
            stale: false
        )
        do {
            try writeCapability(StoredCapability(
                schema: capabilitySchema, account: item, sharedConfig: nil,
                sharedConfigMeta: nil,
                receiverVerifier: receiverVerifier(String(repeating: "a", count: 64))
            ))
            let loaded = try loadCapability()
            guard loaded.account.bookmark == item.bookmark else { exit(2) }
            print("stored")
        } catch let error as HelperError {
            print(error.code)
            exit(1)
        } catch {
            exit(3)
        }
    }
}
"""
        program = self.root / "StorageProbe.swift"
        program.write_text(prefix + driver)
        executable = self.root / "StorageProbe"
        built = subprocess.run(
            [compiler, "-parse-as-library", str(program), "-o", str(executable)],
            capture_output=True,
            timeout=60,
        )
        self.assertEqual(built.returncode, 0, built.stderr.decode(errors="replace"))
        storage = self.root / "synthetic-capability-storage"
        storage.mkdir(mode=0o700)

        def run(value: str, *flags: str):
            return subprocess.run(
                [str(executable), str(storage), value, *flags],
                capture_output=True,
                text=True,
                timeout=10,
            )

        first = run("first")
        self.assertEqual((first.returncode, first.stdout.strip()), (0, "stored"), first.stderr)
        second = run("second")
        self.assertEqual((second.returncode, second.stdout.strip()), (0, "stored"), second.stderr)
        target = storage / "capability.json"
        self.assertEqual(target.stat().st_mode & 0o777, 0o600)
        before = target.read_bytes()
        uncertain = run("third", "--fail-after-publication")
        self.assertEqual(uncertain.returncode, 1, uncertain.stderr)
        self.assertEqual(uncertain.stdout.strip(), "source_access_capability_publication_uncertain")
        self.assertNotEqual(target.read_bytes(), before)
        self.assertEqual(json.loads(target.read_text())["account"]["bookmark"], "dGhpcmQ=")

    def test_receiver_starts_once_before_identity_and_balances_failures(self) -> None:
        for case in ("success", "changed", "denied", "stale"):
            with self.subTest(case=case):
                active = False
                url = mock.Mock()

                def start():
                    nonlocal active
                    active = case != "denied"
                    return active

                def path():
                    self.assertTrue(active, "identity access preceded security scope")
                    return self.account_identity.path

                def identity(*_args, **_kwargs):
                    self.assertTrue(active)
                    return (
                        self.account_identity
                        if case != "changed"
                        else SourceAccessIdentity(
                            self.account_identity.path,
                            self.account_identity.inode + 1,
                            self.account_identity.device,
                            self.account_identity.kind,
                        )
                    )

                url.startAccessingSecurityScopedResource.side_effect = start
                url.path.side_effect = path
                factory = mock.Mock()
                resolver = getattr(
                    factory.alloc.return_value,
                    "initByResolvingBookmarkData_options_relativeToURL_bookmarkDataIsStale_error_",
                )
                resolver.return_value = (url, case == "stale", None)
                foundation = mock.Mock()
                foundation.NSURLBookmarkResolutionWithoutUI = 256
                foundation.NSURLBookmarkResolutionWithoutImplicitStartAccessing = 32768
                with (
                    mock.patch.object(
                        source_access, "_foundation", return_value=(factory, foundation)
                    ),
                    mock.patch.object(
                        source_access, "_kind_identity", side_effect=identity
                    ) as check,
                ):
                    if case == "success":
                        result = source_access._resolve_bookmark("YQ==", self.account_identity)
                        self.assertIs(result, url)
                        source_access._stop_url(result)
                    else:
                        with self.assertRaises(SourceAccessError):
                            source_access._resolve_bookmark("YQ==", self.account_identity)
                self.assertEqual(resolver.call_args.args[1], 256 | 32768)
                self.assertEqual(
                    url.startAccessingSecurityScopedResource.call_count, int(case != "stale")
                )
                self.assertEqual(
                    url.stopAccessingSecurityScopedResource.call_count,
                    int(case in {"success", "changed"}),
                )
                self.assertEqual(check.call_count, int(case in {"success", "changed"}))

    def test_build_rejects_existing_output_without_changing_it(self) -> None:
        output = self.root / "existing.app"
        output.mkdir()
        marker = output / "retained"
        marker.write_bytes(b"existing-helper")
        script = Path(__file__).resolve().parents[2] / "scripts/build-source-access-helper.sh"
        result = subprocess.run([str(script), str(output)], capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 3)
        self.assertEqual(list(output.iterdir()), [marker])
        self.assertEqual(marker.read_bytes(), b"existing-helper")


if __name__ == "__main__":
    unittest.main()
