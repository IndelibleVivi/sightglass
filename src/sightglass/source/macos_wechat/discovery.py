from __future__ import annotations

import hashlib
import importlib
import os
import platform
import plistlib
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

WECHAT_BUNDLE_ID = "com.tencent.xinWeChat"
DEFAULT_WECHAT_APP = Path("/Applications/WeChat.app")
SUPPORTED_PROFILES = {("4.1.13", "269602", "arm64"): "wechat-macos-4.1.13-269602-arm64-v1"}


@dataclass(frozen=True)
class WeChatCandidate:
    candidate_id: str
    app_path: Path
    source_root: Path
    bundle_id: str
    version: str
    build: str
    architecture: str
    running: bool
    profile_id: str | None

    def as_public_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "app_display_name": "WeChat",
            "bundle_id": self.bundle_id,
            "version": self.version,
            "build": self.build,
            "architecture": self.architecture,
            "running": self.running,
            "source_accessible": os.access(self.source_root, os.R_OK | os.X_OK),
            "supported": self.profile_id is not None,
            "profile_id": self.profile_id,
        }


@dataclass(frozen=True)
class _DiscoveryContext:
    app_path: Path
    data_root: Path
    bundle_id: str
    version: str
    build: str
    architecture: str
    running: bool
    profile_id: str | None


def _running_architecture() -> tuple[bool, str]:
    try:
        appkit: Any = importlib.import_module("AppKit")

        applications = appkit.NSRunningApplication.runningApplicationsWithBundleIdentifier_(
            WECHAT_BUNDLE_ID
        )
        verified: list[str] = []
        for application in applications or ():
            bundle = application.bundleURL()
            executable = application.executableURL()
            if bundle is None or executable is None or application.isTerminated():
                continue
            if Path(str(bundle.path())).resolve() != DEFAULT_WECHAT_APP.resolve():
                continue
            expected = DEFAULT_WECHAT_APP / "Contents" / "MacOS" / "WeChat"
            if Path(str(executable.path())).resolve() != expected.resolve():
                continue
            architecture = {0x0100000C: "arm64", 0x01000007: "x86_64"}.get(
                int(application.executableArchitecture())
            )
            if architecture:
                verified.append(architecture)
        return (len(verified) == 1, verified[0] if len(verified) == 1 else platform.machine())
    except Exception:
        return (False, platform.machine())


def _discovery_context() -> _DiscoveryContext | None:
    app = DEFAULT_WECHAT_APP
    plist_path = app / "Contents" / "Info.plist"
    if not plist_path.is_file() or plist_path.is_symlink():
        return None
    with plist_path.open("rb") as handle:
        info = plistlib.load(handle)
    bundle_id = str(info.get("CFBundleIdentifier") or "")
    version = str(info.get("CFBundleShortVersionString") or "")
    build = str(info.get("CFBundleVersion") or "")
    if bundle_id != WECHAT_BUNDLE_ID or not version or not build:
        return None
    running, architecture = _running_architecture()
    data_root = (
        Path.home()
        / "Library"
        / "Containers"
        / WECHAT_BUNDLE_ID
        / "Data"
        / "Documents"
        / "xwechat_files"
    )
    if not data_root.is_dir() or data_root.is_symlink():
        return None
    return _DiscoveryContext(
        app_path=app.resolve(),
        data_root=data_root.resolve(),
        bundle_id=bundle_id,
        version=version,
        build=build,
        architecture=architecture,
        running=running,
        profile_id=SUPPORTED_PROFILES.get((version, build, architecture)),
    )


def _candidate_for_root(
    context: _DiscoveryContext, source_root: Path
) -> WeChatCandidate | None:
    try:
        metadata = source_root.lstat()
        resolved = source_root.resolve()
        resolved.relative_to(context.data_root)
    except (OSError, ValueError):
        return None
    if (
        source_root.name != "db_storage"
        or source_root.is_symlink()
        or not stat.S_ISDIR(metadata.st_mode)
        or not (source_root / "contact" / "contact.db").is_file()
        or not (source_root / "session" / "session.db").is_file()
    ):
        return None
    identity = hashlib.sha256(
        f"sightglass-wechat-candidate-v1\0{context.app_path}\0{resolved}".encode()
    ).hexdigest()[:16]
    return WeChatCandidate(
        candidate_id=f"wxsrc_{identity}",
        app_path=context.app_path,
        source_root=resolved,
        bundle_id=context.bundle_id,
        version=context.version,
        build=context.build,
        architecture=context.architecture,
        running=context.running,
        profile_id=context.profile_id,
    )


def discover_configured_candidate(source_root: Path) -> WeChatCandidate | None:
    """Validate one enrolled source root without rescanning every account directory."""

    context = _discovery_context()
    if context is None:
        return None
    return _candidate_for_root(context, source_root)


def discover_candidates() -> tuple[WeChatCandidate, ...]:
    context = _discovery_context()
    if context is None:
        return ()
    candidates = [
        candidate
        for source_root in context.data_root.rglob("db_storage")
        if (candidate := _candidate_for_root(context, source_root)) is not None
    ]
    return tuple(
        sorted(
            candidates,
            key=lambda item: item.candidate_id,
        )
    )
