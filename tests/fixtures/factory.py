from __future__ import annotations

import hashlib
from functools import wraps
from inspect import signature
from pathlib import Path
from typing import Any

from sightglass.contracts.voice import VoiceReadSettings
from sightglass.mcp.tools import ReaderTools
from sightglass.model.db import WindowDB
from sightglass.model.repositories import WindowRepository
from sightglass.policy.readers import ReaderContext, ReaderPolicy
from sightglass.reader.service import ReaderService
from sightglass.residency.decisions import ResidencySettings
from sightglass.residency.repository import ResidencyRepository
from sightglass.source.direct_wechat import DirectWeChatSourceProvider
from sightglass.source.identity import SignedTokenCodec
from sightglass.voice.repository import VoiceRepository
from sightglass.voice.service import VoiceService


def enable_keep_residency(database: WindowDB) -> None:
    """Opt a deterministic baseline fixture into continuous retained collection.

    Production new/default conversations are ``on_demand`` (see SPEC 8.7).  The
    deterministic synthetic baseline exercises retained reading, so it must state
    that retention premise explicitly rather than relying on a production default.
    """

    ResidencyRepository(database).set_settings(ResidencySettings(default_mode="keep"))


def build_test_stack(
    source_root: Path,
    window_path: Path,
    *,
    policy: ReaderPolicy | None = None,
    paused: bool = False,
    reader_id: str = "codex",
    display_name: str = "Codex",
    default_projection: str | None = "detail",
    voice: VoiceReadSettings | None = None,
    residency_default: str | None = "keep",
    response_profile: str = "diagnostic",
):
    provider = DirectWeChatSourceProvider(source_root)
    database = WindowDB(window_path)
    if residency_default is not None:
        ResidencyRepository(database).set_settings(
            ResidencySettings(default_mode=residency_default)
        )
    repository = WindowRepository(database)
    reader = ReaderContext(
        reader_id,
        display_name,
        policy or ReaderPolicy(mode="all_except_denylist", identity_debug=True),
        paused=paused,
    )
    codec = SignedTokenCodec(hashlib.sha256(b"synthetic-test-secret").digest())
    voice_service = VoiceService(VoiceRepository(database), codec)
    service = ReaderService(
        provider,
        repository,
        reader,
        codec,
        voice_service=voice_service,
        voice_settings=voice or VoiceReadSettings(),
    )
    if default_projection is not None:
        # Pre-projection suites assert the legacy detail shape. New default-contract
        # tests pass None and exercise the production compact default directly.
        read_messages = service.read_messages

        def read_messages_with_legacy_detail(**kwargs: Any):
            if kwargs.get("projection") is None:
                kwargs["projection"] = default_projection
            if kwargs.get("mode") == "message" and kwargs.get("limit") == 100:
                kwargs["limit"] = 1
            elif kwargs["projection"] == "detail" and kwargs.get("limit") == 100:
                kwargs["limit"] = 50
            return read_messages(**kwargs)

        service.read_messages = read_messages_with_legacy_detail  # type: ignore[method-assign]
    tools = ReaderTools(service, voice_service=voice_service)
    # Legacy domain suites inspect full receipts. MCP registration still injects
    # the canonical public brief default; new profile suites select brief here.
    for name in tuple(vars(ReaderTools)):
        if not name.startswith("wechat_"):
            continue
        method = getattr(tools, name)

        def fixture_method(method):
            @wraps(method)
            def invoke(*args, **kwargs):
                kwargs.setdefault("response_profile", response_profile)
                return method(*args, **kwargs)
            setattr(invoke, "__signature__", signature(method))
            return invoke

        setattr(tools, name, fixture_method(method))
    return provider, repository, service, tools
