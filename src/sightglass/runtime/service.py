from __future__ import annotations

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.mcp.tools import ReaderTools
from sightglass.model.db import WindowDB
from sightglass.model.repositories import WindowRepository
from sightglass.policy.readers import ReaderContext, ReaderPolicy
from sightglass.reader.service import ReaderService
from sightglass.semantic.cloudflare import CloudflareBackend
from sightglass.semantic.service import SemanticService
from sightglass.source.identity import SignedTokenCodec, load_or_create_token_secret
from sightglass.source.registry import create_provider
from sightglass.storage import StorageBudget
from sightglass.voice.repository import VoiceRepository
from sightglass.voice.service import VoiceLimits, VoiceService

from .activation import require_core_activation
from .config import SightglassConfig
from .receipts import AsyncReceiptWriter
from .secrets import SecretStore, default_secret_store, semantic_secret_account


def build_daemon_tools(
    config: SightglassConfig, *, secret_store: SecretStore | None = None
) -> ReaderTools:
    provider = create_provider(config)
    storage = StorageBudget(config.data_dir, config.window_db_path, config.storage)
    database = WindowDB(
        config.window_db_path, storage=storage,
        write_guard=lambda: require_core_activation(config),
    )
    repository = WindowRepository(database)
    reader = ReaderContext(
        reader_id=config.reader_id,
        display_name=config.reader_display_name,
        policy=ReaderPolicy(
            mode=config.policy_mode,  # type: ignore[arg-type]
            allowed_conversation_ids=frozenset(config.allowed_conversation_ids),
            denied_conversation_ids=frozenset(config.denied_conversation_ids),
            identity_debug=True,
        ),
        paused=config.paused,
        timezone=config.reader_timezone,
    )
    token_codec = SignedTokenCodec(load_or_create_token_secret(config.window_db_path))
    voice_settings = config.voice_settings()
    voice_service = VoiceService(
        VoiceRepository(database),
        token_codec,
        limits=VoiceLimits(
            first_count=voice_settings.open_item_limit,
            first_duration_ms=voice_settings.open_duration_ms,
        ),
    )
    service = ReaderService(
        provider,
        repository,
        reader,
        token_codec,
        auth_token_hash=config.reader_token_hash,
        voice_service=voice_service,
        voice_settings=voice_settings,
        default_view=config.reader_default_view,
    )
    if config.semantic.enabled:
        try:
            token = (secret_store or default_secret_store()).get(
                semantic_secret_account(config.semantic.cf_account_id)
            )
        except RuntimeError:
            # Optional credential failure must leave the deterministic read plane usable.
            service.semantic_unavailable_reason = "credential_unavailable"
        else:
            try:
                service.semantic = SemanticService(
                    repository,
                    reader,
                    settings=config.semantic,
                    backend=CloudflareBackend(config.semantic, token),
                    epoch_factory=service._projection_inventory_epoch,
                    sidecar_path=config.data_dir / "semantic" / "index.db",
                )
            except SightglassError as exc:
                if exc.code != ErrorCode.STORAGE_PRESSURE:
                    raise
                service.semantic_unavailable_reason = "storage_pressure"
            except RuntimeError:
                service.semantic_unavailable_reason = "sidecar_unavailable"
    receipt_writer = AsyncReceiptWriter(service.record_access_receipt)
    return ReaderTools(
        service,
        receipt_recorder=receipt_writer,
        prefer_cached_status=True,
        voice_service=voice_service,
    )
