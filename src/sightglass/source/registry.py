from __future__ import annotations

from collections.abc import Callable

from sightglass.runtime.config import SightglassConfig
from sightglass.source.base import WeChatSourceProvider
from sightglass.source.direct_wechat import SyntheticSourceProvider

ProviderFactory = Callable[[SightglassConfig], WeChatSourceProvider]


def _synthetic(config: SightglassConfig) -> WeChatSourceProvider:
    if config.source_root is None:
        raise RuntimeError("synthetic provider source root is unavailable")
    return SyntheticSourceProvider(config.source_root)


def _macos_wechat(config: SightglassConfig) -> WeChatSourceProvider:
    try:
        from sightglass.source.macos_wechat import MacOSWeChatSourceProvider
    except ImportError as exc:
        raise RuntimeError(
            "macos-wechat provider dependencies are missing; install sightglass[macos-wechat]"
        ) from exc
    if config.source_settings_path is None:
        raise RuntimeError("macos-wechat provider settings are unavailable")
    return MacOSWeChatSourceProvider(config.source_settings_path)


BUILTIN_PROVIDERS: dict[str, ProviderFactory] = {
    "synthetic": _synthetic,
    "macos-wechat": _macos_wechat,
}


def register_provider(kind: str, factory: ProviderFactory) -> None:
    normalized = str(kind).strip()
    if not normalized:
        raise RuntimeError("provider kind is required")
    if normalized in BUILTIN_PROVIDERS:
        raise RuntimeError(f"duplicate source provider kind: {normalized}")
    BUILTIN_PROVIDERS[normalized] = factory


def create_provider(config: SightglassConfig) -> WeChatSourceProvider:
    try:
        factory = BUILTIN_PROVIDERS[config.source_kind]
    except KeyError as exc:
        raise RuntimeError(f"source provider is unavailable: {config.source_kind}") from exc
    provider = factory(config)
    if provider.descriptor.kind != config.source_kind:
        raise RuntimeError("source provider descriptor does not match configured kind")
    return provider


def provider_descriptors() -> tuple[dict[str, object], ...]:
    return (
        {
            "kind": "synthetic",
            "source_mode": "synthetic",
            "platform": [],
            "dependency_state": "available",
        },
        {
            "kind": "macos-wechat",
            "source_mode": "live",
            "platform": ["darwin"],
            "dependency_state": _native_dependency_state(),
        },
    )


def _native_dependency_state() -> str:
    try:
        import sqlcipher3  # noqa: F401
    except ImportError:
        return "missing"
    return "available"
