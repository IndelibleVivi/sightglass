import base64
import json
from collections.abc import Callable
from typing import TYPE_CHECKING, Annotated, Any, Literal

from mcp.types import (
    AudioContent,
    BlobResourceContents,
    CallToolResult,
    EmbeddedResource,
    ImageContent,
    TextContent,
    TextResourceContents,
)
from pydantic import AnyUrl, Field

from sightglass.contracts.common import utc_now
from sightglass.contracts.errors import ErrorCode, SightglassError, map_unexpected_error
from sightglass.operations import operation_expired
from sightglass.runtime.voice_wait import bound_wait_ms

from .projection import ResponseProfile, mcp_response, receipt_projection

if TYPE_CHECKING:
    from sightglass.reader.service import ReaderService
    from sightglass.voice.service import VoiceService

_ContextAnchor = Annotated[
    str | None,
    Field(
        description=(
            "Signed opaque context anchor returned by message detail; pass an opaque "
            "message ID through message_id instead."
        )
    ),
]
_ResourceProjection = Annotated[
    Literal["none", "indicator", "metadata"] | None,
    Field(
        description=(
            "Resource projection: compact accepts none|indicator; detail accepts "
            "none|metadata. Defaults follow the selected message projection."
        )
    ),
]
_TimelineCursor = Annotated[
    str | None,
    Field(
        description=(
            "Signed continuation cursor accepted by recent, range, and speaker modes. "
            "Context is a two-sided anchor window and deliberately returns no linear "
            "next cursor; re-anchor on a returned edge message to continue it."
        )
    ),
]
_TranscriptWait = Annotated[
    int | None,
    Field(
        description=(
            "Bounded wait in milliseconds: default 8000, 0 disables waiting, and "
            "values above the server ceiling of 15000 are clamped to 15000."
        )
    ),
]


class ReaderTools:
    """Argument projection and error mapping only; domain behavior stays in services."""

    def __init__(
        self,
        service: "ReaderService",
        *,
        receipt_recorder: Callable[..., None] | None = None,
        prefer_cached_status: bool = False,
        voice_service: "VoiceService | None" = None,
    ) -> None:
        self.service = service
        self.receipt_recorder = receipt_recorder or service.record_access_receipt
        self.prefer_cached_status = prefer_cached_status
        self.voice_service = voice_service

    def receipt_writer_status(self) -> dict[str, Any] | None:
        status = getattr(self.receipt_recorder, "status", None)
        value = status() if callable(status) else None
        return value if isinstance(value, dict) else None

    def close(self) -> None:
        if self.service.semantic is not None:
            self.service.semantic.close()
        close = getattr(self.receipt_recorder, "close", None)
        if callable(close):
            close()
        close_provider = getattr(self.service.provider, "close", None)
        if callable(close_provider):
            close_provider()

    def _safe(
        self,
        tool_name: str,
        call: Callable[[], dict[str, Any]],
        *,
        conversation_id: str | None = None,
        scope_kind: str | None = None,
        scope_values: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        started_at = utc_now().isoformat(timespec="microseconds")
        attempts = 3 if self.service.provider.descriptor.source_mode == "live" else 1
        for attempt in range(attempts):
            try:
                result = call()
                break
            except SightglassError as exc:
                if exc.code == ErrorCode.SOURCE_GENERATION_CHANGED and attempt + 1 < attempts:
                    continue
                result = exc.as_dict()
                break
            except Exception as exc:
                result = (
                    SightglassError(ErrorCode.SERVICE_TIMEOUT, retryable=True).as_dict()
                    if operation_expired()
                    else map_unexpected_error(exc)
                )
                break
        try:
            self.receipt_recorder(
                tool_name=tool_name,
                conversation_id=conversation_id,
                scope_kind=scope_kind,
                scope_values=scope_values,
                result=receipt_projection(tool_name, result),
                started_at=started_at,
            )
        except Exception:
            pass
        return result

    def wechat_status(
        self, detail: Literal["summary", "sources", "capabilities"] = "summary",
        *, response_profile: ResponseProfile = "brief",
    ) -> dict[str, Any]:
        """Return service, source, reader, capability, and schema health."""
        call = self.service.cached_status if self.prefer_cached_status else self.service.status
        return self._safe("wechat_status", lambda: call(detail))

    def wechat_find_conversations(
        self,
        query: str,
        account_id: str | None = None,
        kinds: list[Literal["direct", "group"]] | None = None,
        recent_only: bool = False,
        limit: int = 20,
        cursor: str | None = None,
        *, response_profile: ResponseProfile = "brief",
    ) -> dict[str, Any]:
        """Find coverage-aware conversation candidates without auto-selecting ambiguity."""
        return self._safe(
            "wechat_find_conversations",
            lambda: self.service.find_conversations(
                query,
                account_id=account_id,
                kinds=tuple(kinds or ("direct", "group")),
                recent_only=recent_only,
                limit=limit,
                cursor=cursor,
            ),
            scope_values=tuple(value for value in (account_id,) if value),
        )

    def wechat_read_inbox(
        self,
        account_id: str | None = None,
        after: str | None = None,
        before: str | None = None,
        kinds: list[Literal["direct", "group"]] | None = None,
        unread_only: bool = False,
        include_latest: Literal["metadata", "text"] = "metadata",
        cursor: str | None = None,
        limit: int = 50,
        *, response_profile: ResponseProfile = "brief",
    ) -> dict[str, Any]:
        """Read a policy-filtered, cursor-stable account activity inbox."""
        return self._safe(
            "wechat_read_inbox",
            lambda: self.service.read_inbox(
                account_id=account_id,
                after=after,
                before=before,
                kinds=tuple(kinds or ("direct", "group")),
                unread_only=unread_only,
                include_latest=include_latest,
                cursor=cursor,
                limit=limit,
            ),
            scope_kind="account",
            scope_values=tuple(value for value in (account_id,) if value),
        )

    def wechat_find_participants(
        self,
        conversation_id: str,
        query: str,
        active_after: str | None = None,
        detail_level: Literal["compact", "labels", "debug"] = "labels",
        limit: int = 20,
        cursor: str | None = None,
        *, response_profile: ResponseProfile = "brief",
    ) -> dict[str, Any]:
        """Resolve a signed, paginated participant page from layered label evidence."""
        return self._safe(
            "wechat_find_participants",
            lambda: self.service.find_participants(
                conversation_id,
                query,
                active_after=active_after,
                detail_level=detail_level,
                limit=limit,
                cursor=cursor,
            ),
            conversation_id=conversation_id,
            scope_kind="conversation",
            scope_values=(conversation_id,),
        )

    def wechat_read_messages(
        self,
        mode: Literal["recent", "context", "updates", "range", "message", "speaker"] = "recent",
        conversation_id: str | None = None,
        message_id: str | None = None,
        anchor: _ContextAnchor = None,
        before: int = 30,
        after: int = 20,
        limit: int | None = None,
        direction: Literal["forward", "backward"] = "backward",
        cursor: _TimelineCursor = None,
        ack_delivery_id: str | None = None,
        participant_ids: list[str] | None = None,
        speaker_view: Literal["only", "with_context"] = "only",
        time_after: str | None = None,
        time_before: str | None = None,
        query: str | None = None,
        projection: Literal["compact", "detail"] | None = None,
        include_resources: _ResourceProjection = None,
        system_policy: Literal["include", "omit"] = "include",
        strict: Literal[True] = True,
        voice: Literal["auto", "cached", "off"] | None = None,
        refresh: Annotated[
            bool,
            Field(
                strict=True,
                description=(
                    "Bounded live reread of recent/context, including partial neighbors. "
                    "Rejects updates/cursor; source failure never falls back to cache."
                ),
            ),
        ] = False,
        *, response_profile: ResponseProfile = "brief",
    ) -> dict[str, Any]:
        """Read message pages; updates require ACK. Use refresh for a bounded source reread.

        Compact columns use fields/people; next_actions gives each continuation slot.
        Default recent limit is 30. diagnostic preserves full receipts; updates replay exactly.
        """

        def invoke() -> dict[str, Any]:
            return self.service.read_messages(
                mode=mode,
                conversation_id=conversation_id,
                message_id=message_id,
                anchor=anchor,
                before=before,
                after=after,
                limit=limit,
                direction=direction,
                cursor=cursor,
                ack_delivery_id=ack_delivery_id,
                participant_ids=tuple(participant_ids or ()),
                speaker_view=speaker_view,
                time_after=time_after,
                time_before=time_before,
                query=query,
                projection=projection,
                include_resources=include_resources,
                system_policy=system_policy,
                strict=strict,
                voice=voice,
                refresh=refresh,
            )

        participant_scope = tuple(sorted(set(participant_ids or ())))
        has_filter = bool(query or time_after or time_before)
        if len(participant_scope) == 1 and not has_filter:
            scope_kind = "participant"
        elif participant_scope or (mode == "range" and has_filter):
            scope_kind = "filter_set"
        else:
            scope_kind = "conversation"
        return self._safe(
            "wechat_read_messages",
            invoke,
            conversation_id=conversation_id,
            scope_kind=scope_kind,
            scope_values=tuple(
                value for value in (conversation_id, message_id, *participant_scope, mode) if value
            ),
        )

    def wechat_search_messages(
        self,
        query: str,
        account_id: str | None = None,
        conversation_ids: list[str] | None = None,
        participant_ids: list[str] | None = None,
        sender_query: str | None = None,
        after: str | None = None,
        before: str | None = None,
        cursor: str | None = None,
        reading_token: str | None = None,
        limit: int | None = None,
        strict: Literal[True] = True,
        *, response_profile: ResponseProfile = "brief",
    ) -> dict[str, Any]:
        """Search canonical text with hard conversation/time/sender filters; default limit 20.

        Use next_actions; poll, result_page and source_scan have distinct tokens.
        """
        selected_conversations = tuple(conversation_ids or ())
        selected_participants = tuple(participant_ids or ())
        return self._safe(
            "wechat_search_messages",
            lambda: self.service.search_messages(
                query=query,
                account_id=account_id,
                conversation_ids=selected_conversations,
                participant_ids=selected_participants,
                sender_query=sender_query,
                after=after,
                before=before,
                cursor=cursor,
                reading_token=reading_token,
                limit=limit,
                strict=strict,
            ),
            conversation_id=(
                selected_conversations[0] if len(selected_conversations) == 1 else None
            ),
            scope_kind="filter_set",
            scope_values=tuple(
                value
                for value in (account_id, *selected_conversations, *selected_participants)
                if value
            ),
        )

    def wechat_find_links(
        self, query: str = "", account_id: str | None = None,
        conversation_ids: list[str] | None = None, domains: list[str] | None = None,
        hints: list[str] | None = None, after: str | None = None,
        before: str | None = None, cursor: str | None = None,
        reading_token: str | None = None, limit: int | None = None,
        *, response_profile: ResponseProfile = "brief",
    ) -> dict[str, Any]:
        """Find observed URLs (including query/fragment); default limit 20. No URL is fetched.

        Use next_actions; poll, result_page and source_scan have distinct tokens.
        """
        return self._safe(
            "wechat_find_links",
            lambda: self.service.retrieval.find_links(
                query=query, account_id=account_id, conversation_ids=tuple(conversation_ids or ()),
                domains=tuple(domains or ()), hints=tuple(hints or ()), after=after,
                before=before, cursor=cursor, reading_token=reading_token, limit=limit,
            ), scope_kind="link_catalog",
            scope_values=tuple(value for value in (account_id, *(conversation_ids or ())) if value),
        )

    def wechat_retrieve(
        self, concept: str, hints: list[str] | None = None, account_id: str | None = None,
        conversation_ids: list[str] | None = None, participant_ids: list[str] | None = None,
        kinds: list[Literal["message", "link", "image", "file", "voice"]] | None = None,
        count_hint: int | None = None, after: str | None = None, before: str | None = None,
        cursor: str | None = None, reading_token: str | None = None, limit: int | None = None,
        *, response_profile: ResponseProfile = "brief",
    ) -> dict[str, Any]:
        """Retrieve candidate contexts; default limit 3. Proximity does not prove topic identity.

        Use next_actions; poll, result_page and source_scan have distinct tokens.
        """
        return self._safe(
            "wechat_retrieve",
            lambda: self.service.retrieval.retrieve(
                concept=concept, hints=tuple(hints or ()), account_id=account_id,
                conversation_ids=tuple(conversation_ids or ()),
                participant_ids=tuple(participant_ids or ()), kinds=tuple(kinds or ()),
                count_hint=count_hint, after=after, before=before, cursor=cursor,
                reading_token=reading_token, limit=limit,
            ), scope_kind="retrieval",
            scope_values=tuple(value for value in
                               (account_id, *(conversation_ids or ()), *(participant_ids or ()))
                               if value),
        )

    def wechat_list_resources(self, message_id: str,
        *, response_profile: ResponseProfile = "brief",
    ) -> dict[str, Any]:
        """List resource descriptors bound to one opaque message ID."""
        return self._safe(
            "wechat_list_resources",
            lambda: self.service.list_resources(message_id),
            scope_kind="message",
            scope_values=(message_id,),
        )

    def wechat_find_resources(
        self,
        query: str = "",
        account_id: str | None = None,
        conversation_ids: list[str] | None = None,
        kinds: list[str] | None = None,
        format_families: list[
            Literal[
                "image",
                "audio",
                "video",
                "pdf",
                "workbook",
                "presentation",
                "archive",
                "text",
                "office",
                "binary",
            ]
        ]
        | None = None,
        after: str | None = None,
        before: str | None = None,
        availability: list[str] | None = None,
        cursor: str | None = None,
        limit: int = 50,
        *, response_profile: ResponseProfile = "brief",
    ) -> dict[str, Any]:
        """Find authorized materialized resources by name, scope, time, and format."""

        selected_conversations = tuple(conversation_ids or ())
        return self._safe(
            "wechat_find_resources",
            lambda: self.service.find_resources(
                query=query,
                account_id=account_id,
                conversation_ids=selected_conversations,
                kinds=tuple(kinds or ()),
                format_families=tuple(format_families or ()),
                after=after,
                before=before,
                availability=tuple(availability or ()),
                cursor=cursor,
                limit=limit,
            ),
            conversation_id=(
                selected_conversations[0] if len(selected_conversations) == 1 else None
            ),
            scope_kind="resource_catalog",
            scope_values=tuple(
                value for value in (account_id, *selected_conversations) if value
            ),
        )

    def wechat_read_resource(
        self,
        resource_id: str,
        mode: Literal[
            "metadata",
            "preview",
            "original",
            "text",
            "page",
            "members",
            "table",
            "slide",
        ] = "preview",
        page: int | None = None,
        start_line: int | None = None,
        end_line: int | None = None,
        member: str | None = None,
        sheet: str | None = None,
        cell_range: str | None = None,
        max_bytes: int = 4 * 1024 * 1024,
        reading_token: str | None = None,
        *, response_profile: ResponseProfile = "brief",
    ) -> CallToolResult:
        """Read one bound resource, including Office tables/slides and safe ZIP members."""
        started_at = utc_now().isoformat(timespec="microseconds")
        payload = None
        attempts = 3 if self.service.provider.descriptor.source_mode == "live" else 1
        for attempt in range(attempts):
            try:
                payload = self.service.read_resource(
                    resource_id=resource_id,
                    mode=mode,
                    page=page,
                    start_line=start_line,
                    end_line=end_line,
                    max_bytes=max_bytes,
                    member=member,
                    sheet=sheet,
                    cell_range=cell_range,
                    reading_token=reading_token,
                )
                descriptor = payload.descriptor
                break
            except SightglassError as exc:
                payload = None
                if exc.code == ErrorCode.SOURCE_GENERATION_CHANGED and attempt + 1 < attempts:
                    continue
                descriptor = exc.as_dict()
                break
            except Exception as exc:
                payload = None
                descriptor = (
                    SightglassError(ErrorCode.SERVICE_TIMEOUT, retryable=True).as_dict()
                    if operation_expired()
                    else map_unexpected_error(exc)
                )
                break

        content: list[Any] = [
            TextContent(
                type="text",
                text=json.dumps(
                    descriptor,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
        ]
        returned_bytes = 0
        if payload is not None and payload.content_kind is not None:
            uri = AnyUrl(f"sightglass://resource/{resource_id}/{mode}")
            if payload.content_kind == "image" and payload.data is not None and payload.mime_type:
                content.append(
                    ImageContent(
                        type="image",
                        data=base64.b64encode(payload.data).decode("ascii"),
                        mimeType=payload.mime_type,
                    )
                )
                returned_bytes = len(payload.data)
            elif payload.content_kind == "audio" and payload.data is not None and payload.mime_type:
                content.append(
                    AudioContent(
                        type="audio",
                        data=base64.b64encode(payload.data).decode("ascii"),
                        mimeType=payload.mime_type,
                    )
                )
                returned_bytes = len(payload.data)
            elif payload.content_kind == "blob" and payload.data is not None:
                content.append(
                    EmbeddedResource(
                        type="resource",
                        resource=BlobResourceContents(
                            uri=uri,
                            mimeType=payload.mime_type,
                            blob=base64.b64encode(payload.data).decode("ascii"),
                        ),
                    )
                )
                returned_bytes = len(payload.data)
            elif payload.content_kind == "text" and payload.text is not None:
                content.append(
                    EmbeddedResource(
                        type="resource",
                        resource=TextResourceContents(
                            uri=uri,
                            mimeType=payload.mime_type,
                            text=payload.text,
                        ),
                    )
                )
                returned_bytes = len(payload.text.encode("utf-8"))

        try:
            self.receipt_recorder(
                tool_name="wechat_read_resource",
                conversation_id=None,
                scope_kind="resource",
                scope_values=(resource_id,),
                result=receipt_projection("wechat_read_resource", descriptor),
                started_at=started_at,
                binary_bytes_returned=returned_bytes,
            )
        except Exception:
            pass
        return CallToolResult(
            content=content,
            structuredContent=descriptor,
            isError=descriptor.get("ok", True) is False,
        )

    def wechat_search_resource_text(
        self,
        resource_id: str,
        query: str,
        limit: int = 20,
        *, response_profile: ResponseProfile = "brief",
    ) -> dict[str, Any]:
        """Search safely extracted PDF or UTF-8 text within one bound resource."""
        return self._safe(
            "wechat_search_resource_text",
            lambda: self.service.search_resource_text(
                resource_id=resource_id,
                query=query,
                limit=limit,
            ),
            scope_kind="resource",
            scope_values=(resource_id,),
        )

    def wechat_read_transcripts(
        self,
        reading_token: str,
        cursor: str | None = None,
        wait_ms: _TranscriptWait = None,
        *, response_profile: ResponseProfile = "brief",
    ) -> dict[str, Any]:
        """Read committed transcript events using fields.

        Drain next_cursor while has_more_results_now, even when processing_complete.
        Processing progress and committed text pages are independent. Use next_actions.
        """
        return self._safe(
            "wechat_read_transcripts",
            lambda: self.read_voice_transcripts(
                reading_token, cursor=cursor, wait_ms=wait_ms
            ),
            scope_kind="voice_batch",
            scope_values=(reading_token,),
        )

    def read_voice_transcripts(
        self, reading_token: str, *, cursor: str | None = None, wait_ms: int | None = None,
    ) -> dict[str, Any]:
        """Project one committed voice page; the daemon waiter path owns ``wait_ms``."""

        if bound_wait_ms(wait_ms) is None:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if not isinstance(reading_token, str) or not reading_token:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if cursor is not None and not isinstance(cursor, str):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        voice_service = self.voice_service
        if voice_service is None:
            raise SightglassError(ErrorCode.SERVICE_UNAVAILABLE)
        self.service.reader.require_active()
        # A committed transcript is derived from a message-bound resource, so a
        # revoked preview capability must close existing tokens as well as new
        # ones; message authorization alone is not enough to release its text.
        self.service.reader.require_resource("preview")
        conversations = voice_service.repository.batch_conversation_ids(reading_token)
        if not conversations:
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        for conversation_id in conversations:
            self.service.reader.authorize(conversation_id)
        accounts = list(self.service.repository.active_accounts())
        if len(accounts) != 1:
            raise SightglassError(ErrorCode.ACCOUNT_NOT_FOUND)
        account_binding_id = accounts[0]["account_binding_id"]
        page = voice_service.get_transcripts(
            reading_token=reading_token,
            reader_id=self.service.reader.reader_id,
            account_id=str(accounts[0]["account_id"]),
            account_binding_id=(
                None if account_binding_id is None else str(account_binding_id)
            ),
            cursor=cursor,
        )
        return page.as_dict()


# ReaderTools owns the complete public declaration; bridge proxies derive from it.
for _name, _method in tuple(vars(ReaderTools).items()):
    if _name.startswith("wechat_"):
        setattr(ReaderTools, _name, mcp_response(_method))
