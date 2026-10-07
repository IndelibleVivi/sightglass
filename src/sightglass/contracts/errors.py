from __future__ import annotations

from enum import StrEnum
from typing import Any


class ErrorCode(StrEnum):
    SERVICE_PAUSED = "SERVICE_PAUSED"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    SERVICE_TIMEOUT = "SERVICE_TIMEOUT"
    SERVICE_BUSY = "SERVICE_BUSY"
    STORAGE_PRESSURE = "STORAGE_PRESSURE"
    SOURCE_NOT_CONFIGURED = "SOURCE_NOT_CONFIGURED"
    SOURCE_PERMISSION_DENIED = "SOURCE_PERMISSION_DENIED"
    SOURCE_KEY_MISSING = "SOURCE_KEY_MISSING"
    SOURCE_INCOMPLETE = "SOURCE_INCOMPLETE"
    SOURCE_GENERATION_CHANGED = "SOURCE_GENERATION_CHANGED"
    SOURCE_SNAPSHOT_FAILED = "SOURCE_SNAPSHOT_FAILED"
    SOURCE_MESSAGE_DECODE_FAILED = "SOURCE_MESSAGE_DECODE_FAILED"
    ACCOUNT_NOT_FOUND = "ACCOUNT_NOT_FOUND"
    CONVERSATION_NOT_FOUND = "CONVERSATION_NOT_FOUND"
    CONVERSATION_AMBIGUOUS = "CONVERSATION_AMBIGUOUS"
    PARTICIPANT_NOT_FOUND = "PARTICIPANT_NOT_FOUND"
    PARTICIPANT_AMBIGUOUS = "PARTICIPANT_AMBIGUOUS"
    PARTICIPANT_OUT_OF_SCOPE = "PARTICIPANT_OUT_OF_SCOPE"
    MESSAGE_NOT_FOUND = "MESSAGE_NOT_FOUND"
    POLICY_DENIED = "POLICY_DENIED"
    CURSOR_INVALID = "CURSOR_INVALID"
    CURSOR_STALE = "CURSOR_STALE"
    DELIVERY_ACK_INVALID = "DELIVERY_ACK_INVALID"
    RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND"
    RESOURCE_UNAVAILABLE = "RESOURCE_UNAVAILABLE"
    RESOURCE_TOO_LARGE = "RESOURCE_TOO_LARGE"
    RESOURCE_UNSUPPORTED = "RESOURCE_UNSUPPORTED"
    RESOURCE_DECODE_FAILED = "RESOURCE_DECODE_FAILED"
    RESOURCE_BLOCKED = "RESOURCE_BLOCKED"
    QUERY_INVALID = "QUERY_INVALID"
    OUTPUT_BUDGET_EXCEEDED = "OUTPUT_BUDGET_EXCEEDED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


_DEFAULT_MESSAGES = {
    ErrorCode.SERVICE_PAUSED: "Sightglass 当前已暂停。",
    ErrorCode.SERVICE_UNAVAILABLE: "Sightglass 本地读取服务当前不可用。",
    ErrorCode.SERVICE_TIMEOUT: "Sightglass 本地读取超时，请重试。",
    ErrorCode.SERVICE_BUSY: "Sightglass 正在处理其他读取，请稍后重试。",
    ErrorCode.STORAGE_PRESSURE: "Sightglass 存储达到预算或磁盘余量不足，新增写入已暂停。",
    ErrorCode.SOURCE_NOT_CONFIGURED: "消息源尚未配置。",
    ErrorCode.SOURCE_INCOMPLETE: "微信消息源当前不完整，读取未执行。",
    ErrorCode.SOURCE_GENERATION_CHANGED: "读取期间消息源发生变化，读取未执行。",
    ErrorCode.ACCOUNT_NOT_FOUND: "未找到指定账号。",
    ErrorCode.CONVERSATION_NOT_FOUND: "当前覆盖范围内未找到指定会话。",
    ErrorCode.PARTICIPANT_NOT_FOUND: "当前覆盖范围内未找到指定成员。",
    ErrorCode.PARTICIPANT_AMBIGUOUS: "找到多个同名成员，需要明确选择。",
    ErrorCode.PARTICIPANT_OUT_OF_SCOPE: "成员不属于指定会话。",
    ErrorCode.MESSAGE_NOT_FOUND: "当前覆盖范围内未找到指定消息。",
    ErrorCode.POLICY_DENIED: "当前 reader 无权读取该会话。",
    ErrorCode.QUERY_INVALID: "请求参数或当前里程碑不支持该读取方式。",
}


class SightglassError(RuntimeError):
    def __init__(
        self,
        code: ErrorCode | str,
        message: str | None = None,
        *,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.code = ErrorCode(code)
        self.safe_message = message or _DEFAULT_MESSAGES.get(self.code, "Sightglass 请求失败。")
        self.retryable = bool(retryable)
        self.details = dict(details or {})
        super().__init__(self.safe_message)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": "sightglass.error.v1",
            "ok": False,
            "code": self.code.value,
            "message": self.safe_message,
            "retryable": self.retryable,
            "details": self.details,
        }


def map_unexpected_error(_error: Exception) -> dict[str, Any]:
    return SightglassError(ErrorCode.INTERNAL_ERROR).as_dict()
