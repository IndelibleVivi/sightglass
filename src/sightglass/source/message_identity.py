"""Native observed-message token encoding, shared by admission and explicit reply evidence."""

from __future__ import annotations

import base64
import json
from typing import Any


def native_message_token(conversation: str, kind: str, identity: tuple[Any, ...]) -> str:
    body = json.dumps([2, conversation, kind, *identity], separators=(",", ":")).encode()
    return "nmsg_" + base64.urlsafe_b64encode(body).decode().rstrip("=")
