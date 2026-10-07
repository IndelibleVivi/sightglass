"""Read generated messages in disposable state, without WeChat or Keychain access."""

from __future__ import annotations

import json
import secrets
import tempfile
from pathlib import Path

from sightglass.mcp.tools import ReaderTools
from sightglass.model.db import WindowDB
from sightglass.model.repositories import WindowRepository
from sightglass.policy.readers import ReaderContext, ReaderPolicy
from sightglass.reader.service import ReaderService
from sightglass.source.direct_wechat import SyntheticSourceProvider
from sightglass.source.identity import SignedTokenCodec
from sightglass.source.synthetic import create_synthetic_source


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="sightglass-demo-") as directory:
        root = Path(directory)
        source = create_synthetic_source(root / "source")
        provider = SyntheticSourceProvider(source)
        repository = WindowRepository(WindowDB(root / "state" / "window.db"))
        reader = ReaderContext("demo", "Demo Reader", ReaderPolicy(mode="all_except_denylist"))
        service = ReaderService(
            provider, repository, reader, SignedTokenCodec(secrets.token_bytes(32))
        )
        tools = ReaderTools(service)
        try:
            catalog = tools.wechat_find_conversations("Synthetic Group")
            conversation_id = catalog["candidates"][0]["conversation_id"]
            page = tools.wechat_read_messages(
                mode="recent", conversation_id=conversation_id, limit=3, projection="detail"
            )
            assert page["schema"] == "sightglass.message-page.v1", page
            assert page["source_receipt"]["complete"] is True, page
            assert len(page["messages"]) == 3, page
            print(
                json.dumps(
                    {
                        "source": "generated synthetic fixture",
                        "schema": page["schema"],
                        "returned_messages": len(page["messages"]),
                        "source_page_complete": page["source_receipt"]["complete"],
                        "message_kinds": [message["kind"] for message in page["messages"]],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        finally:
            tools.close()


if __name__ == "__main__":
    main()
