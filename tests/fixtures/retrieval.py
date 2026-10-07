"""Explicitly synthetic aggregate-project discussion used by retrieval acceptance."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

from sightglass.source.parser import parse_message


def seed_aggregate_discussion(provider: Any, repository: Any, service: Any) -> dict[str, Any]:
    service.sync_source_once(initial_tail=100, conversation_limit=100)
    account = repository.active_account_ids()[0]
    conversations = repository.account_conversations(account)
    group = next(row for row in conversations if row["kind"] == "group")
    direct = next(row for row in conversations if row["kind"] == "direct")
    group_id = str(group["conversation_id"])
    epoch = service._projection_inventory_epoch()
    seed = repository.materialized_message_rows(
        group_id,
        projection_epoch=epoch,
        observation_watermark=repository.observation_watermark(),
        limit=1,
        direction="forward",
    )[0]
    context = repository.conversation_context(group_id)
    with provider.snapshot() as snapshot:
        original = provider.get_message(
            context["source_account_key"], seed["source_message_id"], snapshot
        )
    assert original is not None
    base = datetime(2026, 9, 20, tzinfo=UTC)
    ids: dict[str, str] = {}

    def admit(name: str, text: str, instant: datetime, conversation: str = group_id) -> str:
        source = replace(
            original,
            source_message_id=f"synthetic-retrieval-{name}",
            source_conversation_id=group["source_conversation_id"]
            if conversation == group_id
            else direct["source_conversation_id"],
            conversation_kind="direct",
            wechat_type=1,
            raw_content=text,
            sent_at_utc=instant.isoformat(timespec="microseconds"),
            source_time_raw=instant.isoformat(),
            source_rowid=50_000 + len(ids),
            observed_at_utc=(instant + timedelta(seconds=1)).isoformat(timespec="microseconds"),
        )
        message_id = repository.upsert_message(
            account,
            conversation,
            seed["sender_id"],
            seed["sender_membership_id"] if conversation == group_id else None,
            source,
            parse_message(source),
            projection_epoch=epoch,
        )
        ids[name] = message_id
        return message_id

    with repository.database.transaction():
        for index in range(1_205):
            admit(
                f"noise-{index}",
                f"Synthetic unrelated weather {index}",
                base + timedelta(seconds=index),
            )
        moment = base + timedelta(days=1)
        admit("description", "这些收录了好些 AI companion 工具，可按类别挑选", moment)
        admit("first", "https://www.ailover-atlas.example/", moment + timedelta(seconds=10))
        admit("interleaved", "Synthetic unrelated: lunch is ready", moment + timedelta(seconds=45))
        admit(
            "second",
            "还有这个 https://lutopia.example/companion/?token=synthetic#fragment",
            moment + timedelta(seconds=130),
        )
        admit("duplicate", "https://www.ailover-atlas.example/", moment + timedelta(seconds=135))
        admit(
            "other-conversation",
            "https://atlas-companion.example/",
            moment,
            str(direct["conversation_id"]),
        )
        admit("distractor-project", "https://awesome-atlas.example/", moment + timedelta(days=1))
    return {
        "account": account,
        "group": group_id,
        "direct": str(direct["conversation_id"]),
        "ids": ids,
        "original": original,
        "seed": seed,
        "admit": admit,
        "moment": moment,
    }
