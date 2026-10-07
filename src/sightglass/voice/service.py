from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from sightglass.contracts.common import parse_aware_datetime, to_utc_iso, utc_now
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.voice import (
    VoiceBatchReceipt,
    VoiceCoverage,
    VoiceSelectionItem,
    VoiceTranscriptItem,
    VoiceTranscriptPage,
)
from sightglass.resources.cache import ResourceObjectStore
from sightglass.source.identity import SignedTokenCodec

from .repository import VoiceRepository


def _json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


TRANSCRIPT_RECIPE_KIND = "derived_transcript"
TRANSCRIPT_RECIPE_ENGINE = "sightglass.voice.daemon-worker.v1"


def transcript_recipe(*, language: str, engine: str = TRANSCRIPT_RECIPE_ENGINE) -> tuple[str, str]:
    """Return the canonical digest and JSON of one transcript recipe.

    The recipe identifies the derivation, so a real recognizer must extend these
    fields with its own engine/model identity instead of reusing this marker.
    """

    recipe = {
        "engine": engine,
        "kind": TRANSCRIPT_RECIPE_KIND,
        "language": language,
        "version": 1,
    }
    encoded = _json(recipe)
    return hashlib.sha256(encoded).hexdigest(), encoded.decode("utf-8")


@dataclass(frozen=True)
class VoiceLimits:
    batch_ttl_seconds: int = 86400
    first_count: int = 3
    first_duration_ms: int = 300000
    step_count: int = 12
    step_duration_ms: int = 300000
    global_count: int = 32
    global_duration_ms: int = 600000
    item_duration_ms: int = 120000

    def __post_init__(self) -> None:
        if any(type(value) is not int or value <= 0 for value in vars(self).values()):
            raise ValueError("voice limits must be positive integers")
        if self.first_count > 3 or self.step_count > 12 or self.global_count > 32:
            raise ValueError("voice count limits exceed the admission ceiling")
        if self.step_duration_ms > 300000 or self.global_duration_ms > 600000:
            raise ValueError("voice duration limits exceed the admission ceiling")


class VoiceService:
    def __init__(
        self, repository: VoiceRepository, token_codec: SignedTokenCodec, *,
        limits: VoiceLimits = VoiceLimits(), clock: Callable[[], datetime] = utc_now,
        object_store: ResourceObjectStore | None = None,
    ) -> None:
        self.repository = repository
        self.token_codec = token_codec
        self.limits = limits
        self.clock = clock
        self._worker_wake: Callable[[], None] | None = None
        self.object_store = object_store or ResourceObjectStore(
            repository.database.path, storage=repository.database.storage,
        )

    def set_worker_wake(self, wake: Callable[[], None] | None) -> None:
        """Bind this daemon context's independent voice queue notification."""
        self._worker_wake = wake

    def _wake_pending(self) -> None:
        if self._worker_wake is not None:
            self.repository.database.wake_after_commit(self._worker_wake)

    @staticmethod
    def selection_digest(
        reader_id: str, account_id: str, selection: Sequence[VoiceSelectionItem],
        recipe_digest: str,
    ) -> str:
        return hashlib.sha256(_json({
            "version": 1, "reader_id": reader_id, "account_id": account_id,
            "selection": [[i.message_id, i.resource_id, i.resource_revision] for i in selection],
            "recipe_digest": recipe_digest,
        })).hexdigest()

    def _binding(self, account_id: str, binding: str | None) -> None:
        account = self.repository.account(account_id)
        if account is None or account["account_binding_id"] != binding:
            raise SightglassError(ErrorCode.CURSOR_INVALID)

    def create_batch(
        self, *, reader_id: str, account_id: str, account_binding_id: str | None,
        selection: Sequence[VoiceSelectionItem], recipe_digest: str,
        recipe_json: str, voice_policy: str = "auto",
    ) -> VoiceBatchReceipt:
        if voice_policy not in {"auto", "cached", "off"} or not recipe_digest:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        try:
            recipe = json.loads(recipe_json)
            if not isinstance(recipe, dict):
                raise ValueError("recipe must be an object")
            canonical_recipe = _json(recipe).decode("utf-8")
        except (ValueError, TypeError) as exc:
            raise SightglassError(ErrorCode.QUERY_INVALID) from exc
        manifest = tuple(selection)
        digest = self.selection_digest(reader_id, account_id, manifest, recipe_digest)
        with self.repository.database.transaction():
            self._binding(account_id, account_binding_id)
            if voice_policy == "off":
                return VoiceBatchReceipt(None, False, "off", None, VoiceCoverage())
            voices: list[tuple[int, VoiceSelectionItem]] = []
            for ordinal, item in enumerate(manifest):
                resource = self.repository.resource(item.resource_id)
                if (resource is None or resource["message_id"] != item.message_id
                        or resource["account_id"] != account_id):
                    raise SightglassError(ErrorCode.RESOURCE_NOT_FOUND)
                if not item.resource_revision or (item.duration_ms is not None and (
                    type(item.duration_ms) is not int or item.duration_ms <= 0
                )):
                    raise SightglassError(ErrorCode.QUERY_INVALID)
                if resource["kind"] == "voice":
                    voices.append((ordinal, item))
            if not voices:
                return VoiceBatchReceipt(None, False, "no_voice", None, VoiceCoverage())
            now = to_utc_iso(self.clock())
            existing = self.repository.existing_batch(digest, account_binding_id, voice_policy, now)
            if existing is not None:
                if voice_policy == "cached":
                    self._refresh_cached_items(existing, now)
                return self._receipt(existing, False)
            storage = self.repository.database.storage
            if storage is not None:
                try:
                    storage.require(background=True)
                except SightglassError as error:
                    if error.code != ErrorCode.STORAGE_PRESSURE:
                        raise
                    return VoiceBatchReceipt(
                        None, False, "storage_pressure", None,
                        VoiceCoverage(selected=len(voices), not_scheduled=len(voices)),
                    )
            batch: dict[str, Any] = dict(
                batch_id="voice_" + uuid4().hex, reader_id=reader_id, account_id=account_id,
                account_binding_id=account_binding_id, selection_digest=digest,
                recipe_digest=recipe_digest, voice_policy=voice_policy, state="open",
                created_at=now, expires_at=to_utc_iso(
                    parse_aware_datetime(now) + timedelta(seconds=self.limits.batch_ttl_seconds)),
            )
            self.repository.insert_batch(batch)
            for ordinal, selected in voices:
                item = dict(batch_id=batch["batch_id"], ordinal=ordinal,
                            message_id=selected.message_id, resource_id=selected.resource_id,
                            resource_revision=selected.resource_revision, job_id=None,
                            admission_step=0, state="queued")
                self.repository.insert_item(item)
                self._admit_item(batch, item, 0, selected.duration_ms, canonical_recipe, now)
                current = next(i for i in self.repository.items(batch["batch_id"])
                               if i["ordinal"] == ordinal)
                self._event(current, now, metadata={
                    "duration_ms": selected.duration_ms, "recipe_json": canonical_recipe,
                })
            receipt = self._receipt(batch, True)
        # The database defers a nested admission's notification until the outer
        # delivery/message transaction commits and releases the writer.
        if receipt.coverage.pending:
            self._wake_pending()
        return receipt

    def _receipt(self, batch: dict[str, Any], created: bool) -> VoiceBatchReceipt:
        return VoiceBatchReceipt(batch["batch_id"], created, "created" if created else "reused",
                                 batch["expires_at"], self._coverage(batch["batch_id"]))

    def _refresh_cached_items(self, batch: dict[str, Any], now: str) -> None:
        """Re-check cache hits for a reused cached-only batch.

        A cached-only batch never admitted work, so its misses stay non-terminal and a
        later reuse may promote them once a transcript has been committed elsewhere.
        """

        for item in self.repository.items(batch["batch_id"]):
            if item["state"] not in {"queued", "rejected"}:
                continue
            job = self.repository.matching_job(
                batch["account_id"], item["resource_id"], item["resource_revision"],
                batch["recipe_digest"],
            )
            if (
                job is None
                or job["state"] != "ready"
                or job["account_binding_id"] != batch["account_binding_id"]
            ):
                continue
            self._load(job["result_digest"])
            self.repository.update_item(batch["batch_id"], item["ordinal"], state="cached",
                                        job_id=job["job_id"],
                                        admission_step=item["admission_step"])
            current = next(i for i in self.repository.items(batch["batch_id"])
                           if i["ordinal"] == item["ordinal"])
            self._event(current, now)

    def _admit_item(
        self, batch: dict[str, Any], item: dict[str, Any], step: int,
        duration: int | None, recipe: str, now: str,
    ) -> None:
        repo = self.repository
        job = repo.matching_job(batch["account_id"], item["resource_id"],
                                item["resource_revision"], batch["recipe_digest"])
        if job is not None and job["account_binding_id"] != batch["account_binding_id"]:
            repo.update_item(batch["batch_id"], item["ordinal"], state="rejected",
                             admission_step=step)
            return
        if job is not None and job["state"] == "ready":
            self._load(job["result_digest"])
            repo.update_item(batch["batch_id"], item["ordinal"], state="cached",
                             job_id=job["job_id"], admission_step=step)
            return
        if batch["voice_policy"] == "cached":
            repo.update_item(batch["batch_id"], item["ordinal"], state="rejected",
                             admission_step=step)
            return
        if job is not None and job["state"] == "blocked":
            repo.update_item(batch["batch_id"], item["ordinal"], state="admitted",
                             job_id=job["job_id"], admission_step=step)
            return
        reserved = duration if duration is not None else self.limits.item_duration_ms
        count_limit = self.limits.first_count if step == 0 else self.limits.step_count
        ms_limit = self.limits.first_duration_ms if step == 0 else self.limits.step_duration_ms
        count, ms = repo.step_budget(batch["batch_id"], step)
        state = "queued"
        if reserved > self.limits.item_duration_ms or reserved > ms_limit:
            state = "rejected"
        elif count < count_limit and ms + reserved <= ms_limit:
            global_count, global_ms = repo.active_budget(self.limits.item_duration_ms)
            if job is None and (global_count >= self.limits.global_count
                                or global_ms + reserved > self.limits.global_duration_ms):
                state = "rejected"
            else:
                if job is None:
                    job = dict(job_id="vjob_" + uuid4().hex, account_id=batch["account_id"],
                               account_binding_id=batch["account_binding_id"],
                               resource_id=item["resource_id"],
                               resource_revision=item["resource_revision"],
                               recipe_digest=batch["recipe_digest"], recipe_json=recipe,
                               state="pending", max_duration_ms=reserved,
                               created_at=now, updated_at=now)
                    repo.insert_job(job)
                repo.update_item(batch["batch_id"], item["ordinal"], state="admitted",
                                 job_id=job["job_id"], admission_step=step)
                return
        repo.update_item(batch["batch_id"], item["ordinal"], state=state, admission_step=step)

    def _store(self, value: dict[str, Any], now: str) -> str:
        data = _json(value)
        obj, path = self.object_store.put(
            data, mime_type="application/json", origin="derived",
            maintenance=self.repository.database.maintenance_write_active,
        )
        self.repository.insert_object(dict(object_digest=obj.digest, local_path_internal=path,
                                           byte_size=len(data), mime_type=obj.mime_type,
                                           origin=obj.origin, created_at=now))
        return obj.digest

    def _load(self, digest: str) -> dict[str, Any]:
        row = self.repository.object(digest)
        if row is None:
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
        value = json.loads(self.object_store.read_binding(row).data)
        if not isinstance(value, dict):
            raise SightglassError(ErrorCode.RESOURCE_BLOCKED)
        return value

    def _state(self, item: dict[str, Any]) -> tuple[str, str | None]:
        state = item.get("job_state")
        if state == "ready":
            text = self._load(item["result_digest"])["text"]
            return ("ready" if text.strip() else "empty"), text
        if state in {"pending", "leased", "running", "blocked", "failed", "cancelled"}:
            return str(state), None
        return "not_scheduled", None

    def _event(
        self, item: dict[str, Any], now: str, *, metadata: dict[str, Any] | None = None,
    ) -> None:
        state, text = self._state(item)
        error_code = item.get("error_code")
        snapshot = {
            "state": state,
            "text": text,
            "error_code": str(error_code) if error_code else None,
            **(metadata or {}),
        }
        digest = self._store(snapshot, now)
        kind = "ready" if state in {"ready", "empty"} else (
            "failed" if state == "failed" else "state-change")
        self.repository.add_event(item["batch_id"], item["ordinal"], item["job_id"],
                                  kind, digest, now)

    def _coverage(self, batch_id: str) -> VoiceCoverage:
        items = self.repository.items(batch_id)
        counts = dict(ready=0, pending=0, not_scheduled=0, blocked=0,
                      failed=0, empty=0, cancelled=0)
        for item in items:
            state, _ = self._state(item)
            counts["pending" if state in {"leased", "running"} else state] += 1
        return VoiceCoverage(selected=len(items), **counts)

    def get_transcripts(
        self, *, reading_token: str, reader_id: str, account_id: str,
        account_binding_id: str | None, cursor: str | None = None,
    ) -> VoiceTranscriptPage:
        wake_pending = False
        with self.repository.database.transaction(maintenance=True):
            batch = self.repository.batch(reading_token)
            if (batch is None or batch["reader_id"] != reader_id
                    or batch["account_id"] != account_id
                    or batch["account_binding_id"] != account_binding_id):
                raise SightglassError(ErrorCode.CURSOR_INVALID)
            self._binding(account_id, account_binding_id)
            now = to_utc_iso(self.clock())
            if batch["expires_at"] <= now or batch["state"] in {"expired", "cancelled"}:
                raise SightglassError(ErrorCode.CURSOR_STALE)
            scope = dict(kind="voice-events-v1", batch_id=reading_token, reader_id=reader_id,
                         account_id=account_id, account_binding_id=account_binding_id)
            after = 0
            if cursor is not None:
                payload = self.token_codec.decode(cursor)
                after = payload.get("event_id")
                if (type(after) is not int or after <= 0
                        or payload != {**scope, "event_id": after}
                        or self.repository.event(reading_token, after) is None):
                    raise SightglassError(ErrorCode.CURSOR_INVALID)
            items = self.repository.items(reading_token)
            storage = self.repository.database.storage
            can_admit = storage is None or storage.status()["background_growth_allowed"]
            if can_admit and after > max(i["admission_step"] for i in items):
                for item in items:
                    if item["state"] != "queued" or item["job_id"] is not None:
                        continue
                    metadata = self._load(self.repository.first_event(
                        reading_token, item["ordinal"])["result_digest"])
                    self._admit_item(batch, item, after, metadata["duration_ms"],
                                     metadata["recipe_json"], now)
                    current = next(i for i in self.repository.items(reading_token)
                                   if i["ordinal"] == item["ordinal"])
                    if self._state(current) != self._state(item):
                        self._event(current, now)
                        wake_pending |= self._state(current)[0] == "pending"
            events = self.repository.events(reading_token, after)
            rows: tuple[VoiceTranscriptItem, ...] = ()
            next_cursor = cursor
            if events:
                event = events[0]
                snapshot = self._load(event["result_digest"])
                rows = (
                    VoiceTranscriptItem(
                        event["message_id"],
                        event["resource_id"],
                        event["item_ordinal"],
                        snapshot["state"],
                        snapshot["text"],
                        snapshot.get("error_code"),
                    ),
                )
                next_cursor = self.token_codec.encode({**scope, "event_id": event["event_id"]})
            coverage = self._coverage(reading_token)
            unscheduled = any(i["state"] == "queued"
                              for i in self.repository.items(reading_token))
            page = VoiceTranscriptPage(
                reading_token, rows, coverage, coverage.pending == 0 and not unscheduled,
                coverage.ready == coverage.selected, len(events) > 1,
                next_cursor, batch["expires_at"],
            )
        if wake_pending:
            self._wake_pending()
        return page

    def _notify(self, job_id: str, now: str) -> None:
        for linked in self.repository.job_items(job_id):
            item = next(i for i in self.repository.items(linked["batch_id"])
                        if i["ordinal"] == linked["ordinal"])
            self._event(item, now)

    def lease(self, job_id: str, *, owner_id: str, lease_seconds: int = 60) -> int:
        if not owner_id or type(lease_seconds) is not int or lease_seconds <= 0:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        with self.repository.database.transaction():
            job = self.repository.job(job_id)
            if job is None or job["state"] != "pending":
                raise SightglassError(ErrorCode.QUERY_INVALID)
            self._binding(job["account_id"], job["account_binding_id"])
            now = to_utc_iso(self.clock())
            fence = (job["fencing_token"] or 0) + 1
            lease_expires_at = to_utc_iso(
                parse_aware_datetime(now) + timedelta(seconds=lease_seconds))
            self.repository.update_job(job_id, state="leased", owner_id=owner_id,
                                       fencing_token=fence, attempt=job["attempt"] + 1,
                                       lease_expires_at=lease_expires_at, updated_at=now)
            self._notify(job_id, now)
            return fence

    def _owned(self, job_id: str, owner_id: str, fence: int, now: str) -> dict[str, Any]:
        job = self.repository.job(job_id)
        if (job is None or type(fence) is not int or job["owner_id"] != owner_id
                or job["fencing_token"] != fence):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        self._binding(job["account_id"], job["account_binding_id"])
        if job["state"] in {"leased", "running"} and job["lease_expires_at"] <= now:
            raise SightglassError(ErrorCode.CURSOR_STALE)
        return job

    def start(self, job_id: str, *, owner_id: str, fencing_token: int) -> None:
        with self.repository.database.transaction():
            now = to_utc_iso(self.clock())
            job = self._owned(job_id, owner_id, fencing_token, now)
            if job["state"] == "running":
                return
            if job["state"] != "leased":
                raise SightglassError(ErrorCode.CURSOR_STALE)
            self.repository.update_job(job_id, state="running", updated_at=now)
            self._notify(job_id, now)

    def complete(
        self, job_id: str, *, owner_id: str, fencing_token: int, text: str,
        provenance: Mapping[str, Any] | None = None,
    ) -> str:
        if not isinstance(text, str):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        value: dict[str, Any] = {"text": text}
        if provenance:
            value["provenance"] = dict(provenance)
        with self.repository.database.transaction():
            now = to_utc_iso(self.clock())
            job = self._owned(job_id, owner_id, fencing_token, now)
            digest = hashlib.sha256(_json(value)).hexdigest()
            if job["state"] == "ready" and job["result_digest"] == digest:
                self._load(digest)
                return digest
            if job["state"] not in {"leased", "running"}:
                raise SightglassError(ErrorCode.CURSOR_STALE)
            digest = self._store(value, now)
            self.repository.update_job(job_id, state="ready", result_digest=digest,
                                       error_code=None, updated_at=now)
            self._notify(job_id, now)
            return digest

    def fail(
        self, job_id: str, *, owner_id: str, fencing_token: int,
        error_code: str, state: str = "failed",
    ) -> None:
        if state not in {"failed", "blocked", "cancelled"} or not error_code:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        with self.repository.database.transaction():
            now = to_utc_iso(self.clock())
            job = self._owned(job_id, owner_id, fencing_token, now)
            if job["state"] == state and job["error_code"] == error_code:
                return
            if job["state"] not in {"leased", "running"}:
                raise SightglassError(ErrorCode.CURSOR_STALE)
            self.repository.update_job(job_id, state=state, error_code=error_code, updated_at=now)
            self._notify(job_id, now)

    def requeue(
        self, job_id: str, *, owner_id: str, fencing_token: int,
        storage_pressure: bool = False,
    ) -> None:
        """Return one leased job to the queue after a transient failure.

        The previous fencing token is spent, so a recognizer that keeps running
        cannot commit a result for this attempt.
        """
        with self.repository.database.transaction(maintenance=storage_pressure):
            now = to_utc_iso(self.clock())
            job = self._owned(job_id, owner_id, fencing_token, now)
            if job["state"] not in {"leased", "running"}:
                raise SightglassError(ErrorCode.CURSOR_STALE)
            self.repository.update_job(job_id, state="pending", owner_id=None,
                                       lease_expires_at=None,
                                       attempt=job["attempt"] - int(storage_pressure),
                                       fencing_token=(job["fencing_token"] or 0) + 1,
                                       updated_at=now)
            self._notify(job_id, now)

    def retry_blocked_jobs(self) -> int:
        """Operator remediation: return every blocked job to the queue.

        ``blocked`` is permanent for the worker itself — it never retries a
        recognizer that reported a non-retryable environment failure.  An
        operator who has repaired that environment (installed the speech
        asset, replaced the helper) may explicitly requeue; the fencing token
        still advances so nothing from the blocked attempt can commit later.
        """
        with self.repository.database.transaction():
            now = to_utc_iso(self.clock())
            blocked = self.repository.rows(
                "SELECT * FROM voice_jobs WHERE state = 'blocked'")
            for job in blocked:
                self.repository.update_job(
                    job["job_id"], state="pending", error_code=None, owner_id=None,
                    lease_expires_at=None,
                    fencing_token=(job["fencing_token"] or 0) + 1, updated_at=now)
                self._notify(job["job_id"], now)
            return len(blocked)

    def recover_expired_leases(self) -> int:
        if not self.repository.expired_leases(to_utc_iso(self.clock())):
            return 0
        with self.repository.database.transaction(maintenance=True):
            now = to_utc_iso(self.clock())
            return self._recover(self.repository.expired_leases(now), now)

    def recover_outstanding_leases(self) -> int:
        """Return every held lease to the queue for a replacing worker context.

        A daemon that takes over the local runtime is the only authorized
        transcriber from that moment on, so a previous context's live lease must not
        block the queue or stay able to commit.
        """
        if not self.repository.outstanding_leases():
            return 0
        with self.repository.database.transaction(maintenance=True):
            now = to_utc_iso(self.clock())
            return self._recover(self.repository.outstanding_leases(), now)

    def next_lease_delay(self) -> float | None:
        deadline = self.repository.next_lease_deadline()
        if deadline is None:
            return None
        return (datetime.fromisoformat(deadline) - self.clock()).total_seconds()

    def _recover(self, jobs: list[dict[str, Any]], now: str) -> int:
        for job in jobs:
            self.repository.update_job(job["job_id"], state="pending", owner_id=None,
                                       lease_expires_at=None,
                                       fencing_token=(job["fencing_token"] or 0) + 1,
                                       updated_at=now)
            self._notify(job["job_id"], now)
        return len(jobs)

    def read_batch(self, batch_id: str, *, limit: int = 32) -> dict[str, Any]:
        """Read one batch's committed items and coverage without creating work.

        Callers must already have verified reader/account authorization for the
        batch.  This accessor never advances admission, appends an event, or leases a
        job, so it is the read-only counterpart of ``get_transcripts``.
        """

        batch = self.repository.batch(batch_id)
        if batch is None:
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        items: list[dict[str, Any]] = []
        for item in self.repository.items(batch_id)[: max(0, int(limit))]:
            state, text = self._state(item)
            items.append({
                "message_id": str(item["message_id"]),
                "resource_id": str(item["resource_id"]),
                "ordinal": int(item["ordinal"]),
                "state": state,
                "text": text,
                "error_code": item.get("error_code"),
            })
        return {
            "reading_token": batch_id,
            "state": str(batch["state"]),
            "expires_at": str(batch["expires_at"]),
            "items": items,
            "coverage": self._coverage(batch_id).as_dict(),
        }
