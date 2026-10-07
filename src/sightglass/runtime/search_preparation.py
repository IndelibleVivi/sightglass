"""Durable, bounded source preparation for ordinary search first pages.

Only preparation scope and a query digest enter the owner-private sidecar. Results
still come from ReaderService's current-source canonical scan, never this queue.
An interrupted conversation restarts its pinned source scan; already admitted
conversations are checkpointed. No source snapshot is claimed to survive restart.

The same bounded machinery also serves the cold link/retrieval discovery path
(``kind="discovery"``). A discovery job admits only the *matching* observed
messages (plus their canonical neighbors through the existing context expansion)
under an on_demand decision, rather than every scanned row. Internal preparation
facts feed the ordinary resident read, which projects only public coverage fields.
Versioned continuation tokens request another bounded attempt; poll tokens do not.
Traversal across independent leases never claims an atomic complete snapshot.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import stat
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sightglass.contracts.common import to_utc_iso, utc_now
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.operations import local_read_only_requested, operation_budget, operation_cancelled
from sightglass.reader.cursors import reader_binding
from sightglass.runtime.config import _fsync_directory
from sightglass.runtime.lanes import RuntimeLanes, WorkClass

if TYPE_CHECKING:
    from sightglass.reader.service import ReaderService
    from sightglass.runtime.source_worker import SourceWorker

SCHEMA = "sightglass.search-preparation.v1"
DISCOVERY_SCHEMA = "sightglass.retrieval-preparation.v1"
MAX_JOBS = 32
MAX_BYTES = 256 * 1024
TTL_SECONDS = 900
ATTEMPT_SECONDS = 120.0
MAX_ATTEMPTS = 3


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _intent(
    *,
    query: str,
    account_id: str | None,
    conversation_ids: tuple[str, ...],
    participant_ids: tuple[str, ...],
    sender_query: str | None,
    after_utc: str | None,
    before_utc: str | None,
    limit: int,
) -> tuple[dict[str, Any], str]:
    scope = {
        "account_id": account_id,
        "conversation_ids": sorted(set(conversation_ids)),
        "after_utc": after_utc,
        "before_utc": before_utc,
        "limit": limit,
    }
    return scope, _digest(
        {
            "scope": scope,
            "query_digest": _digest(" ".join(query.casefold().split())),
            "participants": sorted(set(participant_ids)),
            "sender_digest": _digest(sender_query.casefold() if sender_query else None),
        }
    )


def _discovery_intent(
    *,
    kind: str,
    query: str,
    hints: tuple[str, ...],
    domains: tuple[str, ...],
    kinds: tuple[str, ...],
    account_id: str | None,
    conversation_ids: tuple[str, ...],
    participant_ids: tuple[str, ...],
    after_utc: str | None,
    before_utc: str | None,
    limit: int,
) -> tuple[dict[str, Any], str]:
    """Bound a cold discovery request without persisting any query text.

    The sidecar stores only the scope and the digest of the literal recall terms
    (free-text query, hostname hints and exact domains, casefolded and sorted); the
    raw text stays in memory, exactly like the ordinary search path.
    """

    scope = {
        "account_id": account_id,
        "conversation_ids": sorted(set(conversation_ids)),
        "after_utc": after_utc,
        "before_utc": before_utc,
        "limit": limit,
        "kind": kind,
        "kinds": sorted(set(kinds)),
    }
    return scope, _digest(
        {
            "scope": scope,
            "query_digest": _digest(" ".join(query.casefold().split())),
            "hints": sorted(hint.casefold() for hint in hints),
            "domains": sorted(domains),
            "participants": sorted(set(participant_ids)),
        }
    )


class SearchPreparation:
    def __init__(
        self,
        service: ReaderService,
        path: Path,
        *,
        binding: str,
        lanes: RuntimeLanes,
        source_worker: SourceWorker,
    ) -> None:
        self.service = service
        if service.storage is None:
            raise RuntimeError("search preparation requires the daemon storage budget")
        self.storage = service.storage
        self.path = path
        self.binding = binding
        self.lanes = lanes
        self.source_worker = source_worker
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._progress: dict[str, dict[str, Any]] = {}
        self._pending_failures: dict[str, dict[str, Any]] = {}
        self._jobs: dict[str, dict[str, Any]] = {}
        # Query text stays in memory only. After restart, an identical token poll
        # supplies it again before on-demand body selection resumes. Discovery jobs
        # reuse the same in-memory input channel keyed by job id.
        self._inputs: dict[str, tuple[str, tuple[str, ...], str | None]] = {}
        # Discovery recall terms (free-text query, hostname hints, exact domains)
        # never enter the sidecar or a token; they are re-supplied by a poll.
        self._discovery_terms: dict[str, tuple[str, tuple[str, ...], tuple[str, ...]]] = {}
        self._load()
        self.service.search_preparation = self

    def _load(self) -> None:
        try:
            descriptor = os.open(self.path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except FileNotFoundError:
            return
        with os.fdopen(descriptor, "rb") as handle:
            metadata = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_mode & 0o077
                or metadata.st_uid != os.geteuid()
                or metadata.st_nlink != 1
                or metadata.st_size > MAX_BYTES
            ):
                raise RuntimeError(
                    "search preparation state must be a bounded private regular file"
                )
            data = json.load(handle)
        if data.get("schema") != SCHEMA or not isinstance(data.get("jobs"), dict):
            raise RuntimeError("invalid search preparation state")
        changed = False
        self._jobs = data["jobs"]
        if len(self._jobs) > MAX_JOBS:
            raise RuntimeError("search preparation state exceeds job bound")
        for job in self._jobs.values():
            job.setdefault("kind", "search")
            if job["state"] == "running":
                job["state"] = "preparing"
                job["restart_count"] += 1
            if self.service.reader.paused and job["state"] in {"preparing", "ready"}:
                job.update(
                    state="failed", error=SightglassError(ErrorCode.SERVICE_PAUSED).as_dict()
                )
                changed = True
        self.storage.track(self.path)
        if changed:
            self._save(maintenance=True)

    def _save(self, *, maintenance: bool = False) -> None:
        encoded = json.dumps(
            {"schema": SCHEMA, "jobs": self._jobs}, sort_keys=True, separators=(",", ":")
        ).encode()
        if len(encoded) > MAX_BYTES:
            raise SightglassError(ErrorCode.RESOURCE_TOO_LARGE)
        with self.storage.reserve(len(encoded) + 4096, maintenance=maintenance) as lease:
            descriptor, temporary = tempfile.mkstemp(
                prefix=".search-preparation-", dir=self.path.parent
            )
            temp = Path(temporary)
            try:
                with os.fdopen(descriptor, "wb") as handle:
                    os.fchmod(handle.fileno(), 0o600)
                    handle.write(encoded)
                    handle.flush()
                    os.fsync(handle.fileno())
                self.storage.track(temp)
                lease.verify()
                os.replace(temp, self.path)
                _fsync_directory(self.path.parent)
            finally:
                temp.unlink(missing_ok=True)
                self.storage.track(temp, self.path)

    def _authority(self, job: dict[str, Any]) -> None:
        self.service.reader.require_search()
        if job["binding"] != self.binding or job["policy"] != self.service._policy_revision():
            raise SightglassError(ErrorCode.POLICY_DENIED)
        for conversation_id in job["scope"]["conversation_ids"]:
            self.service.reader.authorize(conversation_id)
        if time.time() >= job["expires_at"]:
            raise SightglassError(ErrorCode.CURSOR_STALE)

    def _token(self, job: dict[str, Any], *, resume: bool = False) -> str:
        return self.service.token_codec.encode(
            {
                "schema": SCHEMA,
                "kind": "search-preparation",
                "job_kind": job.get("kind", "search"),
                **({"run": job.get("run", 0), "resume": resume}
                   if job.get("kind") == "discovery" else {}),
                "job_id": job["id"],
                "reader": reader_binding(self.service.reader.reader_id),
                "digest": job["digest"],
                "policy": job["policy"],
                "binding": self.binding,
                "expires_at": job["expires_at"],
            }
        )

    def _decode(self, token: str) -> dict[str, Any]:
        payload = self.service.token_codec.decode(token)
        if (
            payload.get("schema") != SCHEMA
            or payload.get("kind") != "search-preparation"
            or payload.get("reader") != reader_binding(self.service.reader.reader_id)
            or payload.get("policy") != self.service._policy_revision()
            or payload.get("binding") != self.binding
        ):
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        return payload

    def continuation_limit(self, token: str) -> int:
        """Recover the original request limit a preparation token was issued under.

        A continuation that omits ``limit`` must reuse the first request's limit so
        result pagination stays consistent with the digest/scope the token was bound
        to. Both an ordinary preparation poll token and a discovery continuation
        token resolve here: the private job scope already recorded ``limit``, and a
        caller can never inject a different one. Signature/version/kind/reader/policy
        and binding are checked by ``_decode``; a missing or expired job uses the
        existing stale-cursor semantics. No default is returned, no query text is
        stored or reconstructed, and the poll/discovery digest scope check in their
        own ``request*`` entry points is unchanged.
        """

        payload = self._decode(token)
        with self._lock:
            job = self._jobs.get(str(payload.get("job_id")))
            if job is None or time.time() >= job["expires_at"]:
                raise SightglassError(ErrorCode.CURSOR_STALE)
            if payload.get("digest") != job.get("digest"):
                raise SightglassError(ErrorCode.CURSOR_INVALID)
            limit = job.get("scope", {}).get("limit")
            if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
                raise SightglassError(ErrorCode.CURSOR_INVALID)
            return limit

    def local_request(self, arguments: dict[str, Any]) -> bool:
        token = arguments.get("reading_token") or arguments.get("cursor")
        if not token:
            return True
        try:
            self.service.reader.require_search()
            if arguments.get("strict", True) is not True or (
                arguments.get("reading_token") and arguments.get("cursor")
            ):
                return True
            payload = self.service.token_codec.decode(str(token))
            if payload.get("kind") != "search-preparation":
                return bool(arguments.get("reading_token")) or payload.get("kind") != "search"
            self._decode(str(token))
            # A continuation may omit ``limit``. Recompute the digest the original
            # request was bound to: recover the recorded scope limit for a valid
            # preparation token, and use the service default for a genuinely new
            # first request. Passing ``None`` through ``bound_limit`` raised and was
            # misclassified local-only, so a ready poll never reached the canonical
            # result plane.
            requested_limit = arguments.get("limit")
            if requested_limit is None:
                requested_limit = self.continuation_limit(str(token))
            _scope, digest = _intent(
                query=arguments["query"],
                account_id=arguments.get("account_id"),
                conversation_ids=tuple(arguments.get("conversation_ids") or ()),
                participant_ids=tuple(arguments.get("participant_ids") or ()),
                sender_query=arguments.get("sender_query"),
                after_utc=to_utc_iso(arguments["after"]) if arguments.get("after") else None,
                before_utc=to_utc_iso(arguments["before"]) if arguments.get("before") else None,
                limit=self.service.reader.bound_limit(requested_limit),
            )
            if payload.get("digest") != digest:
                return True
            with self._lock:
                job = self._jobs.get(str(payload.get("job_id")))
                return job is None or job["state"] != "ready" or time.time() >= job["expires_at"]
        except (SightglassError, ValueError, TypeError, KeyError, AttributeError):
            return True

    def _envelope(self, job: dict[str, Any], *, state: str | None = None) -> dict[str, Any]:
        discovery = job.get("kind") == "discovery"
        result: dict[str, Any] = {
            "schema": DISCOVERY_SCHEMA if discovery else SCHEMA,
            "state": state or ("preparing" if job["state"] == "running" else job["state"]),
            "reading_token": self._token(job),
            "retry_after_ms": 1000,
            "results_complete": False,
            "progress": {
                "completed_conversations": job.get("checkpoint", {}).get("done", 0),
                "total_conversations": len(job.get("checkpoint", {}).get("chosen", [])),
                "attempt": job["attempts"],
                "restart_count": job["restart_count"],
                **self._progress.get(job["id"], {}),
            },
            "instruction": (
                (
                    "Repeat identical discovery arguments with reading_token. Preparing is "
                    "not an empty result; resident subset warmth is not full source coverage."
                )
                if discovery
                else (
                    "Repeat identical search arguments with reading_token or cursor. "
                    "Preparing is not an empty search result."
                )
            ),
        }
        if job.get("error"):
            result["error"] = job["error"]
            result["retry_after_ms"] = 0
            result["instruction"] = (
                "Preparation failed. Report the error; do not keep polling this token."
            )
        result["preparation_complete"] = result["state"] == "ready"
        return result

    def request(
        self,
        *,
        query: str,
        account_id: str | None,
        conversation_ids: tuple[str, ...],
        participant_ids: tuple[str, ...],
        sender_query: str | None,
        after_utc: str | None,
        before_utc: str | None,
        limit: int,
        token: str | None,
    ) -> dict[str, Any]:
        scope, digest = _intent(
            query=query,
            account_id=account_id,
            conversation_ids=conversation_ids,
            participant_ids=participant_ids,
            sender_query=sender_query,
            after_utc=after_utc,
            before_utc=before_utc,
            limit=limit,
        )
        with self._lock:
            if token:
                payload = self._decode(token)
                if payload.get("digest") != digest:
                    raise SightglassError(ErrorCode.CURSOR_INVALID)
                job = self._jobs.get(str(payload.get("job_id")))
                if job is None or time.time() >= job["expires_at"]:
                    return {
                        "schema": SCHEMA,
                        "state": "expired",
                        "results_complete": False,
                        "error": SightglassError(ErrorCode.CURSOR_STALE).as_dict(),
                    }
                if job["digest"] != digest or payload.get("expires_at") != job["expires_at"]:
                    raise SightglassError(ErrorCode.CURSOR_INVALID)
                self._authority(job)
                self._inputs[job["id"]] = (query, participant_ids, sender_query)
                self._wake.set()
                if job["state"] == "ready" and not local_read_only_requested():
                    return copy.deepcopy(job["result"])
                return self._envelope(job)
            for conversation_id in conversation_ids:
                self.service.reader.authorize(conversation_id)
            # Join only active preparation. A fresh first page after ready discovers
            # appends instead of indefinitely reusing a completed recall window.
            for job in self._jobs.values():
                if (
                    job["digest"] == digest
                    and job["binding"] == self.binding
                    and job["policy"] == self.service._policy_revision()
                    and job["state"] in {"preparing", "running"}
                    and time.time() < job["expires_at"]
                ):
                    self._inputs[job["id"]] = (query, participant_ids, sender_query)
                    self._wake.set()
                    return self._envelope(job)
            retained = {
                key: job
                for key, job in self._jobs.items()
                if time.time() < job["expires_at"]
                and job["binding"] == self.binding
                and job["policy"] == self.service._policy_revision()
            }
            if len(retained) >= MAX_JOBS:
                raise SightglassError(
                    ErrorCode.SERVICE_BUSY,
                    retryable=True,
                    details={"reason": "search_preparation_capacity"},
                )
            old = self._jobs
            self._jobs = retained
            self._progress = {
                key: value for key, value in self._progress.items() if key in retained
            }
            self._pending_failures = {
                key: value for key, value in self._pending_failures.items() if key in retained
            }
            job = {
                "id": uuid.uuid4().hex,
                "digest": digest,
                "scope": scope,
                "binding": self.binding,
                "policy": self.service._policy_revision(),
                "state": "preparing",
                "expires_at": time.time() + TTL_SECONDS,
                "attempts": 0,
                "restart_count": 0,
                "checkpoint": {},
            }
            self._inputs = {key: value for key, value in self._inputs.items() if key in retained}
            self._jobs[job["id"]] = job
            self._inputs[job["id"]] = (query, participant_ids, sender_query)
            try:
                self._save()
            except Exception:
                self._jobs = old
                raise
            self._wake.set()
            return self._envelope(job)

    def request_discovery(
        self,
        *,
        kind: str,
        query: str,
        hints: tuple[str, ...],
        domains: tuple[str, ...],
        kinds: tuple[str, ...] = (),
        account_id: str | None,
        conversation_ids: tuple[str, ...],
        participant_ids: tuple[str, ...],
        after_utc: str | None,
        before_utc: str | None,
        limit: int,
        token: str | None,
    ) -> dict[str, Any]:
        """Acquire the bounded source subset one cold link/retrieval query needs.

        Reuses the ordinary preparation worker, sidecar and lifecycle: one job per
        (scope, recall-term) digest, checkpointed conversations, restart recovery,
        foreground source ownership and cancellation. It admits only the matching
        observed messages under an on_demand decision (never whole history, never a
        keep grant, never a ReaderPolicy change). The caller receives preparing /
        ready / partial / failed / expired outcomes. The reader projects only public
        facts from terminal preparation and retains the internal bindings privately.
        """

        scope, digest = _discovery_intent(
            kind=kind,
            query=query,
            hints=hints,
            domains=domains,
            kinds=kinds,
            account_id=account_id,
            conversation_ids=conversation_ids,
            participant_ids=participant_ids,
            after_utc=after_utc,
            before_utc=before_utc,
            limit=limit,
        )
        with self._lock:
            if token:
                payload = self._decode(token)
                if payload.get("job_kind") != "discovery" or payload.get("digest") != digest:
                    raise SightglassError(ErrorCode.CURSOR_INVALID)
                job = self._jobs.get(str(payload.get("job_id")))
                if job is None or time.time() >= job["expires_at"]:
                    return {
                        "schema": DISCOVERY_SCHEMA,
                        "state": "expired",
                        "results_complete": False,
                        "error": SightglassError(ErrorCode.CURSOR_STALE).as_dict(),
                    }
                if job["digest"] != digest or payload.get("expires_at") != job["expires_at"]:
                    raise SightglassError(ErrorCode.CURSOR_INVALID)
                self._authority(job)
                token_run = payload.get("run", 0)
                current_run = job.get("run", 0)
                if not isinstance(token_run, int) or token_run > current_run:
                    raise SightglassError(ErrorCode.CURSOR_INVALID)
                if payload.get("resume") and token_run == current_run:
                    if job["state"] != "partial":
                        raise SightglassError(ErrorCode.CURSOR_INVALID)
                    previous = copy.deepcopy(job)
                    job.update(state="preparing", run=current_run + 1)
                    job.pop("result", None)
                    try:
                        self._save()
                    except Exception:
                        job.clear()
                        job.update(previous)
                        raise
                self._inputs[job["id"]] = (query, participant_ids, None)
                self._discovery_terms[job["id"]] = (query, hints, domains)
                self._wake.set()
                if job["state"] in {"ready", "partial"}:
                    result = copy.deepcopy(job["result"])
                    if job["state"] == "partial":
                        result["continuation_token"] = self._token(job, resume=True)
                    return result
                return self._envelope(job)
            for conversation_id in conversation_ids:
                self.service.reader.authorize(conversation_id)
            # Join only an active preparation with the same scope/terms. A fresh
            # first page after ready deliberately starts a new job so later source
            # appends are discoverable rather than masked by a stale recall window.
            for job in self._jobs.values():
                if (
                    job.get("kind") == "discovery"
                    and job["digest"] == digest
                    and job["binding"] == self.binding
                    and job["policy"] == self.service._policy_revision()
                    and job["state"] in {"preparing", "running"}
                    and time.time() < job["expires_at"]
                ):
                    self._inputs[job["id"]] = (query, participant_ids, None)
                    self._discovery_terms[job["id"]] = (query, hints, domains)
                    self._wake.set()
                    return self._envelope(job)
            retained = {
                key: job
                for key, job in self._jobs.items()
                if time.time() < job["expires_at"]
                and job["binding"] == self.binding
                and job["policy"] == self.service._policy_revision()
            }
            if len(retained) >= MAX_JOBS:
                raise SightglassError(
                    ErrorCode.SERVICE_BUSY,
                    retryable=True,
                    details={"reason": "search_preparation_capacity"},
                )
            old = self._jobs
            self._jobs = retained
            self._progress = {
                key: value for key, value in self._progress.items() if key in retained
            }
            self._pending_failures = {
                key: value for key, value in self._pending_failures.items() if key in retained
            }
            job = {
                "id": uuid.uuid4().hex,
                "kind": "discovery",
                "digest": digest,
                "scope": scope,
                "binding": self.binding,
                "policy": self.service._policy_revision(),
                "state": "preparing",
                "expires_at": time.time() + TTL_SECONDS,
                "attempts": 0,
                "restart_count": 0,
                "checkpoint": {},
            }
            self._inputs = {key: value for key, value in self._inputs.items() if key in retained}
            self._discovery_terms = {
                key: value for key, value in self._discovery_terms.items() if key in retained
            }
            self._jobs[job["id"]] = job
            self._inputs[job["id"]] = (query, participant_ids, None)
            self._discovery_terms[job["id"]] = (query, hints, domains)
            try:
                self._save()
            except Exception:
                self._jobs = old
                raise
            self._wake.set()
            return self._envelope(job)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="sightglass-search-preparation", daemon=True
        )
        self._thread.start()

    def stop(self) -> bool:
        self._stop.set()
        self._wake.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=10.0)
        return thread is None or not thread.is_alive()

    @staticmethod
    def _checkpoint_has_progress(checkpoint: dict[str, Any]) -> bool:
        """Whether a discovery checkpoint records committed forward progress.

        A deadline that fires before any page commits carries no reusable position;
        converting it to ``partial`` would publish a continuation that cannot advance,
        so it must stay fail-closed. Only a checkpoint that already admitted a row or
        saved a resume position counts as progress.
        """

        return bool(
            int(checkpoint.get("done", 0)) > 0
            or int(checkpoint.get("message_count", 0)) > 0
            or int(checkpoint.get("matched_count", 0)) > 0
            or any(position for position in checkpoint.get("positions", {}).values())
        )

    def _publish_result(
        self, job: dict[str, Any], *, state: str, result: dict[str, Any],
        maintenance: bool = False,
    ) -> None:
        """Keep terminal success invisible until its sidecar commit succeeds."""
        with self._lock:
            previous = copy.deepcopy(job)
            job.update(state=state, result=result)
            job.pop("error", None)
            try:
                self._save(maintenance=maintenance)
            except Exception:
                job.clear()
                job.update(previous)
                raise

    def _publish_discovery_partial(
        self, job: dict[str, Any], checkpoint: dict[str, Any]
    ) -> None:
        """Publish a resumable partial discovery result for a bounded budget stop.

        The last checkpoint was committed after its page's admission, so the older
        page is durable while the interrupted page (never checkpointed) is absent.
        The partial is terminal for this attempt; a versioned continuation token
        resumes exactly the saved position without re-admitting committed rows.

        Only committed checkpoint evidence is reported. Per-attempt scan/admit
        counts are unknown once the attempt was cut short, so they are
        ``None`` (an explicit unknown) rather than a fabricated zero. The caller
        must re-verify authority before publishing and fall back to ``_fail`` if the
        partial cannot be made durable; a stale/fenced checkpoint is never
        promoted to a successful partial.
        """

        from sightglass.reader.service import (
            DISCOVERY_CONVERSATION_SCAN_BUDGET,
            SEARCH_PREPARATION_CONVERSATION_BUDGET,
            SEARCH_PREPARATION_MESSAGE_BUDGET,
        )

        chosen = list(checkpoint.get("chosen", []))
        done = int(checkpoint.get("done", 0))
        scope = job.get("scope", {})
        generations = checkpoint.get("generations", {})
        preparation: dict[str, Any] = {
            "performed": True,
            "kind": scope.get("kind"),
            "complete": False,
            "conversation_count": len(chosen),
            "prepared_conversation_count": done,
            "message_count": int(checkpoint.get("message_count", 0)),
            "matched_message_count": int(checkpoint.get("matched_count", 0)),
            "message_budget": SEARCH_PREPARATION_MESSAGE_BUDGET,
            "scanned_row_count": None,
            "admitted_this_attempt": None,
            "scan_budget": DISCOVERY_CONVERSATION_SCAN_BUDGET,
            "conversation_budget": SEARCH_PREPARATION_CONVERSATION_BUDGET,
            "unprepared_conversation_count": None,
            "pending_conversation_count": max(0, len(chosen) - done),
            "remaining_conversation_ids": chosen[done:],
            "unprepared_conversation_ids": None,
            "generations": copy.deepcopy(generations),
            "mode": "asynchronous",
            "prepared_at": utc_now().isoformat(),
        }
        with self._lock:
            self._publish_result(
                job, state="partial", maintenance=True,
                result={
                    "preparation": preparation,
                    "bindings": {},
                    "schema": DISCOVERY_SCHEMA,
                    "results_complete": False,
                    "state": "partial",
                    "continuation_available": True,
                    "instruction": (
                        "Partial source preparation. Use the returned continuation "
                        "token with identical arguments to scan older rows."
                    ),
                },
            )
            self._progress[job["id"]] = {
                "phase": "partial",
                "pending_conversation_count": max(0, len(chosen) - done),
            }

    def _fail(self, job: dict[str, Any], error: dict[str, Any]) -> None:
        """Publish terminal failure only after its atomic state is durable.

        Under a real physical-floor failure even maintenance can be unavailable.
        Such a token stays preparing, with an explicit commit-pending phase; the
        worker retries metadata only. Restart may recover the prior durable job,
        but cannot revive a terminal failure that was already delivered.
        """
        with self._lock:
            job.update(state="failed", error=error)
            job.pop("result", None)
            try:
                self._save(maintenance=True)
            except Exception:
                job.pop("error", None)
                job["state"] = "preparing"
                self._pending_failures[job["id"]] = error
                self._progress[job["id"]] = {
                    "phase": "state_commit_pending",
                    "last_error_code": error["code"],
                }
            else:
                self._pending_failures.pop(job["id"], None)

    def _run(self) -> None:
        while not self._stop.is_set():
            with self._lock:
                job = next(
                    (
                        job
                        for job in self._jobs.values()
                        if job["state"] == "preparing"
                        and job["id"] in self._inputs
                        and job["binding"] == self.binding
                        and job["policy"] == self.service._policy_revision()
                    ),
                    None,
                )
            if job is None or self.service.reader.paused:
                self._wake.wait(timeout=1.0)
                self._wake.clear()
                continue
            if job["id"] in self._pending_failures:
                self._fail(job, self._pending_failures[job["id"]])
                self._stop.wait(1.0)
                continue
            try:
                self._attempt(job)
            except Exception as exc:
                error = (
                    exc.as_dict()
                    if isinstance(exc, SightglassError)
                    else SightglassError(ErrorCode.INTERNAL_ERROR).as_dict()
                )
                self._fail(job, error)
                self._stop.wait(1.0)

    def _attempt(self, job: dict[str, Any]) -> None:
        def authority() -> None:
            self._authority(job)

        def progress(value: dict[str, Any]) -> None:
            authority()
            with self._lock:
                if "checkpoint" in value:
                    job["checkpoint"] = copy.deepcopy(value["checkpoint"])
                    self._save()
                    self._progress[job["id"]] = {"phase": "positions", "scanned_rows": 0}
                else:
                    self._progress[job["id"]] = value

        with self._lock:
            job.update(state="running", attempts=job["attempts"] + 1)
            self._save()
        try:
            with operation_budget(ATTEMPT_SECONDS, cancelled=self._stop):
                authority()
                with self.lanes.held(WorkClass.SOURCE_READ, wait=True):
                    self.source_worker.foreground_enter()
                    try:
                        scope = job["scope"]
                        inputs = self._inputs[job["id"]]
                        if job.get("kind") == "discovery":
                            terms = self._discovery_terms[job["id"]]
                            preparation, bindings = (
                                self.service._prepare_discovery_candidates(
                                    kind=str(scope["kind"]),
                                    query=terms[0],
                                    hints=terms[1],
                                    domains=terms[2],
                                    kinds=tuple(scope.get("kinds", ())),
                                    account_id=scope["account_id"],
                                    conversation_ids=tuple(scope["conversation_ids"]),
                                    participant_ids=inputs[1],
                                    after_utc=scope["after_utc"],
                                    before_utc=scope["before_utc"],
                                    limit=scope["limit"],
                                    checkpoint=copy.deepcopy(job["checkpoint"]),
                                    progress=progress,
                                    check_authority=authority,
                                )
                            )
                        else:
                            roster_binding = (
                                self.service._index_search_roster(
                                    scope["account_id"], tuple(scope["conversation_ids"])
                                )
                                if inputs[2] and not inputs[1]
                                else None
                            )
                            preparation, bindings = self.service._prepare_search_candidates(
                                query=inputs[0],
                                account_id=scope["account_id"],
                                conversation_ids=tuple(scope["conversation_ids"]),
                                participant_ids=inputs[1],
                                sender_query=inputs[2],
                                after_utc=scope["after_utc"],
                                before_utc=scope["before_utc"],
                                limit=scope["limit"],
                                roster_binding=roster_binding,
                                incremental=True,
                                checkpoint=copy.deepcopy(job["checkpoint"]),
                                progress=progress,
                                check_authority=authority,
                            )
                        authority()
                    finally:
                        self.source_worker.foreground_exit()
                with self._lock:
                    preparation.update(mode="asynchronous", prepared_at=utc_now().isoformat())
                    discovery = job.get("kind") == "discovery"
                    discovery_complete = discovery and preparation.get("complete", True)
                    result: dict[str, Any] = {
                        "preparation": preparation,
                        "bindings": bindings,
                    }
                    if discovery:
                        result["schema"] = DISCOVERY_SCHEMA
                        result["results_complete"] = False
                        result["state"] = "ready" if discovery_complete else "partial"
                        result["continuation_available"] = not discovery_complete
                        result["instruction"] = (
                            "Complete source preparation."
                            if discovery_complete
                            else (
                                "Partial source preparation. Use the returned continuation "
                                "token with identical arguments to scan older rows."
                            )
                        )
                    # A bounded run that could not finish publishes a reusable
                    # partial result instead of looping forever. It is terminal for
                    # this attempt; a distinct versioned continuation resumes its
                    # checkpoint, so polls and retries cannot hot-loop or advance twice.
                    self._publish_result(
                        job, state="ready" if (not discovery or discovery_complete) else "partial",
                        result=result,
                    )
                    self._progress[job["id"]] = {
                        "phase": "complete" if (not discovery or discovery_complete) else "partial",
                        "pending_conversation_count": preparation.get(
                            "pending_conversation_count", 0
                        ),
                    }
        except SightglassError as exc:
            # A legitimate bounded-budget stop (worker deadline or a per-page
            # quantum) after a committed checkpoint is not a failure: the durable
            # page stays, and the caller gets an honest resumable partial instead
            # of a terminal error that discards the work. A deadline with no
            # committed progress, cancellation, a generation/policy change and real
            # source errors still fail closed.
            if (
                job.get("kind") == "discovery"
                and exc.code == ErrorCode.SERVICE_TIMEOUT
                and exc.details.get("reason") == "operation_deadline"
                and not self._stop.is_set()
                and not operation_cancelled()
                and self._checkpoint_has_progress(job.get("checkpoint", {}))
            ):
                # Re-verify authority for the *current* policy/binding/expiry before
                # publishing: a checkpoint committed under an older fence must not be
                # promoted. If the partial cannot be made durable, fall back to the
                # ordinary failure path instead of disguising it as a partial.
                try:
                    self._authority(job)
                    self._publish_discovery_partial(
                        job, copy.deepcopy(job.get("checkpoint", {}))
                    )
                except SightglassError as authority_exc:
                    self._fail(job, authority_exc.as_dict())
                except Exception:
                    self._fail(job, SightglassError(ErrorCode.INTERNAL_ERROR).as_dict())
                self._stop.wait(0.5)
                return
            with self._lock:
                if self._stop.is_set() or operation_cancelled():
                    job["state"] = "preparing"
                    job["restart_count"] += 1
                elif (
                    exc.code == ErrorCode.SOURCE_GENERATION_CHANGED
                    and job["attempts"] < MAX_ATTEMPTS
                ):
                    job["state"] = "preparing"
                else:
                    self._fail(job, exc.as_dict())
                    return
                self._save(maintenance=True)
            self._stop.wait(0.5)
