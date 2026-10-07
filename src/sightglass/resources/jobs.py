"""Durable lease/fencing state for long resource derivations.

A resource derivation that cannot run inside the synchronous tool budget becomes a row
in ``window.db``'s ``resource_jobs`` table instead of an anonymous background thread.
The row carries the exact validated read arguments, a bounded attempt count, and a
lease with an advancing fencing token, so a worker that lost its lease (expired lease,
daemon replacement, restart) can never publish a derived binding afterwards. The
cached artifact itself stays content-addressed in the private object store; this table
only decides who may publish it.

Nothing here is exposed through MCP. Status is content-free: counters and codes only.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from sightglass.contracts.common import to_utc_iso, utc_now
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.repositories import WindowRepository
from sightglass.model.worker_schedule import pending_resource_jobs, resource_lease_deadline

from .coalesce import DerivationRequest

RESOURCE_JOB_LEASE_SECONDS = 120
RESOURCE_JOB_TERMINAL_KEEP = 256
# Durable ceiling on how many times one job row may be leased before it is retired.
# ``lease`` increments ``attempt`` every time a worker picks the row up, so a job that
# repeatedly crashes the daemon (an expired/outstanding lease returned to ``pending`` by
# recovery) would otherwise be re-driven forever. The in-process worker applies its own
# retry budget for errors it observes; this ceiling is the last line of defence and is
# enforced inside the lease transaction so no caller can exceed it.
RESOURCE_JOB_MAX_ATTEMPTS = 3
# Content-free terminal class recorded when the durable ceiling is reached.
RESOURCE_ATTEMPTS_EXHAUSTED = "resource_attempts_exhausted"


class ResourceJobService:
    """Lease, fence, and recover durable resource-derivation jobs."""

    def __init__(self, repository: WindowRepository, *, clock=utc_now) -> None:
        self.repository = repository
        self.clock = clock

    def _now(self) -> str:
        return to_utc_iso(self.clock())

    # -- creation ----------------------------------------------------------

    def enqueue(self, request: DerivationRequest) -> dict[str, Any]:
        """Record one durable derivation job and return its row.

        Idempotent per ``(resource, revision, recipe)`` while an equivalent job is
        active, so repeated polls and concurrent views never create duplicate work.
        Callers that already hold an admission transaction keep it atomic with the
        source bindings they persist in the same transaction.
        """

        now = self._now()
        recipe = request.as_recipe()
        digest = request.recipe_key()
        with self.repository.database.transaction():
            row = self.repository.active_resource_job(
                request.resource_id, request.resource_revision, digest
            )
            if row is None:
                self.repository.insert_resource_job(
                    job_id=f"wxresjob_{uuid4().hex}",
                    resource_id=request.resource_id,
                    resource_revision=request.resource_revision,
                    recipe_digest=digest,
                    recipe_json=json.dumps(recipe, sort_keys=True, separators=(",", ":")),
                    created_at=now,
                )
                row = self.repository.active_resource_job(
                    request.resource_id, request.resource_revision, digest
                )
        if row is None:
            raise SightglassError(ErrorCode.INTERNAL_ERROR)
        self.repository.prune_resource_jobs(keep_terminal=RESOURCE_JOB_TERMINAL_KEEP)
        return dict(row)

    def latest(self, request: DerivationRequest) -> dict[str, Any] | None:
        row = self.repository.latest_resource_job(
            request.resource_id, request.resource_revision, request.recipe_key()
        )
        return None if row is None else dict(row)

    def by_id(self, job_id: str) -> dict[str, Any] | None:
        row = self.repository.resource_job(job_id)
        return None if row is None else dict(row)

    def active(self, request: DerivationRequest) -> dict[str, Any] | None:
        row = self.repository.active_resource_job(
            request.resource_id, request.resource_revision, request.recipe_key()
        )
        return None if row is None else dict(row)

    @staticmethod
    def request_for(job: dict[str, Any]) -> DerivationRequest:
        try:
            recipe = json.loads(str(job["recipe_json"]))
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise SightglassError(ErrorCode.INTERNAL_ERROR) from exc
        if not isinstance(recipe, dict):
            raise SightglassError(ErrorCode.INTERNAL_ERROR)
        request = DerivationRequest.from_recipe(recipe)
        if request.recipe_key() != str(job["recipe_digest"]):
            raise SightglassError(ErrorCode.INTERNAL_ERROR)
        if request.resource_id != str(job["resource_id"]):
            raise SightglassError(ErrorCode.INTERNAL_ERROR)
        if request.resource_revision != str(job["resource_revision"]):
            raise SightglassError(ErrorCode.INTERNAL_ERROR)
        return request

    # -- leases ------------------------------------------------------------

    def lease(
        self,
        job_id: str,
        *,
        owner_id: str,
        lease_seconds: int = RESOURCE_JOB_LEASE_SECONDS,
    ) -> int:
        """Take one pending job and return the fencing token for this attempt."""

        if not owner_id or type(lease_seconds) is not int or lease_seconds <= 0:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        with self.repository.database.transaction():
            job = self.repository.resource_job(job_id)
            if job is None or str(job["state"]) != "pending":
                raise SightglassError(ErrorCode.CURSOR_STALE)
            now = self._now()
            exhausted = int(job["attempt"]) >= RESOURCE_JOB_MAX_ATTEMPTS
            if not exhausted:
                fence = int(job["fencing_token"] or 0) + 1
                expires = to_utc_iso(self.clock() + timedelta(seconds=lease_seconds))
                self.repository.update_resource_job(
                    job_id,
                    state="leased",
                    owner_id=owner_id,
                    fencing_token=fence,
                    attempt=int(job["attempt"]) + 1,
                    lease_expires_at=expires,
                    updated_at=now,
                )
        if exhausted:
            # Retire the row in its own committed transaction: the lease transaction
            # above must not roll the retirement back when this method then fails closed.
            self.exhaust_pending(job_id)
            raise SightglassError(
                ErrorCode.CURSOR_STALE,
                details={"reason": RESOURCE_ATTEMPTS_EXHAUSTED},
            )
        return fence

    def exhaust_pending(self, job_id: str) -> bool:
        """Retire a pending job whose durable attempt budget is already spent.

        Idempotent and content-free: returns ``True`` when this call retired the row.
        The worker uses it so an exhausted row is counted as failed deterministically
        even when it was returned to ``pending`` by crash recovery rather than by an
        error the worker observed itself.
        """

        with self.repository.database.transaction():
            job = self.repository.resource_job(job_id)
            if (
                job is None
                or str(job["state"]) != "pending"
                or int(job["attempt"]) < RESOURCE_JOB_MAX_ATTEMPTS
            ):
                return False
            now = self._now()
            self.repository.update_resource_job(
                job_id,
                state="failed",
                error_code=RESOURCE_ATTEMPTS_EXHAUSTED,
                owner_id=None,
                lease_expires_at=None,
                completed_at=now,
                updated_at=now,
            )
            return True

    def mark_running(self, job_id: str, *, owner_id: str, fencing_token: int) -> None:
        with self.repository.database.transaction():
            self.owned(
                job_id, owner_id=owner_id, fencing_token=fencing_token, now=self._now()
            )
            self.repository.update_resource_job(
                job_id,
                state="running",
                updated_at=self._now(),
            )

    def owned(
        self, job_id: str, *, owner_id: str, fencing_token: int, now: str
    ) -> dict[str, Any]:
        """Fail closed unless this owner still holds an unexpired lease."""

        job = self.repository.resource_job(job_id)
        if (
            job is None
            or type(fencing_token) is not int
            or job["owner_id"] != owner_id
            or job["fencing_token"] != fencing_token
            or str(job["state"]) not in {"leased", "running"}
        ):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        expires = job["lease_expires_at"]
        if expires is not None and str(expires) <= now:
            raise SightglassError(ErrorCode.CURSOR_STALE)
        return dict(job)

    def verify(self, job_id: str, *, owner_id: str, fencing_token: int) -> None:
        """Re-check ownership inside a caller's open transaction before publishing."""

        self.owned(job_id, owner_id=owner_id, fencing_token=fencing_token, now=self._now())

    def complete(self, job_id: str, *, owner_id: str, fencing_token: int) -> None:
        with self.repository.database.transaction():
            job = self.owned(
                job_id, owner_id=owner_id, fencing_token=fencing_token, now=self._now()
            )
            if str(job["state"]) == "ready":
                return
            self.repository.update_resource_job(
                job_id,
                state="ready",
                error_code=None,
                owner_id=None,
                lease_expires_at=None,
                completed_at=self._now(),
                updated_at=self._now(),
            )

    def fail(
        self,
        job_id: str,
        *,
        owner_id: str,
        fencing_token: int,
        error_code: str,
        state: str = "failed",
    ) -> None:
        if state not in {"failed", "blocked", "cancelled"} or not error_code:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        with self.repository.database.transaction():
            job = self.owned(
                job_id, owner_id=owner_id, fencing_token=fencing_token, now=self._now()
            )
            if str(job["state"]) == state and job["error_code"] == error_code:
                return
            self.repository.update_resource_job(
                job_id,
                state=state,
                error_code=error_code,
                owner_id=None,
                lease_expires_at=None,
                completed_at=self._now(),
                updated_at=self._now(),
            )

    def requeue(
        self,
        job_id: str,
        *,
        owner_id: str,
        fencing_token: int,
        storage_pressure: bool = False,
    ) -> None:
        """Return a transiently failed attempt to the queue with a new fence."""

        with self.repository.database.transaction(maintenance=storage_pressure):
            job = self.owned(
                job_id, owner_id=owner_id, fencing_token=fencing_token, now=self._now()
            )
            self.repository.update_resource_job(
                job_id,
                state="pending",
                error_code=None,
                owner_id=None,
                lease_expires_at=None,
                fencing_token=int(job["fencing_token"] or 0) + 1,
                completed_at=None,
                updated_at=self._now(),
            )

    # -- recovery ----------------------------------------------------------

    def recover_expired_leases(self) -> int:
        if not self.repository.expired_resource_job_leases(self._now()):
            return 0
        with self.repository.database.transaction(maintenance=True):
            now = self._now()
            return self._recover(self.repository.expired_resource_job_leases(now))

    def recover_outstanding_leases(self) -> int:
        """Return every held lease to the queue when a new daemon context takes over."""

        if not self.repository.outstanding_resource_job_leases():
            return 0
        with self.repository.database.transaction(maintenance=True):
            return self._recover(self.repository.outstanding_resource_job_leases())

    def _recover(self, jobs: list[Any]) -> int:
        now = self._now()
        for job in jobs:
            self.repository.update_resource_job(
                str(job["job_id"]),
                state="pending",
                owner_id=None,
                lease_expires_at=None,
                fencing_token=int(job["fencing_token"] or 0) + 1,
                updated_at=now,
            )
        return len(jobs)

    # -- inspection --------------------------------------------------------

    def pending_jobs(self, limit: int, *, exclude: tuple[str, ...] = ()) -> list[dict[str, Any]]:
        return pending_resource_jobs(self.repository.database, limit, exclude=exclude)

    def next_lease_delay(self) -> float | None:
        deadline = resource_lease_deadline(self.repository.database)
        if deadline is None:
            return None
        return (datetime.fromisoformat(deadline) - self.clock()).total_seconds()

    def status(self) -> dict[str, Any]:
        counts = self.repository.resource_job_counts()
        active = counts["pending"] + counts["leased"] + counts["running"]
        return {
            "schema": "sightglass.resource-jobs-status.v1",
            "state_counts": counts,
            "active_count": active,
        }
