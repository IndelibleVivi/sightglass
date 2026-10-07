"""Stage-level coalescing for immutable resource work.

The daemon already joins identical tool calls by full arguments. That is not enough
for resources: ``metadata``, ``preview``, and ``text`` of one cold resource share a
single source acquisition, and identical derivations of the same immutable object must
publish one verified artifact. Coalescing is keyed by stable work identity, never by
tool arguments, so two callers that never looked alike still share the work while a
different resource revision or a different derivation recipe never aliases.
"""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from sightglass.operations import wait_for_event


@dataclass(frozen=True)
class DerivationRequest:
    """One bounded resource read, keyed by the exact immutable work it needs.

    ``resource_revision`` binds the resolved resource row; the remaining fields are the
    validated, already-bounded read arguments. The recipe key is a digest of exactly
    these values, so two views that need the same immutable object and the same
    derivation share a key, while a different revision or recipe does not.
    """

    resource_id: str
    resource_revision: str
    mode: str
    page: int | None
    start_line: int | None
    end_line: int | None
    member: str | None
    sheet: str | None
    cell_range: str | None
    max_bytes: int

    def as_recipe(self) -> dict[str, Any]:
        return {
            "resource_id": self.resource_id,
            "resource_revision": self.resource_revision,
            "mode": self.mode,
            "page": self.page,
            "start_line": self.start_line,
            "end_line": self.end_line,
            "member": self.member,
            "sheet": self.sheet,
            "cell_range": self.cell_range,
            "max_bytes": self.max_bytes,
        }

    @classmethod
    def from_recipe(cls, recipe: dict[str, Any]) -> DerivationRequest:
        return cls(
            resource_id=str(recipe["resource_id"]),
            resource_revision=str(recipe["resource_revision"]),
            mode=str(recipe["mode"]),
            page=_optional_int(recipe.get("page")),
            start_line=_optional_int(recipe.get("start_line")),
            end_line=_optional_int(recipe.get("end_line")),
            member=_optional_str(recipe.get("member")),
            sheet=_optional_str(recipe.get("sheet")),
            cell_range=_optional_str(recipe.get("cell_range")),
            max_bytes=int(recipe["max_bytes"]),
        )

    def recipe_key(self) -> str:
        encoded = json.dumps(
            self.as_recipe(), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


_T = TypeVar("_T")


@dataclass
class _Stage(Generic[_T]):
    done: threading.Event
    result: _T | None = None
    error: BaseException | None = None


class StageCoalescer:
    """Run one producer per stage key; concurrent requesters join that one result.

    The producer always runs on the first (owner) thread, so the owner's operation
    budget and cancellation apply. Joiners receive the owner's value or its terminal
    error; a failed acquisition is never silently re-driven by the same stage key.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stages: dict[str, _Stage[Any]] = {}
        self._joined = 0
        self._completed = 0

    def run(self, key: str, produce: Callable[[], _T]) -> _T:
        with self._lock:
            stage = self._stages.get(key)
            owner = stage is None
            if stage is None:
                stage = _Stage(done=threading.Event())
                self._stages[key] = stage
            else:
                self._joined += 1
        assert stage is not None
        if not owner:
            # A joiner owns its own operation deadline.  Timing out one caller must not
            # cancel the producer or leave the other joiners waiting forever.
            wait_for_event(stage.done)
            if stage.error is not None:
                raise stage.error
            return stage.result  # type: ignore[return-value]
        try:
            stage.result = produce()
        except BaseException as exc:  # noqa: BLE001 - shared with every joiner
            stage.error = exc
            raise
        finally:
            with self._lock:
                self._stages.pop(key, None)
                self._completed += 1
            stage.done.set()
        return stage.result

    def status(self) -> dict[str, Any]:
        with self._lock:
            inflight = len(self._stages)
            joined = self._joined
            completed = self._completed
        return {
            "schema": "sightglass.stage-coalescing.v1",
            "inflight_count": inflight,
            "joined_count": joined,
            "completed_count": completed,
        }

    @staticmethod
    def acquisition_key(resource_id: str, revision: str) -> str:
        return f"acquire:{resource_id}:{revision}"

    @staticmethod
    def derivation_key(object_digest: str, recipe: str) -> str:
        return f"derive:{object_digest}:{recipe}"
