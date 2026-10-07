"""Reader-facing application services."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .service import ReaderService

__all__ = ["ReaderService"]


def __getattr__(name: str):
    # Importing the request-local budget must not load the reader into the stdio
    # bridge. Preserve the package's existing ReaderService export lazily.
    if name == "ReaderService":
        from .service import ReaderService

        return ReaderService
    raise AttributeError(name)
