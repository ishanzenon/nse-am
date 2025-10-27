"""Shared typing helpers for FuData modules."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class SupportsDict(Protocol):
    """Objects that expose a dict-like representation."""

    def to_dict(self) -> dict[str, object]:
        """Return a dictionary representation of the object."""
        ...


__all__ = ["SupportsDict"]
