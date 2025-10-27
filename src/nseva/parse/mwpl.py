"""Combined OI / MWPL parser stubs (Implementation Plan §6.2)."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def mwpl_to_silver(path: Path) -> Any:
    """Convert a combined OI feed into the silver schema.

    Full normalization logic is implemented in FUT-006.
    """
    raise NotImplementedError("MWPL parsing arrives in FUT-006.")


__all__ = ["mwpl_to_silver"]
