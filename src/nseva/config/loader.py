"""Config loading entry points for FuData.

Concrete logic arrives with FUT-002; for now we expose placeholders so imports
remain stable.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .models import PlaceholderConfig


def load_config(path: Path | None = None) -> PlaceholderConfig:
    """Return a temporary config object until the real loader is implemented."""
    return PlaceholderConfig()


def dump_example_config(_: Path) -> None:
    """Placeholder exported config writer."""
    raise NotImplementedError("Config export will be implemented in FUT-002.")


__all__ = ["load_config", "dump_example_config"]
