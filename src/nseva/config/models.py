"""Pydantic models describing FuData configuration.

The full model graph is implemented in FUT-002 using specs from the design docs.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PlaceholderConfig:
    """Temporary placeholder to keep the module importable."""

    placeholder: bool = True


__all__ = ["PlaceholderConfig"]
