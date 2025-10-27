"""Logging setup utilities (Implementation Plan §8 & §11)."""

from __future__ import annotations

import logging
from pathlib import Path


def configure_logging(*, log_path: Path | None = None) -> logging.Logger:
    """Configure project-wide logging handlers."""
    raise NotImplementedError("Logging configuration arrives in FUT-011.")


__all__ = ["configure_logging"]
