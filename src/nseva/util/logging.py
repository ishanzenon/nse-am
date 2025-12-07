"""Logging setup utilities (Implementation Plan §8 & §11)."""

from __future__ import annotations

import logging
import sys
from pathlib import Path


def configure_logging(*, log_path: Path | None = None) -> logging.Logger:
    """Configure project-wide logging handlers."""

    logger = logging.getLogger("nseva")
    logger.setLevel(logging.INFO)

    has_stream = any(isinstance(h, logging.StreamHandler) for h in logger.handlers)
    existing_files = {getattr(h, "baseFilename", None) for h in logger.handlers}

    formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s %(name)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    if not has_stream:
        stream_handler = logging.StreamHandler(stream=sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    if log_path and str(log_path) not in existing_files:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


__all__ = ["configure_logging"]
