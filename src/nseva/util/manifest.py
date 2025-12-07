"""Run manifest helpers (Implementation Plan §11)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping


def write_manifest(payload: Mapping[str, Any], *, root: Path) -> Path:
    """Write a manifest JSON under root/logs/run_manifests with timestamped name."""

    manifests_dir = root / "logs" / "run_manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    dest = manifests_dir / f"run_{timestamp}.json"
    dest.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return dest


__all__ = ["write_manifest"]
