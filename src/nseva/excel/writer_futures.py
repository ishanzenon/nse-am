"""FuData Excel rendering stubs (Implementation Plan §6.8 & §9)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable


def render_futures_workbook(
    symbol: str,
    impacted_expiries: Iterable[Any],
    *,
    template_path: Path,
    output_path: Path,
    context: Any,
) -> None:
    """Render/update the FuData workbook for `symbol`.

    Template handling and block layout will be implemented in FUT-010.
    """
    raise NotImplementedError("Excel writer arrives in FUT-010.")


__all__ = ["render_futures_workbook"]
