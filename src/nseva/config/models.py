"""Pydantic models describing FuData configuration."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class UdiffoSourceConfig(BaseModel):
    """Source definition for the UDiFF FO bhavcopy."""

    model_config = ConfigDict(extra="allow")

    url_pattern: str
    rate_limit_seconds: float = 0.6
    retries: int = Field(default=3, ge=0)
    timeout_seconds: int = Field(default=30, ge=1)
    column_aliases: Dict[str, List[str]] = Field(default_factory=dict)


class MwplSourceConfig(BaseModel):
    """Source definition for the combined MWPL daily file."""

    model_config = ConfigDict(extra="allow")

    discovery: str
    rate_limit_seconds: float = 0.6
    retries: int = Field(default=3, ge=0)
    timeout_seconds: int = Field(default=30, ge=1)


class LotSizeConfig(BaseModel):
    """Lot size sourcing policy configuration."""

    model_config = ConfigDict(extra="allow")

    mode: str = "bhavcopy_daily"
    planned_upgrade: Optional[str] = None
    conflict_policy: str = "permitted_wins"


class FuturesSourcesConfig(BaseModel):
    """All upstream sources relevant to FuData futures processing."""

    model_config = ConfigDict(extra="allow")

    udiff_fo: UdiffoSourceConfig
    mwpl_combined: MwplSourceConfig
    oi: str = "bhavcopy_only"
    lot_size: LotSizeConfig = Field(default_factory=LotSizeConfig)


class WeekdayPolicyConfig(BaseModel):
    """Policy markers describing NSE expiry weekday changes."""

    model_config = ConfigDict(extra="allow")

    legacy_weekday: str = "Thursday"
    revised_weekday: str = "Tuesday"
    effective_eod: date


class ExpiryPolicyConfig(BaseModel):
    """Composite expiry policy hints for deriving windows."""

    model_config = ConfigDict(extra="allow")

    weekday_policy: WeekdayPolicyConfig


class MwplPolicyConfig(BaseModel):
    """MWPL refresh policies controlling summary calculations."""

    model_config = ConfigDict(extra="allow")

    refresh_daily: bool = True
    use_latest: bool = True


class WindowRuleConfig(BaseModel):
    """Definition of a window boundary rule."""

    model_config = ConfigDict(extra="allow")

    start_rule: str
    end_rule: str
    summary_scope: Optional[Literal["W1", "W3"]] = None


class WindowsConfig(BaseModel):
    """Dual-window configuration for monthly blocks."""

    model_config = ConfigDict(extra="allow")

    dual: bool = True
    primary: WindowRuleConfig
    overlap: WindowRuleConfig


class FuturesLayoutConfig(BaseModel):
    """Excel-oriented layout settings for FuData futures output."""

    model_config = ConfigDict(extra="allow")

    sheet_name: str = "FuData"
    layout: Literal["left_to_right"] = "left_to_right"
    table_spacing_rows: int = Field(default=2, ge=0)
    windows: WindowsConfig
    expiry_policy: ExpiryPolicyConfig
    sources: FuturesSourcesConfig
    mwpl_policy: MwplPolicyConfig


class DisplayRoundingDigits(BaseModel):
    """Per-metric rounding precision settings."""

    model_config = ConfigDict(extra="allow")

    prices: int = 2
    quantities: int = 0
    contracts: int = 0


class DisplayRoundingConfig(BaseModel):
    """Display rounding controls (write-time only)."""

    model_config = ConfigDict(extra="allow")

    enabled: bool = True
    digits: DisplayRoundingDigits = Field(default_factory=DisplayRoundingDigits)


class RuntimeConfig(BaseModel):
    """Execution-time configuration such as symbols and workspace roots."""

    model_config = ConfigDict(extra="allow")

    storage_root: Path = Path("./data")
    symbols: List[str] = Field(default_factory=list)
    symbols_file: Optional[Path] = None
    parallelism: int = Field(default=1, ge=1)
    fail_fast: bool = False


class FuDataConfig(BaseModel):
    """Root configuration object for the FuData pipeline."""

    model_config = ConfigDict(extra="allow")

    futures: FuturesLayoutConfig
    display_rounding: DisplayRoundingConfig
    runtime: RuntimeConfig


__all__ = [
    "DisplayRoundingConfig",
    "DisplayRoundingDigits",
    "ExpiryPolicyConfig",
    "FuDataConfig",
    "FuturesLayoutConfig",
    "FuturesSourcesConfig",
    "MwplPolicyConfig",
    "LotSizeConfig",
    "MwplSourceConfig",
    "RuntimeConfig",
    "UdiffoSourceConfig",
    "WeekdayPolicyConfig",
    "WindowRuleConfig",
    "WindowsConfig",
]
