"""Pydantic models describing FuData configuration."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, RootModel, model_validator


class HashingPolicyConfig(BaseModel):
    """Hashing policy applied to downloaded source artifacts."""

    model_config = ConfigDict(extra="allow")

    enabled: bool = True
    algorithm: Literal["sha256"] = "sha256"
    manifest_extension: str = ".sha256"


class SourceRegistryEntry(BaseModel):
    """Single source definition with download and parsing hints."""

    model_config = ConfigDict(extra="allow")

    identifier: str
    strategy: Literal["url_pattern", "discovery"]
    cache_subdir: Path
    url_pattern: Optional[str] = None
    discovery: Optional[str] = None
    rate_limit_seconds: float = 0.6
    retries: int = Field(default=3, ge=0)
    timeout_seconds: int = Field(default=30, ge=1)
    column_aliases: Dict[str, List[str]] = Field(default_factory=dict)
    hashing: HashingPolicyConfig = Field(default_factory=HashingPolicyConfig)
    quarantine_on_unexpected_columns: bool = False

    @model_validator(mode="after")
    def _validate_strategy(self) -> "SourceRegistryEntry":
        """Ensure required fields exist for the chosen strategy."""

        if self.strategy == "url_pattern" and not self.url_pattern:
            raise ValueError("url_pattern is required when strategy='url_pattern'.")
        if self.strategy == "discovery" and not self.discovery:
            raise ValueError("discovery is required when strategy='discovery'.")
        return self


class SourceRegistry(RootModel[Dict[str, SourceRegistryEntry]]):
    """Collection of all known upstream sources."""

    def get(self, key: str) -> SourceRegistryEntry:
        try:
            return self.root[key]
        except KeyError as exc:  # pragma: no cover - simple passthrough
            raise KeyError(f"Source '{key}' not found in registry.") from exc


class LotSizeConfig(BaseModel):
    """Lot size sourcing policy configuration."""

    model_config = ConfigDict(extra="allow")

    mode: str = "bhavcopy_daily"
    planned_upgrade: Optional[str] = None
    conflict_policy: str = "permitted_wins"


class FuturesSourcesConfig(BaseModel):
    """All upstream sources relevant to FuData futures processing."""

    model_config = ConfigDict(extra="allow")

    registry: SourceRegistry = Field(
        default_factory=lambda: SourceRegistry.model_validate({})
    )
    oi: str = "bhavcopy_only"
    lot_size: LotSizeConfig = Field(default_factory=LotSizeConfig)

    def require(self, key: str) -> SourceRegistryEntry:
        """Return a source definition, raising if missing."""

        return self.registry.get(key)

    @property
    def udiff_fo(self) -> SourceRegistryEntry:
        """Convenience accessor for the FUTSTK UDiFF source."""

        return self.require("udiff_fo")

    @property
    def mwpl_combined(self) -> SourceRegistryEntry:
        """Convenience accessor for the MWPL/combined OI source."""

        return self.require("mwpl_combined")


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
    "HashingPolicyConfig",
    "MwplPolicyConfig",
    "LotSizeConfig",
    "SourceRegistry",
    "SourceRegistryEntry",
    "RuntimeConfig",
    "WeekdayPolicyConfig",
    "WindowRuleConfig",
    "WindowsConfig",
]
