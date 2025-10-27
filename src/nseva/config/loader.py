"""Config loading entry points for FuData."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml

from .models import FuDataConfig, RuntimeConfig

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - fallback for earlier interpreters
    import tomli as tomllib  # type: ignore[assignment]

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "nseva.default.yaml"
DEFAULT_SYMBOLS_PATH = PROJECT_ROOT / "configs" / "symbols.yaml"


class ConfigError(RuntimeError):
    """Raised when configuration files cannot be loaded or validated."""


def load_config(
    path: Path | None = None,
    *,
    overrides: Mapping[str, Any] | None = None,
    symbols_path: Path | None = None,
) -> FuDataConfig:
    """Load the FuData configuration applying optional overrides."""

    default_data = _expect_mapping(_read_structured_file(DEFAULT_CONFIG_PATH), DEFAULT_CONFIG_PATH)

    if path:
        config_path = path
        config_data = _expect_mapping(_read_structured_file(config_path), config_path)
    else:
        config_path = DEFAULT_CONFIG_PATH
        config_data = {}

    merged: dict[str, Any] = _deep_merge(default_data, config_data)

    if overrides:
        merged = _deep_merge(merged, _expand_override_keys(overrides))

    config = FuDataConfig.model_validate(merged)

    resolved_symbols = _resolve_symbols(
        config.runtime,
        symbols_path=symbols_path,
    )
    if resolved_symbols is not None:
        runtime = config.runtime.model_copy(
            update={
                "symbols": resolved_symbols,
                "symbols_file": symbols_path or config.runtime.symbols_file,
            }
        )
        config = config.model_copy(update={"runtime": runtime})

    return config


def dump_example_config(dest: Path) -> None:
    """Write the consolidated default configuration to ``dest``."""

    dest.parent.mkdir(parents=True, exist_ok=True)

    merged = _read_structured_file(DEFAULT_CONFIG_PATH)
    if dest.suffix.lower() == ".toml":
        raise ConfigError("TOML export is not supported yet; use a YAML destination.")
    if dest.suffix.lower() in {".json"}:
        dest.write_text(json.dumps(merged, indent=2), encoding="utf-8")
        return
    dest.write_text(
        yaml.safe_dump(merged, sort_keys=False),
        encoding="utf-8",
    )


def _expect_mapping(payload: Any, source: Path) -> dict[str, Any]:
    if payload is None:
        return {}
    if not isinstance(payload, Mapping):
        raise ConfigError(f"Expected mapping data in {source}, got {type(payload)!r}.")
    return dict(payload)


def _resolve_symbols(runtime: RuntimeConfig, *, symbols_path: Path | None) -> list[str] | None:
    """Resolve the preferred symbol list considering overrides and environment."""

    env_symbols = os.getenv("NSEVA_SYMBOLS")
    if env_symbols:
        members = [symbol.strip() for symbol in env_symbols.split(",") if symbol.strip()]
    else:
        members = []

    path_candidates: Sequence[Path] = tuple(
        filter(
            None,
            [
                symbols_path,
                runtime.symbols_file,
                DEFAULT_SYMBOLS_PATH if DEFAULT_SYMBOLS_PATH.exists() else None,
            ],
        )
    )

    for candidate in path_candidates:
        symbols = _load_symbols(candidate)
        if symbols:
            members = list(symbols)
            break

    if members:
        return members
    if runtime.symbols:
        return list(runtime.symbols)
    return None


def _load_symbols(path: Path) -> list[str]:
    """Load symbol list from YAML/TOML or newline-separated text."""

    if not path.exists():
        raise ConfigError(f"Symbols file {path} does not exist.")

    if path.suffix.lower() in {".yaml", ".yml", ".toml", ".json"}:
        payload = _read_structured_file(path)
    else:
        payload = path.read_text(encoding="utf-8").splitlines()

    if isinstance(payload, Mapping):
        if "symbols" not in payload:
            raise ConfigError(f"Symbols file {path} is missing a 'symbols' key.")
        sequence = payload["symbols"]
    else:
        sequence = payload

    if isinstance(sequence, str):
        sequence = [sequence]
    if not isinstance(sequence, Sequence):
        raise ConfigError(f"Symbols payload in {path} must be a list of strings.")

    result = [str(item).strip() for item in sequence if str(item).strip()]
    return result


def _read_structured_file(path: Path) -> Any:
    """Return the parsed contents of a YAML/TOML/JSON file."""

    if not path.exists():
        raise ConfigError(f"Config file {path} does not exist.")

    suffix = path.suffix.lower()
    text = path.read_text(encoding="utf-8")

    if suffix in {".yaml", ".yml"}:
        return yaml.safe_load(text) or {}
    if suffix == ".toml":
        return tomllib.loads(text)
    if suffix == ".json":
        return json.loads(text)

    raise ConfigError(f"Unsupported config format for {path}")


def _deep_merge(base: Mapping[str, Any], extra: Mapping[str, Any]) -> dict[str, Any]:
    """Recursively merge two mappings returning a new dictionary."""

    result: dict[str, Any] = {key: value for key, value in base.items()}
    for key, value in extra.items():
        if (
            key in result
            and isinstance(result[key], Mapping)
            and isinstance(value, Mapping)
        ):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _expand_override_keys(overrides: Mapping[str, Any]) -> dict[str, Any]:
    """Support dotted-notation overrides like ``runtime.symbols``."""

    result: dict[str, Any] = {}
    for key, value in overrides.items():
        converted = _expand_single_override(key, value)
        result = _deep_merge(result, converted)
    return result


def _expand_single_override(key: Any, value: Any) -> dict[str, Any]:
    if isinstance(key, str) and "." in key:
        parts = key.split(".")
        cursor: dict[str, Any] = {}
        root = cursor
        for segment in parts[:-1]:
            next_cursor: dict[str, Any] = {}
            cursor[segment] = next_cursor
            cursor = next_cursor
        cursor[parts[-1]] = value
        return root
    return {key: value}


__all__ = [
    "ConfigError",
    "DEFAULT_CONFIG_PATH",
    "load_config",
    "dump_example_config",
]
