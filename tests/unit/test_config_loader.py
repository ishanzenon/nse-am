from __future__ import annotations

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import yaml

from nseva.config import ConfigError, dump_example_config, load_config


class ConfigLoaderTests(unittest.TestCase):
    def test_load_config_defaults(self) -> None:
        with patch.dict(os.environ, {"NSEVA_SYMBOLS": ""}, clear=False):
            config = load_config()

        self.assertEqual(config.futures.sheet_name, "FuData")
        self.assertTrue(config.display_rounding.enabled)
        self.assertEqual(config.runtime.parallelism, 1)
        self.assertEqual(config.runtime.symbols, ["FEDERALBNK"])

    def test_load_config_applies_overrides(self) -> None:
        overrides = {
            "runtime.parallelism": 4,
            "runtime": {"fail_fast": True},
            "futures": {"sheet_name": "CustomSheet"},
        }

        with patch.dict(os.environ, {"NSEVA_SYMBOLS": ""}, clear=False):
            config = load_config(overrides=overrides)

        self.assertEqual(config.runtime.parallelism, 4)
        self.assertTrue(config.runtime.fail_fast)
        self.assertEqual(config.futures.sheet_name, "CustomSheet")

    def test_symbols_from_path_override(self) -> None:
        with TemporaryDirectory() as tmpdir:
            symbols_file = Path(tmpdir) / "symbols.yaml"
            symbols_file.write_text(
                "symbols:\n  - ABC\n  - XYZ\n",
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"NSEVA_SYMBOLS": ""}, clear=False):
                config = load_config(symbols_path=symbols_file)

        self.assertEqual(config.runtime.symbols, ["ABC", "XYZ"])
        self.assertEqual(Path(config.runtime.symbols_file), symbols_file)

    def test_env_symbols_override(self) -> None:
        with patch.dict(os.environ, {"NSEVA_SYMBOLS": "AAA, BBB"}, clear=False):
            config = load_config()

        self.assertEqual(config.runtime.symbols, ["AAA", "BBB"])

    def test_dump_example_config_yaml(self) -> None:
        with TemporaryDirectory() as tmpdir:
            dest = Path(tmpdir) / "example.yaml"
            dump_example_config(dest)
            data = yaml.safe_load(dest.read_text(encoding="utf-8"))

        self.assertIn("futures", data)
        self.assertIn("runtime", data)

    def test_dump_example_config_rejects_toml(self) -> None:
        with TemporaryDirectory() as tmpdir:
            dest = Path(tmpdir) / "config.toml"
            with self.assertRaises(ConfigError):
                dump_example_config(dest)


if __name__ == "__main__":
    unittest.main()
