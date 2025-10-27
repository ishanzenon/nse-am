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

    def test_source_registry_aliases_and_policies(self) -> None:
        with patch.dict(os.environ, {"NSEVA_SYMBOLS": ""}, clear=False):
            config = load_config()

        sources = config.futures.sources
        udiff = sources.udiff_fo
        self.assertEqual(udiff.identifier, "udiff_fo")
        self.assertEqual(udiff.strategy, "url_pattern")
        self.assertEqual(udiff.cache_subdir, Path("raw/fo_udiff"))
        self.assertTrue(udiff.hashing.enabled)
        self.assertEqual(udiff.hashing.algorithm, "sha256")
        self.assertEqual(udiff.hashing.manifest_extension, ".sha256")
        self.assertIn("VAL_INLAKH", udiff.column_aliases["value_lakhs"])
        self.assertIn("VAL_IN_LAKH", udiff.column_aliases["value_lakhs"])
        self.assertIn("OPEN_INT", udiff.column_aliases["open_interest_contracts"])
        self.assertIn("OPENINT", udiff.column_aliases["open_interest_contracts"])
        self.assertIn("CHG_IN_OI", udiff.column_aliases["change_in_oi_contracts"])
        self.assertIn("NO_OF_CONTRACTS", udiff.column_aliases["contracts"])
        self.assertIn("LOT_SIZE", udiff.column_aliases["lot_size_shares"])

        mwpl = sources.mwpl_combined
        self.assertEqual(mwpl.strategy, "discovery")
        self.assertEqual(mwpl.cache_subdir, Path("raw/mwpl"))
        self.assertIn("MWPL_SHARES", mwpl.column_aliases["mwpl_shares"])
        self.assertIn("COMBINED_OI", mwpl.column_aliases["combined_oi_shares"])
        self.assertTrue(mwpl.quarantine_on_unexpected_columns)

        with self.assertRaises(KeyError):
            sources.require("does_not_exist")


if __name__ == "__main__":
    unittest.main()
