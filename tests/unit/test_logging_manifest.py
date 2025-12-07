from __future__ import annotations

import json
import logging
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from nseva.util.logging import configure_logging
from nseva.util.manifest import write_manifest


class LoggingManifestTests(unittest.TestCase):
    def test_configure_logging_creates_handlers(self) -> None:
        with TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "nseva.log"
            logger = configure_logging(log_path=log_path)
            logger.info("hello")

            self.assertTrue(log_path.exists())
            contents = log_path.read_text(encoding="utf-8")
            self.assertIn("hello", contents)

    def test_write_manifest(self) -> None:
        with TemporaryDirectory() as tmpdir:
            dest = write_manifest({"status": "ok", "count": 1}, root=Path(tmpdir))

            self.assertTrue(dest.exists())
            payload = json.loads(dest.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["count"], 1)
            self.assertIn("run_", dest.name)


if __name__ == "__main__":
    unittest.main()
