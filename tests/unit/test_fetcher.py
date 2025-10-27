from __future__ import annotations

import hashlib
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import requests

from nseva.io.cache import cache_path_for
from nseva.io.fetcher import fetch_file, file_needs_refresh
from nseva.util.hashing import sha256sum
from nseva.util.retry import retry


class DummyResponse:
    def __init__(self, content: bytes, status_code: int = 200) -> None:
        self.content = content
        self.status_code = status_code

    def iter_content(self, chunk_size: int = 8192):
        for idx in range(0, len(self.content), chunk_size):
            yield self.content[idx : idx + chunk_size]

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FetcherTests(unittest.TestCase):
    def test_cache_path_for_layout(self) -> None:
        root = Path("/workspace/data")
        path = cache_path_for("fo_udiff", trade_date="2024-01-05", root=root)
        self.assertEqual(
            path, root / "raw" / "fo_udiff" / "2024" / "01" / "2024-01-05"
        )

    def test_sha256sum(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "file.txt"
            path.write_text("hello world", encoding="utf-8")
            self.assertEqual(
                sha256sum(path),
                hashlib.sha256(b"hello world").hexdigest(),
            )

    def test_retry_eventual_success(self) -> None:
        attempts: dict[str, int] = {"count": 0}

        def op() -> str:
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise ValueError("not yet")
            return "ok"

        with patch("nseva.util.retry.time.sleep") as sleeper:
            result = retry(op, attempts=3, backoff_seconds=0.01)

        self.assertEqual(result, "ok")
        self.assertEqual(attempts["count"], 3)
        self.assertEqual(sleeper.call_count, 2)

    def test_file_needs_refresh_with_hash(self) -> None:
        with TemporaryDirectory() as tmpdir:
            dest = Path(tmpdir) / "file.bin"
            dest.write_bytes(b"abc")
            digest = sha256sum(dest)
            manifest = dest.with_suffix(dest.suffix + ".sha256")
            manifest.write_text(digest, encoding="utf-8")

            self.assertFalse(
                file_needs_refresh(dest, expected_hash=digest, manifest_extension=".sha256")
            )
            self.assertTrue(
                file_needs_refresh(dest, expected_hash="different", manifest_extension=".sha256")
            )

    def test_fetch_file_downloads_and_writes_manifest(self) -> None:
        with TemporaryDirectory() as tmpdir:
            dest = Path(tmpdir) / "file.bin"
            content = b"payload"
            digest = hashlib.sha256(content).hexdigest()
            response = DummyResponse(content)

            with patch("nseva.io.fetcher.requests.get", return_value=response) as mock_get:
                path = fetch_file(
                    "http://example.com/file",
                    dest,
                    expected_hash=digest,
                    retries=1,
                    rate_limit_seconds=None,
                )

            self.assertEqual(path, dest)
            self.assertTrue(dest.exists())
            self.assertEqual(dest.read_bytes(), content)
            self.assertEqual(dest.with_suffix(dest.suffix + ".sha256").read_text().strip(), digest)
            mock_get.assert_called_once()

    def test_fetch_file_skips_when_up_to_date(self) -> None:
        with TemporaryDirectory() as tmpdir:
            dest = Path(tmpdir) / "file.bin"
            dest.write_bytes(b"cached")
            digest = sha256sum(dest)
            dest.with_suffix(dest.suffix + ".sha256").write_text(digest, encoding="utf-8")

            with patch("nseva.io.fetcher.requests.get") as mock_get:
                path = fetch_file(
                    "http://example.com/file",
                    dest,
                    expected_hash=digest,
                    retries=1,
                    rate_limit_seconds=None,
                )

            self.assertEqual(path, dest)
            mock_get.assert_not_called()

    def test_fetch_file_raises_for_missing(self) -> None:
        with TemporaryDirectory() as tmpdir:
            dest = Path(tmpdir) / "file.bin"
            response = DummyResponse(b"", status_code=404)

            with patch("nseva.io.fetcher.requests.get", return_value=response):
                with self.assertRaises(FileNotFoundError):
                    fetch_file("http://example.com/file", dest, retries=1, rate_limit_seconds=None)


if __name__ == "__main__":
    unittest.main()
