"""Exercise receipt completeness, failure retention, and portable archives."""

import argparse
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location(
    "bbr_lab", Path(__file__).with_name("run-bbr-lab.py")
)
LAB = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LAB)


class ReceiptCollectionTest(unittest.TestCase):
    def receipt(self, algorithm, position, success=True):
        return dict(
            schema=1,
            kind="upload",
            revision="fixture",
            network="fixture",
            algorithm=algorithm,
            bytes=1024,
            os="linux",
            arch="aarch64",
            trial=1,
            position=position,
            status="ok" if success else "error",
            error="fixture failure",
            upload_mbps=10,
            p95_smoothed_rtt_ms=40,
            sent_packets=100,
            lost_packets=2,
            congestion_events=1,
        )

    def run_fixture(self, output, receipts, exit_code, expected_gso_enabled=None):
        fixture = output / "fixture.py"
        fixture.write_text(
            f"print({repr(chr(10).join(json.dumps(r) for r in receipts))})\nraise SystemExit({exit_code})\n"
        )
        return LAB.receipt_run(
            argparse.Namespace(runs=1),
            output,
            [sys.executable, str(fixture)],
            expected_gso_enabled=expected_gso_enabled,
        )

    def test_netem_rejects_receipts_without_transport_gso_confirmation(self):
        with tempfile.TemporaryDirectory() as temporary:
            records = [
                self.receipt("cubic", 1),
                self.receipt("bbr1", 2),
                self.receipt("bbr3", 3),
            ]
            result = self.run_fixture(
                Path(temporary), records, 0, expected_gso_enabled=False
            )
            self.assertFalse(result["transport_config_matches"])
            self.assertFalse(result["complete"])

    def test_summary_keeps_transport_gso_settings_separate(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            records = [
                dict(self.receipt("cubic", 1), gso_enabled=enabled)
                for enabled in (True, False)
            ]
            self.run_fixture(output, records, 0)
            groups = [
                json.loads(line)
                for line in (output / "summary.jsonl").read_text().splitlines()
            ]
            self.assertEqual(len(groups), 2)
            self.assertEqual({group["gso_enabled"] for group in groups}, {True, False})

    def test_nonzero_exit_keeps_failed_attempt_and_summary(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            records = [
                self.receipt("cubic", 1),
                self.receipt("bbr1", 2, False),
                self.receipt("bbr3", 3),
            ]
            result = self.run_fixture(output, records, 1)
            self.assertTrue(result["complete"])
            self.assertEqual(result["client_exit_code"], 1)
            self.assertEqual(result["successful_attempts"], 2)
            groups = [
                json.loads(line)
                for line in (output / "summary.jsonl").read_text().splitlines()
            ]
            self.assertEqual(sum(group["failures"] for group in groups), 1)

    def test_missing_attempt_is_incomplete_even_with_zero_exit(self):
        with tempfile.TemporaryDirectory() as temporary:
            result = self.run_fixture(Path(temporary), [self.receipt("cubic", 1)], 0)
            self.assertFalse(result["complete"])
            self.assertEqual(result["expected_attempts"], 3)
            self.assertEqual(result["recorded_attempts"], 1)

    def test_archive_contains_manifest_and_verifiable_checksum(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "result"
            output.mkdir()
            (output / "receipts.jsonl").write_text('{"status":"error"}\n')
            with contextlib.redirect_stdout(io.StringIO()):
                LAB.archive(output, dict(complete=False, error="interrupted"))
            archive = output.with_name("result.tar.gz")
            expected = hashlib.sha256(archive.read_bytes()).hexdigest()
            self.assertTrue(
                output.with_name("result.tar.gz.sha256")
                .read_text()
                .startswith(expected)
            )
            with tarfile.open(archive) as bundle:
                manifest = json.load(bundle.extractfile("result/manifest.json"))
                self.assertFalse(manifest["complete"])
                self.assertIn("result/receipts.jsonl", bundle.getnames())


if __name__ == "__main__":
    unittest.main()
