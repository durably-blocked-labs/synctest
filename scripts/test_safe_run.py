"""No real workload/evidence is touched: every fixture lives in TemporaryDirectory."""
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

SCRIPT = Path(__file__).with_name('safe-run.py')


class SafeRunTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.package = self.root / 'workload'
        self.package.mkdir()
        (self.package / 'bench_test.go').write_text('fixture')
        self.sentinel = self.package / 'benchmarking/data/sentinel.json'
        self.sentinel.parent.mkdir(parents=True)
        self.sentinel.write_bytes(b'preserved historical evidence\x00')
        self.go = self.root / 'bin/go'
        self.go.parent.mkdir()
        self.go.write_text('''#!/usr/bin/env python3
import os,sys,time
from pathlib import Path
mode=Path(__file__).parents[1].joinpath('mode').read_text()
if '-list' in sys.argv:
 if mode=='discovery-failed': sys.exit(3)
 if mode!='no-tests': print('TestBench_Fixture')
else:
 Path(os.environ['BENCH_DIR']).joinpath('partial.json').write_text('readable')
 if mode in ['timeout','interrupt']: time.sleep(5)
 if mode!='no-tests-executed': print('{"Action":"pass","Test":"TestBench_Fixture"}')
 if mode=='target-claim': print('BUG FOUND: fixture text is not a classification protocol')
 if mode in ['test-failed-unclassified','target-claim']: sys.exit(2)
''')
        self.go.chmod(0o755)

    def run_fixture(self, mode, name='run'):
        (self.root / 'mode').write_text(mode)
        destination = self.root / name
        result = subprocess.run([sys.executable, str(SCRIPT), '--go', str(self.go),
                                 '--package', str(self.package), '--run-dir', str(destination),
                                 '--timeout', '1'], capture_output=True, text=True)
        self.assertEqual(self.sentinel.read_bytes(), b'preserved historical evidence\x00')
        return result, destination

    def test_exclusive_directory_and_success(self):
        result, dest = self.run_fixture('completed')
        self.assertEqual(result.returncode, 0, result.stderr)
        before = {str(p): p.read_bytes() for p in dest.rglob('*') if p.is_file()}
        result, _ = self.run_fixture('completed')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(before, {str(p): p.read_bytes() for p in dest.rglob('*') if p.is_file()})

    def test_missing_workload_preserves_sentinels(self):
        (self.package / 'bench_test.go').unlink()
        result, dest = self.run_fixture('completed')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(dest.exists())

    def test_explicit_failure_statuses(self):
        for mode in ['discovery-failed', 'no-tests', 'no-tests-executed', 'test-failed-unclassified', 'target-claim', 'timeout']:
            with self.subTest(mode=mode):
                result, dest = self.run_fixture(mode, mode)
                self.assertNotEqual(result.returncode, 0)
                status = json.loads((dest / 'status.json').read_text())
                expected = {'timeout': 'infrastructure-timeout', 'target-claim': 'test-failed-unclassified'}.get(mode, mode)
                self.assertEqual(status['status'], expected)
                if mode in ['test-failed-unclassified', 'timeout']:
                    self.assertTrue(list(dest.rglob('partial.json')))
                    self.assertEqual(status['attempts'][-1]['status'], expected)

    def test_interruption_preserves_partial_attempt(self):
        (self.root / 'mode').write_text('interrupt')
        dest = self.root / 'interrupted'
        child = subprocess.Popen([sys.executable, str(SCRIPT), '--go', str(self.go),
                                  '--package', str(self.package), '--run-dir', str(dest),
                                  '--timeout', '10'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 5
            while not list(dest.rglob('partial.json')) and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(list(dest.rglob('partial.json')))
            child.send_signal(signal.SIGINT)
            child.communicate(timeout=5)
            self.assertEqual(child.returncode, 130)
            status = json.loads((dest / 'status.json').read_text())
            self.assertEqual(status['status'], 'interrupted')
            self.assertEqual(status['attempts'][-1]['status'], 'interrupted')
            self.assertEqual(self.sentinel.read_bytes(), b'preserved historical evidence\x00')
        finally:
            if child.poll() is None:
                child.kill(); child.communicate()


if __name__ == '__main__':
    unittest.main()
