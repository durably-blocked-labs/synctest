"""Builder containment and sealing fixtures; no Go executable is invoked."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest


SCRIPT = Path(__file__).with_name('build-toolchain.py')


class ToolchainBuilderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.addCleanup(self.unlock)
        self.source = self.root / 'source'
        (self.source / 'src' / 'runtime').mkdir(parents=True)
        (self.source / 'src' / 'runtime' / 'source.go').write_text('original source')
        (self.source / 'src' / 'make.bash').write_text('''#!/bin/sh
set -eu
cd ..
if [ "$(cat mode)" = timeout ]; then
  python3 -c 'import sys,time;from pathlib import Path;p=Path(sys.argv[1]);[(p.write_text(str(i)),time.sleep(.02)) for i in range(1500)]' "$GOROOT/../heartbeat" &
  echo "$!" > "$GOROOT/../descendant.pid"
  wait
fi
if [ "$(cat mode)" = build-failure ]; then exit 7; fi
mkdir -p bin pkg/tool/fixture lib/time
cp fake-go bin/go
cp fake-go bin/gofmt
chmod +x bin/go bin/gofmt
echo tool > pkg/tool/fixture/compile
echo generated > src/runtime/generated.go
echo timezone > lib/time/zoneinfo.zip
''')
        fake = '''#!/usr/bin/env python3
import json,os,sys
from pathlib import Path
root=Path(__file__).resolve().parent.parent
if len(sys.argv)>1 and sys.argv[1]=='version':
 print('fixture go version')
else:
 assert root.name=='goroot', 'smoke must use copied GOROOT'
 assert os.environ['GOROOT']==str(root)
 assert os.environ['GOTOOLCHAIN']=='local'
 assert os.environ['GOMAXPROCS'] in ('1','4')
 if (root/'mode').read_text()=='smoke-mutation':
  p=root/'src/runtime/generated.go';p.chmod(0o644);p.write_text('changed')
 if (root/'mode').read_text()!='no-tests':
  print(json.dumps({'Action':'pass','Test':'TestSealedToolchain'}))
'''
        (self.source / 'fake-go').write_text(fake)
        (self.source / 'mode').write_text('success')
        (self.source / 'VERSION').write_text('go1.27.0\n')
        # A source tree may already contain build outputs; they must not be reused.
        (self.source / 'bin').mkdir()
        (self.source / 'bin' / 'stale').write_text('not build input')
        self.bootstrap = self.root / 'bootstrap'
        (self.bootstrap / 'bin').mkdir(parents=True)
        (self.bootstrap / 'bin' / 'go').write_text('#!/bin/sh\necho bootstrap-fixture\n')
        (self.bootstrap / 'bin' / 'go').chmod(0o755)

    def unlock(self):
        # The builder deliberately removes write permission from its output tree.
        for directory, names, files in os.walk(self.root):
            Path(directory).chmod(0o755)
            for name in files:
                (Path(directory) / name).chmod(0o644)

    def command(self, output, timeout=10):
        return [sys.executable, str(SCRIPT), '--source', str(self.source),
                '--bootstrap', str(self.bootstrap), '--output', str(output),
                '--timeout', str(timeout)]

    def run_builder(self, mode='success', timeout=10):
        (self.source / 'mode').write_text(mode)
        output = self.root / 'output'
        result = subprocess.run(self.command(output, timeout), capture_output=True, text=True, timeout=15)
        status = json.loads((output / 'status.json').read_text())
        return result, output, status

    def test_complete_copy_is_sealed_and_smoked_without_touching_source(self):
        result, output, status = self.run_builder()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(status['status'], 'qualified')
        self.assertEqual([c['name'] for c in status['commands']],
                         ['bootstrap-version', 'build', 'sealed-version', 'smoke'])
        for path in ('bin/go', 'bin/gofmt', 'pkg/tool/fixture/compile',
                     'src/runtime/generated.go', 'lib/time/zoneinfo.zip'):
            self.assertTrue((output / 'goroot' / path).is_file(), path)
            self.assertEqual((output / 'goroot' / path).stat().st_mode & 0o222, 0)
        self.assertFalse((output / 'goroot/bin/stale').exists())
        self.assertFalse((self.source / 'src/runtime/generated.go').exists())
        self.assertEqual((self.source / 'src/runtime/source.go').read_text(), 'original source')
        self.assertEqual((self.source / 'bin/stale').read_text(), 'not build input')
        rows = json.loads((output / 'goroot-inventory.json').read_text())
        self.assertIn('src/runtime/generated.go', [r['path'] for r in rows])
        self.assertIn('go_binary_sha256', status)

    def test_existing_output_is_preserved(self):
        result, output, _ = self.run_builder()
        self.assertEqual(result.returncode, 0)
        before = (output / 'status.json').read_bytes()
        retry = subprocess.run(self.command(output), capture_output=True, text=True)
        self.assertNotEqual(retry.returncode, 0)
        self.assertEqual((output / 'status.json').read_bytes(), before)

    def test_git_development_snapshot_gets_archival_version_not_stale_cache(self):
        (self.source / 'VERSION').unlink()
        (self.source / 'VERSION.cache').write_text('stale development version')
        goversion = self.source / 'src/internal/goversion'
        goversion.mkdir(parents=True)
        (goversion / 'goversion.go').write_text('package goversion\nconst Version = 27\n')
        # Fake read-only Git metadata, without creating or changing any repository.
        fake_bin = self.root / 'fake-bin'
        fake_bin.mkdir()
        git = fake_bin / 'git'
        git.write_text('''#!/usr/bin/env python3
import sys
a=sys.argv
if '--show-toplevel' in a: print(a[a.index('-C')+1])
elif 'rev-parse' in a: print('1234567890abcdef1234567890abcdef12345678')
elif 'log' in a: print('1234567 Sun Oct 25 12:00:00 2026 -0700')
elif 'status' in a: print(' M src/runtime/source.go')
else: raise SystemExit(3)
''')
        git.chmod(0o755)
        env = dict(os.environ, PATH=str(fake_bin) + os.pathsep + os.environ['PATH'])
        output = self.root / 'development-output'
        result = subprocess.run(self.command(output), env=env, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        expected = 'go1.27-devel_1234567 Sun Oct 25 12:00:00 2026 -0700\n'
        self.assertEqual((output / 'goroot/VERSION').read_text(), expected)
        self.assertFalse((self.source / 'VERSION').exists())
        self.assertEqual((self.source / 'VERSION.cache').read_text(), 'stale development version')
        version = json.loads((output / 'source-version.json').read_text())
        self.assertEqual(version['origin'], 'git')
        self.assertEqual(version['dirty_status'], ' M src/runtime/source.go')
        self.assertEqual(version['head'], '1234567890abcdef1234567890abcdef12345678')

    def test_failure_and_false_smoke_are_not_qualified(self):
        for mode, phase in [('build-failure', 'build'), ('no-tests', 'sealed_smoke'),
                            ('smoke-mutation', 'sealed_smoke')]:
            with self.subTest(mode=mode):
                (self.source / 'mode').write_text(mode)
                output = self.root / mode
                result = subprocess.run(self.command(output), capture_output=True, text=True, timeout=15)
                self.assertNotEqual(result.returncode, 0)
                status = json.loads((output / 'status.json').read_text())
                self.assertEqual(status['status'], 'failed')
                self.assertEqual(status['phase'], phase)
                self.assertTrue((output / 'build.log').exists())

    def test_watchdog_kills_owned_descendant_and_preserves_evidence(self):
        result, output, status = self.run_builder('timeout', 0.5)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(status['status'], 'infrastructure-timeout')
        self.assertTrue((output / 'descendant.pid').exists())
        heartbeat = (output / 'heartbeat').read_bytes()
        time.sleep(0.2)
        self.assertEqual((output / 'heartbeat').read_bytes(), heartbeat,
                         'owned descendant continued after watchdog returned')
        self.assertTrue((output / 'build.log').exists())

    def test_input_validation_does_not_create_output(self):
        for suffix in (['--jobs', '5'], ['--timeout', '601']):
            output = self.root / ('bad-' + suffix[0][2:])
            result = subprocess.run(self.command(output) + suffix, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(output.exists())
        output = self.source / 'nested-output'
        result = subprocess.run(self.command(output), capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
