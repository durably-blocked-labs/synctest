#!/usr/bin/env python3
"""Build a disposable Go source copy, seal a complete GOROOT, and smoke-test it."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import sys
import time


def save(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


def inventory(root, source=False):
    rows = []
    for directory, names, files in os.walk(root, followlinks=False):
        relative = Path(directory).relative_to(root)
        names[:] = sorted(n for n in names if n != '.git' and
                          not (source and relative == Path('.') and n in ('bin', 'pkg')))
        for name in names + sorted(n for n in files if n != '.git'):
            path = Path(directory) / name
            if path.is_symlink():
                raise ValueError('symlinks are unsupported in the toolchain snapshot: ' + str(path))
            mode = path.stat().st_mode
            if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                raise ValueError('unsupported snapshot file: ' + str(path))
            row = {'path': str(path.relative_to(root)), 'mode': stat.S_IMODE(mode),
                   'kind': 'directory' if path.is_dir() else 'file'}
            if path.is_file():
                row.update(bytes=path.stat().st_size, sha256=digest(path))
            rows.append(row)
    return sorted(rows, key=lambda row: row['path'])


def source_ignore(source):
    def ignored(directory, names):
        return [name for name in names if name == '.git' or
                (Path(directory) == source and name in ('bin', 'pkg'))]
    return ignored


def seal(root):
    # Permission sealing prevents accidental writes; the owner can undo chmod.
    for directory, names, files in os.walk(root, topdown=False):
        for name in files + names:
            path = Path(directory) / name
            path.chmod(stat.S_IMODE(path.stat().st_mode) & ~0o222)
    root.chmod(stat.S_IMODE(root.stat().st_mode) & ~0o222)


def clean_environment(output, jobs):
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('GO') and key not in ('GODEBUG',)}
    env.update(GOTOOLCHAIN='local', GOENV='off', GOWORK='off',
               GOMAXPROCS=str(jobs), GOFLAGS='-p=' + str(jobs),
               GOCACHE=str(output / 'cache'), GOPATH=str(output / 'gopath'),
               GOTMPDIR=str(output / 'tmp'))
    return env


def source_version(source):
    """Mirror cmd/dist findgoversion for a Git-free source distribution.

    VERSION is authoritative when present. Do not trust VERSION.cache: it may
    describe an earlier HEAD and dist bootstrap -a can delete it anyway.
    """
    version_file = source / 'VERSION'
    if version_file.is_file() and version_file.read_text().strip():
        return {'origin': 'VERSION', 'contents': version_file.read_text()}

    def git(*args):
        return subprocess.check_output(['git', '--no-optional-locks', '-C', str(source), *args],
                                       text=True, stderr=subprocess.STDOUT).rstrip()

    if Path(git('rev-parse', '--show-toplevel')).resolve() != source:
        raise ValueError('source without VERSION must be the root of its Go Git repository')
    version_source = (source / 'src/internal/goversion/goversion.go').read_text()
    match = re.search(r'^const Version = (\d+)', version_source, re.MULTILINE)
    if not match:
        raise ValueError('cannot derive Go development version from internal/goversion/goversion.go')
    head = git('rev-parse', 'HEAD')
    # This is the exact git log format used by the pinned cmd/dist.
    description = git('log', '-n', '1', '--format=format:%h %cd', 'HEAD')
    if not description or '\n' in description:
        raise ValueError('invalid Git development-version description')
    return {'origin': 'git', 'head': head, 'git_description': description,
            'dirty_status': git('status', '--porcelain', '--untracked-files=normal'),
            'contents': 'go1.' + match[1] + '-devel_' + description + '\n'}


def worker(args):
    output, source, bootstrap = args.output, args.source, args.bootstrap
    status_path = output / 'status.json'
    status = json.loads(status_path.read_text())

    def phase(name):
        status.update(status='running', phase=name)
        save(status_path, status)

    def command(name, argv, cwd, env):
        record = {'name': name, 'argv': argv, 'cwd': str(cwd), 'status': 'running'}
        status['commands'].append(record)
        save(status_path, status)
        with (output / (name + '.log')).open('x') as log:
            code = subprocess.call(argv, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT)
        record.update(returncode=code, status='completed' if code == 0 else 'failed')
        save(status_path, status)
        if code:
            raise RuntimeError(name + ' command failed: ' + str(code))

    try:
        phase('source_snapshot')
        version = source_version(source)
        original = inventory(source, source=True)
        build = output / 'build-source'
        shutil.copytree(source, build, ignore=source_ignore(source))
        copied = inventory(build)
        if (copied != original or inventory(source, source=True) != original
                or source_version(source) != version):
            raise ValueError('source changed while taking the build snapshot')
        save(output / 'source-inventory.json', copied)
        # Archive-style VERSION belongs only to our disposable build copy. Keep
        # the original input inventory and generated metadata distinct.
        if version['origin'] == 'git':
            (build / 'VERSION').write_text(version['contents'])
        save(output / 'source-version.json', version)
        status['source_version'] = version
        status['source_inventory_sha256'] = digest(output / 'source-inventory.json')
        status['bootstrap_binary_sha256'] = digest(bootstrap / 'bin' / 'go')
        for name in ('cache', 'gopath', 'tmp'):
            (output / name).mkdir()
        env = clean_environment(output, args.jobs)
        env.update(GOROOT_BOOTSTRAP=str(bootstrap), GOROOT=str(build))
        status['environment'] = {key: env[key] for key in
                                 ('GOROOT_BOOTSTRAP', 'GOROOT', 'GOTOOLCHAIN', 'GOENV',
                                  'GOWORK', 'GOMAXPROCS', 'GOFLAGS', 'GOCACHE', 'GOPATH', 'GOTMPDIR')}
        phase('bootstrap')
        bootstrap_env = dict(env, GOROOT=str(bootstrap))
        command('bootstrap-version', [str(bootstrap / 'bin' / 'go'), 'version'], output, bootstrap_env)
        phase('build')
        command('build', ['bash', './make.bash'], build / 'src', env)
        for required in ('bin/go', 'bin/gofmt', 'src/runtime', 'pkg/tool'):
            if not (build / required).exists():
                raise ValueError('build omitted required GOROOT path: ' + required)
        phase('copy_and_seal')
        built = inventory(build)
        sealed = output / 'goroot'
        shutil.copytree(build, sealed)
        if inventory(sealed) != built or inventory(build) != built:
            raise ValueError('built GOROOT changed during complete copy')
        seal(sealed)
        before = inventory(sealed)
        save(output / 'goroot-inventory.json', before)
        status['goroot_inventory_sha256'] = digest(output / 'goroot-inventory.json')
        status['go_binary_sha256'] = digest(sealed / 'bin' / 'go')
        status['goroot'] = str(sealed)
        phase('sealed_smoke')
        smoke = output / 'smoke'
        smoke.mkdir()
        (smoke / 'go.mod').write_text('module toolchainsmoke\n\ngo 1.25\n')
        (smoke / 'smoke_test.go').write_text('''package toolchainsmoke
import ("testing"; "testing/synctest")
func TestSealedToolchain(t *testing.T) {
    synctest.Test(t, func(t *testing.T) {
        done := make(chan struct{})
        go func() { close(done) }()
        <-done
    })
}
''')
        smoke_env = dict(env, GOROOT=str(sealed), GODEBUG='asyncpreemptoff=1')
        command('sealed-version', [str(sealed / 'bin' / 'go'), 'version'], smoke, smoke_env)
        command('smoke', [str(sealed / 'bin' / 'go'), 'test', '-json', '-count=1',
                          '-run=^TestSealedToolchain$', '.'], smoke, smoke_env)
        events = []
        for line in (output / 'smoke.log').read_text().splitlines():
            try:
                events.append(json.loads(line))
            except ValueError:
                pass
        if not any(isinstance(e, dict) and e.get('Action') == 'pass' and
                   e.get('Test') == 'TestSealedToolchain' for e in events):
            raise ValueError('smoke command executed no passing smoke test')
        if inventory(sealed) != before:
            raise ValueError('sealed GOROOT changed during smoke')
        status.update(status='qualified', phase='complete',
                      qualification='build, complete read-only GOROOT copy, basic synctest smoke only')
        save(status_path, status)
        return 0
    except Exception as error:
        status.update(status='failed', error=str(error))
        save(status_path, status)
        return 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True, help='Go source root; never built in place')
    parser.add_argument('--bootstrap', type=Path, required=True, help='explicit bootstrap GOROOT containing bin/go')
    parser.add_argument('--output', type=Path, required=True, help='new exclusive directory outside both inputs')
    parser.add_argument('--jobs', type=int, choices=(1, 4), default=4,
                        help='Go dist supports serial or four-job bootstrap; default 4')
    parser.add_argument('--timeout', type=float, default=600, help='whole workflow host watchdog, at most 600 seconds')
    parser.add_argument('--_worker', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    args.source, args.bootstrap, args.output = (p.resolve() for p in (args.source, args.bootstrap, args.output))
    if args._worker:
        return worker(args)
    if not 0 < args.timeout <= 600:
        parser.error('timeout must be positive and at most 600 seconds')
    if not (args.source / 'src' / 'make.bash').is_file():
        parser.error('source must contain src/make.bash')
    if not os.access(args.bootstrap / 'bin' / 'go', os.X_OK):
        parser.error('bootstrap must contain executable bin/go')
    if any(args.output == p or p in args.output.parents for p in (args.source, args.bootstrap)):
        parser.error('output must be outside the source and bootstrap trees')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        args.output.mkdir()
    except FileExistsError:
        parser.error('output exists; choose a new directory')
    status_path = args.output / 'status.json'
    save(status_path, {'schema': 1, 'status': 'running', 'phase': 'supervisor',
                       'source': str(args.source), 'bootstrap': str(args.bootstrap),
                       'jobs': args.jobs, 'watchdog_seconds': args.timeout, 'commands': []})
    process, interrupted, timed_out = None, False, False
    started = time.monotonic()
    old_handlers = {}

    def cancelled(*_):
        raise InterruptedError('build interrupted')

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            old_handlers[sig] = signal.signal(sig, cancelled)
        with (args.output / 'worker.log').open('x') as log:
            # Do not leave an unregistered child if cancellation arrives at spawn.
            blocked = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM})
            try:
                process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()),
                    '--_worker', '--source', str(args.source), '--bootstrap', str(args.bootstrap),
                    '--output', str(args.output), '--jobs', str(args.jobs)],
                    stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                    preexec_fn=lambda: signal.pthread_sigmask(signal.SIG_SETMASK, blocked))
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, blocked)
            try:
                process.wait(timeout=args.timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
    except (InterruptedError, KeyboardInterrupt):
        interrupted = True
    finally:
        # Block repeated cancellation until the entire owned process group is killed.
        blocked = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM})
        try:
            if process is not None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
            status = json.loads(status_path.read_text())
            if timed_out or interrupted:
                status['status'] = 'infrastructure-timeout' if timed_out else 'interrupted'
                for record in status['commands']:
                    if record['status'] == 'running':
                        record['status'] = status['status']
            elif process is None or process.returncode or status['status'] != 'qualified':
                status['status'] = 'failed'
            status.update(supervisor_returncode=process.returncode if process else None,
                          elapsed_seconds=time.monotonic() - started)
            save(status_path, status)
        finally:
            for sig, handler in old_handlers.items():
                signal.signal(sig, handler)
            signal.pthread_sigmask(signal.SIG_SETMASK, blocked)
    print(status['status'] + ': ' + str(status_path))
    return 130 if interrupted else int(status['status'] != 'qualified')


if __name__ == '__main__':
    sys.exit(main())
