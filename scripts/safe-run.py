#!/usr/bin/env python3
"""Exclusive, non-destructive Go test runs. Nonzero tests are NOT assumed bugs."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys


def invoke(command, cwd, env, timeout, output):
    with output.open('x') as log:
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--go', required=True)
    parser.add_argument('--package', required=True, help='package directory, including nested modules')
    parser.add_argument('--run-dir', required=True, help='new directory; existing paths refused')
    parser.add_argument('--mode', choices=['benchmark', 'test'], default='benchmark')
    parser.add_argument('--test', default='.')
    parser.add_argument('--attempts', type=int, default=1)
    parser.add_argument('--timeout', type=int, default=60, help='host watchdog seconds per subprocess')
    args = parser.parse_args()
    if args.timeout <= 0 or args.attempts <= 0:
        parser.error('positive timeout and attempts required')
    package = Path(args.package).resolve()
    if not package.is_dir() or not list(package.glob('*_test.go')):
        parser.error('missing package tests (no output directory created)')
    destination = Path(args.run_dir).absolute()
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        destination.mkdir()  # Exclusive ownership; never clean/reuse existing evidence.
    except FileExistsError:
        parser.error('run directory exists; choose a new one')
    go = str(Path(args.go).resolve())
    status = {'schema': 1, 'status': 'running', 'package': str(package),
              'go': go, 'mode': args.mode, 'attempts': [],
              'failure_semantics': 'nonzero is unclassified failure, never inferred target bug'}
    status_path = destination / 'status.json'

    def save():
        temp = destination / 'status.tmp'
        temp.write_text(json.dumps(status, indent=2) + '\n')
        temp.replace(status_path)

    save()
    env = dict(os.environ, GODEBUG='asyncpreemptoff=1', GOTOOLCHAIN='local', GOWORK='off')
    # An explicitly supplied immutable Go binary also needs its own GOROOT.
    env['GOROOT'] = str(Path(go).parent.parent)
    try:
        if args.mode == 'benchmark':
            code = invoke([go, 'test', '-p=4', '-list', '^TestBench(_|$)', '.'], package, env,
                          args.timeout, destination / 'discovery.log')
            if code:
                status.update(status='discovery-failed', exit_code=code)
                return 1
            tests = [line for line in (destination / 'discovery.log').read_text().splitlines()
                     if re.fullmatch(r'TestBench(?:_\w+)?', line)]
            if not tests:
                status['status'] = 'no-tests'
                return 1
        else:
            tests = [args.test]
        failed = False
        for attempt in range(1, args.attempts + 1):
            for number, test in enumerate(tests):
                run = destination / f'attempt-{attempt:03d}-test-{number:03d}'
                run.mkdir()
                data = run / 'data'
                data.mkdir()
                item = {'attempt': attempt, 'test': test, 'directory': run.name, 'status': 'running'}
                status['attempts'].append(item)
                save()
                child_env = dict(env, BENCH_DIR=str(data), BENCH_ATTEMPT=str(attempt),
                                 METRICS_FILE=str(data / 'metrics.jsonl'))
                pattern = '^' + re.escape(test) + '$' if args.mode == 'benchmark' else test
                code = invoke([go, 'test', '-p=4', '-json', '-count=1', f'-timeout={args.timeout}s',
                               '-run', pattern, '.'], package, child_env, args.timeout,
                              run / 'test.jsonl')
                events = []
                for line in (run / 'test.jsonl').read_text().splitlines():
                    try:
                        events.append(json.loads(line))
                    except ValueError:
                        pass  # Preserve mixed compiler output; nonzero remains unclassified.
                passed_tests = any(isinstance(event, dict) and event.get('Test') and
                                   event.get('Action') == 'pass' for event in events)
                outcome = ('test-failed-unclassified' if code else
                           'passed' if passed_tests else 'no-tests-executed')
                item.update(exit_code=code, status=outcome)
                failed |= outcome != 'passed'
                save()
                if failed:
                    break  # Stop on failure; partial output remains readable.
            if failed:
                break
        status['status'] = status['attempts'][-1]['status'] if failed else 'completed'
        return int(failed)
    except subprocess.TimeoutExpired:
        status['status'] = 'infrastructure-timeout'
        return 1
    except KeyboardInterrupt:
        status['status'] = 'interrupted'
        return 130
    except Exception as error:
        status.update(status='infrastructure-error', error=str(error))
        return 1
    finally:
        if status['attempts'] and status['attempts'][-1]['status'] == 'running':
            status['attempts'][-1]['status'] = status['status']
        save()
        print(f"{status['status']}: {status_path}")


if __name__ == '__main__':
    sys.exit(main())
