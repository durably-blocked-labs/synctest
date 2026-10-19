#!/usr/bin/env python3
"""Fail closed before experiments if a local dependency is not the declared pin."""
import argparse
import json
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[1]
manifest = json.loads((root / 'experiments/dependencies.json').read_text())
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--only', choices=manifest['local_sources'])
args = parser.parse_args()
for name, dependency in manifest['local_sources'].items():
    if args.only and name != args.only:
        continue
    path = root / dependency['path']
    def git(*args):
        return subprocess.check_output(['git', '-C', str(path), *args], text=True).strip()
    if git('rev-parse', 'HEAD') != dependency['commit']:
        raise SystemExit(name + ': wrong checkout pin')
    if git('status', '--porcelain', '--untracked-files=no'):
        raise SystemExit(name + ': modified tracked source')
    if name == 'etcd' and git('rev-parse', 'v3.6.15^{}') != dependency['commit']:
        raise SystemExit('etcd: tag mismatch')
    print(name + ': ' + dependency['commit'])
