#!/usr/bin/env python3
"""Verify preserved bytes; added module/runner metadata is outside the inventory."""
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
inventory = json.loads((root / 'archive/legacy/inventory.json').read_text())
for item in inventory['files']:
    data = (root / item['path']).read_bytes()
    if len(data) != item['bytes'] or hashlib.sha256(data).hexdigest() != item['sha256']:
        raise SystemExit('archive mismatch: ' + item['path'])
print(f"verified {len(inventory['files'])} preserved files from {inventory['source_commit']}")
