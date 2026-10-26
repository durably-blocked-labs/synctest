#!/usr/bin/env python3
"""Bounded, process-isolated schedule search of the preserved etcd #6873 kernel.

The oracle requires both a test timeout and the historical mutex/channel cycle
in the goroutine dump. The worker saves each schedule choice before it runs, so
a timed-out trial can be replayed in a fresh process. A timeout alone is never
counted as a detected bug. These are reduced GoBench-derived kernels, not etcd.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import struct
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "experiments/etcdkernels/etcd6873/etcd6873.go"
SOURCE_MANIFEST = ROOT / "experiments/etcdkernels/sources.json"


def write_new(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")


def update(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command, environment, cwd, log_path, timeout):
    started = time.monotonic()
    interrupted_signal = None

    def interrupt(signum, _frame):
        nonlocal interrupted_signal
        interrupted_signal = signum

    previous_handler = signal.signal(signal.SIGTERM, interrupt)
    try:
        with log_path.open("x") as log:
            process = subprocess.Popen(command, cwd=cwd, env=environment,
                                       stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=True)
            try:
                deadline = started + timeout
                while True:
                    code = process.poll()
                    if code is not None:
                        timed_out = False
                        break
                    if interrupted_signal is not None:
                        raise InterruptedError(f"signal {interrupted_signal} interrupted trial")
                    if time.monotonic() >= deadline:
                        raise subprocess.TimeoutExpired(command, timeout)
                    time.sleep(0.05)
            except subprocess.TimeoutExpired:
                timed_out = True
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                code = process.wait()
            except BaseException:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
                raise
    finally:
        signal.signal(signal.SIGTERM, previous_handler)
    return {"returncode": code, "external_timeout": timed_out,
            "seconds": time.monotonic() - started, "command": command}


def cycle_6873(log):
    if "panic: test timed out" not in log:
        return False
    blocks = log.split("\ngoroutine ")[1:]
    worker = set()
    shutdown = set()
    for block in blocks:
        if "[sync.Mutex.Lock" in block.splitlines()[0]:
            worker.update(re.findall(r"etcd6873\.\(\*watchBroadcasts\)\.coalesce\((0x[0-9a-fA-F]+)", block))
        if "[chan receive" in block.splitlines()[0]:
            shutdown.update(re.findall(r"etcd6873\.\(\*watchBroadcasts\)\.stop\((0x[0-9a-fA-F]+)", block))
    return bool(worker & shutdown)


def choices_from(path):
    if not path.exists():
        return []
    data = path.read_bytes()
    if len(data) != 8 + 1024 * 72 or data[:4] != b"BKC1":
        raise ValueError("invalid decision journal")
    count, = struct.unpack_from("<I", data, 4)
    if count > 1024:
        raise ValueError("decision journal count exceeds capacity")
    choices = []
    for step in range(count):
        offset = 8 + step * 72
        selected, size = struct.unpack_from("<II", data, offset)
        if size > 16:
            raise ValueError("runnable count exceeds journal slot")
        ids = list(struct.unpack_from("<" + "I" * size, data, offset + 8))
        choices.append({"bgids": ids, "chosen": selected})
    for choice in choices:
        ids, selected = choice["bgids"], choice["chosen"]
        if not ids or len(ids) != len(set(ids)) or not 0 <= selected < len(ids):
            raise ValueError("malformed or impossible saved decision")
    return choices


def run_trial(output, worker, environment, number, request):
    trial = output / (f"trial-{number:04d}" if isinstance(number, int) else f"trial-{number}")
    trial.mkdir()
    write_new(trial / "request.json", request)
    run_environment = dict(environment, BUBBLES_ETCD_REQUEST=str(trial / "request.json"),
                           BUBBLES_ETCD_RESULT=str(trial / "result.json"),
                           GOGC="100", GOMAXPROCS="4")
    status = run([str(worker), "-test.run=^TestKernelWorker$", "-test.timeout=2s"],
                 run_environment, ROOT, trial / "worker.log", 4)
    output_data = json.loads((trial / "result.json").read_text()) if (trial / "result.json").exists() else None
    choices = choices_from(trial / "result.json.choices.bin")
    log = (trial / "worker.log").read_text()
    if status["external_timeout"]:
        classification = "incomplete-external-timeout"
    elif output_data and (output_data["kernel"] != request["kernel"] or output_data["mode"] != request["mode"]
                          or output_data["choices"] != choices):
        classification = "incomplete-inconsistent-artifact"
    elif status["returncode"] == 0 and output_data and output_data["ok"] and not output_data["worker_unsafe"]:
        classification = "completed"
    elif status["returncode"] == 2 and cycle_6873(log) and choices:
        classification = "stack-matched-6873-deadlock"
    else:
        classification = "incomplete-other"
    if request["mode"] == "replay" and choices != request["prefix"]:
        classification = "incomplete-replay-mismatch"
    status.update(classification=classification, decisions=len(choices),
                  result_saved=output_data is not None)
    write_new(trial / "status.json", status)
    return status, choices


def branch(choices, prefix_length):
    for position in range(prefix_length, len(choices)):
        for alternative in reversed(range(len(choices[position]["bgids"]))):
            if alternative == choices[position]["chosen"]:
                continue
            new_prefix = [dict(item) for item in choices[:position + 1]]
            new_prefix[-1]["chosen"] = alternative
            yield new_prefix


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", required=True, help="qualified custom Go binary")
    parser.add_argument("--output", required=True, help="new exclusive output directory")
    parser.add_argument("--strategy", choices=("fifo", "pct", "random", "dfs"), default="pct")
    parser.add_argument("--max-runs", type=int, default=20)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--depth", type=int, default=2)
    args = parser.parse_args()
    if not 1 <= args.max_runs <= 1000 or not 1 <= args.depth <= 128:
        parser.error("max-runs must be 1..1000 and depth 1..128")
    manifest = json.loads(SOURCE_MANIFEST.read_text())
    historical = next(entry for entry in manifest["kernels"] if entry["bug"] == "6873")
    if sha256(SOURCE) != historical["sha256"]:
        parser.error("preserved #6873 source differs from its recorded hash")
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    go = Path(args.go).resolve()
    goroot = go.parents[1]
    environment = dict(os.environ, GOROOT=str(goroot), GOTOOLCHAIN="local", GOWORK="off",
                       GOENV="off", GOMAXPROCS="4", GOFLAGS="-p=4",
                       GOCACHE=str(output / "cache"))
    worker = output / "worker.test"
    build = run([str(go), "test", "-p=4", "-c", "-o", str(worker),
                 "./experiments/etcdkernels"], environment, ROOT, output / "build.log", 60)
    write_new(output / "build-status.json", build)
    if build["returncode"] != 0 or build["external_timeout"]:
        print(f"worker build failed: {output / 'build.log'}", file=sys.stderr)
        return 1
    summary = {"schema": 1, "scope": "reduced historical Bubbles kernel, not full etcd",
               "source_commit": historical["source_commit"], "source_sha256": sha256(SOURCE),
               "worker_sha256": sha256(worker), "go": str(go), "strategy": args.strategy,
               "seed": args.seed, "depth": args.depth, "max_runs": args.max_runs,
               "runs": [], "witnesses": [], "incomplete": False, "exhausted": False}
    update(output / "summary.json", summary)
    pending = [[]]
    seen = set()
    for index in range(1, args.max_runs + 1):
        if args.strategy == "dfs":
            if not pending:
                summary["exhausted"] = not summary["incomplete"]
                break
            prefix = pending.pop()
            key = json.dumps(prefix, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            mode = "prefix"
        else:
            prefix = []
            mode = args.strategy
        request = {"kernel": "6873", "mode": mode, "seed": args.seed + index - 1,
                   "depth": args.depth, "prefix": prefix}
        status, choices = run_trial(output, worker, environment, index, request)
        summary["runs"].append({"number": index, "classification": status["classification"],
                                "decisions": len(choices), "prefix_length": len(prefix)})
        if status["classification"] == "stack-matched-6873-deadlock":
            replay_request = dict(request, mode="replay", prefix=choices)
            replay_status, replay_choices = run_trial(output, worker, environment,
                                                      f"{index:04d}-replay", replay_request)
            reproduced = (replay_status["classification"] == status["classification"]
                          and replay_choices == choices)
            summary["witnesses"].append({"trial": index, "replay_confirmed": reproduced,
                                         "choices": choices})
            if not reproduced:
                summary["incomplete"] = True
        elif status["classification"] != "completed":
            summary["incomplete"] = True
        if args.strategy == "dfs" and status["classification"] in (
                "completed", "stack-matched-6873-deadlock"):
            pending.extend(branch(choices, len(prefix)))
        update(output / "summary.json", summary)
        if summary["incomplete"]:
            break
    if args.strategy == "dfs" and not pending and not summary["incomplete"]:
        summary["exhausted"] = True
    update(output / "summary.json", summary)
    print(json.dumps({key: summary[key] for key in ("strategy", "runs", "witnesses", "incomplete", "exhausted")}))
    return 1 if summary["incomplete"] else 0


if __name__ == "__main__":
    sys.exit(main())
