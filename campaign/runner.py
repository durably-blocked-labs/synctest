#!/usr/bin/env python3
"""Bounded independent-task process runner. No in-trial parallelism or DFS resume.

Each child must create task-dir/artifact using the versioned campaign store and
emit a validated terminal record. Exit zero alone is never success. Resume keeps
all old attempts; only explicitly independent pending tasks are restarted.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import tempfile
from contextlib import contextmanager


@contextmanager
def cancellation_masked():
    # A pending cancellation is delivered only after ownership is registered.
    old = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM, signal.SIGINT})
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, old)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def checkpoint(path, value):
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    temp = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(canonical(value) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        sync_dir(path.parent)
    finally:
        temp.unlink(missing_ok=True)


def validate_terminal(directory, expected, binding, accepted_digest=None):
    raw = (directory / "manifest.json").read_bytes()
    if len(raw) > 1024 * 1024:
        raise ValueError("manifest too large")
    manifest = json.loads(raw)
    required = ("workload", "input_digest", "executable_digest", "runtime_identity", "source_identity", "reset_policy", "policy", "environment", "max_decisions")
    if manifest.get("version") != 1 or any(not manifest.get(key) for key in required):
        raise ValueError("unsupported artifact manifest")
    if manifest.get("task") != binding or manifest.get("input_digest") != binding["input_digest"] or manifest.get("seed") != binding["seed"]:
        raise ValueError("artifact task binding mismatch")
    artifact_hash = hashlib.sha256(raw + b"\x00")
    identity = hashlib.sha256(raw).hexdigest()
    cap = manifest["max_bytes"]
    linecap = manifest["max_record_bytes"]
    if not 0 < cap <= 64 * 1024 * 1024 or not 0 < linecap <= 4 * 1024 * 1024:
        raise ValueError("artifact budget outside runner limits")
    sequence = 0
    active = None
    last_trial = 0
    terminal = None
    total = 0
    with (directory / "events.jsonl").open("rb") as stream:
        while True:
            line = stream.readline(linecap + 1)
            if not line:
                break
            total += len(line)
            artifact_hash.update(line)
            if len(line) > linecap or total > cap or not line.endswith(b"\n"):
                raise ValueError("oversize/truncated artifact")
            record = json.loads(line)
            sequence += 1
            if record["version"] != 1 or record["sequence"] != sequence or record["manifest"] != identity:
                raise ValueError("artifact identity/sequence mismatch")
            # Go marshals payload in compact form in-place. Extract the original
            # bytes, rather than re-encoding floats/HTML with Python semantics.
            marker = b'"payload":'
            start = line.index(marker) + len(marker)
            text = line[start:].decode("utf-8")
            _, end = json.JSONDecoder().raw_decode(text)
            payload = text[:end].encode("utf-8")
            if hashlib.sha256(payload).hexdigest() != record["checksum"]:
                raise ValueError("payload checksum mismatch")
            kind, trial = record["kind"], record["trial"]
            if type(trial) is not int or trial <= 0:
                raise ValueError("invalid artifact trial ID")
            if kind == "start" and active is None and trial == last_trial + 1:
                active = trial
                last_trial = trial
                terminal = None
            elif kind == "checkpoint" and active is not None and trial == active:
                pass
            elif kind == "terminal" and active is not None and trial == active:
                terminal = record["payload"]
                if not terminal.get("complete") or terminal.get("worker_unsafe"):
                    raise ValueError("incomplete/unsafe worker")
                if terminal["replay"]["terminal"] not in {"passed", "assertion-failed", "diverged"}:
                    raise ValueError("unexpected terminal outcome")
                active = None
            else:
                raise ValueError("invalid artifact trial order")
    if active is not None or terminal is None:
        raise ValueError("missing terminal artifact")
    if not terminal.get("complete") or terminal.get("worker_unsafe"):
        raise ValueError("incomplete/unsafe worker")
    if terminal["replay"]["terminal"] not in {"passed", "assertion-failed", "diverged"} or terminal["replay"]["terminal"] not in expected:
        raise ValueError("unexpected terminal outcome")
    accepted = artifact_hash.hexdigest()
    if accepted_digest is not None and accepted != accepted_digest:
        raise ValueError("completed artifact replaced")
    return {"outcome": terminal["replay"]["terminal"], "artifact_digest": accepted}


def terminate_all(processes):
    for process in processes:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + 1
    for process in processes:
        try:
            process.wait(timeout=max(0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            pass
    for process in processes:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    for process in processes:
        process.wait()


def terminate(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def run(manifest_path, directory, resume=False, workers=1, only_task=None):
    if not 1 <= workers <= 16:
        raise ValueError("workers must be in [1,16]")
    raw = manifest_path.read_bytes()
    if len(raw) > 1024 * 1024:
        raise ValueError("task manifest too large")
    spec = json.loads(raw)
    tasks = spec["tasks"]
    if spec.get("version") != 1 or not tasks or len(tasks) > 10000:
        raise ValueError("unsupported/empty/oversize task manifest")
    ids = set()
    for index, task in enumerate(tasks):
        if not task.get("independent") or task["id"] in ids:
            raise ValueError("tasks must be independent with unique IDs")
        ids.add(task["id"])
        if not task.get("argv") or not all(isinstance(x, str) for x in task["argv"]):
            raise ValueError("missing command (no shell expansion)")
        if not 0 < task["seconds"] <= 300 or not 0 < task["max_bytes"] <= 64 * 1024 * 1024:
            raise ValueError("invalid task budgets")
        task["effective_seed"] = task.get("seed", index)
        if not isinstance(task["effective_seed"], int) or not -(1 << 63) <= task["effective_seed"] < (1 << 63):
            raise ValueError("invalid task seed")
    if only_task is not None and only_task not in ids:
        raise ValueError("unknown task ID")
    identity = hashlib.sha256(raw).hexdigest()
    if not resume:
        directory.mkdir(mode=0o700)
    lock = directory / "runner.lock"
    # Hard kill leaves this lease deliberately. Never assume orphaned workers
    # were joined; a supervising process must establish containment first.
    with lock.open("xb") as lease:
        lease.write(str(os.getpid()).encode())
        lease.flush()
        os.fsync(lease.fileno())
    containment = {"safe": True}
    try:
        return execute_tasks(raw, identity, tasks, directory, resume, workers, only_task, containment)
    finally:
        if containment["safe"]:
            with cancellation_masked():
                lock.unlink()
                sync_dir(directory)


def execute_tasks(raw, identity, tasks, directory, resume, workers, only_task, containment):
    immutable = [{"id": task["id"], "seed": task["effective_seed"],
                  "expected": task.get("expected", ["passed"]), "binding": {
                      "manifest_digest": identity, "task_id": task["id"], "seed": task["effective_seed"],
                      "command_digest": hashlib.sha256(canonical({k: task.get(k) for k in ("argv", "cwd", "env")})).hexdigest(),
                      "input_digest": task.get("input_digest", hashlib.sha256(canonical(task.get("input", {}))).hexdigest())
                  }} for task in tasks]
    state_path = directory / "checkpoint.json"
    if resume:
        state = json.loads(state_path.read_bytes())
        if state["manifest"] != identity or state.get("only_task") != only_task:
            raise ValueError("resume manifest changed")
        if state.get("version") != 1 or len(state["tasks"]) != len(immutable):
            raise ValueError("resume task metadata changed")
        for item, expected_item in zip(state["tasks"], immutable):
            if any(item.get(key) != value for key, value in expected_item.items()):
                raise ValueError("resume task metadata changed")
            if item["status"] == "done":
                validate_terminal(directory / item["attempt"] / "artifact", item["expected"], item["binding"], item["artifact_digest"])
    else:
        with (directory / "tasks.json").open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        state = {"version": 1, "manifest": identity, "only_task": only_task,
                 "tasks": [dict(item, status="pending", attempts=0) for item in immutable]}
        checkpoint(state_path, state)
    pending = [i for i, item in enumerate(state["tasks"]) if item["status"] != "done"
               and (only_task is None or item["id"] == only_task)]
    running = {}
    failure = None
    try:
        while pending or running:
            while pending and len(running) < workers and failure is None:
                index = pending.pop(0)
                task, item = tasks[index], state["tasks"][index]
                item["attempts"] += 1
                item["attempt"] = f"task-{index:05d}-attempt-{item['attempts']:05d}"
                attempt = directory / item["attempt"]
                output = None
                try:
                    attempt.mkdir(mode=0o700)
                    item["status"] = "running"
                    checkpoint(state_path, state)
                    env = os.environ.copy()
                    env.update(task.get("env", {}))
                    env["CAMPAIGN_TASK_DIR"] = str(attempt.resolve())
                    env["CAMPAIGN_TASK_ID"] = str(item["id"])
                    env["CAMPAIGN_TASK_SEED"] = str(item["seed"])
                    env["CAMPAIGN_TASK_MANIFEST_DIGEST"] = item["binding"]["manifest_digest"]
                    env["CAMPAIGN_COMMAND_DIGEST"] = item["binding"]["command_digest"]
                    env["CAMPAIGN_INPUT_DIGEST"] = item["binding"]["input_digest"]
                    output = (attempt / "child.log").open("xb")
                    with cancellation_masked():
                        process = subprocess.Popen(task["argv"], cwd=task.get("cwd"), env=env,
                                                   stdout=output, stderr=subprocess.STDOUT,
                                                   start_new_session=True,
                                                   # Runner is single-threaded. Do not inherit its
                                                   # short registration-only cancellation mask.
                                                   preexec_fn=lambda: signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGTERM, signal.SIGINT}))
                        containment["safe"] = False
                        running[index] = (process, output, attempt, time.monotonic() + task["seconds"])
                except BaseException as error:
                    if index not in running and output is not None:
                        output.close()
                    item["status"] = "failed"
                    item["error"] = str(error)
                    raise
            for index in list(running):
                process, output, attempt, deadline = running[index]
                task, item = tasks[index], state["tasks"][index]
                try:
                    size = sum(p.stat().st_size for p in attempt.rglob("*") if p.is_file())
                    if time.monotonic() >= deadline or size > task["max_bytes"]:
                        raise RuntimeError("host watchdog/resource budget exhausted")
                    if process.poll() is None:
                        continue
                    item["returncode"] = process.returncode
                    terminate(process)  # join/kill remaining group members too
                    output.close()
                    del running[index]
                    if process.returncode not in task.get("exit_codes", [0]):
                        raise RuntimeError(f"child exit {process.returncode}")
                    item.update(validate_terminal(attempt / "artifact", item["expected"], item["binding"]))
                    item["status"] = "done"
                    checkpoint(state_path, state)
                except BaseException as error:
                    item["status"] = "failed"
                    item["error"] = str(error)
                    failure = error
                    break
            if failure is not None:
                raise failure
            if running:
                time.sleep(0.02)  # host supervision only, never target scheduling
    except BaseException as error:
        with cancellation_masked():
            terminate_all([entry[0] for entry in running.values()])
            for index, (_, output, _, _) in running.items():
                output.close()
                item = state["tasks"][index]
                if item["status"] != "failed":
                    item["status"] = "cancelled"
                    item["error"] = str(error)
            containment["safe"] = True
            checkpoint(state_path, state)
        raise
    containment["safe"] = True
    return state


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--only-task", help="single-task rerun in a fresh output directory (not strict trace replay); use --workers 1")
    def interrupted(signum, frame):
        raise InterruptedError(f"runner signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    arguments = parser.parse_args()
    run(arguments.manifest, arguments.directory, arguments.resume, arguments.workers, arguments.only_task)
