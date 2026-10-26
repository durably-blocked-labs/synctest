#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--go", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    if args.timeout <= 0 or args.timeout > 600:
        parser.error("timeout must be between 1 and 600 seconds")

    output = Path(args.output).absolute()
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        output.mkdir()
    except FileExistsError:
        parser.error("output directory exists; choose a new one")

    go = str(Path(args.go).resolve())
    commands = [
        [go, "test", "-p=4", "-c", "-o", str(output / "bubblegun-worker"), "./evaluations/worker"],
        [go, "build", "-p=4", "-o", str(output / "bubblegun"), "./evaluations/cmd/bubblegun"],
    ]
    status = {"schema": 1, "status": "running", "go": go, "commands": []}
    status_path = output / "status.json"
    save(status_path, status)
    env = dict(os.environ, GOROOT=str(Path(go).parent.parent), GOTOOLCHAIN="local", GOWORK="off")

    interrupted_signal = None

    def interrupted(signum, _frame):
        nonlocal interrupted_signal
        interrupted_signal = signum

    previous_term = signal.signal(signal.SIGTERM, interrupted)
    try:
        for number, command in enumerate(commands, 1):
            item = {"command": command, "status": "running"}
            status["commands"].append(item)
            save(status_path, status)
            started = time.monotonic()
            with (output / f"build-{number}.log").open("x") as log:
                process = subprocess.Popen(
                    command,
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                try:
                    deadline = started + args.timeout
                    while True:
                        code = process.poll()
                        if code is not None:
                            break
                        if interrupted_signal is not None:
                            raise InterruptedError(f"signal {interrupted_signal} interrupted build")
                        if time.monotonic() >= deadline:
                            raise subprocess.TimeoutExpired(command, args.timeout)
                        time.sleep(0.05)
                except (subprocess.TimeoutExpired, KeyboardInterrupt, InterruptedError):
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()
                    raise
            item.update(exit_code=code, elapsed_seconds=time.monotonic() - started)
            if code:
                item["status"] = "failed"
                status["status"] = "failed"
                return code
            item["status"] = "completed"
        status["status"] = "completed"
        return 0
    except subprocess.TimeoutExpired:
        status["status"] = "infrastructure-timeout"
        return 1
    except KeyboardInterrupt:
        status["status"] = "interrupted"
        return 130
    except InterruptedError:
        status["status"] = "interrupted"
        return 143
    except Exception as error:
        status.update(status="infrastructure-error", error=str(error))
        return 1
    finally:
        signal.signal(signal.SIGTERM, previous_term)
        save(status_path, status)
        print(f"{status['status']}: {status_path}")


if __name__ == "__main__":
    sys.exit(main())
