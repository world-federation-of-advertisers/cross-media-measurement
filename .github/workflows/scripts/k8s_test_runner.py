#!/usr/bin/env python3
# Copyright 2026 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run a Kubernetes test and stop it if an in-memory database pod is lost."""

import argparse
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys


POD_READ_FAILURE_LIMIT = 3
LOG_BYTE_LIMIT = 16384


class _Interrupted(Exception):
    def __init__(self, signum):
        self.signum = signum


def _kubectl(namespace, arguments):
    return subprocess.run(
        ["kubectl", "--namespace", namespace, "--request-timeout=5s", *arguments],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=10,
    )


def _stop(process):
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()


def _check_pod_uid(namespace, name, uid):
    result = _kubectl(namespace, ["get", "pods", name, "--ignore-not-found", "-o", "json"])
    if result.returncode:
        raise RuntimeError(result.stderr.strip()[:2000])
    response = json.loads(result.stdout) if result.stdout.strip() else {"kind": "List", "items": []}
    pods = response.get("items", []) if response.get("kind") == "List" else [response]
    current_uid = next(
        (pod["metadata"]["uid"] for pod in pods if pod["metadata"]["name"] == name), None,
    )
    if current_uid != uid:
        raise RuntimeError(f"Pod {name} UID {uid} is unavailable; current UID is {current_uid or 'none'}.")


def _capture(args, pods, observed, message):
    args.output_dir.mkdir(parents=True, exist_ok=True)
    # Pod specs can contain credentials; retain only identity and runtime status.
    snapshots = [
        {"name": pod["metadata"]["name"], "uid": pod["metadata"]["uid"],
         "role": "original" if pod["metadata"]["uid"] == observed[pod["metadata"]["name"]] else "replacement",
         "status": pod.get("status", {})}
        for pod in pods.values()
    ]
    (args.output_dir / "status.json").write_text(
        json.dumps({"failure": message, "pods": snapshots}, indent=2) + "\n"
    )
    errors = []
    for pod in snapshots:
        for container in pod["status"].get("containerStatuses", []):
            arguments = [
                "logs", pod["name"], "--container", container["name"],
                "--tail=80", f"--limit-bytes={LOG_BYTE_LIMIT}",
            ]
            variants = [arguments]
            if container.get("restartCount", 0):
                variants.append(arguments + ["--previous"])
            for command in variants:
                try:
                    # Log requests use names; verify identity on both sides of the read.
                    _check_pod_uid(args.namespace, pod["name"], pod["uid"])
                    result = _kubectl(args.namespace, command)
                    if result.returncode:
                        raise RuntimeError(result.stderr.strip()[:2000])
                    _check_pod_uid(args.namespace, pod["name"], pod["uid"])
                    suffix = "-previous" if "--previous" in command else ""
                    path = args.output_dir / f"{pod['name']}-{pod['uid']}-{container['name']}{suffix}.log"
                    path.write_bytes(result.stdout.encode("utf-8")[-LOG_BYTE_LIMIT:])
                except (OSError, RuntimeError, subprocess.TimeoutExpired, json.JSONDecodeError,
                        KeyError, TypeError, AttributeError) as error:
                    errors.append({"command": command, "uid": pod["uid"], "error": str(error)[:2000]})
    (args.output_dir / "errors.json").write_text(json.dumps(errors, indent=2) + "\n")
    text = f"## Kubernetes test dependency failure\n\n{message}\n\nStatus and bounded logs: `{args.output_dir}`.\n"
    print(text, file=sys.stderr)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(text)


def _lost(pod, previous_uid):
    name = pod["metadata"]["name"]
    if previous_uid is not None and pod["metadata"]["uid"] != previous_uid:
        return f"Pod {name} was replaced ({previous_uid} -> {pod['metadata']['uid']}); its in-memory database cannot be reused."
    for container in pod.get("status", {}).get("containerStatuses", []):
        terminated = container.get("state", {}).get("terminated")
        if terminated is not None:
            return (
                f"Pod {name} container {container['name']} exited: "
                f"reason={terminated.get('reason', 'unknown')}, "
                f"exit={terminated.get('exitCode', 'unknown')}."
            )
        if container.get("restartCount", 0):
            return f"Pod {name} container {container['name']} restarted; its in-memory database was lost."
    phase = pod.get("status", {}).get("phase")
    if phase in ("Failed", "Succeeded"):
        return f"Pod {name} reached terminal phase {phase}."
    return None


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--pod", action="append", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--poll-interval-seconds", type=float, default=5)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if not math.isfinite(args.poll_interval_seconds) or args.poll_interval_seconds <= 0:
        parser.error("--poll-interval-seconds must be positive and finite")
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a test command is required after --")
    requested = set(args.pod)
    observed = {}
    snapshots = {}
    failures = 0

    def interrupt(signum, frame):
        raise _Interrupted(signum)

    previous_handler = signal.signal(signal.SIGTERM, interrupt)
    process = None
    try:
        process = subprocess.Popen(command, start_new_session=True)
        while process.poll() is None:
            message = None
            try:
                result = _kubectl(
                    args.namespace,
                    ["get", "pods", *sorted(requested), "--ignore-not-found", "-o", "json"],
                )
                if result.returncode:
                    raise RuntimeError(result.stderr.strip()[:2000])
                response = json.loads(result.stdout) if result.stdout.strip() else {"kind": "List", "items": []}
                pods = response.get("items", []) if response.get("kind") == "List" else [response]
                current = {pod["metadata"]["name"]: pod for pod in pods}
                for name in sorted(requested):
                    if name not in current:
                        if name in observed:
                            message = f"Pod {name} disappeared; its in-memory database was lost."
                            break
                        continue  # The test creates these pods during startup.
                    pod = current[name]
                    previous_uid = observed.setdefault(name, pod["metadata"]["uid"])
                    snapshots[pod["metadata"]["uid"]] = pod
                    message = _lost(pod, previous_uid)
                    if message:
                        break
                failures = 0
            except (OSError, RuntimeError, subprocess.TimeoutExpired, json.JSONDecodeError,
                    KeyError, TypeError, AttributeError) as error:
                failures += 1
                if failures >= POD_READ_FAILURE_LIMIT:
                    message = f"Cannot monitor database pods after {failures} reads: {str(error)[:2000]}"
            if process.poll() is not None:
                break
            if message:
                _stop(process)
                _capture(args, snapshots, observed, message)
                return 1
            try:
                process.wait(timeout=args.poll_interval_seconds)
            except subprocess.TimeoutExpired:
                pass
        code = process.wait()
        return code if code >= 0 else 128 - code
    except _Interrupted as error:
        return 128 + error.signum
    except KeyboardInterrupt:
        return 130
    except OSError as error:
        print(f"Unable to start test command: {error}", file=sys.stderr)
        return 127
    finally:
        if process is not None:
            _stop(process)
        signal.signal(signal.SIGTERM, previous_handler)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
