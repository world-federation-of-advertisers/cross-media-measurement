#!/usr/bin/env python3
# Copyright 2026 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Collect scoped Kubernetes rollout status and bounded init-container logs."""

import argparse
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selector", required=True)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    errors = []
    status = {"deployments": [], "replica_sets": [], "pods": [], "events": []}
    object_uids = set()

    def kubectl(arguments, log_file):
        command = ["kubectl", "--namespace", args.namespace, "--request-timeout=10s"] + arguments
        try:
            result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=20)
            if result.returncode:
                raise RuntimeError(result.stderr.strip()[:2000])
            if log_file is not None:
                log_file.write_text(result.stdout)
                return None
            return json.loads(result.stdout)
        except (OSError, RuntimeError, subprocess.TimeoutExpired, json.JSONDecodeError) as error:
            errors.append({"command": arguments, "error": str(error)[:2000]})
            return None

    def identity(resource):
        metadata = resource["metadata"]
        object_uids.add(metadata["uid"])
        return {"name": metadata["name"], "uid": metadata["uid"]}

    def selector(spec):
        labels = [f"{key}={value}" for key, value in sorted(spec.get("matchLabels", {}).items())]
        for expression in spec.get("matchExpressions", []):
            key, operator = expression["key"], expression["operator"]
            if operator in ("In", "NotIn"):
                op = "in" if operator == "In" else "notin"
                labels.append(f"{key} {op} ({','.join(expression['values'])})")
            elif operator == "Exists":
                labels.append(key)
            elif operator == "DoesNotExist":
                labels.append("!" + key)
            else:
                raise ValueError(f"Unsupported label selector operator {operator}")
        if not labels:
            raise ValueError("Refusing to collect pods with an empty deployment selector")
        return ",".join(labels)

    def container_state(state):
        return {
            kind: {key: value for key, value in details.items()
                   if key in ("reason", "message", "exitCode", "startedAt", "finishedAt")}
            for kind, details in state.items()
        }

    deployments = kubectl(["get", "deployments", "--selector", args.selector, "-o", "json"], None)
    for deployment in (deployments or {}).get("items", []):
        status["deployments"].append({**identity(deployment), "status": deployment.get("status", {})})
        try:
            labels = selector(deployment["spec"]["selector"])
        except ValueError as error:
            errors.append({"deployment": deployment["metadata"]["name"], "error": str(error)})
            continue
        replica_sets = kubectl(["get", "replicasets", "--selector", labels, "-o", "json"], None)
        for replica_set in (replica_sets or {}).get("items", []):
            status["replica_sets"].append({**identity(replica_set), "status": replica_set.get("status", {})})
        pods = kubectl(["get", "pods", "--selector", labels, "-o", "json"], None)
        for pod in (pods or {}).get("items", []):
            pod_status = pod.get("status", {})
            containers = []
            for key in ("initContainerStatuses", "containerStatuses"):
                for container in pod_status.get(key, []):
                    containers.append({
                        "name": container["name"],
                        "init": key == "initContainerStatuses",
                        "ready": container.get("ready", False),
                        "restartCount": container.get("restartCount", 0),
                        "state": container_state(container.get("state", {})),
                        "lastState": container_state(container.get("lastState", {})),
                    })
            status["pods"].append({
                **identity(pod),
                "phase": pod_status.get("phase"),
                "conditions": pod_status.get("conditions", []),
                "containers": containers,
            })
            for container in containers:
                completed = container["state"].get("terminated", {}).get("exitCode") == 0
                not_started = "waiting" in container["state"] and not container["lastState"] and not container["restartCount"]
                if not container["init"] or not_started or (completed and not container["restartCount"]):
                    continue
                arguments = [
                    "logs", pod["metadata"]["name"], "--container", container["name"],
                    "--tail=80", "--limit-bytes=16384",
                ]
                prefix = pod["metadata"]["name"] + "-" + container["name"]
                kubectl(arguments, args.output_dir / (prefix + ".log"))
                if container["restartCount"]:
                    kubectl(arguments + ["--previous"], args.output_dir / (prefix + "-previous.log"))

    events = kubectl(["get", "events", "-o", "json"], None) if object_uids else None
    matching_events = []
    for event in (events or {}).get("items", []):
        involved_object = event.get("involvedObject") or event.get("regarding") or {}
        if involved_object.get("uid") not in object_uids:
            continue
        series = event.get("series") or {}
        matching_events.append({
            "involvedObject": involved_object,
            "type": event.get("type"),
            "reason": event.get("reason"),
            "message": event.get("message") or event.get("note"),
            "count": series.get("count", event.get("count", event.get("deprecatedCount"))),
            "lastTimestamp": (
                series.get("lastObservedTime") or event.get("lastTimestamp")
                or event.get("deprecatedLastTimestamp") or event.get("eventTime")
                or event.get("metadata", {}).get("creationTimestamp", "")
            ),
        })
    matching_events.sort(key=lambda event: event["lastTimestamp"])
    status["events"] = matching_events[-100:]

    (args.output_dir / "status.json").write_text(json.dumps(status, indent=2) + "\n")
    (args.output_dir / "errors.json").write_text(json.dumps(errors, indent=2) + "\n")
    summary = ["## EDP Aggregator rollout diagnostics", ""]
    for deployment in status["deployments"]:
        reasons = [condition.get("reason", "") for condition in deployment["status"].get("conditions", [])]
        summary.append(f"- Deployment `{deployment['name']}`: {', '.join(reasons)}.")
    for pod in status["pods"]:
        for container in pod["containers"]:
            state = container["state"]
            details = next(iter(state.values()), {})
            reason = details.get("reason", next(iter(state), "unknown"))
            summary.append(
                f"- Pod `{pod['name']}`, {'init ' if container['init'] else ''}container "
                f"`{container['name']}`: {reason}; restarts={container['restartCount']}; "
                f"exit={details.get('exitCode', 'n/a')}; "
                f"previous exit={container['lastState'].get('terminated', {}).get('exitCode', 'n/a')}."
            )
    if not status["deployments"]:
        summary.append("- No deployments were available for the requested selector.")
    if errors:
        summary.append(f"- Collection incomplete: {len(errors)} read(s) failed; see `errors.json`.")
    summary.append("- Bounded init-container logs and scoped status are in the rollout diagnostics artifact.")
    text = "\n".join(summary) + "\n"
    print(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(text)
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
