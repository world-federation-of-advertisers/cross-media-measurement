#!/usr/bin/env python3
# Copyright 2026 The Cross-Media Measurement Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by law, software is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
"""Read-only, fail-closed verification of Terraform-owned VID worker readiness."""

import argparse
import json
import subprocess
import time

PREFIX = "ghcr.io/world-federation-of-advertisers"
MIGS = {
    "subpool-assigner-mig": ("edp-aggregator/subpool_assigner", {
        "--subscription-id": "subpool-assigner-subscription",
        "--vid-rank-builder-queue": "vid-rank-builder-queue",
        "--vid-labeler-queue": "vid-labeler-queue"}),
    "vid-rank-builder-mig": ("edp-aggregator/vid_rank_builder", {
        "--subscription-id": "vid-rank-builder-subscription",
        "--vid-labeler-queue": "vid-labeler-queue"}),
    "vid-labeler-mig": ("edp-aggregator/vid_labeler", {
        "--subscription-id": "vid-labeler-subscription"}),
}
DEPLOYMENTS = {
    "edp-aggregator-internal-api-server": "edp-aggregator/internal-api",
    "edp-aggregator-system-api-server": "edp-aggregator/system-api",
}

def option(command, flag):
    values = []
    for index, value in enumerate(command):
        if value == flag:
            require(index + 1 < len(command) and not command[index + 1].startswith("--"),
                    f"Missing value for {flag}")
            values.append(command[index + 1])
        elif value.startswith(flag + "="):
            values.append(value.split("=", 1)[1])
    require(len(values) == 1 and bool(values[0]), f"Missing/duplicate value for {flag}")
    return values[0]



def read_json(command):
    result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=30)
    return json.loads(result.stdout)


def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


def check(project, region, image_tag, read=read_json):
    require(bool(project and region and image_tag), "Expected deployment identity is required")
    deployments = read(["kubectl", "get", "deployments", "-l",
                        "app.kubernetes.io/component=edp-aggregator", "-o", "json"])
    found = {item["metadata"]["name"]: item for item in deployments["items"]}
    require(set(DEPLOYMENTS) <= found.keys(), "Metadata deployment missing")
    for name in DEPLOYMENTS:
        item = found[name]
        replicas = item["spec"].get("replicas", 1)
        status = item.get("status", {})
        require(replicas > 0, f"{name}: no metadata replicas")
        require(status.get("observedGeneration", 0) >= item["metadata"]["generation"],
                f"{name}: stale rollout observation")
        require(status.get("updatedReplicas", 0) == replicas and
                status.get("availableReplicas", 0) == replicas,
                f"{name}: rollout incomplete")
        template_spec = item["spec"]["template"]["spec"]
        if name == "edp-aggregator-internal-api-server":
            updater = [c for c in template_spec.get("initContainers", [])
                       if c["name"] == "update-edp-aggregator-schema"]
            require(len(updater) == 1 and updater[0]["image"] == PREFIX + "/edp-aggregator/update-schema:" + image_tag,
                    "Unexpected schema-updater image")
        containers = template_spec["containers"]
        require(containers and all(c["image"] == PREFIX + "/" + DEPLOYMENTS[name] + ":" + image_tag for c in containers),
                f"{name}: unexpected image")
    pods = read(["kubectl", "get", "pods", "-l",
                 "app.kubernetes.io/component=edp-aggregator", "-o", "json"])
    for name in DEPLOYMENTS:
        selected = [p for p in pods["items"] if
                    p["metadata"]["name"].startswith(name + "-")]
        require(selected, f"{name}: no observed pods")
        for pod in selected:
            require(not pod["metadata"].get("deletionTimestamp"), f"{name}: old pod terminating")
            statuses = pod.get("status", {}).get("containerStatuses", [])
            require(statuses and all(c.get("ready") is True for c in statuses),
                    f"{name}: container not ready")
            require(all(c["image"] == PREFIX + "/" + DEPLOYMENTS[name] + ":" + image_tag
                        for c in pod["spec"]["containers"]), f"{name}: old pod image")
            if name == "edp-aggregator-internal-api-server":
                init = {c["name"]: c for c in pod["status"].get("initContainerStatuses", [])}
                completed = init.get("update-edp-aggregator-schema", {}).get("state", {}).get("terminated", {})
                require(completed.get("exitCode") == 0, "Schema updater has not succeeded")

    for mig, (repository, required_options) in MIGS.items():
        manager = read(["gcloud", "compute", "instance-groups", "managed", "describe", mig,
                        "--project", project, "--region", region, "--format=json"])
        state = manager.get("status", {})
        require(state.get("isStable") is True and
                state.get("versionTarget", {}).get("isReached") is True,
                f"{mig}: rollout not stable")
        template = manager.get("instanceTemplate", "")
        require(template, f"{mig}: template missing")
        template_data = read(["gcloud", "compute", "instance-templates", "describe",
                              template.rsplit("/", 1)[-1], "--project", project, "--format=json"])
        metadata = {i["key"]: i.get("value", "") for i in
                    template_data.get("properties", {}).get("metadata", {}).get("items", [])}
        require(metadata.get("tee-image-reference", "") == PREFIX + "/" + repository + ":" + image_tag,
                f"{mig}: wrong image")
        require(metadata.get("tee-env-WORK_ITEM_CONSUMPTION_ENABLED") == "true",
                f"{mig}: consumption disabled")
        command = json.loads(metadata.get("tee-cmd", "null"))
        require(isinstance(command, list) and all(isinstance(v, str) for v in command),
                f"{mig}: worker arguments unavailable")
        for flag, value in required_options.items():
            require(option(command, flag) == value, f"{mig}: incorrect {flag}")
        instances = read(["gcloud", "compute", "instance-groups", "managed", "list-instances",
                          mig, "--project", project, "--region", region, "--format=json"])
        for instance in instances:
            require(instance.get("currentAction") == "NONE" and
                    instance.get("instanceStatus") == "RUNNING", f"{mig}: instance not ready")
            require(instance.get("version", {}).get("instanceTemplate") == template,
                    f"{mig}: old instance template")
        # A scale-to-zero group is ready only after its desired template was verified.
        # The separately run real smoke test proves the worker can consume actual input.


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--image-tag", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    args = parser.parse_args()
    require(args.timeout_seconds > 0, "Positive timeout required")
    end = time.monotonic() + args.timeout_seconds
    while True:
        try:
            check(args.project, args.region, args.image_tag)
            print("PASS: metadata schema/services and desired VID worker templates are ready")
            return
        except (RuntimeError, KeyError, ValueError, subprocess.SubprocessError) as error:
            if time.monotonic() >= end:
                raise RuntimeError(f"VID readiness failed closed: {error}") from error
            time.sleep(min(5, max(0, end - time.monotonic())))


if __name__ == "__main__":
    main()
