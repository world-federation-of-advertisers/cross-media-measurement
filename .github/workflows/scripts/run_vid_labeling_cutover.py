#!/usr/bin/env python3
# Copyright 2026 The Cross-Media Measurement Authors
# Licensed under the Apache License, Version 2.0.
"""Checked cutover boundaries. Deployment remains in the normal Terraform workflow."""
import argparse
import hashlib
import json
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from check_vid_labeling_readiness import (
    check, check_functions, check_compatibility, require_manifest, read_json, require, MIGS,
)


def scope_id(manifest):
    require_manifest(manifest)
    providers = manifest.get("providers")
    queues = manifest.get("queues")
    require(isinstance(providers, list) and providers and
            all(isinstance(p, str) and p.startswith("dataProviders/") and p.count("/") == 1 and p.split("/")[1] for p in providers),
            "Complete provider scope is required")
    require(isinstance(queues, list) and queues and all(isinstance(q, str) and q.startswith("queues/") and q.count("/") == 1 and q.split("/")[1]
                                                     for q in queues),
            "Complete queue scope is required")
    require(len(queues) >= 4 and set(manifest.get("migs", [])) == set(MIGS),
            "Phase0/1/2 and availability scope is incomplete")
    require(len(set(providers)) == len(providers) and len(set(queues)) == len(queues),
            "Scope is duplicated")
    return hashlib.sha256(json.dumps(
        {"providers": sorted(providers), "queues": sorted(queues)},
        sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()


def control(url, action, manifest, execute=subprocess.run, opener=urllib.request.urlopen):
    """The publisher-owned control must attest its complete scope; stopping workers is not pause."""
    parsed = urllib.parse.urlparse(url)
    require(parsed.scheme == "https" and parsed.netloc and not parsed.username and not parsed.password,
            "Maintenance requires a publisher-owned HTTPS intake/backfill control")
    identity = execute(["gcloud", "auth", "print-identity-token", "--audiences=" + url],
                       check=True, capture_output=True, text=True, timeout=30).stdout.strip()
    require(bool(identity), "Control identity unavailable")
    payload = json.dumps({"action": action, "scope": {
        "providers": manifest["providers"], "queues": manifest["queues"]},
        "scope_id": scope_id(manifest), "request_id": str(uuid.uuid4())}).encode()
    request = urllib.request.Request(url, data=payload, method="POST",
                                     headers={"Authorization": "Bearer " + identity,
                                              "Content-Type": "application/json"})
    with opener(request, timeout=30) as response:
        value = json.load(response)
    require(value.get("scope_id") == scope_id(manifest), "Control attested another scope")
    require(value.get("paused") is (action == "pause"), "Publisher/backfill control not confirmed")
    return value


def begin(mode, manifest, receipt, pause, preflight, ready=check_compatibility):
    scope = scope_id(manifest)
    if mode == "online":
        require(isinstance(receipt, dict) and receipt.get("scope_id") == scope,
                "No matching successful preceding cutover; use checked maintenance")
        ready(manifest["project"], manifest["region"], manifest, receipt)
    else:
        require(mode == "maintenance", "Unknown cutover mode")
        pause()
        result = preflight()
        require(isinstance(result, dict) and result.get("status") == "PASS" and
                result.get("mode") == "MAINTENANCE" and
                sorted(result.get("providers", [])) == sorted(manifest["providers"]) and
                sorted(result.get("queues", [])) == sorted(manifest["queues"]),
                "Preflight omitted lease checks or deployment scope")
    return {"status": "IN_PROGRESS", "schema_version": 1, "scope_id": scope, "mode": mode,
            "contract_id": manifest["contract_id"], "shared_code_id": manifest.get("shared_code_id"), "project": manifest["project"],
            "region": manifest["region"], "started_at": int(time.time()),
            "request_id": str(uuid.uuid4())}


def complete(manifest, journal, image_tag, readiness, smoke, resume, checkpoint=lambda _: None):
    require(journal.get("status") == "IN_PROGRESS" and journal.get("scope_id") == scope_id(manifest),
            "No matching checked cutover boundary")
    require(all(journal.get(k) == manifest.get(k) for k in ("project", "region", "contract_id")),
            "Deployment/config contract changed during cutover")
    require(journal.get("mode") in ("online", "maintenance"), "Unknown checked cutover mode")
    require(isinstance(journal.get("started_at"), int) and
            0 <= time.time() - journal["started_at"] <= 8 * 3600,
            "Stale or future maintenance boundary")
    require(journal.get("shared_code_id") == manifest.get("shared_code_id"),
            "Shared platform source changed during cutover")
    readiness()
    result = smoke()
    require(isinstance(result, dict) and result.get("status") == "PASS" and
            result.get("competing_writers") == 2 and result.get("sidecar_winners") == 1 and
            result.get("decrypted_records") > 0 and result.get("cleanup_complete") is True,
            "Concurrent-write/sidecar smoke did not prove successful publication")
    verified = dict(manifest, status="VERIFIED", scope_id=scope_id(manifest), image_tag=image_tag,
                    smoke=result, request_id=journal["request_id"])
    checkpoint(verified)
    if journal["mode"] == "maintenance":
        resume()
    return dict(manifest, status="READY", scope_id=scope_id(manifest), image_tag=image_tag,
                smoke=result, completed_at=int(time.time()), request_id=journal["request_id"])


def object_generation(uri, execute=subprocess.run):
    require(uri.startswith("gs://") and "/rollout/" in uri and "#" not in uri,
            "Unsafe receipt namespace")
    result = execute(["gcloud", "storage", "objects", "describe", uri,
                      "--format=value(generation)"], check=True, capture_output=True,
                     text=True, timeout=30)
    value = int(result.stdout.strip())
    require(value > 0, "Receipt generation unavailable")
    return value


def get_object(uri, generation=None, execute=subprocess.run):
    require(uri.startswith("gs://") and "/rollout/" in uri,
            "Receipt must use a dedicated GCS rollout prefix")
    result = execute(["gcloud", "storage", "cat", uri if generation is None else uri + "#" + str(generation)], check=True, capture_output=True,
                     text=True, timeout=30)
    return json.loads(result.stdout)


def put_object(uri, value, expected_generation, execute=subprocess.run):
    require(uri.startswith("gs://") and "/rollout/" in uri, "Unsafe receipt namespace")
    require(isinstance(expected_generation, int) and expected_generation >= 0,
            "A precise preceding receipt generation is required")
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "receipt.json"
        path.write_text(json.dumps(value, sort_keys=True))
        execute(["gcloud", "storage", "cp", str(path), uri,
                 "--if-generation-match=" + str(expected_generation)], check=True, timeout=30)
        return object_generation(uri, execute)



def prepare_manifest(manifest, config_file, execute=subprocess.run):
    require(config_file, "Committed provider config is required")
    result = execute(["bazelisk", "run",
        "//src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:VidLabelingRolloutScope",
        "--", "--vid-labeling-config=" + config_file], check=True, capture_output=True,
        text=True, timeout=300)
    value = dict(manifest, **json.loads(result.stdout.splitlines()[-1]))
    tree = execute(["git", "ls-tree", "-r", "HEAD"], check=True, capture_output=True,
                   text=True, timeout=30).stdout
    selected = [line for line in tree.splitlines() if line.split("\t", 1)[1].startswith((
        "src/main/kotlin/org/wfanet/measurement/securecomputation/",
        "src/main/kotlin/org/wfanet/measurement/kingdom/",
        "src/main/kotlin/org/wfanet/measurement/duchy/",
        "src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/",
        "src/main/kotlin/org/wfanet/measurement/dataprovider/",
        "src/main/proto/wfa/measurement/securecomputation/")) or
        line.split("\t", 1)[1] in ("MODULE.bazel", "MODULE.bazel.lock")]
    value["shared_code_id"] = hashlib.sha256("\n".join(selected).encode()).hexdigest()
    scope_id(value)
    return value


def preflight_argv(manifest, folder, execute=subprocess.run):
    # Existing Terraform-managed EDPA operator identity. Never print or persist key content.
    secrets = {"client.pem": "edpa-tee-app-tls-pem", "client.key": "edpa-tee-app-tls-key",
               "edpa-roots.pem": "edpaggregator-root-ca", "control-roots.pem": "securecomputation-root-ca"}
    for filename, secret in secrets.items():
        with (Path(folder) / filename).open("w") as output:
            execute(["gcloud", "secrets", "versions", "access", "latest", "--secret=" + secret,
                     "--project=" + manifest["project"]], check=True, stdout=output, timeout=30)
        (Path(folder) / filename).chmod(0o600)
    result = ["--mode=MAINTENANCE", "--spanner-project=" + manifest["project"],
              "--spanner-instance=" + manifest["spanner_instance"],
              "--spanner-database=" + manifest["spanner_database"],
              "--edpa-public-api-target=" + __import__("os").environ["METADATA_STORAGE_PUBLIC_API_TARGET"],
              "--control-plane-target=" + __import__("os").environ["SECURE_COMPUTATION_PUBLIC_API_TARGET"]]
    result += ["--data-provider=" + p for p in manifest["providers"]]
    result += ["--queue=" + q for q in manifest["queues"]]
    for flag, filename in {"--tls-cert-file": "client.pem", "--tls-key-file": "client.key",
        "--cert-collection-file": "edpa-roots.pem", "--control-plane-tls-cert-file": "client.pem",
        "--control-plane-tls-key-file": "client.key",
        "--control-plane-cert-collection-file": "control-roots.pem"}.items():
        result += [flag + "=" + str(Path(folder) / filename)]
    return result

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=["begin", "complete"], required=True)
    parser.add_argument("--mode", choices=["online", "maintenance"], required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--receipt-uri", required=True)
    parser.add_argument("--image-tag", required=True)
    parser.add_argument("--intake-control-url", default="")
    parser.add_argument("--preflight-arguments", help="JSON argv for the existing read-only CLI")
    parser.add_argument("--smoke-arguments", help="JSON argv for the final-stack production-sink smoke")
    parser.add_argument("--vid-labeling-config", help="Committed deployment config for begin")
    parser.add_argument("--new-receipt", action="store_true", help="First maintenance cutover only")
    parser.add_argument("--cutover-id", help="Exact checked begin request ID")
    args = parser.parse_args()
    manifest = json.loads(Path(args.manifest).read_text())
    if args.phase == "begin":
        manifest = prepare_manifest(manifest, args.vid_labeling_config)
    scope_id(manifest)

    def command(file, target):
        require(file, "Required checked CLI arguments are missing")
        argv = json.loads(Path(file).read_text())
        require(isinstance(argv, list) and all(isinstance(a, str) for a in argv), "Invalid CLI argv")
        result = subprocess.run(["bazelisk", "run", target, "--", *argv], check=True,
                                capture_output=True, text=True, timeout=900)
        # Tools may emit normal logging before their final machine-readable result.
        return json.loads(result.stdout.splitlines()[-1])

    if args.phase == "begin":
        require(not args.new_receipt or args.mode == "maintenance", "Online requires preceding evidence")
        generation = 0 if args.new_receipt else object_generation(args.receipt_uri)
        receipt = get_object(args.receipt_uri, generation) if args.mode == "online" else None
        with tempfile.TemporaryDirectory() as folder:
            argv_file = Path(folder) / "arguments.json"
            def preflight():
                if args.preflight_arguments:
                    return command(args.preflight_arguments,
                      "//src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:VidLabelingRolloutCheck")
                argv_file.write_text(json.dumps(preflight_argv(manifest, folder)))
                return command(str(argv_file),
                  "//src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:VidLabelingRolloutCheck")
            journal = begin(args.mode, manifest, receipt,
                            lambda: control(args.intake_control_url, "pause", manifest), preflight)
        put_object(args.receipt_uri, journal, generation)
        print(json.dumps(dict(journal, manifest=manifest)))
    else:
        generation = object_generation(args.receipt_uri)
        journal = get_object(args.receipt_uri, generation)
        require(args.cutover_id and journal.get("request_id") == args.cutover_id,
                "Another workflow owns this receipt")
        current_generation = [generation]
        def checkpoint(verified):
            current_generation[0] = put_object(args.receipt_uri, verified, current_generation[0])
        require(journal.get("mode") == args.mode, "Cutover mode changed")
        def readiness():
            check(manifest["project"], manifest["region"], args.image_tag)
            check_functions(manifest["project"], manifest["region"], manifest)
        receipt = complete(manifest, journal, args.image_tag, readiness,
                           lambda: command(args.smoke_arguments,
                            "//src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:VidLabelingPublicationSmoke"),
                           lambda: control(args.intake_control_url, "resume", manifest),
                           checkpoint)
        put_object(args.receipt_uri, receipt, current_generation[0])
        print(json.dumps(receipt))


if __name__ == "__main__":
    main()
