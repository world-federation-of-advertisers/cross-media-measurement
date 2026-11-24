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


def control(url, action, manifest, owner_id, execute=subprocess.run, opener=urllib.request.urlopen):
    """The publisher-owned control must attest its complete scope; stopping workers is not pause."""
    parsed = urllib.parse.urlparse(url)
    require(parsed.scheme == "https" and parsed.netloc and not parsed.username and not parsed.password,
            "Maintenance requires a publisher-owned HTTPS intake/backfill control")
    identity = execute(["gcloud", "auth", "print-identity-token", "--audiences=" + url],
                       check=True, capture_output=True, text=True, timeout=30).stdout.strip()
    require(bool(identity), "Control identity unavailable")
    payload = json.dumps({"action": action, "scope": {
        "providers": manifest["providers"], "queues": manifest["queues"]},
        "scope_id": scope_id(manifest), "owner_id": owner_id,
        "request_id": str(uuid.uuid5(uuid.NAMESPACE_URL, owner_id + ":" + action))}).encode()
    request = urllib.request.Request(url, data=payload, method="POST",
                                     headers={"Authorization": "Bearer " + identity,
                                              "Content-Type": "application/json"})
    with opener(request, timeout=30) as response:
        value = json.load(response)
    require(value.get("owner_id") == owner_id, "Control belongs to another cutover owner")
    require(value.get("scope_id") == scope_id(manifest), "Control attested another scope")
    require(value.get("paused") is (action == "pause"), "Publisher/backfill control not confirmed")
    return value


def claim_receipt(mode, manifest, previous, generation, write, ready=check_compatibility):
    """Reserve exclusive ownership before any publisher pause or component mutation."""
    require(mode in ("maintenance", "online"), "Unknown cutover mode")
    if generation:
        require(isinstance(previous, dict) and previous.get("status") == "READY",
                "Another cutover/recovery owns this receipt")
        require(previous.get("scope_id") == scope_id(manifest) and
                all(previous.get(k) == manifest.get(k) for k in ("project", "region")),
                "Receipt belongs to another deployment")
    else:
        require(mode == "maintenance" and previous is None, "Online requires preceding evidence")
    if mode == "online":
        ready(manifest["project"], manifest["region"], manifest, previous)
    owner_id = str(uuid.uuid4())
    reserved = dict(manifest, status="RESERVED", request_id=owner_id,
                    scope_id=scope_id(manifest), mode=mode, started_at=int(time.time()),
                    previous_receipt=previous)
    return owner_id, write(reserved, generation)


def abort(receipt, generation, owner_id, release_intake, restore, remove):
    """Release a begin that crashed or failed while it held RESERVED ownership.

    Only the exact owner may abort. Maintenance intake is released first; then the
    preceding READY receipt is restored (or a first receipt removed) with a
    generation precondition, so a concurrent writer cannot be overwritten.
    """
    require(isinstance(receipt, dict) and receipt.get("status") == "RESERVED",
            "Only a RESERVED begin can be aborted; complete or diagnose later states")
    require(bool(owner_id) and receipt.get("request_id") == owner_id,
            "Abort requires the exact cutover owner")
    require(receipt.get("mode") in ("online", "maintenance"), "Unknown cutover mode")
    previous = receipt.get("previous_receipt")
    require(previous is None or (isinstance(previous, dict) and previous.get("status") == "READY"),
            "Preceding receipt is not READY")
    if receipt["mode"] == "maintenance":
        release_intake()
    if previous is None:
        remove(generation)
    else:
        restore(previous, generation)


def guarded_begin(start, release):
    """Run the begin boundary; on any failure release ownership before re-raising."""
    try:
        return start()
    except BaseException:
        release()
        raise


def resume_owned(manifest, journal, generation, read_generation, read, publish):
    observed = read_generation()
    require(observed == generation, "Receipt owner changed before resume")
    current = read(observed)
    require(current.get("status") == "VERIFIED" and
            current.get("request_id") == journal["request_id"],
            "Resume no longer belongs to this cutover")
    publish(journal["request_id"])


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
    require(journal.get("status") in ("IN_PROGRESS", "VERIFIED") and journal.get("scope_id") == scope_id(manifest),
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
                    smoke=result, request_id=journal["request_id"], mode=journal["mode"],
                    started_at=journal["started_at"])
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


def put_object(uri, value, expected_generation, execute=subprocess.run, opener=urllib.request.urlopen):
    """Return the generation from this exact conditional write, never a later latest-object read."""
    parsed = urllib.parse.urlparse(uri)
    require(parsed.scheme == "gs" and parsed.netloc and "/rollout/" in uri and
            not parsed.fragment and not parsed.query, "Unsafe receipt namespace")
    require(isinstance(expected_generation, int) and expected_generation >= 0,
            "Precise preceding generation required")
    token = execute(["gcloud", "auth", "print-access-token"], check=True,
                    capture_output=True, text=True, timeout=30).stdout.strip()
    require(token, "Storage identity unavailable")
    url = "https://storage.googleapis.com/upload/storage/v1/b/" + urllib.parse.quote(parsed.netloc, safe="") + "/o?"
    url += urllib.parse.urlencode({"uploadType": "media", "name": parsed.path.lstrip("/"),
                                  "ifGenerationMatch": expected_generation})
    request = urllib.request.Request(url, data=json.dumps(value, sort_keys=True).encode(), method="POST",
                                    headers={"Authorization": "Bearer " + token,
                                             "Content-Type": "application/json"})
    with opener(request, timeout=30) as response:
        metadata = json.load(response)
    require(metadata.get("bucket") == parsed.netloc and metadata.get("name") == parsed.path.lstrip("/"),
            "Write returned another object")
    generation = int(metadata["generation"])
    require(generation > expected_generation, "Conditional write generation unavailable")
    return generation


# Deployed VID artifacts: Cloud Run function uber JARs and Phase 0/1/2 TEE images.
DEPLOYED_TARGETS = (
    "//src/main/kotlin/org/wfanet/measurement/securecomputation/deploy/gcloud/datawatcher:DataWatcherFunction",
    "//src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/dataavailability:DataAvailabilitySyncFunction",
    "//src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/dataavailability:DataAvailabilityCleanupFunction",
    "//src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/dataavailability:DataAvailabilityMonitorFunction",
    "//src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/vidlabeling:VidLabelingDispatcherFunction",
    "//src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/vidlabeling:VidLabelingMonitorFunction",
    "//src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/vidlabeling:VidLabelingHealingControllerFunction",
    "//src/main/kotlin/org/wfanet/measurement/edpaggregator/subpoolassigner:subpool_assigner_image",
    "//src/main/kotlin/org/wfanet/measurement/edpaggregator/vidrankbuilder:vid_rank_builder_image",
    "//src/main/kotlin/org/wfanet/measurement/edpaggregator/vidlabeler:vid_labeler_image",
)
# External dependency resolution shared by every deployed artifact.
DEPENDENCY_LOCKS = ("MODULE.bazel", "MODULE.bazel.lock", "maven_install.json")
# EDPA implementation may roll online; its wire/storage contract is contract_id.
EDPA_PREFIXES = (
    "src/main/kotlin/org/wfanet/measurement/edpaggregator/",
    "src/main/proto/wfa/measurement/edpaggregator/",
    "src/main/proto/wfa/measurement/internal/edpaggregator/",
    "src/main/resources/edpaggregator/",
)


def label_path(label):
    """Main-repository file label to repository path; external labels are None."""
    if not label.startswith("//"):
        return None
    package, _, name = label[2:].partition(":")
    return (package + "/" + name) if package else name


def delete_object(uri, expected_generation, execute=subprocess.run,
                  opener=urllib.request.urlopen):
    """Delete exactly the observed generation; a concurrent writer makes this fail."""
    parsed = urllib.parse.urlparse(uri)
    require(parsed.scheme == "gs" and parsed.netloc and "/rollout/" in uri and
            not parsed.fragment and not parsed.query, "Unsafe receipt namespace")
    require(isinstance(expected_generation, int) and expected_generation > 0,
            "Precise receipt generation required")
    token = execute(["gcloud", "auth", "print-access-token"], check=True,
                    capture_output=True, text=True, timeout=30).stdout.strip()
    require(token, "Storage identity unavailable")
    url = ("https://storage.googleapis.com/storage/v1/b/" +
           urllib.parse.quote(parsed.netloc, safe="") + "/o/" +
           urllib.parse.quote(parsed.path.lstrip("/"), safe="") + "?" +
           urllib.parse.urlencode({"ifGenerationMatch": expected_generation}))
    request = urllib.request.Request(url, method="DELETE",
                                     headers={"Authorization": "Bearer " + token})
    with opener(request, timeout=30):
        pass


def shared_platform_inputs(tree, labels):
    """Tracked shared (non-EDPA) source and build inputs of the deployed artifacts."""
    paths = {label_path(label) for label in labels}
    paths.discard(None)
    paths.update(DEPENDENCY_LOCKS)
    output = []
    for line in tree.splitlines():
        path = line.split("\t", 1)[1]
        if path in paths and not path.startswith(EDPA_PREFIXES):
            output.append(line)
    return sorted(output)


def deployed_input_labels(execute=subprocess.run):
    targets = "set(" + " ".join(DEPLOYED_TARGETS) + ")"
    query = f'kind("source file", deps({targets})) union buildfiles(deps({targets}))'
    result = execute(["bazelisk", "query", query, "--output=label"], check=True,
                     capture_output=True, text=True, timeout=900)
    labels = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    require(labels, "Deployed artifact inputs unavailable")
    return labels


def prepare_manifest(manifest, config_file, execute=subprocess.run):
    require(config_file, "Committed provider config is required")
    result = execute(["bazelisk", "run",
        "//src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:VidLabelingRolloutScope",
        "--", "--vid-labeling-config=" + config_file], check=True, capture_output=True,
        text=True, timeout=300)
    value = dict(manifest, **json.loads(result.stdout.splitlines()[-1]))
    tree = execute(["git", "ls-tree", "-r", "HEAD"], check=True, capture_output=True,
                   text=True, timeout=30).stdout
    selected = shared_platform_inputs(tree, deployed_input_labels(execute))
    require(selected, "Shared deployed inputs unavailable")
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
    result = ["--mode=MAINTENANCE",
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

def release_intake(url, manifest, owner_id):
    """Replay the owner's idempotent pause, then resume, so release works whether or not
    the crash preceded the original pause."""
    control(url, "pause", manifest, owner_id)
    control(url, "resume", manifest, owner_id)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=["begin", "complete", "abort"], required=True)
    parser.add_argument("--mode", choices=["online", "maintenance"])
    parser.add_argument("--manifest")
    parser.add_argument("--receipt-uri", required=True)
    parser.add_argument("--image-tag")
    parser.add_argument("--intake-control-url", default="")
    parser.add_argument("--preflight-arguments", help="JSON argv for the existing read-only CLI")
    parser.add_argument("--smoke-arguments", help="JSON argv for the final-stack production-sink smoke")
    parser.add_argument("--vid-labeling-config", help="Committed deployment config for begin")
    parser.add_argument("--new-receipt", action="store_true", help="First maintenance cutover only")
    parser.add_argument("--cutover-id", help="Exact checked begin request ID")
    args = parser.parse_args()
    if args.phase == "abort":
        # Releases a RESERVED begin left by a crash: resume intake, restore the receipt.
        require(args.cutover_id, "Abort requires --cutover-id")
        generation = object_generation(args.receipt_uri)
        receipt = get_object(args.receipt_uri, generation)
        abort(receipt, generation, args.cutover_id,
              lambda: release_intake(args.intake_control_url, receipt, args.cutover_id),
              lambda previous, expected: put_object(args.receipt_uri, previous, expected),
              lambda expected: delete_object(args.receipt_uri, expected))
        print(json.dumps({"status": "ABORTED", "request_id": args.cutover_id}))
        return
    require(args.mode and args.manifest and args.image_tag,
            "--mode, --manifest and --image-tag are required")
    manifest = json.loads(Path(args.manifest).read_text())
    if args.phase == "begin":
        manifest = prepare_manifest(manifest, args.vid_labeling_config)
    scope_id(manifest)

    def command(file, target):
        require(file, "Required checked CLI arguments are missing")
        argv = json.loads(Path(file).read_text())
        require(isinstance(argv, list) and all(isinstance(a, str) for a in argv), "Invalid CLI argv")
        # Covers a cold build plus the checker's own deadline, which includes the lease wait.
        result = subprocess.run(["bazelisk", "run", target, "--", *argv], check=True,
                                capture_output=True, text=True, timeout=2700)
        # Tools may emit normal logging before their final machine-readable result.
        return json.loads(result.stdout.splitlines()[-1])

    if args.phase == "begin":
        require(not args.new_receipt or args.mode == "maintenance", "Online requires preceding evidence")
        generation = 0 if args.new_receipt else object_generation(args.receipt_uri)
        receipt = get_object(args.receipt_uri, generation) if generation else None
        owner_id, generation = claim_receipt(args.mode, manifest, receipt, generation,
            lambda value, expected: put_object(args.receipt_uri, value, expected))
        with tempfile.TemporaryDirectory() as folder:
            argv_file = Path(folder) / "arguments.json"
            def preflight():
                if args.preflight_arguments:
                    return command(args.preflight_arguments,
                      "//src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:VidLabelingRolloutCheck")
                argv_file.write_text(json.dumps(preflight_argv(manifest, folder)))
                return command(str(argv_file),
                  "//src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:VidLabelingRolloutCheck")
            reserved = {"status": "RESERVED", "request_id": owner_id, "mode": args.mode,
                        "previous_receipt": receipt}
            journal = guarded_begin(
                lambda: begin(args.mode, manifest, receipt,
                              lambda: control(args.intake_control_url, "pause", manifest, owner_id),
                              preflight, ready=lambda *_: None),
                lambda: abort(reserved, generation, owner_id,
                              lambda: release_intake(args.intake_control_url, manifest, owner_id),
                              lambda previous, expected: put_object(args.receipt_uri, previous, expected),
                              lambda expected: delete_object(args.receipt_uri, expected)))
        journal["request_id"] = owner_id
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
        def resume():
            resume_owned(manifest, journal, current_generation[0],
                lambda: object_generation(args.receipt_uri),
                lambda observed: get_object(args.receipt_uri, observed),
                lambda owner: control(args.intake_control_url, "resume", manifest, owner))
        receipt = complete(manifest, journal, args.image_tag, readiness,
                           lambda: command(args.smoke_arguments,
                            "//src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:VidLabelingPublicationSmoke"),
                           resume, checkpoint)
        put_object(args.receipt_uri, receipt, current_generation[0])
        print(json.dumps(receipt))


if __name__ == "__main__":
    main()
