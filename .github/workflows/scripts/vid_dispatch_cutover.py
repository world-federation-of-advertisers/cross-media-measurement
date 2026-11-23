#!/usr/bin/env python3
"""Read-only cutover gates and an isolated existing-API dispatch smoke test."""

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
import zipfile

QUEUE = "vid-labeling-dispatch-queue"
CONFIG_V2 = "data-watcher-config.dispatch-v2.textproto"
FUNCTIONS = ("data-watcher", "vid-labeling-dispatcher", "vid-labeling-healing-controller")
MIGS = ("subpool-assigner-mig", "vid-rank-builder-mig", "vid-labeler-mig")
OPERATOR = ["bazelisk", "run", "//src/main/kotlin/org/wfanet/measurement/edpaggregator/tools:VidLabelingHeal", "--"]


class GateError(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise GateError(message)


def timestamp(value):
    compatible = re.sub('(\\.\\d{6})\\d+(?=Z|[+-]\\d{2}:\\d{2}$)', '\\1', value)
    parsed = dt.datetime.fromisoformat(compatible.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, "Timestamp must include its UTC offset")
    return parsed


def exact_timestamp(value):
    parsed = timestamp(value)
    fraction = re.search('\\.(\\d{1,9})(?:Z|[+-]\\d{2}:\\d{2})$', value)
    nanos = int(fraction.group(1).ljust(9, "0")) if fraction else 0
    return parsed.astimezone(dt.timezone.utc).replace(microsecond=0), nanos


class Cloud:
    def __init__(self, project, region):
        self.project, self.region = project, region

    def run(self, command):
        result = subprocess.run(command, text=True, capture_output=True, timeout=180)
        require(result.returncode == 0, "Command failed: " + " ".join(command[:5]) + "\n" + result.stderr)
        return result.stdout

    def gcloud(self, *command):
        return json.loads(self.run(["gcloud", *command, "--project=" + self.project, "--format=json"]))

    def function(self, name):
        return self.gcloud("functions", "describe", name, "--gen2", "--region=" + self.region)

    def monitoring(self, collection, query=None):
        token = self.run(["gcloud", "auth", "print-access-token"]).strip()
        params = dict(query or {})
        result, seen = [], set()
        while True:
            uri = "https://monitoring.googleapis.com/v3/projects/" + self.project + "/" + collection
            uri += "?" + urllib.parse.urlencode(params)
            request = urllib.request.Request(uri, headers={"Authorization": "Bearer " + token})
            with urllib.request.urlopen(request, timeout=60) as response:
                page = json.load(response)
            result.extend(page.get(collection, []))
            next_token = page.get("nextPageToken")
            if not next_token:
                return result
            require(next_token not in seen, "Repeated Monitoring page token")
            seen.add(next_token)
            params["pageToken"] = next_token


def verify_artifact(cloud, function, expected_path):
    """Match the actual deployed source archive to the compatible artifact."""
    expected = Path(expected_path).read_bytes()
    with zipfile.ZipFile(expected_path) as jar:
        names = jar.namelist()
        payload = b"".join(jar.read(name) for name in names if name.endswith(".class"))
        require(b"VidLabelingDispatchWorkItemParams" in payload, "Artifact lacks typed dispatch parameters")
        if function["name"].endswith("/vid-labeling-dispatcher"):
            require(any(name.endswith("/VidLabelingDispatcherWorkItemProcessor.class") for name in names),
                    "Artifact lacks the dual-route dispatcher worker")
        else:
            require(b"DataWatcherConfigTypes" in payload, "Artifact lacks the compatible watcher/parser registry")
    source = function["buildConfig"]["source"]["storageSource"]
    require(all(source.get(key) for key in ("bucket", "object", "generation")), "Unpinned deployed function source")
    uri = "gs://" + source["bucket"] + "/" + source["object"] + "#" + str(source["generation"])
    with tempfile.TemporaryDirectory(prefix="vid-dispatch-source-") as directory:
        archive = Path(directory) / "source.zip"
        cloud.run(["gcloud", "storage", "cp", uri, str(archive), "--quiet"])
        with zipfile.ZipFile(archive) as deployed:
            jars = [name for name in deployed.namelist() if name.endswith(".jar")]
            require(len(jars) == 1, "Ambiguous deployed function JAR")
            require(hashlib.sha256(deployed.read(jars[0])).digest() == hashlib.sha256(expected).digest(),
                    "Deployed function source differs from the prepared artifact")


def function_readiness(cloud, expected_artifacts):
    functions = {}
    for name in FUNCTIONS:
        function = cloud.function(name)
        require(function.get("state") == "ACTIVE", name + " is not ACTIVE")
        require(name in expected_artifacts, "Missing expected artifact for " + name)
        verify_artifact(cloud, function, expected_artifacts[name])
        service = cloud.gcloud("run", "services", "describe", name, "--region=" + cloud.region)
        status = service["status"]
        ready = status.get("latestReadyRevisionName")
        require(ready and ready == status.get("latestCreatedRevisionName"), name + " has an unready revision")
        require(any(c.get("type") == "Ready" and c.get("status") == "True" for c in status.get("conditions", [])),
                name + " service is not ready")
        require(sum(t.get("percent", 0) for t in status.get("traffic", []) if t.get("revisionName") == ready) == 100,
                name + " does not route all new requests to its ready revision")
        functions[name] = function
    return functions


def queue_readiness(cloud, dispatcher):
    subscription = cloud.gcloud("pubsub", "subscriptions", "describe", "vid-labeling-dispatch-subscription")
    endpoint = "https://" + cloud.region + "-" + cloud.project + ".cloudfunctions.net/vid-labeling-dispatcher"
    push = subscription.get("pushConfig", {})
    oidc = push.get("oidcToken", {})
    account = dispatcher["serviceConfig"]["serviceAccountEmail"]
    require(subscription.get("topic", "").endswith("/" + QUEUE), "Dispatch topic mapping differs")
    require(push.get("pushEndpoint") == endpoint and oidc.get("audience") == endpoint,
            "Dispatch push endpoint/audience differs")
    require(oidc.get("serviceAccountEmail") == account, "Dispatch push identity differs")
    require(push.get("noWrapper", {}).get("writeMetadata") is True, "Dispatch push payload/metadata wrapping differs")
    require(subscription.get("ackDeadlineSeconds") == 600, "Dispatch acknowledgment deadline differs")
    require(subscription.get("deadLetterPolicy", {}).get("deadLetterTopic", "").endswith("/" + QUEUE + "-dlq"),
            "Dispatch DLQ is missing")
    policy = cloud.gcloud("run", "services", "get-iam-policy", "vid-labeling-dispatcher", "--region=" + cloud.region)
    require(any(b.get("role") == "roles/run.invoker" and "serviceAccount:" + account in b.get("members", [])
                for b in policy.get("bindings", [])), "Push identity cannot invoke dispatcher")
    project = cloud.gcloud("projects", "describe", cloud.project)
    pubsub_agent = "serviceAccount:service-" + str(project["projectNumber"]) + "@gcp-sa-pubsub.iam.gserviceaccount.com"
    token_policy = cloud.gcloud("iam", "service-accounts", "get-iam-policy", account)
    require(any(b.get("role") == "roles/iam.serviceAccountTokenCreator" and pubsub_agent in b.get("members", [])
                for b in token_policy.get("bindings", [])), "Pub/Sub cannot mint the dispatch push token")


def infrastructure_readiness(cloud):
    for name in MIGS:
        group = cloud.gcloud("compute", "instance-groups", "managed", "describe", name, "--region=" + cloud.region)
        require(group.get("status", {}).get("isStable") is True and group.get("versions"),
                name + " is not stable on its prepared template")
    deployments = json.loads(cloud.run(["kubectl", "get", "deployment",
        "secure-computation-internal-api-server", "secure-computation-public-api-server", "-o", "json"]))
    require(len(deployments.get("items", [])) == 2, "Control-plane deployment inventory is incomplete")
    for deployment in deployments["items"]:
        desired = deployment["spec"].get("replicas", 1)
        status = deployment.get("status", {})
        require(desired > 0 and status.get("observedGeneration", 0) >= deployment["metadata"]["generation"]
                and status.get("updatedReplicas", 0) == desired and status.get("availableReplicas", 0) == desired,
                "Control-plane replicas are not ready")
    policies = cloud.monitoring("alertPolicies")
    for display in ("EDPA VID dispatch failed", "EDPA VID dispatch retry exhausted"):
        matches = [p for p in policies if p.get("displayName") == display]
        require(len(matches) == 1 and matches[0].get("enabled", True)
                and matches[0].get("notificationChannels"), display + " has no enabled notification route")
    channels = {c["name"]: c for c in cloud.monitoring("notificationChannels")}
    for policy in policies:
        if policy.get("displayName") in ("EDPA VID dispatch failed", "EDPA VID dispatch retry exhausted"):
            for name in policy["notificationChannels"]:
                channel = channels.get(name, {})
                require(channel.get("enabled", True) and channel.get("verificationStatus") == "VERIFIED",
                        "Unverified dispatch notification channel: " + name)


def zero_backlog(cloud, subscription, after):
    now = dt.datetime.now(dt.timezone.utc)
    series = cloud.monitoring("timeSeries", {
        "filter": 'metric.type="pubsub.googleapis.com/subscription/num_undelivered_messages" AND resource.labels.subscription_id="' + subscription + '"',
        "interval.startTime": after.isoformat(),
        "interval.endTime": now.isoformat(),
        "view": "FULL",
    })
    require(len(series) == 1, "Missing or ambiguous backlog evidence for " + subscription)
    points = sorted(series[0].get("points", []), key=lambda p: timestamp(p["interval"]["endTime"]), reverse=True)
    require(len(points) >= 2, "Need two observed backlog samples for " + subscription)
    for point in points[:2]:
        end = timestamp(point["interval"]["endTime"])
        require(end > after and now - end < dt.timedelta(minutes=5), "Stale backlog evidence for " + subscription)
        require("int64Value" in point["value"] and int(point["value"]["int64Value"]) == 0,
                "Legacy notifications remain outstanding in " + subscription)


def legacy_preflight(cloud, paused_at):
    require(paused_at is not None, "Maintenance preflight requires the confirmed done-marker pause time")
    watcher = cloud.function("data-watcher")
    trigger_path = watcher["eventTrigger"]["trigger"]
    trigger = cloud.gcloud("eventarc", "triggers", "describe", trigger_path.rsplit("/", 1)[-1],
                           "--location=" + watcher["eventTrigger"]["triggerRegion"])
    subscription = trigger["transport"]["pubsub"]["subscription"].rsplit("/", 1)[-1]
    # Outstanding messages include deliveries awaiting the synchronous HTTP adapter.
    # A timeout is not a completion signal; both delivery queues must actually drain.
    zero_backlog(cloud, subscription, paused_at)
    zero_backlog(cloud, "data-watcher-dlq-sub", paused_at)



def capture_legacy(cloud):
    revisions = []
    for service_name in ("data-watcher", "vid-labeling-healing-controller"):
        service = cloud.gcloud("run", "services", "describe", service_name, "--region=" + cloud.region)
        revision = service["status"]["latestReadyRevisionName"]
        revisions.append({"service": service_name, "revision": revision})
    return revisions


def observe_legacy(cloud, revisions, after):
    require(after is not None, "Provide the activation timestamp")
    deadline = time.monotonic() + 1200
    while True:
        try:
            now = dt.datetime.now(dt.timezone.utc)
            for revision in revisions:
                service = cloud.gcloud("run", "services", "describe", revision["service"], "--region=" + cloud.region)
                require(all(t.get("percent", 0) == 0 for t in service["status"].get("traffic", [])
                            if t.get("revisionName") == revision["revision"]), "Legacy caller still receives new traffic")
                series = cloud.monitoring("timeSeries", {
                    "filter": 'metric.type="run.googleapis.com/container/max_request_concurrencies" AND resource.labels.revision_name="' + revision["revision"] + '"',
                    "interval.startTime": after.isoformat(), "interval.endTime": now.isoformat(), "view": "FULL",
                })
                require(series, "No fresh completion evidence for the legacy caller")
                for entry in series:
                    points = sorted(entry.get("points", []), key=lambda p: timestamp(p["interval"]["endTime"]), reverse=True)
                    require(len(points) >= 2, "Need two observed idle samples for the legacy caller")
                    for point in points[:2]:
                        end = timestamp(point["interval"]["endTime"])
                        require(end > after and now - end < dt.timedelta(minutes=5), "Stale legacy completion evidence")
                        value = point["value"]
                        if "int64Value" in value:
                            idle = int(value["int64Value"]) == 0
                        elif "distributionValue" in value:
                            distribution = value["distributionValue"]
                            idle = int(distribution.get("count", 0)) > 0 and float(distribution.get("mean", 0)) == 0
                        else:
                            idle = False
                        require(idle, "Legacy caller still has an outstanding invocation")
            print("PASS: legacy watcher/healing invocations finished; the compatible HTTP adapter is retained")
            return
        except GateError:
            require(time.monotonic() < deadline, "Cannot prove legacy completion; retain the adapter and use the documented fallback")
            time.sleep(10)

def write_tls_flags(cloud, profile, directory):
    expected = {"tls-cert-file", "tls-key-file", "cert-collection-file",
                "control-plane-tls-cert-file", "control-plane-tls-key-file", "control-plane-cert-collection-file"}
    secrets = profile.get("tlsSecrets", {})
    require(set(secrets) == expected, "Profile must define all existing EDPA/control-plane TLS secret references")
    flags = []
    for flag, resource in secrets.items():
        match = re.fullmatch(r"projects/([^/]+)/secrets/([^/]+)/versions/([^/]+)", resource)
        require(match is not None, "Use an exact Secret Manager version resource")
        project, secret, version = match.groups()
        contents = cloud.run(["gcloud", "secrets", "versions", "access", version, "--secret=" + secret, "--project=" + project])
        path = Path(directory) / flag
        path.write_text(contents)
        path.chmod(0o600)
        flags.append("--" + flag + "=" + str(path))
    return flags


def operator_flags(cloud, profile, directory, edpa_target, control_target):
    flags = write_tls_flags(cloud, profile, directory)
    flags.extend(["--edpa-public-api-target=" + edpa_target, "--control-plane-api-target=" + control_target])
    for field, flag in (("edpaCertHost", "edpa-public-api-cert-host"), ("controlPlaneCertHost", "control-plane-api-cert-host")):
        if profile.get(field):
            flags.append("--" + flag + "=" + profile[field])
    return flags


def profile_providers(profile, config_path):
    isolated = profile["isolatedDataProvider"]
    live = set(profile["liveDataProviders"])
    configured = set(re.findall(r'(?m)^\s*data_provider:\s*"([^"]+)"', Path(config_path).read_text()))
    require(configured and isolated in configured and live == configured - {isolated},
            "Profile must isolate one dedicated test provider and list every other configured provider")
    return isolated, live, configured


def select_config(cloud):
    try:
        watcher = cloud.function("data-watcher")
    except GateError as error:
        # Only a confirmed NOT_FOUND identifies first deployment. Access errors fail closed.
        if "NOT_FOUND" in str(error):
            return False
        raise
    key = watcher["serviceConfig"]["environmentVariables"]["CONFIG_BLOB_KEY"]
    require(key in ("data-watcher-config.textproto", CONFIG_V2), "Unknown active DataWatcher config key")
    return key == CONFIG_V2



def is_queued(cloud):
    try:
        watcher = cloud.function("data-watcher")
    except GateError as error:
        if "NOT_FOUND" in str(error):
            return False
        raise
    env = watcher["serviceConfig"]["environmentVariables"]
    if env["CONFIG_BLOB_KEY"] != CONFIG_V2:
        return False
    require(env.get("DATA_WATCHER_CONFIG_GENERATION"), "Missing pinned active configuration generation")
    uri = env["EDPA_CONFIG_STORAGE_BUCKET"].rstrip("/") + "/" + CONFIG_V2 + "#" + str(env["DATA_WATCHER_CONFIG_GENERATION"])
    content = cloud.run(["gcloud", "storage", "cat", uri])
    return QUEUE in content

def smoke(cloud, args, profile, tls_flags, directory):
    isolated, live, _ = profile_providers(profile, args.vid_labeling_config)
    files = []
    for field in ("ordinaryWorkItem", "recoveryWorkItem", "approval"):
        path = Path(directory) / (field + ".json")
        path.write_text(json.dumps(profile[field]))
        path.chmod(0o600)
        files.append(path)
    for field, recovery in (("ordinaryWorkItem", False), ("recoveryWorkItem", True)):
        frame = profile[field]["workItemParams"]
        app = frame["appParams"]
        require(app["dataProvider"] == isolated and ("recovery" in app) == recovery, "Smoke fixture is not isolated")
        source = app["recovery"]["source"] if recovery else frame["dataPathParams"]
        uri = source["dataPath"]
        require(uri.startswith("gs://") and uri.endswith("/done"), "Smoke requires a raw done-object source")
        observed = cloud.gcloud("storage", "objects", "describe", uri + "#" + str(source["generation"]))
        require(str(observed["generation"]) == str(source["generation"]), "Smoke source version is unavailable")
        creation = observed.get("timeCreated") or observed.get("creation_time")
        require(creation and exact_timestamp(creation) == exact_timestamp(source["createTime"]), "Smoke source creation time differs")
        if not recovery:
            current = cloud.gcloud("storage", "objects", "describe", uri)
            require(str(current["generation"]) == str(source["generation"]), "Ordinary smoke source is stale")
    command = args.operator_command + ["smoke-dispatch-cutover"] + tls_flags + [
        "--isolated-data-provider=" + isolated, "--live-data-provider=" + ",".join(sorted(live)),
        "--ordinary-work-item-file=" + str(files[0]), "--recovery-work-item-file=" + str(files[1]),
        "--approval-file=" + str(files[2])]
    result = cloud.run(command)
    names = re.findall(r"^SMOKE_WORK_ITEM=(workItems/[^\s]+)$", result, flags=re.M)
    require(len(names) == 2, "Smoke did not report both queued executions")
    deadline = time.monotonic() + 300
    while True:
        entries = cloud.gcloud("logging", "read",
            'resource.type="cloud_run_revision" AND resource.labels.service_name="vid-labeling-dispatcher" AND '
            '(textPayload:"event=edpa.vid_labeling.dispatch.work_item" OR jsonPayload.message:"event=edpa.vid_labeling.dispatch.work_item")',
            "--freshness=30m", "--limit=1000")
        successful = set()
        for entry in entries:
            message = entry.get("textPayload") or entry.get("jsonPayload", {}).get("message", "")
            fields = dict(re.findall(r"([A-Za-z0-9_.]+)=([^\s]+)", message))
            if fields.get("xmm.outcome") in ("succeeded", "no_work"):
                successful.add(fields.get("xmm.work_item.name"))
        if all(name in successful for name in names):
            print("PASS: both isolated queued dispatch/recovery executions produced qualified application outcomes")
            return
        require(time.monotonic() < deadline, "Missing qualified smoke outcome; stale/no-attempt success is not readiness")
        time.sleep(10)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("select-config", "is-queued", "capture-legacy", "observe-legacy", "preflight", "ready", "smoke"))
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--profile-file")
    parser.add_argument("--vid-labeling-config")
    parser.add_argument("--edpa-target")
    parser.add_argument("--control-plane-target")
    parser.add_argument("--expected-artifact", action="append", default=[])
    parser.add_argument("--markers-paused-at", type=timestamp)
    parser.add_argument("--activated-at", type=timestamp)
    parser.add_argument("--legacy-revisions-file")
    parser.add_argument("--operator-command", type=shlex.split, default=OPERATOR)
    args = parser.parse_args(argv)
    cloud = Cloud(args.project, args.region)
    if args.mode == "is-queued":
        print(str(is_queued(cloud)).lower())
        return
    if args.mode == "capture-legacy":
        print(json.dumps(capture_legacy(cloud)))
        return
    if args.mode == "observe-legacy":
        require(args.legacy_revisions_file, "Provide captured legacy caller revisions")
        observe_legacy(cloud, json.loads(Path(args.legacy_revisions_file).read_text()), args.activated_at)
        return
    if args.mode == "select-config":
        print(str(select_config(cloud)).lower())
        return
    if args.mode == "ready":
        artifacts = dict(entry.split("=", 1) for entry in args.expected_artifact)
        functions = function_readiness(cloud, artifacts)
        queue_readiness(cloud, functions["vid-labeling-dispatcher"])
        infrastructure_readiness(cloud)
        print("PASS: prepared code, function/MIG/control-plane readiness, authenticated queue and alert routes")
        return
    require(args.profile_file and args.vid_labeling_config and args.edpa_target and args.control_plane_target,
            "Provide the operator profile, full provider config and existing API targets")
    profile = json.loads(Path(args.profile_file).read_text())
    _, _, configured = profile_providers(profile, args.vid_labeling_config)
    with tempfile.TemporaryDirectory(prefix="vid-dispatch-operator-") as directory:
        tls_flags = operator_flags(cloud, profile, directory, args.edpa_target, args.control_plane_target)
        if args.mode == "preflight":
            legacy_preflight(cloud, args.markers_paused_at)
            edpa_flags = [flag for flag in tls_flags if not flag.startswith("--control-plane")]
            cloud.run(args.operator_command + ["preflight-dispatch-cutover",
                "--data-provider=" + ",".join(sorted(configured))] + edpa_flags)
            print("PASS: outstanding HTTP notifications, incomplete registrations and active healing are drained")
        else:
            smoke(cloud, args, profile, tls_flags, directory)


if __name__ == "__main__":
    try:
        main()
    except (GateError, KeyError, ValueError, OSError, subprocess.TimeoutExpired) as error:
        print("FAIL: " + str(error), file=sys.stderr)
        sys.exit(1)
