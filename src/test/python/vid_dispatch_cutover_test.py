"""Operator cutover contract tests using recorded Cloud API response shapes."""

import copy
import datetime as dt
import importlib.util
import io
import json
import subprocess
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import zipfile

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("cutover", ROOT / ".github/workflows/scripts/vid_dispatch_cutover.py")
cutover = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cutover)


class FakeCloud:
    project, region = "test-project", "us-central1"

    def __init__(self):
        self.calls = []
        self.artifact = b""
        self.source_changed = False
        self.active = True
        self.queue = {
            "topic": "projects/test-project/topics/vid-labeling-dispatch-queue",
            "ackDeadlineSeconds": 600,
            "deadLetterPolicy": {"deadLetterTopic": "projects/test-project/topics/vid-labeling-dispatch-queue-dlq"},
            "pushConfig": {
                "pushEndpoint": "https://us-central1-test-project.cloudfunctions.net/vid-labeling-dispatcher",
                "oidcToken": {"audience": "https://us-central1-test-project.cloudfunctions.net/vid-labeling-dispatcher",
                              "serviceAccountEmail": "dispatcher@test-project.iam.gserviceaccount.com"},
                "noWrapper": {"writeMetadata": True},
            },
        }
        self.policy_enabled = True
        self.channel_verified = True
        now = dt.datetime.now(dt.timezone.utc)
        self.points = [{"interval": {"endTime": (now - dt.timedelta(seconds=offset)).isoformat()},
                        "value": {"int64Value": "0"}} for offset in (30, 90)]

    def function(self, name):
        return {"name": "projects/test-project/locations/us-central1/functions/" + name,
                "state": "ACTIVE" if self.active else "DEPLOYING",
                "serviceConfig": {"serviceAccountEmail": "dispatcher@test-project.iam.gserviceaccount.com",
                                  "environmentVariables": {"CONFIG_BLOB_KEY": "data-watcher-config.textproto"}},
                "buildConfig": {"source": {"storageSource": {"bucket": "source-bucket", "object": "source.zip", "generation": "7"}}}}

    def gcloud(self, *command):
        self.calls.append(command)
        if command[:3] == ("run", "services", "describe"):
            return {"status": {"latestReadyRevisionName": "ready", "latestCreatedRevisionName": "ready",
                               "conditions": [{"type": "Ready", "status": "True"}],
                               "traffic": [{"revisionName": "ready", "percent": 100}]}}
        if command[:3] == ("pubsub", "subscriptions", "describe"):
            return copy.deepcopy(self.queue)
        if command[:3] == ("run", "services", "get-iam-policy"):
            return {"bindings": [{"role": "roles/run.invoker",
                                  "members": ["serviceAccount:dispatcher@test-project.iam.gserviceaccount.com"]}]}
        if command[:2] == ("projects", "describe"):
            return {"projectNumber": "123"}
        if command[:3] == ("iam", "service-accounts", "get-iam-policy"):
            return {"bindings": [{"role": "roles/iam.serviceAccountTokenCreator",
                                  "members": ["serviceAccount:service-123@gcp-sa-pubsub.iam.gserviceaccount.com"]}]}
        if command[:4] == ("compute", "instance-groups", "managed", "describe"):
            return {"status": {"isStable": True}, "versions": [{"instanceTemplate": "prepared"}]}
        raise AssertionError("Unexpected Cloud command: " + repr(command))

    def run(self, command):
        self.calls.append(tuple(command))
        if command[:3] == ["gcloud", "storage", "cp"]:
            require_pinned = "#" in command[3]
            assert require_pinned
            with zipfile.ZipFile(command[4], "w") as archive:
                archive.writestr("function.jar", self.artifact + (b"changed" if self.source_changed else b""))
            return ""
        if command[:3] == ["kubectl", "get", "deployment"]:
            return json.dumps({"items": [{"metadata": {"generation": 2},
                "spec": {"replicas": 2}, "status": {"observedGeneration": 2, "updatedReplicas": 2,
                "availableReplicas": 2}} for _ in range(2)]})
        raise AssertionError("Unexpected process: " + repr(command))

    def monitoring(self, collection, query=None):
        self.calls.append(("monitoring", collection))
        if collection == "timeSeries":
            return [{"points": copy.deepcopy(self.points)}]
        if collection == "alertPolicies":
            return [{"displayName": display, "enabled": self.policy_enabled, "notificationChannels": ["channels/one"]}
                    for display in ("EDPA VID dispatch failed", "EDPA VID dispatch retry exhausted")]
        if collection == "notificationChannels":
            return [{"name": "channels/one", "verificationStatus": "VERIFIED" if self.channel_verified else "UNVERIFIED"}]
        raise AssertionError(collection)


class CutoverTest(unittest.TestCase):
    def jar(self, directory):
        path = Path(directory) / "prepared.jar"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("pkg/VidLabelingDispatcherWorkItemProcessor.class", b"VidLabelingDispatchWorkItemParams")
            archive.writestr("pkg/DataWatcherConfigTypes.class", b"VidLabelingDispatchWorkItemParams DataWatcherConfigTypes")
        return path

    def test_readiness_requires_actual_deployed_artifact_and_uses_only_reads(self):
        cloud = FakeCloud()
        with tempfile.TemporaryDirectory() as directory:
            artifact = self.jar(directory)
            cloud.artifact = artifact.read_bytes()
            functions = cutover.function_readiness(cloud, {name: str(artifact) for name in cutover.FUNCTIONS})
            cutover.queue_readiness(cloud, functions["vid-labeling-dispatcher"])
            cutover.infrastructure_readiness(cloud)
        forbidden = {"apply", "create", "delete", "update", "pause", "resume", "add-iam-policy-binding"}
        self.assertFalse(any(forbidden.intersection(command) for command in cloud.calls))

    def test_changed_source_archive_fails_before_activation(self):
        cloud = FakeCloud()
        cloud.source_changed = True
        with tempfile.TemporaryDirectory() as directory:
            artifact = self.jar(directory)
            cloud.artifact = artifact.read_bytes()
            with self.assertRaises(cutover.GateError):
                cutover.function_readiness(cloud, {name: str(artifact) for name in cutover.FUNCTIONS})

    def test_unready_function_fails_closed(self):
        cloud = FakeCloud()
        cloud.active = False
        with self.assertRaises(cutover.GateError):
            cutover.function_readiness(cloud, {})

    def test_push_identity_audience_and_wrapping_must_all_match(self):
        for mutation in (
            lambda q: q["pushConfig"]["oidcToken"].update(audience="wrong"),
            lambda q: q["pushConfig"]["oidcToken"].update(serviceAccountEmail="wrong"),
            lambda q: q["pushConfig"]["noWrapper"].update(writeMetadata=False),
            lambda q: q.pop("deadLetterPolicy"),
        ):
            cloud = FakeCloud()
            mutation(cloud.queue)
            with self.assertRaises(cutover.GateError):
                cutover.queue_readiness(cloud, cloud.function("vid-labeling-dispatcher"))

    def test_notifications_must_be_enabled_and_verified(self):
        for field in ("policy_enabled", "channel_verified"):
            cloud = FakeCloud()
            setattr(cloud, field, False)
            with self.assertRaises(cutover.GateError):
                cutover.infrastructure_readiness(cloud)

    def test_maintenance_accepts_two_fresh_observed_zero_backlog_samples(self):
        cutover.zero_backlog(FakeCloud(), "legacy", dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=3))

    def test_missing_stale_single_and_outstanding_backlog_evidence_fail_closed(self):
        for values in ([], [0], [0, 1], [0, 0]):
            cloud = FakeCloud()
            if not values:
                cloud.monitoring = lambda *_: []
            else:
                cloud.points = cloud.points[:len(values)]
                for point, value in zip(cloud.points, values):
                    point["value"]["int64Value"] = str(value)
                if values == [0, 0]:
                    for point in cloud.points:
                        point["interval"]["endTime"] = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=10)).isoformat()
            with self.assertRaises(cutover.GateError):
                cutover.zero_backlog(cloud, "legacy", dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=15))

    def test_preflight_never_substitutes_a_timeout_for_a_confirmed_pause(self):
        with self.assertRaises(cutover.GateError):
            cutover.legacy_preflight(FakeCloud(), None)

    def test_profile_must_cover_all_live_providers_and_a_distinct_test_provider(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "config.textproto"
            config.write_text('data_provider: "dataProviders/live"\ndata_provider: "dataProviders/test"')
            profile = {"isolatedDataProvider": "dataProviders/test", "liveDataProviders": ["dataProviders/live"]}
            self.assertEqual(cutover.profile_providers(profile, config)[0], "dataProviders/test")
            profile["liveDataProviders"].append("dataProviders/test")
            with self.assertRaises(cutover.GateError):
                cutover.profile_providers(profile, config)

    def test_legacy_observation_requires_fresh_zero_invocations_without_pausing_traffic(self):
        cloud = FakeCloud()
        cutover.observe_legacy(cloud, [{"service": "data-watcher", "revision": "old"}],
                               dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=3))
        self.assertFalse(any("pause" in command or "update" in command for command in cloud.calls))
        cloud.points[0]["value"]["int64Value"] = "1"
        with mock.patch.object(cutover.time, "monotonic", side_effect=[0, 1300]):
            with self.assertRaises(cutover.GateError):
                cutover.observe_legacy(cloud, [{"service": "data-watcher", "revision": "old"}],
                                       dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=3))

    def test_v2_key_alone_is_not_evidence_of_queued_dispatch(self):
        cloud = FakeCloud()
        function = cloud.function("data-watcher")
        function["serviceConfig"]["environmentVariables"] = {
            "CONFIG_BLOB_KEY": cutover.CONFIG_V2,
            "EDPA_CONFIG_STORAGE_BUCKET": "gs://config-bucket",
            "DATA_WATCHER_CONFIG_GENERATION": "5",
        }
        cloud.function = lambda _: function
        cloud.run = lambda command: 'http_endpoint_sink { endpoint_uri: "https://dispatcher" }'
        self.assertFalse(cutover.is_queued(cloud))
        cloud.run = lambda command: 'control_plane_queue_sink { queue: "vid-labeling-dispatch-queue" }'
        self.assertTrue(cutover.is_queued(cloud))

    def test_source_timestamp_nanoseconds_are_checked_before_smoke_mutations(self):
        cloud = FakeCloud()
        called_operator = []
        def describe(*command):
            cloud.calls.append(command)
            return {"generation": "7", "timeCreated": "2026-11-23T12:00:00.000000001Z"}
        cloud.gcloud = describe
        cloud.run = lambda command: called_operator.append(command)
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "vid.textproto"
            config.write_text('data_provider: "dataProviders/live"' + chr(10) + 'data_provider: "dataProviders/test"')
            profile = {
                "isolatedDataProvider": "dataProviders/test", "liveDataProviders": ["dataProviders/live"],
                "ordinaryWorkItem": {"workItemParams": {
                    "appParams": {"dataProvider": "dataProviders/test"},
                    "dataPathParams": {"dataPath": "gs://test/raw/done", "generation": "7",
                                      "createTime": "2026-11-23T12:00:00.000000002Z"}}},
                "recoveryWorkItem": {}, "approval": {},
            }
            args = type("Args", (), {"vid_labeling_config": str(config), "operator_command": ["operator"]})()
            with self.assertRaises(cutover.GateError):
                cutover.smoke(cloud, args, profile, [], directory)
        self.assertEqual(called_operator, [])

    def test_operator_runner_preserves_partial_ids_on_failure_and_timeout(self):
        cloud = cutover.Cloud("test-project", "us-central1")
        output = "unrelated output\nSMOKE_WORK_ITEM=workItems/dispatch-smoke-one\n"
        for outcome in (
            subprocess.CompletedProcess(["operator"], 1, output, "failed"),
            subprocess.TimeoutExpired(["operator"], 2400, output=output.encode()),
        ):
            capture = io.StringIO()
            with mock.patch.object(cutover.subprocess, "run", side_effect=outcome if isinstance(outcome, Exception) else None,
                                   return_value=outcome), mock.patch("sys.stdout", capture):
                with self.assertRaises(cutover.GateError):
                    cloud.run_operator(["operator"], timeout=2400)
            self.assertIn("SMOKE_WORK_ITEM=workItems/dispatch-smoke-one", capture.getvalue())
            self.assertNotIn("unrelated output", capture.getvalue())

    def test_operator_runner_uses_separate_smoke_deadline_and_retains_success_ids(self):
        cloud = cutover.Cloud("test-project", "us-central1")
        capture = io.StringIO()
        output = "SMOKE_WORK_ITEM=workItems/dispatch-smoke-two\n"
        with mock.patch.object(cutover.subprocess, "run",
                               return_value=subprocess.CompletedProcess(["operator"], 0, output, "")) as process, \
             mock.patch("sys.stdout", capture):
            self.assertEqual(cloud.run_operator(["operator"], timeout=2400), output)
        self.assertEqual(process.call_args.kwargs["timeout"], 2400)
        self.assertGreater(process.call_args.kwargs["timeout"], 20 * 60)
        self.assertIn("SMOKE_WORK_ITEM=workItems/dispatch-smoke-two", capture.getvalue())

    def test_naive_pause_timestamp_is_rejected(self):
        with self.assertRaises(cutover.GateError):
            cutover.timestamp("2026-11-23T12:00:00")


if __name__ == "__main__":
    unittest.main()
