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
                                   "timeoutSeconds": 600, "revision": "ready",
                                   "environmentVariables": {"CONFIG_BLOB_KEY": "data-watcher-config.textproto"}},
                "eventTrigger": {"trigger": "projects/test-project/locations/us-central1/triggers/watcher", "triggerRegion": "us-central1"},
                "buildConfig": {"source": {"storageSource": {"bucket": "source-bucket", "object": "source.zip", "generation": "7"}}}}

    def gcloud(self, *command):
        self.calls.append(command)
        if command[:3] == ("run", "services", "describe"):
            return {"status": {"latestReadyRevisionName": "ready", "latestCreatedRevisionName": "ready",
                               "conditions": [{"type": "Ready", "status": "True"}],
                               "traffic": [{"revisionName": "ready", "percent": 100}]}}
        if command[:3] == ("eventarc", "triggers", "describe"):
            return {"transport": {"pubsub": {"subscription": "projects/test-project/subscriptions/legacy"}}}
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


def inspection(**facts):
    return "INFO: unrelated tool output" + chr(10) + cutover.INSPECTION_PREFIX + json.dumps(facts) + chr(10)


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
        cloud = FakeCloud()
        commands = []
        def operator(command, timeout):
            commands.append(command)
            return inspection(dataProviders=["dataProviders/live", "dataProviders/test"])
        cloud.run_operator = operator
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "config.textproto"
            # The script never reads the textproto itself; the protobuf loader reports providers.
            config.write_text('configs { data_provider: "dataProviders/live" } configs { data_provider : "dataProviders/test" }')
            profile = {"isolatedDataProvider": "dataProviders/test", "liveDataProviders": ["dataProviders/live"]}
            isolated, live, configured = cutover.profile_providers(cloud, ["operator"], profile, config)
            self.assertEqual((isolated, live, configured),
                             ("dataProviders/test", {"dataProviders/live"}, {"dataProviders/live", "dataProviders/test"}))
            self.assertEqual(commands[0], ["operator", "inspect-dispatch-cutover-config",
                                           "--vid-labeling-config=" + str(config.resolve())])
            profile["liveDataProviders"].append("dataProviders/test")
            with self.assertRaises(cutover.GateError):
                cutover.profile_providers(cloud, ["operator"], profile, config)

    def test_profile_omitting_a_parsed_provider_fails_before_preflight(self):
        cloud = FakeCloud()
        cloud.run_operator = lambda command, timeout: inspection(
            dataProviders=["dataProviders/live", "dataProviders/other", "dataProviders/test"])
        profile = {"isolatedDataProvider": "dataProviders/test", "liveDataProviders": ["dataProviders/live"]}
        with self.assertRaises(cutover.GateError):
            cutover.profile_providers(cloud, ["operator"], profile, "config.textproto")

    def test_missing_or_ambiguous_inspection_result_fails_closed(self):
        cloud = FakeCloud()
        for output in ("no structured result", inspection(dataProviders=[]) + inspection(dataProviders=[])):
            cloud.run_operator = lambda command, timeout, output=output: output
            with self.assertRaises(cutover.GateError):
                cutover.inspect_config(cloud, ["operator"], vid_labeling_config="config.textproto")

    def test_legacy_scaled_to_zero_needs_no_container_metrics(self):
        cloud = FakeCloud()
        clock = [0.0]
        base = dt.datetime.now(dt.timezone.utc)
        class SimulatedDateTime(dt.datetime):
            @classmethod
            def now(cls, tz=None):
                return base + dt.timedelta(seconds=clock[0])
        def metrics(collection, query):
            self.assertIn("num_undelivered_messages", query["filter"])
            now = SimulatedDateTime.now()
            return [{"points": [{"interval": {"endTime": (now - dt.timedelta(seconds=offset)).isoformat()},
                                "value": {"int64Value": "0"}} for offset in (30, 90)]}]
        cloud.monitoring = metrics
        revision = {"service": "data-watcher", "revision": "old", "timeoutSeconds": 600, "sourceSubscription": "legacy"}
        with mock.patch.object(cutover.dt, "datetime", SimulatedDateTime), mock.patch.object(cutover.time, "monotonic", side_effect=lambda: clock[0]), mock.patch.object(cutover.time, "sleep", side_effect=lambda seconds: clock.__setitem__(0, clock[0] + seconds)):
            cutover.observe_legacy(cloud, [revision], base)
        self.assertGreaterEqual(clock[0], 600)
        self.assertFalse(any("pause" in command or "update" in command for command in cloud.calls))

    def test_missing_lifetime_and_renewed_legacy_traffic_fail_closed(self):
        cloud = FakeCloud()
        revision = {"service": "data-watcher", "revision": "old", "sourceSubscription": "legacy"}
        with self.assertRaises(cutover.GateError):
            cutover.observe_legacy(cloud, [revision], dt.datetime.now(dt.timezone.utc))
        revision["timeoutSeconds"] = 600
        calls = [0]
        original = cloud.gcloud
        def renewed(*args):
            result = original(*args)
            calls[0] += 1
            if calls[0] > 1:
                result["status"]["traffic"] = [{"revisionName": "old", "percent": 100}]
            return result
        cloud.gcloud = renewed
        with self.assertRaises(cutover.GateError):
            cutover.observe_legacy(cloud, [revision], dt.datetime.now(dt.timezone.utc))

    def test_capture_preserves_declared_lifetime_and_source_subscription(self):
        revisions = cutover.capture_legacy(FakeCloud())
        self.assertEqual(len(revisions), 2)
        self.assertEqual(revisions[0]["timeoutSeconds"], 600)
        self.assertEqual(revisions[0]["sourceSubscription"], "legacy")

    def test_v2_key_alone_is_not_evidence_of_queued_dispatch(self):
        cloud = FakeCloud()
        function = cloud.function("data-watcher")
        function["serviceConfig"]["environmentVariables"] = {
            "CONFIG_BLOB_KEY": cutover.CONFIG_V2,
            "EDPA_CONFIG_STORAGE_BUCKET": "gs://config-bucket",
            "DATA_WATCHER_CONFIG_GENERATION": "5",
        }
        cloud.function = lambda _: function
        cloud.run = lambda command: "active pinned configuration"
        inspected = []
        def operator(facts):
            def run(command, timeout):
                path = Path(command[-1].split("=", 1)[1])
                inspected.append((command[:2], command[-1].split("=", 1)[0], path.read_text()))
                return inspection(**facts)
            return run
        cloud.run_operator = operator({"hasDispatchRoutes": True, "anyDispatchRouteQueued": False,
                                       "allDispatchRoutesQueued": False})
        self.assertFalse(cutover.is_queued(cloud, ["operator"]))
        cloud.run_operator = operator({"hasDispatchRoutes": True, "anyDispatchRouteQueued": True,
                                       "allDispatchRoutesQueued": True})
        self.assertTrue(cutover.is_queued(cloud, ["operator"]))
        self.assertEqual(inspected[-1], (["operator", "inspect-dispatch-cutover-config"],
                                         "--data-watcher-config", "active pinned configuration"))

    def test_snapshot_main_uses_workflow_arguments_without_api_or_profile(self):
        cloud = FakeCloud()
        cloud.function = lambda _: {"serviceConfig": {"environmentVariables": {
            "CONFIG_BLOB_KEY": cutover.CONFIG_V2, "DATA_WATCHER_CONFIG_GENERATION": "5",
            "EDPA_CONFIG_STORAGE_BUCKET": "gs://config"}}}
        cloud.run = lambda _: "committed canary configuration"
        output = io.StringIO()
        with mock.patch.object(cutover, "Cloud", return_value=cloud), mock.patch("sys.stdout", output):
            cutover.main(["snapshot-config", "--project=test-project", "--region=us-central1"])
        self.assertEqual(output.getvalue(), "committed canary configuration")

    def test_canary_main_uses_workflow_arguments_without_api_targets(self):
        cloud = FakeCloud()
        called = []
        def operator(command, timeout):
            if "inspect-dispatch-cutover-config" in command:
                return inspection(dataProviders=["dataProviders/live", "dataProviders/test"])
            called.append(command)
            return ""
        cloud.run_operator = operator
        with tempfile.TemporaryDirectory() as directory:
            profile = Path(directory) / "profile.json"
            config = Path(directory) / "vid.textproto"
            profile.write_text(json.dumps({"isolatedDataProvider": "dataProviders/test",
                "liveDataProviders": ["dataProviders/live"]}))
            config.write_text("")
            with mock.patch.object(cutover, "Cloud", return_value=cloud):
                cutover.main(["canary-config", "--project=test-project", "--region=us-central1",
                    "--profile-file=" + str(profile), "--vid-labeling-config=" + str(config),
                    "--data-watcher-config=" + str(config), "--output=" + str(Path(directory) / "out")])
        self.assertEqual(len(called), 1)
        self.assertIn("prepare-dispatch-canary-config", called[0])

    def test_canary_is_not_full_activation_and_malformed_config_fails(self):
        cloud = FakeCloud()
        cloud.function = lambda _: {"serviceConfig": {"environmentVariables": {
            "CONFIG_BLOB_KEY": cutover.CONFIG_V2, "DATA_WATCHER_CONFIG_GENERATION": "5",
            "EDPA_CONFIG_STORAGE_BUCKET": "gs://config"}}}
        cloud.run = lambda _: "canary configuration"
        cloud.run_operator = lambda command, timeout: inspection(
            hasDispatchRoutes=True, anyDispatchRouteQueued=True, allDispatchRoutesQueued=False)
        self.assertFalse(cutover.is_queued(cloud, ["operator"]))
        cloud.run_operator = lambda command, timeout: inspection(
            hasDispatchRoutes=False, anyDispatchRouteQueued=False, allDispatchRoutesQueued=False)
        with self.assertRaises(cutover.GateError):
            cutover.is_queued(cloud, ["operator"])
        def malformed(command, timeout):
            raise cutover.GateError("Operator command failed; unparseable configuration")
        cloud.run_operator = malformed
        with self.assertRaises(cutover.GateError):
            cutover.is_queued(cloud, ["operator"])

    def test_declares_queue_main_reports_parsed_committed_routing(self):
        for facts, expected in (({"anyDispatchRouteQueued": True}, "true"), ({"anyDispatchRouteQueued": False}, "false")):
            cloud = FakeCloud()
            commands = []
            cloud.run_operator = lambda command, timeout, facts=facts: commands.append(command) or inspection(**facts)
            output = io.StringIO()
            with mock.patch.object(cutover, "Cloud", return_value=cloud), mock.patch("sys.stdout", output):
                cutover.main(["declares-queue", "--project=test-project", "--region=us-central1",
                              "--data-watcher-config=/committed/data-watcher-config.textproto"])
            self.assertEqual(output.getvalue().strip(), expected)
            self.assertEqual(commands, [cutover.OPERATOR + ["inspect-dispatch-cutover-config",
                "--data-watcher-config=/committed/data-watcher-config.textproto"]])

    def test_storage_smoke_finalizes_generations_and_waits_for_watcher_intake(self):
        cloud = FakeCloud()
        events = []
        objects = {"gs://test/raw/recovery/done": {"generation": "7", "timeCreated": "2026-11-23T12:00:00Z", "metadata": {}}}
        def process(command):
            if command[:3] == ["gcloud", "storage", "cp"]:
                destination = command[4]
                if destination.startswith("gs://"):
                    expected = next(v.split("=", 1)[1] for v in command if v.startswith("--if-generation-match="))
                    self.assertEqual(expected, objects.get(destination, {}).get("generation", "0"))
                    metadata_flag = next(v for v in command if v.startswith("--custom-metadata="))
                    metadata = dict(part.split("=", 1) for part in metadata_flag.split("=", 1)[1].split(","))
                    self.assertIn("vid-dispatch-smoke-run", metadata)
                    objects[destination] = {"generation": str(int(expected) + 1), "timeCreated": "2026-11-23T12:01:00Z", "metadata": metadata}
                    events.append("finalize")
                else:
                    Path(destination).write_bytes(b"")
                return ""
            raise AssertionError(command)
        def describe(*command):
            if command[:3] == ("storage", "objects", "describe"):
                return objects[command[3].split("#", 1)[0]].copy()
            if command[:2] == ("logging", "read"):
                return [{"resource": {"labels": {"service_name": "vid-labeling-dispatcher"}},
                         "textPayload": "xmm.outcome=succeeded xmm.work_item.name=workItems/dw-" + phase}
                        for phase in ("ordinary", "recovery")] + [
                        {"resource": {"labels": {"service_name": "data-watcher"}},
                         "textPayload": "Submitted work item to control plane queue: config=vid-labeling-dispatch, pathHash=hash, queue=vid-labeling-dispatch-queue, workItemId=dw-" + phase}
                        for phase in ("ordinary", "recovery")]
            raise AssertionError(command)
        def operator(command, timeout):
            if "inspect-dispatch-cutover-config" in command:
                return inspection(dataProviders=["dataProviders/live", "dataProviders/test"])
            phase = next(v.split("=", 1)[1] for v in command if v.startswith("--phase="))
            events.append(phase)
            if phase == "ordinary":
                objects["gs://test/raw/recovery/done"] = {"generation": "8", "timeCreated": "2026-11-23T12:01:00Z",
                    "metadata": {"recovery-source-upload": "dataProviders/test/rawImpressionUploads/source",
                                 "upload-healing-operation": "dataProviders/test/uploadHealingOperations/op"}}
                events.append("controller-finalize")
            return "SMOKE_WORK_ITEM=workItems/dw-" + phase + chr(10) + ("RECOVERY_READY=dataProviders/test/uploadHealingOperations/op" + chr(10) + "RECOVERY_SOURCE_GENERATION=8" if phase == "ordinary" else "")
        cloud.run, cloud.gcloud, cloud.run_operator = process, describe, operator
        profile = {"isolatedDataProvider": "dataProviders/test", "liveDataProviders": ["dataProviders/live"],
                   "rawImpressionsPrefix": "gs://test/raw/", "watcherIdentifier": "vid-labeling-dispatch"}
        run = {"ordinarySourceUri": "gs://test/raw/ordinary/done", "recoverySourceUri": "gs://test/raw/recovery/done",
               "recoverySourceGeneration": "7",
               "recoverySourceUpload": "dataProviders/test/rawImpressionUploads/source",
               "approval": {"name": "dataProviders/test/uploadHealingOperations/op", "etag": "fresh"}}
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "config.textproto"
            config.write_text("")
            args = type("Args", (), {"vid_labeling_config": str(config), "operator_command": ["operator"], "smoke_inputs": json.dumps(run)})()
            cutover.smoke(cloud, args, profile, [], directory)
        self.assertEqual(events, ["finalize", "ordinary", "controller-finalize", "recovery"])
        self.assertEqual(objects[run["ordinarySourceUri"]]["generation"], "1")
        self.assertEqual(objects[run["recoverySourceUri"]]["generation"], "8")
        self.assertEqual(objects[run["recoverySourceUri"]]["metadata"]["recovery-source-upload"], run["recoverySourceUpload"])
        self.assertEqual(objects[run["recoverySourceUri"]]["metadata"]["upload-healing-operation"], run["approval"]["name"])

    def test_changed_historical_recovery_generation_is_not_rewritten(self):
        cloud = FakeCloud()
        cloud.gcloud = lambda *_: {"generation": "8", "timeCreated": "2026-11-23T12:00:00Z"}
        writes = []
        cloud.run = lambda command: writes.append(command)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(cutover.GateError):
                cutover.controller_recovery_source(cloud, {"recoverySourceGeneration": "7"}, "7")
        self.assertEqual(writes, [])

    def test_controller_recovery_requires_recorded_generation_and_authorization_metadata(self):
        cloud = FakeCloud()
        cloud.gcloud = lambda *_: {"generation": "8", "timeCreated": "2026-11-23T12:00:00Z", "metadata": {}}
        run = {"recoverySourceGeneration": "7", "recoverySourceUri": "gs://test/raw/recovery/done",
               "recoverySourceUpload": "dataProviders/test/rawImpressionUploads/source",
               "approval": {"name": "dataProviders/test/uploadHealingOperations/op"}}
        with self.assertRaises(cutover.GateError):
            cutover.controller_recovery_source(cloud, run, "8")

    def test_controller_direct_replay_cannot_replace_actual_watcher_function_evidence(self):
        cloud = FakeCloud()
        names = ["workItems/dw-ordinary", "workItems/dw-recovery"]
        entries = [{"resource": {"labels": {"service_name": "vid-labeling-dispatcher"}},
                    "textPayload": "xmm.outcome=succeeded xmm.work_item.name=" + name}
                   for name in names] + [
                   {"resource": {"labels": {"service_name": "vid-labeling-healing-controller"}},
                    "textPayload": "Submitted work item to control plane queue: config=vid-labeling-dispatch, queue=vid-labeling-dispatch-queue, workItemId=" + name.split("/")[-1]}
                   for name in names]
        cloud.gcloud = lambda *_: entries
        with mock.patch.object(cutover.time, "monotonic", side_effect=[0, 301]):
            with self.assertRaises(cutover.GateError):
                cutover.verify_smoke_outcomes(cloud, {"watcherIdentifier": "vid-labeling-dispatch"}, names)

    def test_replaced_finalized_marker_cannot_supply_smoke_identity(self):
        cloud = FakeCloud()
        cloud.run = lambda _: ""
        cloud.gcloud = lambda *_: {"generation": "11", "timeCreated": "2026-11-23T12:00:00Z", "metadata": {}}
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(cutover.GateError):
                cutover.finalize_marker(cloud, "gs://test/raw/ordinary/done", directory)

    def test_run_configuration_rejects_live_sources_and_embedded_static_workitems(self):
        profile = {"isolatedDataProvider": "dataProviders/test", "rawImpressionsPrefix": "gs://test/raw/"}
        run = {"ordinarySourceUri": "gs://live/raw/done", "recoverySourceUri": "gs://test/raw/recovery/done",
               "recoverySourceGeneration": "7",
               "recoverySourceUpload": "dataProviders/test/rawImpressionUploads/source",
               "approval": {"name": "dataProviders/test/uploadHealingOperations/op", "etag": "fresh"}}
        with self.assertRaises(cutover.GateError):
            cutover.smoke_inputs(profile, json.dumps(run))
        run["ordinarySourceUri"] = "gs://test/raw/ordinary/done"
        run["ordinaryWorkItem"] = {}
        with self.assertRaises(cutover.GateError):
            cutover.smoke_inputs(profile, json.dumps(run))

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
