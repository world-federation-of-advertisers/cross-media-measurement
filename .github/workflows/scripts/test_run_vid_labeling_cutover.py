# Copyright 2026 The Cross-Media Measurement Authors
# Licensed under the Apache License, Version 2.0.
import copy
import json
import unittest
from run_vid_labeling_cutover import begin, complete, scope_id, control, put_object
from check_vid_labeling_readiness import REQUIRED_FUNCTIONS, MIGS


class CutoverTest(unittest.TestCase):
    def setUp(self):
        self.manifest = {"schema_version": 1, "contract_id": "v2", "project": "p", "region": "r",
                         "providers": ["dataProviders/dp"], "queues": ["queues/phase0", "queues/phase1", "queues/phase2", "queues/availability"], "migs": list(MIGS),
                         "functions": [{"role": role, "name": role, "artifact_sha256": "a"*64}
                                       for role in sorted(REQUIRED_FUNCTIONS)]}
        self.events = []
        self.good_smoke = {"status": "PASS", "competing_writers": 2, "sidecar_winners": 1,
                           "decrypted_records": 1, "cleanup_complete": True}

    def preflight(self):
        self.events.append("preflight")
        return {"status": "PASS", "mode": "MAINTENANCE", "providers": self.manifest["providers"],
                "queues": self.manifest["queues"]}

    def maintenance(self):
        return begin("maintenance", self.manifest, None,
                     lambda: self.events.append("pause"), self.preflight)

    def test_maintenance_order_and_success_only_resume(self):
        journal = self.maintenance()
        result = complete(self.manifest, journal, "tag",
                          lambda: self.events.append("ready"),
                          lambda: self.events.append("smoke") or self.good_smoke,
                          lambda: self.events.append("resume"),
                          lambda _: self.events.append("checkpoint"))
        self.assertEqual(self.events, ["pause", "preflight", "ready", "smoke", "checkpoint", "resume"])
        self.assertEqual(result["status"], "READY")

    def test_online_never_pauses_or_runs_a_manual_drain(self):
        receipt = dict(copy.deepcopy(self.manifest), status="READY", scope_id=scope_id(self.manifest))
        journal = begin("online", self.manifest, receipt,
                        lambda: self.fail("online pause"), lambda: self.fail("online drain"),
                        lambda *args: self.events.append("compatible"))
        result = complete(self.manifest, journal, "tag", lambda: None,
                          lambda: self.good_smoke, lambda: self.fail("online resume"))
        self.assertEqual(result["status"], "READY")
        self.assertEqual(self.events, ["compatible"])

    def test_missing_or_foreign_previous_receipt_is_not_online(self):
        for receipt in [None, {}, {"scope_id": "different"}]:
            with self.subTest(receipt=receipt), self.assertRaises(RuntimeError):
                begin("online", self.manifest, receipt, lambda: None, self.preflight)

    def test_basic_drain_or_incomplete_scope_is_not_complete_preflight(self):
        for value in [{"status": "PASS", "mode": "DRAINED"},
                      {"status": "PASS", "mode": "MAINTENANCE", "providers": [], "queues": []}]:
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                begin("maintenance", self.manifest, None, lambda: None, lambda: value)

    def test_each_smoke_predicate_blocks_resume_independently(self):
        journal = self.maintenance()
        for field, value in [("status", "FAIL"), ("competing_writers", 1),
                             ("sidecar_winners", 2), ("decrypted_records", 0),
                             ("cleanup_complete", False)]:
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                complete(self.manifest, journal, "tag", lambda: None,
                         lambda: dict(self.good_smoke, **{field: value}),
                         lambda: self.fail("unsafe resume"))

    def test_readiness_or_checkpoint_failure_never_resumes(self):
        journal = self.maintenance()
        def failure(*_):
            raise RuntimeError("failure")
        with self.assertRaises(RuntimeError):
            complete(self.manifest, journal, "tag", failure, lambda: self.good_smoke,
                     lambda: self.fail("unsafe resume"))
        with self.assertRaises(RuntimeError):
            complete(self.manifest, journal, "tag", lambda: None, lambda: self.good_smoke,
                     lambda: self.fail("unsafe resume"), failure)

    def test_changed_contract_or_foreign_journal_never_runs_smoke(self):
        journal = self.maintenance()
        for update in [{"contract_id": "other"}, {"region": "other"}, {"status": "READY"}]:
            with self.subTest(update=update), self.assertRaises(RuntimeError):
                complete(self.manifest, dict(journal, **update), "tag",
                         lambda: self.fail("unchecked readiness"), lambda: self.good_smoke,
                         lambda: self.fail("unsafe resume"))

    def test_receipt_generation_comes_from_the_conditional_write(self):
        import io
        calls = []
        class Result:
            stdout = "access-token"
        def execute(command, **kwargs):
            calls.append(command)
            return Result()
        def opener(request, **kwargs):
            self.assertIn("ifGenerationMatch=10", request.full_url)
            return io.StringIO(json.dumps({"bucket": "b", "name": "rollout/environment.json", "generation": "11"}))
        generation = put_object("gs://b/rollout/environment.json", {"status": "RESERVED"}, 10, execute, opener)
        self.assertEqual(generation, 11)
        self.assertFalse(any("describe" in command for command in calls))
        with self.assertRaises(RuntimeError):
            put_object("gs://b/production/environment.json", {}, 10, execute, opener)

    def test_conditional_write_does_not_adopt_a_competing_latest_generation(self):
        import io
        class Result:
            stdout = "token"
        calls = []
        def execute(argv, **kwargs):
            calls.append(argv)
            return Result()
        def opener(request, **kwargs):
            # A subsequent owner may already be writing generation12; this response is still11.
            return io.StringIO(json.dumps({"bucket": "b", "name": "rollout/r.json", "generation": "11"}))
        self.assertEqual(put_object("gs://b/rollout/r.json", {}, 10, execute, opener), 11)
        self.assertEqual(len(calls), 1)

    def test_control_is_idempotent_and_owner_bound(self):
        import io
        requests = []
        class Result:
            stdout = "identity"
        def execute(*args, **kwargs): return Result()
        def opener(request, **kwargs):
            value = json.loads(request.data)
            requests.append(value)
            return io.StringIO(json.dumps({"scope_id": scope_id(self.manifest),
                "owner_id": value["owner_id"], "paused": value["action"] == "pause"}))
        control("https://publisher.example/control", "pause", self.manifest, "owner1", execute, opener)
        control("https://publisher.example/control", "pause", self.manifest, "owner1", execute, opener)
        control("https://publisher.example/control", "resume", self.manifest, "owner1", execute, opener)
        self.assertEqual(requests[0]["request_id"], requests[1]["request_id"])
        self.assertNotEqual(requests[0]["request_id"], requests[2]["request_id"])
        def foreign(request, **kwargs):
            return io.StringIO(json.dumps({"scope_id": scope_id(self.manifest),
                "owner_id": "owner2", "paused": False}))
        with self.assertRaises(RuntimeError):
            control("https://publisher.example/control", "resume", self.manifest, "owner1", execute, foreign)


    def test_competing_begin_cannot_pause_or_replace_reserved_owner(self):
        from run_vid_labeling_cutover import claim_receipt
        previous = dict(self.manifest, status="READY", scope_id=scope_id(self.manifest))
        state = {"generation": 10, "value": previous}
        def write(value, expected):
            if expected != state["generation"]:
                raise RuntimeError("generation precondition failed")
            state.update(generation=expected + 1, value=value)
            return state["generation"]
        owner, generation = claim_receipt("maintenance", self.manifest, previous, 10, write)
        self.assertEqual(state["value"]["request_id"], owner)
        self.assertEqual(generation, 11)
        # The second begin read before reservation; its CAS fails before it can pause.
        with self.assertRaises(RuntimeError):
            claim_receipt("maintenance", self.manifest, previous, 10, write)
            self.fail("competing begin was allowed to pause")
        for status in ("RESERVED", "IN_PROGRESS", "VERIFIED"):
            with self.subTest(status=status), self.assertRaises(RuntimeError):
                claim_receipt("maintenance", self.manifest,
                    dict(state["value"], status=status), 11, write)
        self.assertEqual(state["value"]["request_id"], owner)
        self.assertEqual(state["generation"], 11)

    def test_losing_reservation_and_incompatible_online_do_not_pause(self):
        from run_vid_labeling_cutover import claim_receipt
        previous = dict(self.manifest, status="READY", scope_id=scope_id(self.manifest))
        events = []
        def fail_write(value, expected):
            events.append("claim")
            raise RuntimeError("412")
        with self.assertRaises(RuntimeError):
            claim_receipt("maintenance", self.manifest, previous, 10, fail_write)
            events.append("pause")
        self.assertEqual(events, ["claim"])
        def incompatible(*args):
            raise RuntimeError("incompatible")
        with self.assertRaises(RuntimeError):
            claim_receipt("online", self.manifest, previous, 10,
                          lambda *_: self.fail("incompatible reservation"), incompatible)

    def test_resume_rejects_changed_generation_or_owner(self):
        from run_vid_labeling_cutover import resume_owned
        journal = self.maintenance()
        for generation, owner, status in [(12, journal["request_id"], "VERIFIED"),
                                         (11, "other", "VERIFIED"),
                                         (11, journal["request_id"], "IN_PROGRESS")]:
            with self.subTest(generation=generation, owner=owner, status=status), self.assertRaises(RuntimeError):
                resume_owned(self.manifest, journal, 11, lambda: generation,
                    lambda _: dict(journal, request_id=owner, status=status),
                    lambda _: self.fail("foreign resume"))
        owners = []
        resume_owned(self.manifest, journal, 11, lambda: 11,
                     lambda _: dict(journal, status="VERIFIED"), owners.append)
        self.assertEqual(owners, [journal["request_id"]])

    def test_ambiguous_or_conflicting_write_is_not_retried_or_adopted(self):
        from urllib.error import HTTPError
        calls = []
        class Result:
            stdout = "token"
        def execute(argv, **kwargs):
            calls.append(argv)
            return Result()
        for failure in [TimeoutError("response lost"),
                        HTTPError("https://storage.googleapis.com", 412, "conflict", {}, None)]:
            def opener(*args, **kwargs):
                raise failure
            with self.subTest(failure=type(failure)), self.assertRaises(type(failure)):
                put_object("gs://b/rollout/r.json", {}, 10, execute, opener)
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(argv == ["gcloud", "auth", "print-access-token"] for argv in calls))

    def test_failed_resume_can_retry_the_same_verified_owner(self):
        journal = self.maintenance()
        saved = []
        def fail():
            raise RuntimeError("resume failed")
        with self.assertRaises(RuntimeError):
            complete(self.manifest, journal, "tag", lambda: None, lambda: self.good_smoke,
                     fail, lambda value: saved.append(value))
        self.assertEqual(saved[0]["status"], "VERIFIED")
        self.assertEqual(saved[0]["request_id"], journal["request_id"])
        result = complete(self.manifest, saved[0], "tag", lambda: None, lambda: self.good_smoke,
                          lambda: None)
        self.assertEqual(result["status"], "READY")
        self.assertEqual(result["request_id"], journal["request_id"])

    def test_missing_shared_schema_api_or_deployment_input_changes_identity(self):
        from run_vid_labeling_cutover import shared_platform_inputs
        paths = ["src/main/proto/wfa/measurement/internal/securecomputation/controlplane/work_item.proto",
                 "src/main/proto/wfa/measurement/internal/kingdom/job.proto",
                 "src/main/resources/securecomputation/spanner/schema.sql",
                 "src/main/k8s/dev/kingdom.cue", "src/main/terraform/gcloud/cmms/envs/dev.tfvars",
                 "src/main/kotlin/org/wfanet/measurement/securecomputation/service/internal/Services.kt",
                 "MODULE.bazel", ".github/workflows/configure-secure-computation-control-plane.yml"]
        for path in paths:
            with self.subTest(path=path):
                first = "100644 blob " + "a"*40 + "\t" + path
                second = "100644 blob " + "b"*40 + "\t" + path
                self.assertNotEqual(shared_platform_inputs(first), shared_platform_inputs(second))


    def test_shared_build_modification_addition_and_deletion_change_identity(self):
        import hashlib
        from run_vid_labeling_cutover import shared_platform_inputs
        def identity(tree):
            return hashlib.sha256("\n".join(shared_platform_inputs(tree)).encode()).hexdigest()
        baseline = "100644 blob " + "a"*40 + "\tsrc/main/kotlin/org/wfanet/measurement/kingdom/App.kt"
        paths = ["build/defs.bzl", "imports/java/com/google/BUILD.bazel",
                 "third_party/rbe_configs/cc/cc_toolchain_config.bzl", "BUILD.bazel",
                 ".bazelversion", "maven_install.json", "container.bazelrc",
                 "maven.bazelrc", "remote.bazelrc", "results.bazelrc",
                 "future/shared-build-input.toml"]
        for path in paths:
            with self.subTest(path=path):
                original = baseline + "\n100644 blob " + "b"*40 + "\t" + path
                changed = baseline + "\n100644 blob " + "c"*40 + "\t" + path
                self.assertNotEqual(identity(original), identity(changed))
                self.assertNotEqual(identity(baseline), identity(original))  # Addition.
                self.assertNotEqual(identity(original), identity(baseline))  # Deletion.

    def test_only_explicit_edpa_directories_are_excluded_from_shared_inputs(self):
        from run_vid_labeling_cutover import shared_platform_inputs
        for path in ["src/main/kotlin/org/wfanet/measurement/edpaggregator/App.kt",
                     "src/main/proto/wfa/measurement/edpaggregator/job.proto",
                     "src/main/proto/wfa/measurement/internal/edpaggregator/job.proto",
                     "src/main/resources/edpaggregator/spanner/schema.sql"]:
            with self.subTest(path=path):
                self.assertEqual(shared_platform_inputs("100644 blob "+"a"*40+"\t"+path), [])
        path = "src/main/kotlin/org/wfanet/measurement/edpaggregator_shared/App.kt"
        self.assertTrue(shared_platform_inputs("100644 blob "+"a"*40+"\t"+path))

    def test_manifest_output_round_trips_through_github_output(self):
        from pathlib import Path
        import subprocess
        root = Path(__file__).resolve().parents[3]
        text = (root/".github/workflows/terraform-cmms-v2.yml").read_text()
        line = next(line.strip() for line in text.splitlines() if "printf 'manifest=" in line)
        with __import__("tempfile").TemporaryDirectory() as folder:
            output = Path(folder)/"output"
            subprocess.run(["bash","-c",line],env={"manifest":json.dumps({"contract_id":"test"}),
                                                "GITHUB_OUTPUT":str(output)},check=True)
            self.assertEqual(output.read_text().splitlines(), ['manifest={"contract_id": "test"}'])
            self.assertEqual(json.loads(output.read_text().split("=",1)[1]),{"contract_id":"test"})

    def test_monitor_only_packing_change_is_in_contract_identity(self):
        from pathlib import Path
        root = Path(__file__).resolve().parents[3]
        text = (root/"src/main/terraform/gcloud/modules/edp-aggregator/main.tf").read_text()
        identity = text[text.index("vid_labeling_contract_id"):text.index("vid_labeling_rollout_functions")]
        self.assertIn("filesha256(var.vid_labeling_monitor_config.local_path)",identity)
        self.assertIn("filesha256(var.data_availability_monitor_config.local_path)",identity)


class WorkflowWiringTest(unittest.TestCase):
    def test_checked_boundary_precedes_every_component_change(self):
        import yaml
        root = __import__("pathlib").Path(__file__).resolve().parents[3]
        workflow = yaml.safe_load((root / ".github/workflows/update-cmms.yml").read_text())
        jobs = workflow["jobs"]
        self.assertIn("prepare-vid-labeling-cutover", jobs["pause-requisition-fetcher"]["needs"])
        for name in ["update-kingdom", "update-secure-computation", "update-edp-aggregator-service"]:
            self.assertIn("prepare-vid-labeling-cutover", jobs[name]["needs"])
            self.assertIn("needs.prepare-vid-labeling-cutover.result == 'success'", jobs[name]["if"])
        self.assertEqual(jobs["plan-vid-labeling-cutover"]["with"]["apply"], False)
        self.assertTrue(jobs["plan-vid-labeling-cutover"]["with"]["export-vid-rollout-manifest"])

    def test_final_smoke_uses_normal_apply_and_its_exact_boundary(self):
        import yaml
        root = __import__("pathlib").Path(__file__).resolve().parents[3]
        jobs = yaml.safe_load((root / ".github/workflows/update-cmms.yml").read_text())["jobs"]
        final = jobs["complete-vid-labeling-cutover"]
        self.assertEqual(set(final["needs"]), {"publish-images", "terraform", "prepare-vid-labeling-cutover"})
        step = final["steps"][-1]
        self.assertIn("--cutover-id=", step["run"])
        self.assertIn("--phase=complete", step["run"])
        self.assertNotIn("terraform apply", step["run"])
        self.assertIn("inputs.vid-labeling-cutover-mode", jobs["pause-requisition-fetcher"]["if"])

    def test_reader_readiness_is_a_terraform_prerequisite_for_writer_rollout(self):
        root = __import__("pathlib").Path(__file__).resolve().parents[3]
        source = (root / "src/main/terraform/gcloud/modules/edp-aggregator/main.tf").read_text()
        start = source.index('module "vid_labeling_tee_app"')
        module = source[start:source.index("\n}", start)]
        self.assertIn("terraform_data.vid_labeling_reader_readiness", module)
        gate = source[source.index('resource "terraform_data" "vid_labeling_reader_readiness"'):start]
        self.assertIn("--functions-only", gate)
        self.assertIn("module.vid_labeling_healing_controller_cloud_function", gate)

    def test_future_or_expired_boundary_never_resumes(self):
        helper = CutoverTest()
        helper.setUp()
        journal = helper.maintenance()
        for started in [0, __import__("time").time() + 120]:
            with self.subTest(started=started), self.assertRaises(RuntimeError):
                complete(helper.manifest, dict(journal, started_at=started), "tag",
                         lambda: self.fail("stale readiness"), lambda: helper.good_smoke,
                         lambda: self.fail("unsafe resume"))

if __name__ == "__main__":
    unittest.main()
