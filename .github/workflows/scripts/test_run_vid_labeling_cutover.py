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

    @staticmethod
    def identity(tree, labels):
        import hashlib
        from run_vid_labeling_cutover import shared_platform_inputs
        return hashlib.sha256("\n".join(shared_platform_inputs(tree, labels)).encode()).hexdigest()

    @staticmethod
    def tree(entries):
        return "\n".join("100644 blob " + blob * 40 + "\t" + path for path, blob in entries)

    def test_unrelated_repository_changes_do_not_change_shared_identity(self):
        shared = "src/main/kotlin/org/wfanet/measurement/securecomputation/service/internal/Services.kt"
        labels = ["//src/main/kotlin/org/wfanet/measurement/securecomputation/service/internal:Services.kt",
                  "@maven//:com_google_guava_guava"]
        baseline = [(shared, "a"), ("MODULE.bazel.lock", "a")]
        for path in ["docs/edpaggregator/deployment-guide.md",
                     "src/test/kotlin/org/wfanet/measurement/kingdom/AppTest.kt",
                     "src/main/kotlin/org/wfanet/measurement/reporting/service/api/v2alpha/Reports.kt",
                     "src/main/k8s/testing/secretfiles/fixture.pem"]:
            with self.subTest(path=path):
                self.assertEqual(self.identity(self.tree(baseline), labels),
                                 self.identity(self.tree(baseline + [(path, "b")]), labels))

    def test_deployed_shared_source_build_and_lock_changes_change_identity(self):
        labels = ["//src/main/kotlin/org/wfanet/measurement/securecomputation/service/internal:Services.kt",
                  "//src/main/proto/wfa/measurement/securecomputation/controlplane/v1alpha:work_item.proto",
                  "//build:defs.bzl", "//src/main/kotlin/org/wfanet/measurement/common:BUILD.bazel"]
        paths = ["src/main/kotlin/org/wfanet/measurement/securecomputation/service/internal/Services.kt",
                 "src/main/proto/wfa/measurement/securecomputation/controlplane/v1alpha/work_item.proto",
                 "build/defs.bzl", "src/main/kotlin/org/wfanet/measurement/common/BUILD.bazel",
                 "MODULE.bazel", "MODULE.bazel.lock", "maven_install.json"]
        for path in paths:
            with self.subTest(path=path):
                self.assertNotEqual(self.identity(self.tree([(path, "a")]), labels),
                                    self.identity(self.tree([(path, "b")]), labels))
                self.assertNotEqual(self.identity(self.tree([]), labels),
                                    self.identity(self.tree([(path, "a")]), labels))

    def test_edpa_inputs_are_left_to_the_contract_identity(self):
        from run_vid_labeling_cutover import shared_platform_inputs
        paths = ["src/main/kotlin/org/wfanet/measurement/edpaggregator/vidlabeler/VidLabelingSink.kt",
                 "src/main/proto/wfa/measurement/edpaggregator/v1alpha/vid_labeling_job.proto",
                 "src/main/proto/wfa/measurement/internal/edpaggregator/vid_labeling_job.proto",
                 "src/main/resources/edpaggregator/spanner/changelog.yaml"]
        labels = ["//" + path.rsplit("/", 1)[0] + ":" + path.rsplit("/", 1)[1] for path in paths]
        for path in paths:
            with self.subTest(path=path):
                self.assertEqual(shared_platform_inputs(self.tree([(path, "a")]), labels), [])

    def test_shared_inputs_come_from_the_deployed_target_query(self):
        from run_vid_labeling_cutover import deployed_input_labels, DEPLOYED_TARGETS
        calls = []
        class Result:
            stdout = "//a:b.kt\n@x//:y\n"
        def execute(argv, **kwargs):
            calls.append(argv)
            return Result()
        self.assertEqual(deployed_input_labels(execute), ["//a:b.kt", "@x//:y"])
        self.assertEqual(calls[0][:2], ["bazelisk", "query"])
        for target in DEPLOYED_TARGETS:
            self.assertIn(target, calls[0][2])
        self.assertIn("buildfiles(deps(", calls[0][2])

    def reserved(self, mode, previous):
        from run_vid_labeling_cutover import claim_receipt
        state = {}
        def write(value, expected):
            state.update(value=value, generation=expected + 1)
            return expected + 1
        owner, generation = claim_receipt(mode, self.manifest, previous, 10 if previous else 0,
                                          write, ready=lambda *_: None)
        return owner, generation, state["value"]

    def test_abort_after_crash_releases_intake_and_restores_preceding_receipt(self):
        from run_vid_labeling_cutover import abort, claim_receipt
        previous = dict(self.manifest, status="READY", scope_id=scope_id(self.manifest))
        # Crash immediately after reservation, and crash after the publisher pause.
        for paused in (False, True):
            with self.subTest(paused=paused):
                owner, generation, receipt = self.reserved("maintenance", previous)
                publisher = {"paused_by": owner if paused else None}
                def release():
                    publisher["paused_by"] = owner  # Idempotent replay of the owner's pause.
                    publisher["paused_by"] = None   # Owner-matched resume.
                restored = []
                abort(receipt, generation, owner, release,
                      lambda value, expected: restored.append((value, expected)),
                      lambda expected: self.fail("first-receipt removal"))
                self.assertIsNone(publisher["paused_by"])
                self.assertEqual(restored, [(previous, generation)])
                # The restored READY receipt admits the next begin.
                claim_receipt("maintenance", self.manifest, restored[0][0], generation + 1,
                              lambda value, expected: expected + 1)

    def test_abort_removes_a_first_receipt_and_skips_intake_for_online(self):
        from run_vid_labeling_cutover import abort
        owner, generation, receipt = self.reserved("maintenance", None)
        removed = []
        abort(receipt, generation, owner, lambda: None,
              lambda *_: self.fail("restore"), removed.append)
        self.assertEqual(removed, [generation])
        previous = dict(self.manifest, status="READY", scope_id=scope_id(self.manifest))
        owner, generation, receipt = self.reserved("online", previous)
        abort(receipt, generation, owner, lambda: self.fail("online never paused"),
              lambda *_: None, lambda _: self.fail("remove"))

    def test_abort_rejects_foreign_owner_or_later_state(self):
        from run_vid_labeling_cutover import abort
        previous = dict(self.manifest, status="READY", scope_id=scope_id(self.manifest))
        owner, generation, receipt = self.reserved("maintenance", previous)
        cases = [(receipt, "other")] + [(dict(receipt, status=status), owner)
                                        for status in ("IN_PROGRESS", "VERIFIED", "READY")]
        for value, requester in cases:
            with self.subTest(status=value["status"], owner=requester), self.assertRaises(RuntimeError):
                abort(value, generation, requester, lambda: self.fail("foreign release"),
                      lambda *_: self.fail("foreign restore"), lambda _: self.fail("foreign remove"))

    def test_failed_begin_releases_ownership_then_reraises(self):
        from run_vid_labeling_cutover import guarded_begin
        events = []
        def failing_preflight():
            events.append("pause")
            raise RuntimeError("preflight failed")
        with self.assertRaises(RuntimeError):
            guarded_begin(failing_preflight, lambda: events.append("release"))
        self.assertEqual(events, ["pause", "release"])
        self.assertEqual(guarded_begin(lambda: "journal", lambda: self.fail("release")), "journal")

    def test_conditional_delete_targets_the_exact_generation(self):
        from run_vid_labeling_cutover import delete_object
        requests = []
        class Result:
            stdout = "token"
        class Response:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
        def opener(request, timeout):
            requests.append(request)
            return Response()
        delete_object("gs://b/rollout/env/vid-cutover.json", 11, lambda *a, **k: Result(), opener)
        self.assertEqual(requests[0].get_method(), "DELETE")
        self.assertIn("ifGenerationMatch=11", requests[0].full_url)
        self.assertIn("/o/rollout%2Fenv%2Fvid-cutover.json?", requests[0].full_url)
        with self.assertRaises(RuntimeError):
            delete_object("gs://b/rollout/env/vid-cutover.json", 0, lambda *a, **k: Result(), opener)

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
        identity = text[text.index("vid_labeling_contract_sources"):text.index("vid_labeling_rollout_roles")]
        self.assertIn("filesha256(var.vid_labeling_monitor_config.local_path)",identity)
        self.assertIn("filesha256(var.data_availability_monitor_config.local_path)",identity)

    def test_storage_sources_include_their_encryption_and_container_libraries(self):
        import re
        from pathlib import Path
        root = Path(__file__).resolve().parents[3]
        text = (root/"src/main/terraform/gcloud/modules/edp-aggregator/main.tf").read_text()
        identity = text[text.index("vid_labeling_contract_sources"):text.index("vid_labeling_rollout_roles")]
        listed = set(re.findall(r'"(src/main/kotlin/[^"]+\.kt)"', identity))
        edpa = root/"src/main/kotlin/org/wfanet/measurement/edpaggregator"
        # Every in-repo Encrypted* library used by a listed storage source shares its format.
        for source in sorted(listed):
            for name in set(re.findall(r"\b(Encrypted[A-Za-z]+)\b", (root/source).read_text())):
                for match in edpa.glob("**/" + name + ".kt"):
                    with self.subTest(source=source, dependency=name):
                        self.assertIn(str(match.relative_to(root)), listed)

    def test_contract_identity_hashes_wire_schema_and_storage_sources(self):
        from pathlib import Path
        root = Path(__file__).resolve().parents[3]
        text = (root/"src/main/terraform/gcloud/modules/edp-aggregator/main.tf").read_text()
        identity = text[text.index("vid_labeling_contract_sources"):text.index("vid_labeling_rollout_roles")]
        for pattern in ['"src/main/proto/wfa/measurement/edpaggregator/**/*.proto"',
                        '"src/main/proto/wfa/measurement/internal/edpaggregator/**/*.proto"',
                        '"src/main/resources/edpaggregator/spanner/**"']:
            self.assertIn(pattern, identity)
        for source in ["EncryptedStorage.kt", "EncryptedRecordIoStore.kt", "RankIndexStore.kt",
                       "SubpoolFingerprintsStore.kt", "LabeledImpressionsBlobKey.kt",
                       "VidLabelingSink.kt"]:
            path = next(root.glob("src/main/kotlin/org/wfanet/measurement/edpaggregator/**/" + source))
            self.assertIn(str(path.relative_to(root)), identity)
        self.assertNotIn("protocol", identity.split("vid_labeling_contract_id", 1)[1])

    def test_contract_identity_hashes_mixed_version_coordination_sources(self):
        import re
        from pathlib import Path
        root = Path(__file__).resolve().parents[3]
        text = (root/"src/main/terraform/gcloud/modules/edp-aggregator/main.tf").read_text()
        identity = text[text.index("vid_labeling_contract_sources"):text.index("vid_labeling_rollout_roles")]
        # A commented-out entry is not hashed, so strip HCL comments first.
        code = "\n".join(re.sub(r"(^|\s)(#|//).*$", "", line) for line in identity.splitlines())
        listed = set(re.findall(r'"(src/main/kotlin/[^"]+\.kt)"', code))
        edpa = "src/main/kotlin/org/wfanet/measurement/edpaggregator/"
        # Old and new workers must agree on each: partial-publication behavior,
        # Phase-2 file membership and job creation, dedup identities, and the
        # fingerprint/rank-index byte formats.
        for source in ["rawimpressions/CompleteGcsStorageClient.kt",
                       "rawimpressions/RawImpressionFileBinPacker.kt",
                       "vidlabeling/MemoizedVidLabelingJobDispatcher.kt",
                       "vidlabeling/VidLabelingDispatchSequencer.kt",
                       "vidlabeling/WorkItemIds.kt",
                       "vidlabeling/RequestIds.kt",
                       "rawimpressions/EventIdDigest.kt",
                       "rawimpressions/EventIdDigestExtractor.kt",
                       "vidrankbuilder/EventIdDigestBytes.kt",
                       "vidrankbuilder/LastSeenDayBytes.kt"]:
            with self.subTest(source=source):
                self.assertTrue((root/(edpa + source)).is_file())
                self.assertIn(edpa + source, listed)
        # Every listed source exists, so a rename cannot silently drop it.
        for source in sorted(listed):
            with self.subTest(listed=source):
                self.assertTrue((root/source).is_file())


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

    def test_reader_gate_function_set_equals_its_dependencies(self):
        import re
        root = __import__("pathlib").Path(__file__).resolve().parents[3]
        source = (root / "src/main/terraform/gcloud/modules/edp-aggregator/main.tf").read_text()
        roles_block = source[source.index("vid_labeling_rollout_roles = ["):]
        roles = set(re.findall(r'"([a-z_]+)"', roles_block[:roles_block.index("]")]))
        gate = source[source.index('resource "terraform_data" "vid_labeling_reader_readiness"'):]
        depends = gate[gate.index("depends_on = ["):]
        modules = set(re.findall(r"module\.([a-z_]+)_cloud_function", depends[:depends.index("]")]))
        self.assertEqual(roles, modules)
        self.assertEqual(roles, set(REQUIRED_FUNCTIONS))
        functions = source[source.index("vid_labeling_rollout_functions = ["):]
        self.assertIn("for role in local.vid_labeling_rollout_roles", functions[:200])

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
