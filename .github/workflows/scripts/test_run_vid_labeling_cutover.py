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

    def test_receipt_writes_are_generation_guarded(self):
        calls = []
        class Result:
            stdout = "11"
        def execute(command, **kwargs):
            calls.append(command)
            return Result()
        put_object("gs://b/rollout/environment.json", {"status": "IN_PROGRESS"}, 10, execute)
        self.assertIn("--if-generation-match=10", calls[0])
        self.assertTrue(all("delete" not in command for command in calls))
        with self.assertRaises(RuntimeError):
            put_object("gs://b/production/environment.json", {}, 10, execute)



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
