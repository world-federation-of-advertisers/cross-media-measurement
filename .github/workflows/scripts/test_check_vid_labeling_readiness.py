# Copyright 2026 The Cross-Media Measurement Authors
# Licensed under the Apache License, Version 2.0 (the "License").
import unittest
from check_vid_labeling_readiness import check, DEPLOYMENTS


class ReadinessTest(unittest.TestCase):
    def setUp(self):
        self.commands = []
        self.stale = False
        self.bad_image = False
        self.failed_schema = False
        self.missing_flag = False
        self.api_error = False

    def read(self, command):
        self.commands.append(command)
        if self.api_error:
            raise RuntimeError("query failed")
        if command[:3] == ["kubectl", "get", "deployments"]:
            return {"items": [{"metadata": {"name": name, "generation": 2},
                "spec": {"replicas": 1, "template": {"spec": {"containers": [{"image": "image:tag"}], "initContainers": [{"name": "update-edp-aggregator-schema", "image": "schema:tag"}]}}},
                "status": {"observedGeneration": 1 if self.stale else 2,
                           "updatedReplicas": 1, "availableReplicas": 1}}
                for name in DEPLOYMENTS]}
        if command[:3] == ["kubectl", "get", "pods"]:
            return {"items": [{"metadata": {"name": name + "-pod"},
                "spec": {"containers": [{"image": "image:tag"}]},
                "status": {"containerStatuses": [{"ready": True}],
                           "initContainerStatuses": [{"name": "update-edp-aggregator-schema",
                             "state": {"terminated": {"exitCode": 1 if self.failed_schema else 0}}}]}}
                for name in DEPLOYMENTS]}
        if "instance-templates" in command:
            return {"properties": {"metadata": {"items": [
                {"key": "tee-image-reference", "value": "image:old" if self.bad_image else "image:tag"},
                {"key": "tee-env-WORK_ITEM_CONSUMPTION_ENABLED", "value": "true"},
                {"key": "tee-cmd", "value": "[]" if self.missing_flag else '["--vid-labeler-queue", "queue"]'}]}}}
        if "list-instances" in command:
            return []
        return {"status": {"isStable": True, "versionTarget": {"isReached": True}},
                "instanceTemplate": "projects/p/global/instanceTemplates/current"}

    def test_ready_scale_to_zero_templates_and_schema(self):
        check("p", "r", "tag", self.read)
        self.assertTrue(self.commands)
        self.assertFalse(any(word in command for command in self.commands
                             for word in ["apply", "patch", "update", "resize", "pause", "resume"]))

    def test_incomplete_or_unknown_readiness_fails(self):
        for field in ["stale", "bad_image", "failed_schema", "missing_flag", "api_error"]:
            with self.subTest(field=field):
                self.setUp()
                setattr(self, field, True)
                with self.assertRaises(RuntimeError):
                    check("p", "r", "tag", self.read)


if __name__ == "__main__":
    unittest.main()
