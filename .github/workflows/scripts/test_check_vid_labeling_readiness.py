# Copyright 2026 The Cross-Media Measurement Authors
# Licensed under the Apache License, Version 2.0 (the "License").
import copy
import unittest
from check_vid_labeling_readiness import check, check_functions, check_compatibility, require_manifest, REQUIRED_FUNCTIONS, DEPLOYMENTS, MIGS, PREFIX


class ReadinessTest(unittest.TestCase):
    def setUp(self):
        self.commands = []
        self.stale = False
        self.bad_image = False
        self.wrong_repository = False
        self.wrong_queue = False
        self.duplicate_flag = False
        self.empty_flag = False
        self.failed_schema = False
        self.missing_flag = False
        self.api_error = False

    def read(self, command):
        self.commands.append(command)
        if self.api_error:
            raise RuntimeError("query failed")
        if command[:3] == ["kubectl", "get", "deployments"]:
            return {"items": [{"metadata": {"name": name, "generation": 2},
                "spec": {"replicas": 1, "template": {"spec": {"containers": [{"image": PREFIX + "/" + DEPLOYMENTS[name] + ":tag"}], "initContainers": [{"name": "update-edp-aggregator-schema", "image": PREFIX + "/edp-aggregator/update-schema:tag"}]}}},
                "status": {"observedGeneration": 1 if self.stale else 2,
                           "updatedReplicas": 1, "availableReplicas": 1}}
                for name in DEPLOYMENTS]}
        if command[:3] == ["kubectl", "get", "pods"]:
            return {"items": [{"metadata": {"name": name + "-pod"},
                "spec": {"containers": [{"image": PREFIX + "/" + DEPLOYMENTS[name] + ":tag"}]},
                "status": {"containerStatuses": [{"ready": True}],
                           "initContainerStatuses": [{"name": "update-edp-aggregator-schema",
                             "state": {"terminated": {"exitCode": 1 if self.failed_schema else 0}}}]}}
                for name in DEPLOYMENTS]}
        if "instance-templates" in command:
            mig = command[4]
            repository, options = MIGS[mig]
            arguments = []
            for flag, value in options.items():
                if flag == "--vid-labeler-queue" and self.missing_flag:
                    continue
                arguments += [flag, "wrong-queue" if self.wrong_queue else value]
                if flag == "--vid-labeler-queue" and self.duplicate_flag:
                    arguments += [flag, value]
                if flag == "--vid-labeler-queue" and self.empty_flag:
                    arguments[-1] = ""
            return {"properties": {"metadata": {"items": [
                {"key": "tee-image-reference", "value": PREFIX + "/" +
                    ("edp-aggregator/other" if self.wrong_repository else repository) +
                    (":old" if self.bad_image else ":tag")},
                {"key": "tee-env-WORK_ITEM_CONSUMPTION_ENABLED", "value": "true"},
                {"key": "tee-cmd", "value": __import__("json").dumps(arguments)}]}}}
        if "list-instances" in command:
            return []
        return {"status": {"isStable": True, "versionTarget": {"isReached": True}},
                "instanceTemplate": "projects/p/global/instanceTemplates/" + command[5]}

    def test_ready_scale_to_zero_templates_and_schema(self):
        check("p", "r", "tag", self.read)
        self.assertTrue(self.commands)
        self.assertFalse(any(word in command for command in self.commands
                             for word in ["apply", "patch", "update", "resize", "pause", "resume"]))

    def test_incomplete_or_unknown_readiness_fails(self):
        for field in ["stale", "bad_image", "wrong_repository", "failed_schema", "missing_flag", "wrong_queue", "duplicate_flag", "empty_flag", "api_error"]:
            with self.subTest(field=field):
                self.setUp()
                setattr(self, field, True)
                with self.assertRaises(RuntimeError):
                    check("p", "r", "tag", self.read)



class FunctionReadinessTest(unittest.TestCase):
    def setUp(self):
        self.manifest = {"schema_version": 1, "contract_id": "immutable-config-v2",
                         "functions": [{"role": role, "name": role.replace("_", "-"), "artifact_sha256": "a" * 64}
                                       for role in sorted(REQUIRED_FUNCTIONS)]}
        self.function = {
            "state": "ACTIVE",
            "serviceConfig": {
                "allTrafficOnLatestRevision": True,
                "revision": "vid-dispatch-00002",
                "service": "projects/p/locations/r/services/vid-dispatch",
                "environmentVariables": {"CMMS_ARTIFACT_SHA256": "a" * 64,
                                         "VID_LABELING_CONTRACT_ID": "immutable-config-v2"}}}
        self.service = {"status": {"latestReadyRevisionName": "vid-dispatch-00002",
                                  "traffic": [{"percent": 100, "revisionName": "vid-dispatch-00002"}]}}
        self.revision = {
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            "spec": {"containers": [{"env": [
                {"name": "CMMS_ARTIFACT_SHA256", "value": "a" * 64},
                {"name": "VID_LABELING_CONTRACT_ID", "value": "immutable-config-v2"}]}]}}
        self.commands = []

    def read(self, command):
        self.commands.append(command)
        if command[1:3] == ["functions", "describe"]:
            value = copy.deepcopy(self.function)
            name = command[3]
            value["name"] = "projects/p/locations/r/functions/" + name
            value["serviceConfig"]["service"] = "projects/p/locations/r/services/" + name
            if value["serviceConfig"].get("revision") == "vid-dispatch-00002":
                value["serviceConfig"]["revision"] = name + "-00002"
            return value
        if command[1:3] == ["run", "services"]:
            value = copy.deepcopy(self.service)
            replacement = command[4] + "-00002"
            if value["status"].get("latestReadyRevisionName") == "vid-dispatch-00002":
                value["status"]["latestReadyRevisionName"] = replacement
            for target in value["status"]["traffic"]:
                if target.get("revisionName") == "vid-dispatch-00002":
                    target["revisionName"] = replacement
            return value
        if command[1:3] == ["run", "revisions"]:
            return self.revision
        raise AssertionError(command)

    def test_serving_artifact_and_contract_are_verified(self):
        check_functions("p", "r", self.manifest, self.read)
        self.assertEqual(len(self.commands), 3 * len(REQUIRED_FUNCTIONS))
        self.assertTrue(all("describe" in command for command in self.commands))

    def test_unsafe_revision_or_artifact_fails_closed(self):
        mutations = [
            lambda: self.function.update(state="DEPLOYING"),
            lambda: self.function["serviceConfig"].update(allTrafficOnLatestRevision=False),
            lambda: self.function["serviceConfig"].pop("revision"),
            lambda: self.function["serviceConfig"]["environmentVariables"].update(CMMS_ARTIFACT_SHA256="b"*64),
            lambda: self.service["status"].update(latestReadyRevisionName="old"),
            lambda: self.service["status"]["traffic"].append({"percent": 1, "revisionName": "old"}),
            lambda: self.service["status"]["traffic"][0].update(percent=99),
            lambda: self.revision["status"]["conditions"].clear(),
            lambda: self.revision["spec"]["containers"][0]["env"].clear(),
        ]
        for index, mutation in enumerate(mutations):
            with self.subTest(index=index):
                self.setUp()
                mutation()
                with self.assertRaises(RuntimeError):
                    check_functions("p", "r", self.manifest, self.read)

    def test_unknown_or_missing_manifest_is_rejected(self):
        for manifest in [None, {}, {"schema_version": 2}, dict(self.manifest, functions=[]),
                         dict(self.manifest, functions=self.manifest["functions"] * 2)]:
            with self.subTest(manifest=manifest), self.assertRaises(RuntimeError):
                require_manifest(manifest)

    def test_unknown_legacy_or_drifted_contract_cannot_roll_online(self):
        good = dict(copy.deepcopy(self.manifest), status="READY", project="p", region="r")
        check_compatibility("p", "r", self.manifest, good, self.read)
        for update in [{"contract_id": "legacy"}, {"project": "other"},
                       {"status": "IN_PROGRESS"}, {"functions": []}]:
            with self.subTest(update=update), self.assertRaises(RuntimeError):
                check_compatibility("p", "r", self.manifest, dict(good, **update), self.read)

    def test_query_error_is_never_interpreted_as_ready(self):
        def error(_):
            raise RuntimeError("permission denied")
        with self.assertRaisesRegex(RuntimeError, "permission denied"):
            check_functions("p", "r", self.manifest, error)

if __name__ == "__main__":
    unittest.main()
