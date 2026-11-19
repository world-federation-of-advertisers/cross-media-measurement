# Copyright 2026 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pathlib import Path
import sys
import unittest
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parents[5] / ".github/workflows/scripts"))
import ci_test_groups


def query_xml(rules: list[tuple[str, list[str], bool]]) -> str:
    query = ET.Element("query")
    for label, tags, local in rules:
        rule = ET.SubElement(query, "rule", name=label)
        tag_list = ET.SubElement(rule, "list", name="tags")
        for tag in tags:
            ET.SubElement(tag_list, "string", value=tag)
        ET.SubElement(rule, "boolean", name="local", value=str(local).lower())
    return ET.tostring(query, encoding="unicode")


KUBERNETES_RULES = [
    (ci_test_groups.CORRECTNESS_TARGET, ["manual", "no-remote-exec"], False),
    (ci_test_groups.PANELMATCH_TARGET, ["manual", "no-remote-exec"], False),
]


class CiTestGroupsTest(unittest.TestCase):
    def test_partitions_cover_every_selected_test_once(self):
        rules = [
            ("//example:remote", [], False),
            ("//example:manual_suite_member", ["manual"], False),
            ("//example:emulator", ["no-remote-exec"], False),
            ("//example:uncached_remote", ["no-cache"], False),
            ("//example:local_attribute", [], True),
            ("//example:exclusive", ["exclusive"], False),
            ("//example:no_remote", ["no-remote"], False),
        ]
        groups = ci_test_groups.partition_tests(query_xml(rules), {})
        self.assertEqual(
            groups["remote"],
            ["//example:manual_suite_member", "//example:remote", "//example:uncached_remote"],
        )
        assigned = [label for labels in groups.values() for label in labels]
        self.assertEqual(len(assigned), len(set(assigned)))
        self.assertEqual(set(assigned), {label for label, _, _ in rules})
        self.assertTrue(all(groups[f"local-{i}"] for i in range(4)))

    def test_new_local_tests_are_assigned_without_timing_data(self):
        groups = ci_test_groups.partition_tests(
            query_xml([("//example:new_test", ["local"], False)]), {}
        )
        self.assertIn("//example:new_test", [label for labels in groups.values() for label in labels])
        self.assertNotIn("//example:new_test", groups["remote"])

    def test_empty_test_selection_is_rejected(self):
        with self.assertRaises(ValueError):
            ci_test_groups.partition_tests(query_xml([]), {})

    def test_forced_execution_accepts_every_executed_test(self):
        log = "//example:first PASSED in 1.0s\n//example:second FLAKY, failed in 1 out of 2 attempts\nExecuted 2 out of 2 tests: 2 tests pass.\n"
        result = ci_test_groups.verify_results({"//example:first", "//example:second"}, log, True)
        self.assertEqual(result, {"assigned": 2, "executed": 2, "cached": 0})

    def test_forced_execution_rejects_cached_results(self):
        log = "//example:test (cached) PASSED in 1.0s\nExecuted 0 out of 1 test: 1 test passes.\n"
        with self.assertRaises(ValueError):
            ci_test_groups.verify_results({"//example:test"}, log, True)

    def test_normal_execution_allows_cached_results(self):
        log = "//example:test (cached) PASSED in 1.0s\nExecuted 0 out of 1 test: 1 test passes.\n"
        result = ci_test_groups.verify_results({"//example:test"}, log, False)
        self.assertEqual(result, {"assigned": 1, "executed": 0, "cached": 1})

    def test_forced_execution_rejects_incomplete_execution_count(self):
        log = "//example:test PASSED in 1.0s\nExecuted 0 out of 1 test: 1 test passes.\n"
        with self.assertRaises(ValueError):
            ci_test_groups.verify_results({"//example:test"}, log, True)

    def test_missing_extra_or_duplicate_test_results_are_rejected(self):
        for lines in ["", "//example:extra PASSED in 1.0s\n", "//example:test PASSED in 1.0s\n//example:test PASSED in 1.0s\n"]:
            with self.subTest(lines=lines):
                log = lines + "Executed 1 out of 1 test: 1 test passes.\n"
                with self.assertRaises(ValueError):
                    ci_test_groups.verify_results({"//example:test"}, log, False)


if __name__ == "__main__":
    unittest.main()
