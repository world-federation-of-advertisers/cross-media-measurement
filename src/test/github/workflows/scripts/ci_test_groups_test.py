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
import re
import shlex
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


class CiTestGroupsTest(unittest.TestCase):
    def test_workflow_selection_only_excludes_manual_root_tags(self):
        workflow = Path(__file__).resolve().parents[5] / ".github/workflows/build-test.yml"
        queries = [
            shlex.split(line.strip())[2]
            for line in workflow.read_text().splitlines()
            if line.strip().startswith("bazel query ")
        ]
        self.assertTrue(queries)
        for query in queries:
            pattern = re.search(r'attr\("tags", "([^"]+)"', query).group(1)
            for tags in ["[]", "[cpu:2, no-remote-exec]", "[manual-example]"]:
                with self.subTest(query=query, tags=tags):
                    self.assertIsNone(re.search(pattern, tags))
            for tags in ["[manual]", "[cpu:2, manual]", "[manual, no-remote-exec]"]:
                with self.subTest(query=query, tags=tags):
                    self.assertIsNotNone(re.search(pattern, tags))

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


if __name__ == "__main__":
    unittest.main()
