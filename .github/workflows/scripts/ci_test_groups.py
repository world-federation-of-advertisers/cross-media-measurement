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

"""Distribute Bazel tests across CI runners."""

import argparse
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET


LOCAL_TAGS = {"no-remote-exec", "no-remote", "local", "exclusive"}
LOCAL_GROUP_COUNT = 4
DEFAULT_DURATION_SECONDS = 30


def partition_tests(query: str, durations: dict[str, float]) -> dict[str, list[str]]:
    groups = {"remote": [], **{f"local-{i}": [] for i in range(LOCAL_GROUP_COUNT)}}
    local = []
    expected = set()
    rules = {
        rule.attrib["name"]: rule for rule in ET.fromstring(query).findall("rule")
    }
    for label, rule in sorted(rules.items()):
        tags = {
            tag.attrib["value"] for tag in rule.findall("./list[@name='tags']/string")
        }
        expected.add(label)
        local_attribute = rule.find("./boolean[@name='local']")
        if LOCAL_TAGS & tags or (
            local_attribute is not None and local_attribute.attrib["value"] == "true"
        ):
            local.append(label)
        else:
            groups["remote"].append(label)
    loads = [0.0] * LOCAL_GROUP_COUNT
    for label in sorted(
        local,
        key=lambda name: (-durations.get(name, DEFAULT_DURATION_SECONDS), name),
    ):
        index = min(range(LOCAL_GROUP_COUNT), key=lambda i: (loads[i], i))
        groups[f"local-{index}"].append(label)
        loads[index] += durations.get(label, DEFAULT_DURATION_SECONDS)
    assigned = [label for group in groups.values() for label in group]
    if (
        not expected
        or len(assigned) != len(set(assigned))
        or set(assigned) != expected
    ):
        raise ValueError("Test groups must cover every selected test exactly once")
    return {name: sorted(labels) for name, labels in groups.items()}


def write_plan(query_path: Path, output: Path) -> None:
    durations = json.loads(
        Path(__file__).with_name("ci_test_durations.json").read_text()
    )
    groups = partition_tests(query_path.read_text(), durations)
    output.mkdir(parents=True, exist_ok=True)
    for name, labels in groups.items():
        (output / f"{name}.txt").write_text("".join(f"{label}\n" for label in labels))
        print(f"{name}: {len(labels)} tests")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        write_plan(args.query, args.output)
        return 0
    except (ValueError, OSError) as error:
        print(f"Test planning failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
