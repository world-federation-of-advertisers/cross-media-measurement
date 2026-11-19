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

"""Partition CI tests and verify that Bazel reports every assigned target."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET


LOCAL_TAGS = {"no-remote-exec", "no-remote", "local", "exclusive"}
LOCAL_GROUP_COUNT = 4
DEFAULT_DURATION_SECONDS = 30
CORRECTNESS_TARGET = (
    "//src/test/kotlin/org/wfanet/measurement/integration/k8s:EmptyClusterCorrectnessTest"
)
PANELMATCH_TARGET = (
    "//src/test/kotlin/org/wfanet/panelmatch/integration/k8s:EmptyClusterPanelMatchCorrectnessTest"
)


def partition_tests(query: str, durations: dict[str, float]) -> dict[str, list[str]]:
    groups = {"remote": [], **{f"local-{i}": [] for i in range(LOCAL_GROUP_COUNT)}}
    local = []
    expected = set()
    rules = {
        rule.attrib["name"]: rule for rule in ET.fromstring(query).findall("rule")
    }
    if not {CORRECTNESS_TARGET, PANELMATCH_TARGET} <= rules.keys():
        raise ValueError("The Kubernetes correctness targets are missing")
    for label, rule in sorted(rules.items()):
        tags = {
            tag.attrib["value"] for tag in rule.findall("./list[@name='tags']/string")
        }
        if "manual" in tags:
            continue
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
        raise ValueError("Test groups must cover every non-manual test exactly once")
    return {name: sorted(labels) for name, labels in groups.items()}


def write_plan(query_path: Path, output: Path) -> None:
    durations = json.loads(
        Path(__file__).with_name("ci_test_durations.json").read_text()
    )
    groups = partition_tests(query_path.read_text(), durations)
    output.mkdir(parents=True, exist_ok=True)
    for name, labels in {
        **groups,
        "all": sorted(label for group in groups.values() for label in group),
        "correctness": [CORRECTNESS_TARGET],
        "panelmatch": [PANELMATCH_TARGET],
    }.items():
        (output / f"{name}.txt").write_text("".join(f"{label}\n" for label in labels))
    (output / "groups.json").write_text(json.dumps(groups, indent=2) + "\n")
    for name, labels in groups.items():
        print(f"{name}: {len(labels)} tests")


def verify_results(expected: set[str], log: str, force_execution: bool) -> dict[str, int]:
    matches = re.findall(
        r"^(//\S+)\s+(?:(\(cached\))\s+)?(?:PASSED|FLAKY)\b",
        log,
        re.MULTILINE,
    )
    reported = {label for label, _ in matches}
    if reported != expected or len(matches) != len(expected):
        raise ValueError(
            f"Unexpected test results: missing={sorted(expected - reported)}, "
            f"extra={sorted(reported - expected)}"
        )
    summaries = re.findall(r"Executed (\d+) out of (\d+) tests?:", log)
    if len(summaries) != 1 or int(summaries[0][1]) != len(expected):
        raise ValueError("Bazel's test count does not match the assigned targets")
    executed, total = map(int, summaries[0])
    cached = sum(bool(cache) for _, cache in matches)
    if force_execution and (executed != total or cached):
        raise ValueError("Forced execution must execute every test without cached results")
    return {"assigned": total, "executed": executed, "cached": cached}


def run_tests(
    targets: Path, log_path: Path, force_execution: bool, test_output: str
) -> int:
    labels = targets.read_text().splitlines()
    if len(labels) != len(set(labels)):
        raise ValueError("Duplicate test targets")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if not labels:
        log_path.write_text("No tests assigned to this group.\n")
        return 0
    args = [
        "tools/bazel-with-results",
        "bazel",
        "test",
        f"--target_pattern_file={targets}",
        f"--test_output={test_output}",
        "--test_summary=short",
    ]
    if force_execution:
        args.append("--cache_test_results=no")
    with log_path.open("w") as log_file:
        with subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        ) as process:
            for line in process.stdout:
                print(line, end="", flush=True)
                log_file.write(line)
            code = process.wait()
    if code:
        return code
    result = verify_results(set(labels), log_path.read_text(), force_execution)
    log_path.with_suffix(".json").write_text(json.dumps(result, indent=2) + "\n")
    print(f"Verified test execution: {json.dumps(result)}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--query", type=Path, required=True)
    plan.add_argument("--output", type=Path, required=True)
    run = commands.add_parser("run")
    run.add_argument("--targets-file", type=Path, required=True)
    run.add_argument("--log-file", type=Path, required=True)
    run.add_argument("--force-test-execution", choices=["true", "false"], required=True)
    run.add_argument("--test-output", choices=["errors", "streamed"], required=True)
    args = parser.parse_args()
    try:
        if args.command == "plan":
            write_plan(args.query, args.output)
            return 0
        return run_tests(
            args.targets_file,
            args.log_file,
            args.force_test_execution == "true",
            args.test_output,
        )
    except (ValueError, OSError) as error:
        print(f"CI test verification failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
