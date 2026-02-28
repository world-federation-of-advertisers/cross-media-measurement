# Copyright 2025 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import os
import json
import subprocess
from dataclasses import dataclass, asdict

ALERT_SEVERITIES = ["high", "critical"]

@dataclass(frozen=True)
class Alert:
    alert_id: int
    state: str
    updated_at: str
    rule_id: str
    rule_security_severity_level: str
    rule_description: str


def run_cmd(cmd: list[str], input_text: str | None=None) -> str:
  return subprocess.run(
    cmd, check=True, text=True, stdout=subprocess.PIPE, input=input_text
  ).stdout


def fetch_code_scanning_alerts(severity: str, ref: str) -> list[Alert]:
  out = run_cmd([
    "gh", "api",
    "-H", "Accept: application/vnd.github+json",
    "-H", "X-GitHub-Api-Version: 2022-11-28",
    "--paginate",
    f"/repos/world-federation-of-advertisers/cross-media-measurement/code-scanning/alerts?tool_name=Trivy&per_page=100&ref={ref}&severity={severity}&state=open",
  ])
  alerts = json.loads(out)
  return [Alert(
      alert_id=alert.get("number"),
      state=alert.get("state"),
      updated_at=alert.get("updated_at"),
      rule_id=alert.get("rule", {}).get("id"),
      rule_security_severity_level=alert.get("rule", {}).get("security_severity_level"),
      rule_description=alert.get("rule", {}).get("description")
  )
    for alert in alerts
  ]


def populate_alerts_from_api(alert_ids_by_rule_id: dict[str, set[int]], ref: str):
  for severity in ALERT_SEVERITIES:
    for alert in fetch_code_scanning_alerts(severity, ref):
      print(json.dumps(asdict(alert)))
      alert_ids_by_rule_id.setdefault(alert.rule_id, set()).add(alert.alert_id)


def diff_maps(old: dict[str, set[int]], new: dict[str, set[int]]):
  old_keys = set(old.keys())
  new_keys = set(new.keys())

  added_keys = new_keys - old_keys
  deleted_keys = old_keys - new_keys
  common_keys = old_keys & new_keys

  modified = {}
  for key in common_keys:
    added_vals = new[key] - old[key]
    deleted_vals = old[key] - new[key]
    if added_vals or deleted_vals:
      modified[key] = {"added": added_vals, "deleted": deleted_vals}
  return {"added_keys": added_keys, "deleted_keys": deleted_keys, "modified": modified}

def compute_output(base_ref: str, target_ref: str) -> list[str]:
  base_alert_ids_by_rule_id: dict[str, set[int]] = dict()
  target_alert_ids_by_rule_id: dict[str, set[int]] = dict()

  populate_alerts_from_api(base_alert_ids_by_rule_id, base_ref)
  populate_alerts_from_api(target_alert_ids_by_rule_id, target_ref)

  print(f"---- base: {base_ref} ----")
  for k, v in base_alert_ids_by_rule_id.items():
    print(f"{k}: {v}")

  print(f"---- target: {target_ref} ----")
  for k, v in target_alert_ids_by_rule_id.items():
    print(f"{k}: {v}")

  diff = diff_maps(base_alert_ids_by_rule_id, target_alert_ids_by_rule_id)
  print(f"Debug diff: {diff}")

  output = []
  output.append(f"=== Report comparing alerts({ALERT_SEVERITIES}) between: {base_ref} --> {target_ref}")

  if diff["added_keys"]:
    output.append(f"--- NEW Vulnerabilities in {target_ref} ---")
    for k in diff["added_keys"]:
      output.append(f"  {k}: {target_alert_ids_by_rule_id[k]}")

  if diff["deleted_keys"]:
    output.append(f"--- Fixed/Not present Vulnerabilities in {target_ref} ---")
    for k in diff["deleted_keys"]:
      output.append(f"  {k}: {base_alert_ids_by_rule_id[k]}")

  if diff["modified"]:
    output.append(f"--- Modified alert ids in {target_ref} ---")
    for k, v in diff["modified"].items():
      output.append(f"  {k}: {v}")
  return output

def run(base_ref: str, target_ref: str):
  output = compute_output(base_ref, target_ref)
  for line in output:
    print(line)
  if "GITHUB_OUTPUT" in os.environ:
    with open(os.environ["GITHUB_OUTPUT"], 'a') as f:
      print(f"json={json.dumps(output)}", file=f)

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--base_ref", type=str, required=True, help="Example=refs/tags/v0.5.23")
  parser.add_argument("--target_ref", type=str, required=True)
  args = parser.parse_args()
  run(args.base_ref, args.target_ref)
