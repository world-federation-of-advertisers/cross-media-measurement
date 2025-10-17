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

import os, json


def _compute_note_refs(pr):
  # Prefer issue ids; fall back to the PR number if no issues
  refs = [f"[Issue #{issue['id']}]" for issue in pr.get("issues", [])]
  if not refs:
    refs = [f"[PR #{pr['pr_id']}]"]
  return " ".join(refs)


def main():
  pr_data = json.loads(os.environ["JSON"])
  items = []

  for pr in pr_data:
    note_refs = _compute_note_refs(pr)
    for line in pr.get("relnotes", []) + pr.get("breaking_change", []):
      items.append(f"{line.rstrip('.')}. See {note_refs}")

  json_output = json.dumps(items)
  print(json_output)
  with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
    print(f"json={json_output}", file=f)


if __name__ == "__main__":
  main()