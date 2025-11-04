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

pr_data = json.loads(os.environ["JSON"])

# keep the order in which they were first seen.
items = []
seen = set()

for pr in pr_data:
  for issue in pr.get("issues", []):
    if issue["type"] == "Bug":
      title = issue["title"].rstrip('.')
      ref = issue["id"]
      note = f"{title}. See [Issue #{ref}]"
      if note not in seen:
        seen.add(note)
        items.append(note)

json_output = json.dumps(items)
print(json_output)
with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
  print(f"json={json_output}", file=f)
