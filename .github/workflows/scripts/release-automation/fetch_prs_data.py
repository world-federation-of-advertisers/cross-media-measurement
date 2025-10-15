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

import os
import re
import json
import subprocess


def run_cmd(cmd, input_text=None):
  return subprocess.run(
    cmd, check=True, text=True, stdout=subprocess.PIPE, input=input_text
  ).stdout


def get_commits_in_range(start_exclusive, end_inclusive):
  git_log_out = run_cmd(["git", "--no-pager", "log", "--format=%s", f"{start_exclusive}..{end_inclusive}"])
  return [line for line in git_log_out.splitlines() if line]


def fetch_issue_data(issue_number):
  out = run_cmd([
    "gh", "api",
    "-H", "Accept: application/vnd.github+json",
    "-H", "X-GitHub-Api-Version: 2022-11-28",
    f"/repos/world-federation-of-advertisers/cross-media-measurement/issues/{issue_number}",
  ])
  data = json.loads(out)
  print(f"Processing issue: {issue_number}. Got response: {data}")
  issue_type = (data.get("type") or {}).get("name", "_NO_TYPE_")
  issue_title = data.get("title")
  return issue_type, issue_title


def get_pr_number_from(commit_title):
  match = re.search(r"\(#(\d+)\)$", commit_title)
  return int(match.group(1)) if match else None


def fetch_pr_data(pr_number):
  response = run_cmd(["gh", "pr", "view", str(pr_number), "--json", "body,author"])
  response_json = json.loads(response)
  return response_json["body"], response_json["author"]["login"]


def get_trailers(message):
  # A new line is added before the message. This is to ensure the trailers are parsed correctly in case the body consists only of trailers.
  trailers_text = run_cmd(["git", "interpret-trailers", "--trim-empty", "--parse"], input_text="\n" + message)
  # not a dict because there may be repeated keys
  trailers = []
  for line in trailers_text.splitlines():
    m = re.match(r'^([^:]+):\s(.*)$', line)
    trailers.append({"key": m.group(1), "value": m.group(2)})
  return trailers


def compute_pr_output_from(commit_title):
  pr_number = get_pr_number_from(commit_title)
  if not pr_number:
    print(f"No PR number: {commit_title}")
    return {}

  pr_body, pr_author_login = fetch_pr_data(pr_number)

  trailers = get_trailers(pr_body)

  pr_output = {"pr_id": pr_number, "pr_title": commit_title, "pr_author": pr_author_login}

  for trailer in trailers:
    key, value = trailer["key"], trailer["value"]
    if key == "Issue":
      if re.fullmatch(r"#\d+", value):
        issue_number = int(value[1:])
        issue_type, issue_title = fetch_issue_data(issue_number)
        pr_output.setdefault("issues", []).append({"id": issue_number, "type": issue_type, "title": issue_title})
    elif key in ("RELNOTES", "BREAKING-CHANGE"):
      pr_output.setdefault(key.lower().replace("-", "_"), []).append(value)
  return pr_output

def main():
  commits = get_commits_in_range(os.getenv("START_COMMITISH"), os.getenv("END_COMMITISH"))
  output = []
  for commit_title in commits:
    print(f"Processing: {commit_title}")
    pr_output = compute_pr_output_from(commit_title)
    if pr_output:
      output.append(pr_output)
      print(json.dumps(pr_output))

  json_output = json.dumps(output)
  with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
    print(f"json={json_output}", file=f)

if __name__ == "__main__":
  main()