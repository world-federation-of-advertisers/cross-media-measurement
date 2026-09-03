#!/usr/bin/env python3

import os, json
from collections import defaultdict

issues_data = json.loads(os.environ["JSON"])

print("-------------------")
print("Total issues")
print("-------------------")
print(len(issues_data))

issues_state_by_issue_type = defaultdict(lambda: defaultdict(int))
for issue in issues_data:
  issue_type = (issue.get("issueType") or {}).get("name", "NO_TYPE")
  issue_state = issue.get("state")
  issue_state_reason = issue.get("stateReason")
  if issue_state == "CLOSED":
    computed_state = issue_state_reason
  else:
    computed_state = issue_state
  issues_state_by_issue_type[issue_type][computed_state] +=1

print()
print("-------------------")
print("Detailed summary")
print("-------------------")
for issue_type,issues_state_data in sorted(issues_state_by_issue_type.items()):
  print(f"{issue_type}: {sum(issues_state_data.values())}")
  for issue_state,count in sorted(issues_state_data.items(), key=lambda x: x[1], reverse=True):
    print(f"  {count} - {issue_state}")
  print()