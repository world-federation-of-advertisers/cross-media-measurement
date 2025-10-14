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
