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
    for line in pr.get("relnotes", []):
      items.append(f"{line.rstrip('.')}. See {note_refs}")
    # DO_NOT_SUBMIT: remove the "breaking-change" tag used for debugging?
    for line in pr.get("breaking_change", []):
      items.append(f"(breaking-change) {line.rstrip('.')}. See {note_refs}")

  json_output = json.dumps(items)
  print(json_output)
  with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
    print(f"json={json_output}", file=f)


if __name__ == "__main__":
  main()